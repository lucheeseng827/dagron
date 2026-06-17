use anyhow::{bail, Result};
use petgraph::{algo::is_cyclic_directed, graph::DiGraph};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskSpec {
    pub name: String,
    /// Shell/argv to run for a **leaf** task. Empty for a **call** task (one that
    /// invokes a `template` instead of running a container). Exactly one of
    /// `command` / `template` must be set; the template expander
    /// ([`crate::expand`]) rewrites every call task into leaf tasks before the
    /// graph is built, so a persisted/dispatched task always has a `command`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub input: Option<serde_json::Value>,

    // ── Sub-workflow / templating (Argo-style) ──────────────────────────────
    // These fields are consumed by the template expander and never persist on a
    // leaf task (skip_serializing_if keeps the stored TaskSpec JSON clean).
    /// Name of the `template` (a reusable sub-DAG declared in `DagSpec.templates`)
    /// this task calls. Makes the workflow call another workflow inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// Arguments passed to the called template — values may reference the caller's
    /// scope via `{{ name }}`; inside the template they fill its parameters.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, String>,
    /// Fan-out: expand the call once per item. `{{ item }}` (and `{{ item.key }}`
    /// for object items) substitutes within the expansion — the map/`withItems`
    /// pattern.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_items: Option<Vec<serde_json::Value>>,
    /// Fan-out from a parameter holding a JSON array string (the `withParam`
    /// pattern) — e.g. `with_param: "{{ shards }}"`. Resolved like `with_items`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_param: Option<String>,
    /// Conditional guard, e.g. `"{{ depth }} > 0"`. When it evaluates false the
    /// task (and any sub-DAG it would expand to) is skipped. This is what lets a
    /// recursive template terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// How many times this task may be attempted before it is marked failed.
    /// 1 = no retries (default). Must be ≥ 1.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base delay in seconds between retries. Actual delay = retry_delay_secs * 2^(attempt-1).
    /// 0 = immediate retry.
    #[serde(default)]
    pub retry_delay_secs: u64,
    /// Per-task subprocess timeout in seconds. Falls back to the 25 s hard limit when absent.
    pub timeout_secs: Option<u64>,
    /// Docker image for this task. Used by DockerExecutor; ignored by LocalExecutor.
    /// If absent, DockerExecutor falls back to its configured default image.
    pub docker_image: Option<String>,
    /// Environment variables injected into the task container. Honoured by the
    /// Local (subprocess), Docker, and Kubernetes executors. This is how a
    /// parameterised task image (e.g. the load-test ETL image) is told what to do
    /// — object size, sleep/CPU/mem profile, S3 bucket, DB DSN, etc.
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Per-task CPU/memory `requests`/`limits` applied to the task **pod** so the
    /// Kubernetes scheduler packs pods realistically (pod headroom, eviction, and
    /// OOMKill become observable). Ignored by the Local and Docker executors.
    pub resources: Option<ResourceRequirements>,
    /// ServiceAccount for the task pod — the IRSA seam. Annotating this SA with an
    /// `eks.amazonaws.com/role-arn` lets task pods assume an IAM role and reach S3
    /// (extract/load) without static credentials. Kubernetes executor only.
    pub service_account: Option<String>,
}

/// A single `name=value` environment variable for a task container.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

/// Kubernetes-style resource requests/limits (e.g. `cpu: "250m"`, `memory:
/// "512Mi"`). Both maps are optional; whatever is present is copied verbatim onto
/// the task pod container's `resources` block.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
}

fn default_max_attempts() -> u32 {
    1
}

/// A reusable sub-DAG that tasks can `template:`-call — the dagron analogue of an
/// Argo `template`/`WorkflowTemplate`. Declared under `DagSpec.templates`; its own
/// `parameters` provide defaults that a caller's `arguments` override.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TemplateSpec {
    pub name: String,
    /// Default parameter values for the template, overridable per call via
    /// `arguments`. Referenced inside the template's tasks as `{{ name }}`.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    pub tasks: Vec<TaskSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DagSpec {
    pub name: String,
    /// Top-level workflow parameters (defaults). Referenced as `{{ name }}` in any
    /// task field and overridable when this workflow is itself called as a template.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    /// Reusable sub-DAGs callable via a task's `template:` field. Expanded inline
    /// into the main `tasks` graph at run-creation time (see [`crate::expand`]).
    #[serde(default)]
    pub templates: Vec<TemplateSpec>,
    pub tasks: Vec<TaskSpec>,
}

pub struct DagGraph {
    pub spec: DagSpec,
    graph: DiGraph<String, ()>,
    node_index: HashMap<String, petgraph::graph::NodeIndex>,
}

impl DagGraph {
    /// Parse a workflow YAML, expand any `template:` calls into a flat leaf-only
    /// DAG (Argo-style sub-workflows: recursion, fan-out, parameters), then build
    /// and validate the graph. This is the single entry point every submit path
    /// uses, so sub-workflow support is uniform across the API, cron, and ingest.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let spec: DagSpec = serde_yaml::from_str(yaml)?;
        let spec = crate::expand::expand(spec)?;
        Self::from_spec(spec)
    }

    /// Build the graph from an already-expanded (leaf-only) [`DagSpec`].
    pub fn from_spec(spec: DagSpec) -> Result<Self> {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();

        for task in &spec.tasks {
            if node_index.contains_key(&task.name) {
                bail!("duplicate task name '{}' in DAG '{}'", task.name, spec.name);
            }
            if task.max_attempts == 0 {
                bail!(
                    "invalid max_attempts=0 for task '{}' in DAG '{}'; expected >= 1",
                    task.name,
                    spec.name
                );
            }
            // After expansion every task must be a runnable leaf. A surviving
            // `template` or an empty `command` means expansion missed something.
            if task.template.is_some() {
                bail!(
                    "task '{}' still references template '{}' after expansion in DAG '{}'",
                    task.name,
                    task.template.as_deref().unwrap_or(""),
                    spec.name
                );
            }
            if task.command.is_empty() {
                bail!(
                    "task '{}' has no command in DAG '{}' (a leaf task needs a command)",
                    task.name,
                    spec.name
                );
            }
            let idx = graph.add_node(task.name.clone());
            node_index.insert(task.name.clone(), idx);
        }

        for task in &spec.tasks {
            for dep in &task.depends_on {
                let &from = node_index
                    .get(dep)
                    .ok_or_else(|| anyhow::anyhow!("unknown dependency '{dep}' in task '{}'", task.name))?;
                let &to = node_index.get(&task.name).unwrap();
                graph.add_edge(from, to, ());
            }
        }

        if is_cyclic_directed(&graph) {
            bail!("DAG '{}' contains a cycle", spec.name);
        }

        Ok(Self { spec, graph, node_index })
    }

    /// Number of incoming edges (direct dependencies) for a task.
    pub fn dep_count(&self, task_name: &str) -> usize {
        let idx = self.node_index[task_name];
        self.graph
            .edges_directed(idx, petgraph::Direction::Incoming)
            .count()
    }

    pub fn task_spec(&self, task_name: &str) -> Option<&TaskSpec> {
        self.spec.tasks.iter().find(|t| t.name == task_name)
    }
}
