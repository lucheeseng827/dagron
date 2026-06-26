//! Importers from other orchestrators to dagron DAG YAML.
//!
//! Today: **Argo Workflows**. [`argo_to_dagron`] converts the common case — an
//! entrypoint `dag` (or single `container`) template whose tasks reference
//! container templates — into a dagron spec (name + tasks with `command`,
//! `docker_image`, `depends_on`). Unsupported Argo features (steps templates,
//! artifacts, parameters, when-expressions) are reported as errors so nothing is
//! silently dropped.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

// ── Argo input (the subset we map) ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ArgoWorkflow {
    #[serde(default)]
    metadata: ArgoMeta,
    spec: ArgoSpec,
}

#[derive(Debug, Default, Deserialize)]
struct ArgoMeta {
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "generateName", default)]
    generate_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArgoSpec {
    entrypoint: String,
    #[serde(default)]
    templates: Vec<ArgoTemplate>,
    /// Workflow-level inputs (global parameters/artifacts) — unsupported.
    #[serde(default)]
    arguments: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct ArgoTemplate {
    name: String,
    #[serde(default)]
    container: Option<ArgoContainer>,
    #[serde(default)]
    dag: Option<ArgoDag>,
    #[serde(default)]
    steps: Option<serde_yaml::Value>,
    /// Per-template inputs/outputs (parameters/artifacts) — unsupported; modeled
    /// so their presence is rejected rather than silently dropped.
    #[serde(default)]
    inputs: Option<serde_yaml::Value>,
    #[serde(default)]
    outputs: Option<serde_yaml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct ArgoContainer {
    image: String,
    #[serde(default)]
    command: Vec<String>,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ArgoDag {
    #[serde(default)]
    tasks: Vec<ArgoDagTask>,
}

#[derive(Debug, Deserialize)]
struct ArgoDagTask {
    name: String,
    template: String,
    #[serde(default)]
    dependencies: Vec<String>,
    /// Conditional execution — unsupported (dagron has no `when` gate here).
    #[serde(default)]
    when: Option<serde_yaml::Value>,
    /// Per-task parameter/artifact passing — unsupported.
    #[serde(default)]
    arguments: Option<serde_yaml::Value>,
}

// ── dagron output (minimal, clean) ──────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct DagronSpec {
    name: String,
    tasks: Vec<DagronTask>,
}

#[derive(Debug, Serialize)]
struct DagronTask {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    docker_image: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    command: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
}

/// Convert an Argo `Workflow` YAML into dagron DAG YAML.
pub fn argo_to_dagron(argo_yaml: &str) -> Result<String> {
    let wf: ArgoWorkflow =
        serde_yaml::from_str(argo_yaml).context("input is not a parseable Argo Workflow")?;

    if wf.spec.arguments.is_some() {
        bail!(
            "workflow-level `arguments` (global parameters/artifacts) are not supported by the \
             importer — convert them manually"
        );
    }

    let name = wf
        .metadata
        .name
        .clone()
        .or_else(|| wf.metadata.generate_name.clone().map(|g| g.trim_end_matches('-').to_string()))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "imported-workflow".to_string());

    let by_name: HashMap<&str, &ArgoTemplate> =
        wf.spec.templates.iter().map(|t| (t.name.as_str(), t)).collect();

    let entry = by_name
        .get(wf.spec.entrypoint.as_str())
        .copied()
        .with_context(|| format!("entrypoint template '{}' not found", wf.spec.entrypoint))?;

    let tasks = if let Some(dag) = &entry.dag {
        let mut out = Vec::with_capacity(dag.tasks.len());
        for t in &dag.tasks {
            reject_unsupported_task(t)?;
            let tmpl = by_name.get(t.template.as_str()).copied().with_context(|| {
                format!("dag task '{}' references unknown template '{}'", t.name, t.template)
            })?;
            reject_unsupported_template(tmpl)?;
            let c = tmpl.container.as_ref().with_context(|| {
                format!(
                    "template '{}' (used by task '{}') is not a container template — \
                     only container templates are supported",
                    tmpl.name, t.name
                )
            })?;
            out.push(DagronTask {
                name: t.name.clone(),
                docker_image: Some(c.image.clone()),
                command: merged_command(c),
                depends_on: t.dependencies.clone(),
            });
        }
        out
    } else if let Some(c) = &entry.container {
        // Single-container entrypoint → one task.
        reject_unsupported_template(entry)?;
        vec![DagronTask {
            name: entry.name.clone(),
            docker_image: Some(c.image.clone()),
            command: merged_command(c),
            depends_on: vec![],
        }]
    } else if entry.steps.is_some() {
        bail!("entrypoint '{}' is a `steps` template — not yet supported (use a `dag` template)", entry.name);
    } else {
        bail!("entrypoint '{}' is neither a container nor a dag template", entry.name);
    };

    if tasks.is_empty() {
        bail!("no tasks produced from entrypoint '{}'", wf.spec.entrypoint);
    }

    let spec = DagronSpec { name, tasks };
    serde_yaml::to_string(&spec).context("serializing dagron spec")
}

/// Argo runs `command` then `args`; dagron's `command` is one argv, so concatenate.
fn merged_command(c: &ArgoContainer) -> Vec<String> {
    let mut v = c.command.clone();
    v.extend(c.args.iter().cloned());
    v
}

/// Reject template features the importer can't faithfully convert, so a
/// migration fails loudly instead of silently dropping inputs/outputs.
fn reject_unsupported_template(t: &ArgoTemplate) -> Result<()> {
    if t.inputs.is_some() {
        bail!(
            "template '{}' declares `inputs` (parameters/artifacts) — not supported by the \
             importer; convert it manually",
            t.name
        );
    }
    if t.outputs.is_some() {
        bail!(
            "template '{}' declares `outputs` (parameters/artifacts) — not supported by the \
             importer; convert it manually",
            t.name
        );
    }
    Ok(())
}

/// Reject dag-task features the importer can't convert (conditional execution,
/// parameter/artifact passing).
fn reject_unsupported_task(t: &ArgoDagTask) -> Result<()> {
    if t.when.is_some() {
        bail!(
            "dag task '{}' uses a `when` expression — conditional execution is not supported by \
             the importer",
            t.name
        );
    }
    if t.arguments.is_some() {
        bail!(
            "dag task '{}' passes `arguments` (parameters/artifacts) — not supported by the importer",
            t.name
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARGO_DAG: &str = r#"
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: pipeline-
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: extract
            template: run
          - name: transform
            template: run
            dependencies: [extract]
          - name: load
            template: run
            dependencies: [transform]
    - name: run
      container:
        image: alpine:latest
        command: ["sh", "-c"]
        args: ["echo hi"]
"#;

    #[test]
    fn converts_argo_dag_to_dagron() {
        let yaml = argo_to_dagron(ARGO_DAG).unwrap();
        assert!(yaml.contains("name: pipeline"));
        assert!(yaml.contains("docker_image: alpine:latest"));
        assert!(yaml.contains("- extract")); // transform depends_on extract
        // The generated spec is a VALID dagron DAG (parsed by the real engine).
        let dag = dagron_core::dag::DagGraph::from_yaml(&yaml).expect("valid dagron DAG");
        assert_eq!(dag.spec.tasks.len(), 3);
    }

    #[test]
    fn steps_template_is_rejected_not_dropped() {
        let argo = r#"
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: a
            template: run
    - name: run
      container: { image: busybox }
"#;
        assert!(argo_to_dagron(argo).is_err());
    }

    #[test]
    fn unknown_template_ref_errors() {
        let argo = r#"
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: x
            template: missing
"#;
        assert!(argo_to_dagron(argo).is_err());
    }

    #[test]
    fn task_when_expression_is_rejected_not_dropped() {
        let argo = r#"
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: x
            template: run
            when: "{{tasks.a.outputs.result}} == yes"
    - name: run
      container: { image: busybox }
"#;
        assert!(argo_to_dagron(argo).is_err());
    }

    #[test]
    fn task_arguments_are_rejected_not_dropped() {
        let argo = r#"
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: x
            template: run
            arguments:
              parameters:
                - name: p
                  value: v
    - name: run
      container: { image: busybox }
"#;
        assert!(argo_to_dagron(argo).is_err());
    }

    #[test]
    fn template_inputs_are_rejected_not_dropped() {
        let argo = r#"
spec:
  entrypoint: run
  templates:
    - name: run
      inputs:
        parameters:
          - name: p
      container: { image: busybox }
"#;
        assert!(argo_to_dagron(argo).is_err());
    }

    #[test]
    fn workflow_arguments_are_rejected_not_dropped() {
        let argo = r#"
spec:
  entrypoint: run
  arguments:
    parameters:
      - name: g
        value: 1
  templates:
    - name: run
      container: { image: busybox }
"#;
        assert!(argo_to_dagron(argo).is_err());
    }
}
