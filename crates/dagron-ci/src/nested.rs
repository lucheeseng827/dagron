//! The actions an action uses: a composite action's own steps may use
//! actions in turn, as on GitHub, and those of other repositories have to
//! be fetched before the job starts, as the workflow's own are.
//!
//! The compiler sees a workflow file alone, so this is done where an
//! event is handled, for a workflow that runs: each action a job's steps
//! use is read (its `action.yml`, or `action.yaml`), and for a composite
//! one each `uses:` of its steps is
//!
//! - an action of another repository: held to the same rules as a
//!   workflow's own step (`CI_ACTIONS_ALLOW`, and a pin where it lists
//!   any), added to what the job fetches, and read in its turn;
//! - a local action (`./dir`, of the workspace, as GitHub reads it in an
//!   action too): read at the event's commit, in its turn;
//! - an image (`docker://`): nothing to fetch.
//!
//! As deep as the step runner runs actions ([`MAX_DEPTH`]), no more than a
//! job fetches in all ([`MAX_ACTIONS`]), and no more files read for one
//! event than [`MAX_READS`]: a file anyone may write decides how many are
//! read, so the reading has a bound of its own. An action's file is read
//! from github.com's raw files, or from a mirror's where the operator says
//! where they are (`CI_ACTIONS_MIRROR_RAW`); one of the repository's own
//! owner's, in a trusted run, through the forge with dagron-ci's own
//! credential for it, as it may be private. One that cannot be read, or
//! read as an action, is left to the step runner, which says what is wrong
//! with it as the job runs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use dagron_ci_expr::uses;
use serde_json::{json, Value};

use crate::db::Repo;
use crate::forge::{Forge, ForgeError};
use crate::pipeline::github::{ActionSource, MAX_ACTIONS};
use crate::pipeline::Pipeline;

/// How deep actions run in actions: the step runner's own bound.
pub const MAX_DEPTH: usize = 10;

/// The most action files read for one event, across its workflows.
pub const MAX_READS: usize = 64;

/// Why the actions a workflow's actions use could not be had.
#[derive(Debug)]
pub enum Nested {
    /// One is not allowed here, or there are too many: the workflow is
    /// refused, as it is for a step of its own that uses one.
    Refused(String),
    /// The forge or the mirror would not answer: handled again later.
    Forge(ForgeError),
}

/// The `uses:` of a composite action's steps, as its file has them; none
/// for an action of another kind. `Err` for a file that is no action's.
pub fn used_by(text: &str) -> Result<Vec<String>, String> {
    let doc: serde_yaml::Value = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    let runs = &doc["runs"];
    let composite = runs["using"]
        .as_str()
        .is_some_and(|using| using.trim().eq_ignore_ascii_case("composite"));
    if !composite {
        return Ok(Vec::new());
    }
    Ok(runs["steps"]
        .as_sequence()
        .into_iter()
        .flatten()
        .filter_map(|step| step["uses"].as_str().map(|u| u.trim().to_string()))
        .collect())
}

/// What an action is known by here: a local one by its directory with no
/// `.` or empty part (`./x/./` is `./x`), so that two spellings are one.
fn key(used: &str) -> String {
    match uses::local(used) {
        Some(dir) => {
            let parts: Vec<&str> = dir
                .split('/')
                .filter(|part| !part.is_empty() && *part != ".")
                .collect();
            format!("./{}", parts.join("/"))
        }
        None => used.trim().to_string(),
    }
}

/// What reading for one event has spent, and what it read.
#[derive(Default)]
pub struct Reading {
    /// Each action's file, read once however many jobs use it.
    read: BTreeMap<String, Option<String>>,
    /// Requests made.
    requests: usize,
}

/// Add to each task of `pipeline` the actions of other repositories its
/// actions use, at any depth, read at `sha` of `repo` where they are local.
/// `reading` is the event's: what it read is not read again, and what it
/// spent counts against [`MAX_READS`] across all its workflows. `owner` is
/// whose actions are fetched and read with a credential: the repository's,
/// for a trusted run ([`ActionSource::credentialed`]).
pub async fn resolve(
    pipeline: &mut Pipeline,
    forge: &Forge,
    source: &ActionSource,
    repo: &Repo,
    sha: &str,
    owner: Option<&str>,
    reading: &mut Reading,
) -> Result<(), Nested> {
    for task in &mut pipeline.tasks {
        let mut fetches: Vec<Value> = task
            .fields
            .get("workspace")
            .and_then(|w| w.get("actions"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let before = fetches.len();
        // Breadth first: each action is met first where it is least deep,
        // so that what it uses is fetched for the shallowest place it runs.
        let mut queue: VecDeque<(String, usize)> = task
            .steps
            .iter()
            .filter_map(|step| step.uses.clone())
            .map(|used| (used, 0))
            .collect();
        let mut seen = BTreeSet::new();
        while let Some((used, depth)) = queue.pop_front() {
            let known = key(&used);
            if depth >= MAX_DEPTH || !seen.insert(known.clone()) {
                continue;
            }
            if !reading.read.contains_key(&known) {
                let text = file(forge, source, repo, sha, owner, &used, reading).await?;
                reading.read.insert(known.clone(), text);
            }
            let Some(Some(text)) = reading.read.get(&known) else {
                continue;
            };
            let Ok(inner) = used_by(text) else {
                continue;
            };
            // What an action at the deepest the runner runs uses is never
            // run: nothing of it is fetched.
            if depth + 1 >= MAX_DEPTH {
                continue;
            }
            for step_uses in inner {
                if uses::docker(&step_uses).is_some() {
                    continue;
                }
                if uses::local(&step_uses).is_some() {
                    queue.push_back((step_uses, depth + 1));
                    continue;
                }
                let Some(remote) = uses::remote(&step_uses) else {
                    continue;
                };
                if let Some(why) = source.refuses(&remote) {
                    return Err(Nested::Refused(format!("{used}: a step of it {why}")));
                }
                if source.file_url(&remote, "action.yml").is_none() {
                    return Err(Nested::Refused(format!(
                        "{used}: a step of it uses {step_uses}, an action of another \
                         repository, and where actions come from a mirror \
                         (CI_ACTIONS_MIRROR) dagron-ci reads an action's file only where \
                         CI_ACTIONS_MIRROR_RAW says where"
                    )));
                }
                let fetch = source.fetch(&remote, owner);
                if !fetches.contains(&fetch) {
                    fetches.push(fetch);
                    if fetches.len() > MAX_ACTIONS {
                        return Err(Nested::Refused(format!(
                            "job {}: it and its actions use more than {MAX_ACTIONS} actions \
                             of other repositories, which is as many as a job fetches",
                            task.name
                        )));
                    }
                }
                queue.push_back((step_uses, depth + 1));
            }
        }
        if fetches.len() > before {
            let workspace = task.fields.entry("workspace").or_insert_with(|| json!({}));
            workspace["actions"] = Value::Array(fetches);
        }
    }
    Ok(())
}

/// The file of the action `used` names, where it has one that is text:
/// a local one's at `sha` of `repo`; another repository's through the
/// forge where it is one of `owner`'s, and where
/// [`ActionSource::file_url`] says otherwise. One its host refuses for good
/// is left to the step runner, as one that is not there is; one its host
/// cannot give now is the forge's error, for the event to be handled again.
async fn file(
    forge: &Forge,
    source: &ActionSource,
    repo: &Repo,
    sha: &str,
    owner: Option<&str>,
    used: &str,
    reading: &mut Reading,
) -> Result<Option<String>, Nested> {
    // Where the file is asked for: of the repository; of another of the
    // owner's, through the forge; or at a URL. None where there is nothing
    // to ask anyone, which spends no read.
    let local = uses::local(used).is_some();
    let remote = uses::remote(used);
    if !local && remote.is_none() {
        return Ok(None);
    }
    // One of the owner's may be private: read as dagron-ci reads the
    // repository's own.
    let theirs = remote
        .as_ref()
        .filter(|remote| !local && source.credentialed(remote, owner))
        .map(|remote| {
            let theirs = Repo {
                full_name: format!("{}/{}", remote.owner, remote.repo),
                ..repo.clone()
            };
            (theirs, remote.path.to_string(), remote.git_ref.to_string())
        });
    for name in ["action.yml", "action.yaml"] {
        let url = match &remote {
            Some(remote) if !local && theirs.is_none() => match source.file_url(remote, name) {
                Some(url) => Some(url),
                None => return Ok(None),
            },
            _ => None,
        };
        if reading.requests >= MAX_READS {
            return Err(Nested::Refused(format!(
                "its actions name more actions than dagron-ci reads the files of for one \
                 event ({MAX_READS}), at {used}"
            )));
        }
        reading.requests += 1;
        // A refusal that lasts is left for the step runner to say; a host
        // that cannot answer now has the event handled again.
        let got = if let Some(url) = url {
            forge.action_file(&url).await
        } else if let Some((their_repo, dir, git_ref)) = &theirs {
            // Read only where its repository shares its actions, as GitHub
            // lets a workflow use them: dagron-ci's credential is no way
            // round a repository's own settings.
            if !forge
                .shares_actions(their_repo)
                .await
                .map_err(Nested::Forge)?
            {
                return Ok(None);
            }
            let path = match dir.as_str() {
                "" => name.to_string(),
                dir => format!("{dir}/{name}"),
            };
            forge.file(their_repo, &path, git_ref).await
        } else {
            let dir = key(used);
            let dir = dir.trim_start_matches("./");
            let path = match dir {
                "" => name.to_string(),
                dir => format!("{dir}/{name}"),
            };
            forge.file(repo, &path, sha).await
        };
        match got.map_err(Nested::Forge)? {
            Some(Ok(text)) => return Ok(Some(text)),
            Some(Err(_)) => return Ok(None),
            None => continue,
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_composite_action_s_steps_say_what_they_use() {
        let composite = "runs:\n  using: composite\n  steps:\n    - uses: actions/setup-node@v4\n    - run: echo\n      shell: bash\n    - uses: ./inner\n";
        assert_eq!(
            used_by(composite).unwrap(),
            ["actions/setup-node@v4", "./inner"]
        );
        let node = "runs:\n  using: node20\n  main: index.js\n";
        assert!(used_by(node).unwrap().is_empty(), "no steps of its own");
        assert!(used_by(": not yaml :").is_err());
    }

    #[test]
    fn a_local_action_is_one_however_it_is_spelled() {
        for spelled in ["./x", "./x/", "./x/.", "./x//", "./x/./"] {
            assert_eq!(key(spelled), "./x", "{spelled}");
        }
        assert_eq!(key("./"), "./");
        assert_eq!(key(" octo/tool@v1 "), "octo/tool@v1");
    }
}
