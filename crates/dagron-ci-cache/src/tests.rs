use super::*;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

#[test]
fn a_glob_reads_names_and_directories() {
    for (p, s, m) in [
        ("Cargo.lock", "Cargo.lock", true),
        ("Cargo.lock", "sub/Cargo.lock", false),
        ("**/Cargo.lock", "Cargo.lock", true),
        ("**/Cargo.lock", "a/b/Cargo.lock", true),
        ("**/package-lock.json", "web/package-lock.json", true),
        ("requirements*.txt", "requirements-dev.txt", true),
        ("requirements*.txt", "dir/requirements.txt", false),
        ("a/*/c", "a/b/c", true),
        ("a/*/c", "a/b/x/c", false),
        ("a/**/c", "a/c", true),
        ("go.su?", "go.sum", true),
        ("./go.sum", "go.sum", true),
    ] {
        assert_eq!(glob_match(p, s), m, "{p} against {s}");
    }
}

fn cache(key_files: &[&str], paths: &[&str]) -> Cache {
    Cache {
        prefix: "cargo".into(),
        version: "v".into(),
        key_files: key_files.iter().map(|s| s.to_string()).collect(),
        paths: paths.iter().map(|s| s.to_string()).collect(),
        restore: vec![],
        save: None,
    }
}

#[test]
fn a_key_is_the_prefix_and_what_its_files_hold() {
    let ws = tempfile::tempdir().unwrap();
    std::fs::write(ws.path().join("Cargo.lock"), "one").unwrap();
    std::fs::create_dir_all(ws.path().join("sub/.git")).unwrap();
    std::fs::write(ws.path().join("sub/Cargo.lock"), "two").unwrap();
    std::fs::write(ws.path().join("sub/.git/Cargo.lock"), "not read").unwrap();
    let c = cache(&["**/Cargo.lock"], &["/x"]);
    let found = matching(ws.path(), &c.key_files).unwrap();
    assert_eq!(
        found.into_iter().collect::<Vec<_>>(),
        vec!["Cargo.lock", "sub/Cargo.lock"]
    );
    let k1 = key_of(&c, ws.path()).unwrap();
    assert!(
        k1.starts_with("cargo-") && k1.len() == "cargo-".len() + 32,
        "{k1}"
    );
    std::fs::write(ws.path().join("sub/.git/Cargo.lock"), "changed").unwrap();
    assert_eq!(key_of(&c, ws.path()).unwrap(), k1, ".git is not read");
    std::fs::write(ws.path().join("sub/Cargo.lock"), "three").unwrap();
    assert_ne!(key_of(&c, ws.path()).unwrap(), k1);
    assert_eq!(key_of(&cache(&[], &["/x"]), ws.path()).unwrap(), "cargo");
}

#[test]
fn an_archive_holds_its_paths_and_comes_back_over_them() {
    let ws = tempfile::tempdir().unwrap();
    let a = ws.path().join("reg");
    std::fs::create_dir_all(a.join("index/x")).unwrap();
    std::fs::write(a.join("index/x/crate.toml"), "toml").unwrap();
    std::fs::write(a.join("top"), "t").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("top", a.join("link")).unwrap();
    let single = ws.path().join("one.bin");
    std::fs::write(&single, "single file").unwrap();
    let missing = ws.path().join("not-there");
    let paths = vec![a.clone(), single.clone(), missing];
    let file = tempfile().unwrap();
    assert_eq!(pack(&paths, &file).unwrap(), 2);
    // Over a place that holds other things already: they stay, the cache's
    // files are written over.
    let back = tempfile::tempdir().unwrap();
    let to = vec![
        back.path().join("reg"),
        back.path().join("one.bin"),
        back.path().join("not-there"),
    ];
    std::fs::create_dir_all(&to[0]).unwrap();
    std::fs::write(to[0].join("top"), "old").unwrap();
    std::fs::write(to[0].join("mine"), "kept").unwrap();
    let mut file = file;
    file.rewind().unwrap();
    unpack(&file, &to).unwrap();
    assert_eq!(
        std::fs::read_to_string(to[0].join("index/x/crate.toml")).unwrap(),
        "toml"
    );
    assert_eq!(std::fs::read_to_string(to[0].join("top")).unwrap(), "t");
    assert_eq!(std::fs::read_to_string(to[0].join("mine")).unwrap(), "kept");
    assert_eq!(std::fs::read_to_string(&to[1]).unwrap(), "single file");
    #[cfg(unix)]
    assert_eq!(
        std::fs::read_link(to[0].join("link")).unwrap(),
        PathBuf::from("top")
    );
    assert!(!to[2].exists());
}

#[test]
fn an_archive_that_names_no_path_or_climbs_out_is_refused() {
    let raw = |name: &[u8]| {
        let file = tempfile().unwrap();
        let mut tar = tar::Builder::new(zstd::stream::write::Encoder::new(&file, 1).unwrap());
        let mut h = tar::Header::new_gnu();
        // Written straight into the header, as a hostile archive would be:
        // the tar crate refuses to write `..` through its own setters.
        h.as_old_mut().name[..name.len()].copy_from_slice(name);
        h.set_size(1);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        tar.append(&h, &b"x"[..]).unwrap();
        tar.into_inner().unwrap().finish().unwrap();
        let mut file = file;
        file.rewind().unwrap();
        file
    };
    let to = tempfile::tempdir().unwrap();
    let paths = vec![to.path().join("p")];
    // An archive that makes a link, then an entry beneath it, writes
    // nothing through the link.
    let elsewhere = tempfile::tempdir().unwrap();
    let file = tempfile().unwrap();
    {
        let mut tar = tar::Builder::new(zstd::stream::write::Encoder::new(&file, 1).unwrap());
        let mut link = tar::Header::new_gnu();
        link.set_entry_type(tar::EntryType::Symlink);
        link.set_size(0);
        tar.append_link(&mut link, "0/out", elsewhere.path())
            .unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_size(1);
        h.set_entry_type(tar::EntryType::Regular);
        tar.append_data(&mut h, "0/out/planted", &b"x"[..]).unwrap();
        tar.into_inner().unwrap().finish().unwrap();
    }
    let mut file = file;
    file.rewind().unwrap();
    let err = unpack(&file, &paths).unwrap_err().to_string();
    assert!(err.contains("under a symbolic link"), "{err}");
    assert!(
        !elsewhere.path().join("planted").exists(),
        "nothing was written through the link"
    );
    // Nor one that makes the cache's path itself a link, then writes under
    // it.
    let file = tempfile().unwrap();
    {
        let mut tar = tar::Builder::new(zstd::stream::write::Encoder::new(&file, 1).unwrap());
        let mut link = tar::Header::new_gnu();
        link.set_entry_type(tar::EntryType::Symlink);
        link.set_size(0);
        tar.append_link(&mut link, "0", elsewhere.path()).unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_size(1);
        h.set_entry_type(tar::EntryType::Regular);
        tar.append_data(&mut h, "0/planted", &b"x"[..]).unwrap();
        tar.into_inner().unwrap().finish().unwrap();
    }
    let mut file = file;
    file.rewind().unwrap();
    let fresh = tempfile::tempdir().unwrap();
    let err = unpack(&file, &[fresh.path().join("p")])
        .unwrap_err()
        .to_string();
    assert!(err.contains("would make the cache's path"), "{err}");
    assert!(!fresh.path().join("p").exists(), "no link was made");
    assert!(
        !elsewhere.path().join("planted").exists(),
        "nothing was written through the link"
    );
    for (name, says) in [
        (&b"7/x"[..], "names no path"),
        (b"x/y", "names no path"),
        (b"0/../../etc/x", "climbs out"),
    ] {
        let err = unpack(raw(name), &paths).unwrap_err().to_string();
        assert!(err.contains(says), "{err}");
    }
}

/// What the store holds: each object's bytes by its path.
type Objects = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// A store over plain HTTP: GET and PUT of whole objects, in memory.
fn store() -> (String, Objects) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let objects: Objects = Arc::default();
    let held = Arc::clone(&objects);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let (method, path) = (
                parts.next().unwrap_or("").to_string(),
                parts.next().unwrap_or("").to_string(),
            );
            let mut len = 0usize;
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h.trim().is_empty() {
                    break;
                }
                if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap();
                }
            }
            let reply = match method.as_str() {
                "PUT" => {
                    let mut body = vec![0; len];
                    reader.read_exact(&mut body).unwrap();
                    held.lock().unwrap().insert(path, body);
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                }
                _ => match held.lock().unwrap().get(&path) {
                    Some(body) => {
                        let mut r = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())
                            .into_bytes();
                        r.extend_from_slice(body);
                        r
                    }
                    None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\nConnection: close\r\n\r\nNoSuchKey".to_vec(),
                },
            };
            let _ = stream.write_all(&reply);
        }
    });
    (base, objects)
}

fn signed(method: &str, url: String) -> Signed {
    Signed {
        method: method.into(),
        url,
        headers: vec![("x-ms-blob-type".into(), "BlockBlob".into())],
    }
}

#[test]
fn a_run_saves_what_a_later_run_restores_and_keeps_a_key_it_restored() {
    let (base, objects) = store();
    let ws = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    std::fs::write(ws.path().join("Cargo.lock"), "lock one").unwrap();
    std::fs::create_dir_all(home.path().join(".cargo/registry")).unwrap();
    std::fs::write(home.path().join(".cargo/registry/crate"), "bytes").unwrap();
    let here = Here {
        workspace: ws.path().to_path_buf(),
        home: home.path().to_path_buf(),
    };
    let mut c = cache(&["Cargo.lock"], &["~/.cargo/registry"]);
    c.save = Some(Save {
        archive: signed("PUT", format!("{base}/e1.tar.zst")),
        manifest: signed("PUT", format!("{base}/e1.json")),
    });
    let given = Given {
        v: 1,
        max_bytes: 1 << 20,
        caches: vec![c.clone()],
    };
    // The first run: nothing offered, so it starts cold and saves.
    let mut log = Vec::new();
    let state = restore(&given, &here, &mut log);
    assert!(String::from_utf8_lossy(&log).contains("nothing to restore"));
    save(&given, &state, &here, &mut log);
    let said = String::from_utf8_lossy(&log).to_string();
    assert!(said.contains("cache cargo: saved cargo-"), "{said}");
    assert!(!said.contains(&base), "no URL is said: {said}");
    let manifest: serde_json::Value =
        serde_json::from_slice(&objects.lock().unwrap()["/e1.json"]).unwrap();
    assert_eq!(manifest["key"], serde_json::json!(state.caches[0].key));

    // A later run in a new home is offered that entry under its key: it
    // restores it, and keeps it as it was.
    let home2 = tempfile::tempdir().unwrap();
    let here2 = Here {
        workspace: ws.path().to_path_buf(),
        home: home2.path().to_path_buf(),
    };
    let mut c2 = c.clone();
    c2.restore = vec![Offered {
        key: state.caches[0].key.clone(),
        from: "own".into(),
        get: signed("GET", format!("{base}/e1.tar.zst")),
    }];
    let given2 = Given {
        v: 1,
        max_bytes: 1 << 20,
        caches: vec![c2.clone()],
    };
    let mut log = Vec::new();
    let state2 = restore(&given2, &here2, &mut log);
    assert!(state2.caches[0].hit);
    assert_eq!(
        std::fs::read_to_string(home2.path().join(".cargo/registry/crate")).unwrap(),
        "bytes"
    );
    save(&given2, &state2, &here2, &mut log);
    assert!(String::from_utf8_lossy(&log).contains("is kept as it was"));

    // The lock file changed: the old entry is restored as the newest, and a
    // new one saved under the new key.
    std::fs::write(ws.path().join("Cargo.lock"), "lock two").unwrap();
    c2.save = Some(Save {
        archive: signed("PUT", format!("{base}/e2.tar.zst")),
        manifest: signed("PUT", format!("{base}/e2.json")),
    });
    let given3 = Given {
        v: 1,
        max_bytes: 1 << 20,
        caches: vec![c2.clone()],
    };
    let mut log = Vec::new();
    let state3 = restore(&given3, &here2, &mut log);
    assert!(!state3.caches[0].hit);
    assert!(String::from_utf8_lossy(&log).contains("not this key, the newest"));
    save(&given3, &state3, &here2, &mut log);
    assert!(objects.lock().unwrap().contains_key("/e2.json"));

    // A run given nothing to save with does not save; one whose entry is
    // gone starts cold and says why, with no URL in it.
    let mut c4 = c2.clone();
    c4.save = None;
    c4.restore[0].get = signed("GET", format!("{base}/gone.tar.zst"));
    let given4 = Given {
        v: 1,
        max_bytes: 1 << 20,
        caches: vec![c4],
    };
    let mut log = Vec::new();
    let state4 = restore(&given4, &here2, &mut log);
    save(&given4, &state4, &here2, &mut log);
    let said = String::from_utf8_lossy(&log).to_string();
    assert!(said.contains("the store answered 404: NoSuchKey"), "{said}");
    assert!(said.contains("does not write them"), "{said}");
    assert!(!said.contains("gone.tar.zst"), "no URL is said: {said}");
}

#[test]
fn an_entry_larger_than_the_most_is_not_saved() {
    let (base, objects) = store();
    let ws = tempfile::tempdir().unwrap();
    let big: Vec<u8> = (0..200_000u32)
        .flat_map(|i| i.wrapping_mul(2654435761).to_le_bytes())
        .collect();
    std::fs::write(ws.path().join("blob"), &big).unwrap();
    let here = Here {
        workspace: ws.path().to_path_buf(),
        home: ws.path().to_path_buf(),
    };
    let mut c = cache(&[], &["blob"]);
    c.save = Some(Save {
        archive: signed("PUT", format!("{base}/big.tar.zst")),
        manifest: signed("PUT", format!("{base}/big.json")),
    });
    let given = Given {
        v: 1,
        max_bytes: 1000,
        caches: vec![c],
    };
    let mut log = Vec::new();
    save(&given, &State::default(), &here, &mut log);
    assert!(
        String::from_utf8_lossy(&log).contains("more than the"),
        "{}",
        String::from_utf8_lossy(&log)
    );
    assert!(objects.lock().unwrap().is_empty());
}

#[test]
fn the_engines_variable_is_read_and_a_version_it_does_not_know_is_not() {
    let raw = serde_json::json!({"v": 1, "max_bytes": 10, "caches": [{
        "prefix": "npm", "version": "abc", "key_files": [], "paths": ["~/.npm"],
        "restore": [{"key": "npm", "from": "refs/heads/main", "method": "GET", "url": "https://s/x", "headers": []}],
        "save": null
    }]})
    .to_string();
    let given = Given::parse(&raw).unwrap();
    assert_eq!(given.caches[0].restore[0].from, "refs/heads/main");
    assert!(given.caches[0].save.is_none());
    assert!(Given::parse(&raw.replace("\"v\":1", "\"v\":2"))
        .unwrap_err()
        .contains("version 2"));
}
