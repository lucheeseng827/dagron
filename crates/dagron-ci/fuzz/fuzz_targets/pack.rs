//! What a forge's git answers a shallow fetch with, for a paths filter: the
//! answer's sections and bands, and a pack, read and walked for the paths
//! its commits' trees differ in. The input is a pack's body: its checksum is
//! put after it, or nothing past the checksum would be reached.
#![no_main]

use dagron_ci::fuzzing::sealed_pack;
use dagron_ci::git::{self, pack};
use libfuzzer_sys::fuzz_target;

/// Small, so that an input that inflates, nests or fans out far is refused
/// soon.
const LIMITS: pack::Limits = pack::Limits {
    objects: 4096,
    inflated: 1 << 20,
    delta_depth: 64,
    tree_depth: 32,
    walked: 1 << 20,
    paths: 4096,
    path_len: 4096,
    listed: 1 << 20,
    until: None,
};

fuzz_target!(|body: &[u8]| {
    let _ = git::read_fetch(body.to_vec());
    for format in [pack::ObjectFormat::Sha1, pack::ObjectFormat::Sha256] {
        let Ok(objects) = pack::read(&sealed_pack(body, format), format, &LIMITS) else {
            continue;
        };
        let commits = objects.commits();
        for from in commits.iter().take(4) {
            for to in commits.iter().take(4) {
                let _ = objects.changed(from, to, &LIMITS);
            }
        }
    }
});
