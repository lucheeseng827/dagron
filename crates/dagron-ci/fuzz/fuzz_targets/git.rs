//! What a forge's git answers a listing of refs with, in either protocol:
//! its advertisement, and a version 2 `ls-refs`.
#![no_main]

use dagron_ci::git;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|body: &[u8]| {
    for prefixes in [&["refs/heads/", "refs/tags/"][..], &[][..]] {
        let _ = git::read_advertisement(body, prefixes);
        let _ = git::read_ls_refs(body, prefixes);
    }
});
