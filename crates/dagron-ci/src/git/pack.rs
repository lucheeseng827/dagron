//! A pack, as a forge's git sends one for a shallow fetch, read for what a
//! paths filter asks: the paths where two commits' trees differ, as `git diff
//! --name-only --no-renames` lists them. The forge falls back on it where its
//! compare API cannot list a change (`Forge::changed_files`).
//!
//! The bytes are a forge's, so everything is bounded ([`Limits`]): the
//! objects a pack holds, the bytes they inflate to (a blob's as well as a
//! tree's, and each delta's result), how many deltas deep an object is, how
//! deep a tree is walked, the bytes of trees a walk reads, how many paths a
//! change touches, how long a path is and what the paths listed come to, and
//! a time to be done by. A blob is inflated to find where the next object
//! starts, and not kept: only commits and trees are. Objects are named here,
//! from what they hold, so a pack cannot pass one off as another. Two trees
//! are compared as they are read, an entry of each at a time in git's order,
//! so a walk holds no more than the paths it lists.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Instant;

use flate2::{Decompress, FlushDecompress, Status};
use sha1::Digest as _;

/// How a repository names its objects: SHA-1, or SHA-256.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectFormat {
    Sha1,
    Sha256,
}

impl ObjectFormat {
    /// The format a server's advertisement named (`object-format=`), and
    /// SHA-1 where it named none.
    pub fn named(name: Option<&str>) -> Result<ObjectFormat, String> {
        match name {
            None | Some("sha1") => Ok(ObjectFormat::Sha1),
            Some("sha256") => Ok(ObjectFormat::Sha256),
            Some(_) => Err("the repository names its objects in a format git does not have".into()),
        }
    }

    /// The bytes of an object's id.
    fn len(self) -> usize {
        match self {
            ObjectFormat::Sha1 => 20,
            ObjectFormat::Sha256 => 32,
        }
    }

    fn digest(self, parts: &[&[u8]]) -> Oid {
        let mut id = [0; 32];
        match self {
            ObjectFormat::Sha1 => {
                let mut hash = sha1::Sha1::new();
                parts.iter().for_each(|p| hash.update(p));
                id[..20].copy_from_slice(&hash.finalize());
            }
            ObjectFormat::Sha256 => {
                let mut hash = sha2::Sha256::new();
                parts.iter().for_each(|p| hash.update(p));
                id.copy_from_slice(&hash.finalize());
            }
        }
        Oid(id)
    }

    /// The id git gives an object of `kind` that holds `data`.
    fn object_id(self, kind: Kind, data: &[u8]) -> Oid {
        let header = format!("{} {}\0", kind.name(), data.len());
        self.digest(&[header.as_bytes(), data])
    }

    fn oid(self, bytes: &[u8]) -> Option<Oid> {
        let mut id = [0; 32];
        id.get_mut(..bytes.len())?.copy_from_slice(bytes);
        (bytes.len() == self.len()).then_some(Oid(id))
    }

    fn parse(self, hex: &str) -> Option<Oid> {
        self.oid(&hex::decode(hex).ok()?)
    }

    fn hex(self, id: Oid) -> String {
        hex::encode(&id.0[..self.len()])
    }
}

/// An object's id; a SHA-1 one is the first 20 bytes, the rest zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Oid([u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Commit,
    Tree,
    Blob,
    Tag,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Tree => "tree",
            Kind::Blob => "blob",
            Kind::Tag => "tag",
        }
    }
}

/// How much of a pack is read, and for how long.
#[derive(Clone, Debug)]
pub struct Limits {
    /// The most objects a pack may hold.
    pub objects: usize,
    /// The most bytes inflated, all told: each object's, and each delta's
    /// result.
    pub inflated: usize,
    /// The most deltas an object may be from a whole one.
    pub delta_depth: usize,
    /// The most trees deep a path may be.
    pub tree_depth: usize,
    /// The most bytes of trees a walk reads, all told: a tree it reaches by
    /// many paths is read, and counts, at each.
    pub walked: usize,
    /// The most paths a change may touch: each path listed, and each tree
    /// walked into, counts.
    pub paths: usize,
    /// The longest a path may be, in bytes.
    pub path_len: usize,
    /// The most bytes the paths listed may come to, all told.
    pub listed: usize,
    /// When to give up, if ever.
    pub until: Option<Instant>,
}

impl Limits {
    fn in_time(&self) -> Result<(), String> {
        match self.until {
            Some(until) if Instant::now() > until => Err("out of time".into()),
            _ => Ok(()),
        }
    }
}

/// The commits and trees of a pack (and its tags), by id.
pub struct Objects {
    format: ObjectFormat,
    by_id: HashMap<Oid, (Kind, Vec<u8>)>,
}

/// An object of the pack as it is read: whole, or a delta on another, which
/// is made whole once its base is.
struct Entry {
    offset: usize,
    is: Is,
    /// The object's bytes, or the delta's; none for a blob.
    data: Vec<u8>,
    /// How many deltas it is from a whole object.
    depth: usize,
}

enum Is {
    Whole(Kind),
    /// A delta on the entry of the pack at this index.
    OnEntry(usize),
    /// A delta on the object of this id.
    OnId(Oid),
}

/// What inflating may still come to.
struct Budget {
    left: usize,
    of: usize,
}

impl Budget {
    fn take(&mut self, bytes: usize) -> Result<(), String> {
        self.left = self
            .left
            .checked_sub(bytes)
            .ok_or_else(|| format!("the pack inflates to more than {}", in_units(self.of)))?;
        Ok(())
    }
}

/// Read `pack`, a version 2 or 3 pack named in `format`: its commits and
/// trees, each made whole and named. A delta whose base is not in the pack
/// (a blob's, which is not kept) is left out, and an object that needs it is
/// then missing from [`Objects::changed`]'s walk, which says so.
pub fn read(pack: &[u8], format: ObjectFormat, limits: &Limits) -> Result<Objects, String> {
    let sum = format.len();
    if pack.len() < 12 + sum || &pack[..4] != b"PACK" {
        return Err("the answer is not a pack".into());
    }
    let word = |at: usize| u32::from_be_bytes([pack[at], pack[at + 1], pack[at + 2], pack[at + 3]]);
    if !matches!(word(4), 2 | 3) {
        return Err(format!("pack version {} is not one git writes", word(4)));
    }
    let count = usize::try_from(word(8)).unwrap_or(usize::MAX);
    if count > limits.objects {
        return Err(format!(
            "the pack holds {count} objects, more than {}",
            limits.objects
        ));
    }
    let (body, trailer) = pack.split_at(pack.len() - sum);
    if format.digest(&[body]).0[..sum] != *trailer {
        return Err("the pack's checksum does not match it".into());
    }

    let mut budget = Budget {
        left: limits.inflated,
        of: limits.inflated,
    };
    let mut z = Decompress::new(true);
    let mut entries: Vec<Entry> = Vec::with_capacity(count.min(1 << 16));
    let mut at = 12;
    for n in 0..count {
        if n.is_multiple_of(256) {
            limits.in_time()?;
        }
        let offset = at;
        let (kind, size) = object_header(body, &mut at)?;
        let is = match kind {
            1 => Is::Whole(Kind::Commit),
            2 => Is::Whole(Kind::Tree),
            3 => Is::Whole(Kind::Blob),
            4 => Is::Whole(Kind::Tag),
            6 => {
                let back = base_offset(body, &mut at)?;
                let base = offset
                    .checked_sub(back)
                    .filter(|_| back > 0)
                    .and_then(|base| entries.binary_search_by_key(&base, |e| e.offset).ok())
                    .ok_or("a delta's base is not an object of the pack")?;
                Is::OnEntry(base)
            }
            7 => {
                let id = body.get(at..at + sum).ok_or_else(cut)?;
                at += sum;
                Is::OnId(format.oid(id).ok_or_else(cut)?)
            }
            other => {
                return Err(format!(
                    "an object of type {other}, which git does not have"
                ))
            }
        };
        // A blob, and a delta on one, is inflated to find where the next
        // object starts, and not kept.
        let blob = match is {
            Is::Whole(kind) => kind == Kind::Blob,
            Is::OnEntry(base) => matches!(entries[base].is, Is::Whole(Kind::Blob)),
            Is::OnId(_) => false,
        };
        budget.take(size)?;
        let data = inflate(&mut z, body, &mut at, size, !blob)?;
        entries.push(Entry {
            offset,
            is: if blob { Is::Whole(Kind::Blob) } else { is },
            data,
            depth: 0,
        });
    }
    if at != body.len() {
        return Err("the pack goes on past its last object".into());
    }

    // Each delta is made whole once its base is: what waits on each entry,
    // and on each id, is taken up as that one is named.
    let mut on_entry: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut on_id: HashMap<Oid, Vec<usize>> = HashMap::new();
    let mut ready = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        match entry.is {
            Is::Whole(Kind::Blob) => {}
            Is::Whole(_) => ready.push(i),
            Is::OnEntry(base) => on_entry.entry(base).or_default().push(i),
            Is::OnId(id) => on_id.entry(id).or_default().push(i),
        }
    }
    let mut ids: HashMap<Oid, usize> = HashMap::new();
    let mut n = 0usize;
    while let Some(base) = ready.pop() {
        n += 1;
        if n.is_multiple_of(256) {
            limits.in_time()?;
        }
        let Is::Whole(kind) = entries[base].is else {
            unreachable!("only a whole object is ready");
        };
        let id = format.object_id(kind, &entries[base].data);
        ids.entry(id).or_insert(base);
        let waiting: Vec<usize> = on_entry
            .remove(&base)
            .into_iter()
            .chain(on_id.remove(&id))
            .flatten()
            .collect();
        for delta in waiting {
            let depth = entries[base].depth + 1;
            if depth > limits.delta_depth {
                return Err(format!(
                    "an object is more than {} deltas deep",
                    limits.delta_depth
                ));
            }
            let whole = apply(&entries[base].data, &entries[delta].data, &mut budget)?;
            let entry = &mut entries[delta];
            (entry.is, entry.data, entry.depth) = (Is::Whole(kind), whole, depth);
            ready.push(delta);
        }
    }

    let mut by_id = HashMap::with_capacity(ids.len());
    for (id, i) in ids {
        if let Is::Whole(kind) = entries[i].is {
            by_id.insert(id, (kind, std::mem::take(&mut entries[i].data)));
        }
    }
    Ok(Objects { format, by_id })
}

impl Objects {
    /// The commits of the pack, by id: for the fuzz target, which has no
    /// other way to name them.
    pub fn commits(&self) -> Vec<String> {
        let mut commits: Vec<String> = self
            .by_id
            .iter()
            .filter(|(_, (kind, _))| *kind == Kind::Commit)
            .map(|(id, _)| self.format.hex(*id))
            .collect();
        commits.sort();
        commits
    }

    /// The paths where commit `from`'s tree and commit `to`'s differ, as
    /// `git diff --name-only --no-renames from to` lists them, sorted: a file
    /// added, removed, or changed in its contents or its mode; a link, and a
    /// submodule's commit, the same; and every file under a directory one of
    /// them adds or removes. A tree the walk needs and the pack does not
    /// hold, a tree out of git's order, and a path that is not UTF-8, is an
    /// error.
    pub fn changed(&self, from: &str, to: &str, limits: &Limits) -> Result<Vec<String>, String> {
        let (from, to) = (self.tree_of(from)?, self.tree_of(to)?);
        let mut walked = 0usize;
        let mut open = |tree: Option<Oid>| -> Result<Tree<'_>, String> {
            let tree = self.tree(tree)?;
            walked += tree.rest.len();
            match walked > limits.walked {
                true => Err(format!(
                    "the trees walked come to more than {}",
                    in_units(limits.walked)
                )),
                false => Ok(tree),
            }
        };
        // Depth first, a pair of trees for each directory down, each with
        // the length of its directory's path in `path`, the path of the
        // entry last looked at.
        let mut stack = vec![Pair::new(open(Some(from))?, open(Some(to))?, 0)?];
        let mut path = String::new();
        let (mut listed, mut listed_bytes) = (Vec::new(), 0usize);
        let (mut touched, mut steps) = (0usize, 0usize);
        while let Some(pair) = stack.last_mut() {
            if steps.is_multiple_of(256) {
                limits.in_time()?;
            }
            steps += 1;
            let order = match (pair.x, pair.y) {
                (None, None) => {
                    stack.pop();
                    continue;
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(x), Some(y)) => git_order(x, y),
            };
            // The entry that comes first in git's order, from either tree or
            // both: a file and a directory of one name are two.
            let x = match order.is_le() {
                true => std::mem::replace(&mut pair.x, pair.a.next()?),
                false => None,
            };
            let y = match order.is_ge() {
                true => std::mem::replace(&mut pair.y, pair.b.next()?),
                false => None,
            };
            let dir = pair.dir;
            let Some(entry) = x.or(y) else {
                unreachable!("one of the two has an entry");
            };
            let (x, y) = (x.map(|e| (e.mode, e.id)), y.map(|e| (e.mode, e.id)));
            if x == y {
                continue;
            }
            touched += 1;
            if touched > limits.paths {
                return Err(format!(
                    "the change touches more than {} paths",
                    limits.paths
                ));
            }
            let name = std::str::from_utf8(entry.name).map_err(|_| match &path[..dir] {
                "" => "a path at the top is not UTF-8".to_string(),
                dir => format!("a path in {dir} is not UTF-8"),
            })?;
            path.truncate(dir);
            if dir > 0 {
                path.push('/');
            }
            path.push_str(name);
            if path.len() > limits.path_len {
                return Err(format!("a path is longer than {} bytes", limits.path_len));
            }
            // A directory on either side is walked: every file under one
            // that comes or goes is listed.
            if entry.mode == TREE {
                if stack.len() > limits.tree_depth {
                    return Err(format!("a tree is more than {} deep", limits.tree_depth));
                }
                let pair = Pair::new(open(x.map(|e| e.1))?, open(y.map(|e| e.1))?, path.len())?;
                stack.push(pair);
            } else {
                listed_bytes += path.len();
                if listed_bytes > limits.listed {
                    return Err(format!(
                        "the paths listed come to more than {}",
                        in_units(limits.listed)
                    ));
                }
                listed.push(path.clone());
            }
        }
        listed.sort_unstable();
        Ok(listed)
    }

    /// The tree commit `commit` (its id in hex) names.
    fn tree_of(&self, commit: &str) -> Result<Oid, String> {
        let short = &commit[..commit.len().min(12)];
        let not_held = || format!("commit {short} is not in the pack");
        let id = self.format.parse(commit).ok_or_else(not_held)?;
        let Some((Kind::Commit, data)) = self.by_id.get(&id) else {
            return Err(not_held());
        };
        data.strip_prefix(b"tree ")
            .and_then(|rest| rest.get(..self.format.len() * 2))
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| self.format.parse(hex))
            .ok_or_else(|| format!("commit {short} names no tree"))
    }

    /// The entries of `tree`, to be read (none for none).
    fn tree(&self, tree: Option<Oid>) -> Result<Tree<'_>, String> {
        let empty = |id| Tree {
            id,
            format: self.format,
            rest: &[],
            last: None,
        };
        let Some(id) = tree else {
            return Ok(empty(self.format.object_id(Kind::Tree, b"")));
        };
        match self.by_id.get(&id) {
            Some((Kind::Tree, data)) => Ok(Tree {
                rest: data,
                ..empty(id)
            }),
            // The empty tree, which a pack need not carry.
            None if id == self.format.object_id(Kind::Tree, b"") => Ok(empty(id)),
            _ => Err(format!(
                "tree {} is not in the pack",
                &self.format.hex(id)[..12]
            )),
        }
    }
}

/// An entry of a tree: its name, its mode as git reads it, and its object.
#[derive(Clone, Copy)]
struct TreeEntry<'a> {
    name: &'a [u8],
    mode: u32,
    id: Oid,
}

/// Two entries in git's order: by name, a tree's as if it ended in `/`.
fn git_order(x: TreeEntry, y: TreeEntry) -> Ordering {
    let len = x.name.len().min(y.name.len());
    let end = |e: TreeEntry| {
        e.name
            .get(len)
            .copied()
            .or((e.mode == TREE).then_some(b'/'))
    };
    x.name[..len]
        .cmp(&y.name[..len])
        .then_with(|| end(x).cmp(&end(y)))
}

/// A tree's entries, read one at a time: what is left of it, and the entry
/// read last, which the next must come after.
struct Tree<'a> {
    id: Oid,
    format: ObjectFormat,
    rest: &'a [u8],
    last: Option<TreeEntry<'a>>,
}

impl<'a> Tree<'a> {
    /// The next entry, or none at the tree's end. An entry git would not
    /// write (a name empty, a dot, or with a slash; a mode that is not
    /// octal; one cut short, or not after the one before) is an error.
    fn next(&mut self) -> Result<Option<TreeEntry<'a>>, String> {
        if self.rest.is_empty() {
            return Ok(None);
        }
        let bad = || {
            format!(
                "tree {} is not one git writes",
                &self.format.hex(self.id)[..12]
            )
        };
        let rest = self.rest;
        let space = rest.iter().position(|b| *b == b' ').ok_or_else(bad)?;
        let mode = &rest[..space];
        if mode.is_empty() || mode.len() > 7 || !mode.iter().all(|b| (b'0'..=b'7').contains(b)) {
            return Err(bad());
        }
        let mode = mode.iter().fold(0u32, |m, b| m * 8 + u32::from(b - b'0'));
        let rest = &rest[space + 1..];
        let nul = rest.iter().position(|b| *b == 0).ok_or_else(bad)?;
        let name = &rest[..nul];
        let rest = &rest[nul + 1..];
        let id = rest.get(..self.format.len()).ok_or_else(bad)?;
        if matches!(name, b"" | b"." | b"..") || name.contains(&b'/') {
            return Err(bad());
        }
        let entry = TreeEntry {
            name,
            mode: canonical(mode),
            id: self.format.oid(id).ok_or_else(bad)?,
        };
        if self.last.is_some_and(|last| git_order(last, entry).is_ge()) {
            return Err(bad());
        }
        (self.rest, self.last) = (&rest[self.format.len()..], Some(entry));
        Ok(Some(entry))
    }
}

/// Two trees being compared: each one's entries, the entry each is at, and
/// the length of their path.
struct Pair<'a> {
    a: Tree<'a>,
    b: Tree<'a>,
    x: Option<TreeEntry<'a>>,
    y: Option<TreeEntry<'a>>,
    dir: usize,
}

impl<'a> Pair<'a> {
    fn new(mut a: Tree<'a>, mut b: Tree<'a>, dir: usize) -> Result<Pair<'a>, String> {
        let (x, y) = (a.next()?, b.next()?);
        Ok(Pair { a, b, x, y, dir })
    }
}

/// A tree entry's mode for a tree.
const TREE: u32 = 0o040000;

/// A mode as git reads a tree's (its `canon_mode`): a file is 644 or 755 by
/// its owner's execute bit; a link, a tree; and anything else a submodule.
fn canonical(mode: u32) -> u32 {
    match mode & 0o170000 {
        0o100000 if mode & 0o100 != 0 => 0o100755,
        0o100000 => 0o100644,
        0o120000 => 0o120000,
        TREE => TREE,
        _ => 0o160000,
    }
}

fn cut() -> String {
    "the pack is cut short".into()
}

/// An object's type and size, which start it.
fn object_header(body: &[u8], at: &mut usize) -> Result<(u8, usize), String> {
    let mut byte = *body.get(*at).ok_or_else(cut)?;
    *at += 1;
    let kind = (byte >> 4) & 7;
    let mut size = u64::from(byte & 15);
    let mut shift = 4;
    while byte & 0x80 != 0 {
        byte = *body.get(*at).ok_or_else(cut)?;
        *at += 1;
        if shift > 57 {
            return Err("an object's size is past what git writes".into());
        }
        size |= u64::from(byte & 0x7f) << shift;
        shift += 7;
    }
    let size = usize::try_from(size).map_err(|_| "an object's size is past what can be read")?;
    Ok((kind, size))
}

/// How far back a delta's base is, in git's own encoding for it: each byte
/// after the first adds one before it shifts, so no offset has two
/// spellings.
fn base_offset(body: &[u8], at: &mut usize) -> Result<usize, String> {
    let mut byte = *body.get(*at).ok_or_else(cut)?;
    *at += 1;
    let mut back = usize::from(byte & 0x7f);
    while byte & 0x80 != 0 {
        byte = *body.get(*at).ok_or_else(cut)?;
        *at += 1;
        back = back
            .checked_add(1)
            .and_then(|b| b.checked_mul(128))
            .ok_or("a delta's base is past the pack's start")?
            | usize::from(byte & 0x7f);
    }
    Ok(back)
}

/// Inflate the zlib stream at `at`, which must come to `size` bytes: those
/// bytes where `keep`, and none otherwise. `at` moves past the stream.
fn inflate(
    z: &mut Decompress,
    body: &[u8],
    at: &mut usize,
    size: usize,
    keep: bool,
) -> Result<Vec<u8>, String> {
    z.reset(true);
    let input = &body[*at..];
    let mut out = Vec::new();
    let mut scratch = [0; 8192];
    if keep {
        // One more than it should come to, to see if it comes to more.
        out.reserve_exact(size + 1);
    }
    loop {
        let (taken, made) = (z.total_in(), z.total_out());
        let rest = &input[usize::try_from(taken)
            .unwrap_or(usize::MAX)
            .min(input.len())..];
        let status = if keep {
            z.decompress_vec(rest, &mut out, FlushDecompress::None)
        } else {
            let room = (size + 1 - usize::try_from(made).unwrap_or(size)).min(scratch.len());
            z.decompress(rest, &mut scratch[..room], FlushDecompress::None)
        }
        .map_err(|e| format!("an object does not inflate: {e}"))?;
        if z.total_out() > size as u64 {
            return Err("an object inflates past the size it says".into());
        }
        match status {
            Status::StreamEnd => break,
            _ if z.total_in() == taken && z.total_out() == made => return Err(cut()),
            _ => {}
        }
    }
    if z.total_out() != size as u64 {
        return Err("an object inflates short of the size it says".into());
    }
    *at += usize::try_from(z.total_in()).map_err(|_| cut())?;
    Ok(out)
}

/// A size of a delta's header: seven bits a byte, the lowest first.
fn delta_size(delta: &mut &[u8]) -> Result<usize, String> {
    let mut size = 0u64;
    let mut shift = 0;
    loop {
        let (&byte, rest) = delta.split_first().ok_or("a delta is cut short")?;
        *delta = rest;
        if shift > 57 {
            return Err("a delta's size is past what git writes".into());
        }
        size |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return usize::try_from(size)
                .map_err(|_| "a delta's size is past what can be read".into());
        }
        shift += 7;
    }
}

/// `delta` applied to `base`: the base's size, the result's, then copies
/// from the base and bytes of the delta's own.
fn apply(base: &[u8], mut delta: &[u8], budget: &mut Budget) -> Result<Vec<u8>, String> {
    if delta_size(&mut delta)? != base.len() {
        return Err("a delta's base is not the size the delta says".into());
    }
    let size = delta_size(&mut delta)?;
    budget.take(size)?;
    let mut out = Vec::with_capacity(size);
    while let Some((&op, rest)) = delta.split_first() {
        delta = rest;
        let piece = if op & 0x80 != 0 {
            // A copy: which of the offset's four bytes and the length's
            // three follow, by the low seven bits; a length of 0 is 64 KiB.
            let mut field = |bits: std::ops::Range<u8>| -> Result<usize, String> {
                let mut value = 0;
                for bit in bits.clone() {
                    if op & (1 << bit) != 0 {
                        let (&byte, rest) = delta.split_first().ok_or("a delta is cut short")?;
                        delta = rest;
                        value |= usize::from(byte) << (8 * (bit - bits.start));
                    }
                }
                Ok(value)
            };
            let offset = field(0..4)?;
            let len = match field(4..7)? {
                0 => 0x10000,
                len => len,
            };
            offset
                .checked_add(len)
                .and_then(|end| base.get(offset..end))
                .ok_or("a delta copies from past its base's end")?
        } else if op != 0 {
            let len = usize::from(op);
            let piece = delta.get(..len).ok_or("a delta is cut short")?;
            delta = &delta[len..];
            piece
        } else {
            return Err("a delta has an instruction 0, which git keeps for later".into());
        };
        if out.len() + piece.len() > size {
            return Err("a delta makes more than it says".into());
        }
        out.extend_from_slice(piece);
    }
    if out.len() != size {
        return Err("a delta makes less than it says".into());
    }
    Ok(out)
}

/// `body` with the checksum a pack ends with after it.
pub(crate) fn sealed(body: &[u8], format: ObjectFormat) -> Vec<u8> {
    let sum = format.digest(&[body]);
    [body, &sum.0[..format.len()]].concat()
}

/// `bytes` in MiB where it is a whole number of them.
fn in_units(bytes: usize) -> String {
    match bytes {
        b if b >= 1 << 20 && b.is_multiple_of(1 << 20) => format!("{} MiB", b >> 20),
        b => format!("{b} bytes"),
    }
}

#[cfg(test)]
pub(crate) mod tests;
