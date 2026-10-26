//! A task pod's log, followed as its container writes it.
//!
//! The kubelet hands a container's log over as one stream, stdout and stderr
//! as they were written, and ends a follow of it once the container has
//! stopped and the stream has nothing more. A follow is a long request, and
//! one that is cut while the container runs (the client's read timeout,
//! after nothing has come for a while; a proxy between) is opened again from
//! where it got to. The only places it can be opened from are times, rounded
//! to the second, so each line is asked for with its time (`timestamps`),
//! and what was read already is known by it and not read twice ([`Stamped`]).
//!
//! The kubelet puts a time at the start of each line, and a line that has
//! none is not the task's: the end of one begun before the second a follow
//! was opened from (the kubelet passes over the parts of a long line written
//! before then, and hands over the rest with no time), or the kubelet's own
//! words, which it writes into a follow it cannot go on with, at its end,
//! wherever in a line that is. A line's text is held until it ends (a long
//! one's last part is), so that what the log hands over of it when it is
//! opened again is checked against what was held, and the kubelet's words at
//! the end of a part of one are let go. A log is read once more, and not
//! followed, at its pod's end, and at a deadline that ends the reading of it
//! ([`read_now`]): the kubelet writes no words into a log it is not
//! following.

use std::time::Duration;

use futures_util::AsyncReadExt;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, LogParams};
use tokio::sync::watch;

use crate::lines::WholeLines;

/// The most of a line's start that is read for its time. The kubelet writes
/// it as `2006-01-02T15:04:05.000000000Z07:00`, 35 bytes at most, and a space.
const STAMP_MAX: usize = 64;

/// How long a follow that ended before the pod did waits to be opened again,
/// at first and at most; it waits no longer once the pod is over.
const AGAIN_FIRST: Duration = Duration::from_secs(1);
const AGAIN_MOST: Duration = Duration::from_secs(10);

/// How many times the log is read once the pod is over: what its container
/// wrote last, and once more if that was cut. A follow that never opened is
/// not tried more than that either.
const OPENS_AFTER_OVER: u32 = 2;

/// How long a follow that came to its end before the pod is over waits for
/// the pod to be seen over, before it is opened again: the kubelet ends one
/// a second or so after its container has, and the pod is seen over soon
/// after.
const AFTER_AN_END: Duration = Duration::from_secs(3);

/// How far after the last line read before a line may come, by its time,
/// and still be one of those read before: the two streams' lines are timed
/// as each is read, and may be written to the kubelet's file a little out of
/// that order. A line timed later than that ends what is passed over, where
/// the kubelet no longer has the last line read (it rotated the log while
/// the follow was cut).
const OUT_OF_ORDER_NANOS: i128 = 1_000_000_000;

/// How long the lines begun at each exact time are counted for, back from
/// the latest: longer than a follow opened again goes back (to the start of
/// the second), and than lines come out of order.
const TIES_NANOS: i128 = 3_000_000_000;

/// How much of the end of a long line, handed on as it comes, stays held:
/// more than the kubelet writes of its own words, so that words that ended a
/// part of it are still held, and checked, when the log is opened again.
const KEPT: usize = 512;

/// How long the log is read for, once, at a deadline that ends the reading
/// of it ([`read_now`]).
pub(crate) const LAST_READ: Duration = Duration::from_secs(10);

/// A line begun: its time, how many lines begun before it had the same, how
/// much of it has been read (not counting its newline), and whether it ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Begun {
    at: Timestamp,
    nth: usize,
    into: usize,
    ended: bool,
}

/// What a follow opened again has to pass over: what was read before.
#[derive(Clone, Copy, Debug)]
struct Skip {
    /// The last line begun before.
    upto: Begun,
    /// How many lines of `upto`'s time have come in the new stream.
    seen: usize,
}

/// What is done with the line being read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Taking {
    /// It is read into the log.
    All,
    /// It was read before, whole, or it is not the task's: none of it is.
    None,
    /// It was being read when the follow was cut, and is handed over again
    /// from its start: so much of it has come again. What comes again is
    /// what was read before as far as it is the same; where it is not, what
    /// was held of it was not the task's (the kubelet's words), and goes.
    Again(usize),
    /// It may be the last line read before, which had ended: passed over,
    /// and so many bytes of it have come. Where it ends as long as that line
    /// was, it is; else it is another of that time (on CRI-O, one read's
    /// lines all have one), and what is passed over goes on.
    Candidate(usize),
}

/// A followed log, stamped with each line's time, read into whole lines of
/// its text, the times taken off; and where to open it again from, and what
/// of it to pass over then.
pub(crate) struct Stamped {
    /// Whether the next byte begins a line: its time, then a space.
    at_start: bool,
    /// What has come of the time of the line that is beginning.
    stamp: Vec<u8>,
    /// What was read of the line being read and not yet handed on: held to
    /// its end, or until it is longer than [`WholeLines::HELD`].
    held: Vec<u8>,
    /// The last line begun.
    last: Option<Begun>,
    /// How many lines were begun at each exact time (in nanoseconds), lately,
    /// and the latest time a line was begun at.
    ties: std::collections::BTreeMap<i128, usize>,
    latest: i128,
    /// What the line being read is to this log.
    taking: Taking,
    skip: Option<Skip>,
    /// Whether the next line is the first of a follow opened again.
    resumed: bool,
    /// Whether the line being passed over is the first, with no time, of a
    /// follow opened again, and has not ended.
    in_tail: bool,
    /// Whether what is held of the line being read ends with what a follow
    /// that came to its end inside the line handed over last: the kubelet's
    /// words may be its end, until the log opened again hands it over.
    unconfirmed: bool,
    /// Whether a follow was cut, and the log opened again since has not yet
    /// passed over all it read before.
    cut_open: bool,
    /// Whether a line that was not the task's was passed over since this was
    /// last asked ([`Self::foreign`]).
    foreign: bool,
    /// Whether what was passed over ended where the last line read before
    /// was not found ([`Self::seam`]).
    seam: bool,
}

impl Default for Stamped {
    fn default() -> Self {
        Self {
            at_start: true,
            stamp: Vec::new(),
            held: Vec::new(),
            last: None,
            ties: Default::default(),
            latest: i128::MIN,
            taking: Taking::All,
            skip: None,
            resumed: false,
            in_tail: false,
            unconfirmed: false,
            cut_open: false,
            foreign: false,
            seam: false,
        }
    }
}

impl Stamped {
    /// Where a follow is opened (again) from: the second before the one the
    /// last line begun began in, or the log's start if none has. A second
    /// before, since a line of the other stream written after it may be
    /// timed a moment before it, in the second before. What it hands over
    /// first is then passed over as far as it was read before.
    pub(crate) fn resume_from(&mut self) -> Option<Timestamp> {
        // A new stream begins at a line's start, with its time.
        self.at_start = true;
        self.stamp.clear();
        self.in_tail = false;
        // Cut again while what was read before was being passed over: it is
        // what is still to be passed over.
        let upto = match self.skip {
            Some(skip) => skip.upto,
            None => self.last?,
        };
        self.skip = Some(Skip { upto, seen: 0 });
        self.resumed = true;
        let back = (upto.at.as_nanosecond() - OUT_OF_ORDER_NANOS).div_euclid(1_000_000_000);
        Some(Timestamp::from_second(back as i64).unwrap_or(upto.at))
    }

    /// Whether a line that was not the task's was passed over since this was
    /// last asked: the kubelet's own words, which it writes into a follow it
    /// cannot go on with. Such a follow's end is a cut, not the log's end.
    pub(crate) fn foreign(&mut self) -> bool {
        std::mem::take(&mut self.foreign)
    }

    /// Whether, since this was last asked, the log opened again did not hand
    /// over the last line read before, and what came after it was read from
    /// a line timed well after it: the kubelet rotated the log, and only has
    /// the file it is writing. What was in the one before it after that line
    /// may not be in the log, and a line that was being written across the
    /// two may be in it in part twice, or in part not.
    pub(crate) fn seam(&mut self) -> bool {
        std::mem::take(&mut self.seam)
    }

    /// Whether the last stream ended inside the first thing a follow opened
    /// again handed over with no time. The end of a line read before ends
    /// with a newline, the stamped line after it following: one that does
    /// not is the kubelet's own words.
    pub(crate) fn in_tail(&self) -> bool {
        self.in_tail
    }

    /// A follow came to its end: `clean`, it ended where it was given no
    /// more (`false`: it was cut). Where that was inside a line, what is held
    /// of the line may end with the kubelet's words.
    pub(crate) fn ended(&mut self, clean: bool) {
        let reading = matches!(self.taking, Taking::All | Taking::Again(_));
        if clean && reading && !self.at_start && !self.held.is_empty() {
            self.unconfirmed = true;
        }
        if !clean {
            self.cut_open = true;
        }
    }

    /// Whether, the follow having been cut, what the log opened again had to
    /// pass over is still to be passed over. At a stream's end, the kubelet
    /// no longer has the last line read before: it rotated the log while the
    /// follow was cut.
    pub(crate) fn lost_in_a_cut(&self) -> bool {
        self.skip.is_some() && self.cut_open
    }

    /// The log's reading ended before it had handed over again all of the
    /// line being read (out of time, or cut): what was held of it stays as
    /// it was read, and is not taken to be more than the log has.
    pub(crate) fn cut(&mut self) {
        if let Taking::Again(_) = self.taking {
            self.taking = Taking::All;
        }
    }

    /// Bytes of the stream as they came. The whole lines of text the log now
    /// has, if any.
    pub(crate) fn feed(&mut self, mut bytes: &[u8]) -> Option<String> {
        let mut text = Vec::with_capacity(bytes.len());
        while !bytes.is_empty() {
            if self.at_start {
                let room = STAMP_MAX - self.stamp.len();
                match bytes
                    .iter()
                    .take(room)
                    .position(|b| *b == b' ' || *b == b'\n')
                {
                    Some(at) if bytes[at] == b' ' => {
                        self.stamp.extend_from_slice(&bytes[..at]);
                        bytes = &bytes[at + 1..];
                        let at = std::str::from_utf8(&self.stamp)
                            .ok()
                            .and_then(|s| s.parse::<Timestamp>().ok());
                        self.stamp.clear();
                        match at {
                            Some(at) => self.begin(at, &mut text),
                            None => self.unstamped(),
                        }
                        self.at_start = false;
                    }
                    // A line with no time, or one that does not end where
                    // a time would.
                    Some(_) => {
                        self.stamp.clear();
                        self.unstamped();
                        self.at_start = false;
                    }
                    None if bytes.len() < room => {
                        self.stamp.extend_from_slice(bytes);
                        bytes = &[];
                    }
                    None => {
                        self.stamp.clear();
                        bytes = &bytes[room..];
                        self.unstamped();
                        self.at_start = false;
                    }
                }
                continue;
            }
            let (line, ended) = match bytes.iter().position(|b| *b == b'\n') {
                Some(at) => (&bytes[..at], true),
                None => (bytes, false),
            };
            bytes = &bytes[line.len() + usize::from(ended)..];
            self.read(line, ended, &mut text);
            if ended {
                self.at_start = true;
                self.in_tail = false;
                self.taking = Taking::All;
            }
        }
        if text.is_empty() {
            return None;
        }
        Some(String::from_utf8_lossy(&text).into_owned())
    }

    /// What is left when the log has ended: a last line that had no newline.
    /// Where it was being handed over again, as far as it came: what was
    /// held past that is not the task's.
    pub(crate) fn rest(&mut self) -> Option<String> {
        if let (Taking::Again(at), Some(last)) = (self.taking, self.last) {
            let handed = last.into.saturating_sub(self.held.len());
            self.held.truncate(at.saturating_sub(handed));
            self.taking = Taking::All;
            self.unconfirmed = false;
        }
        // A part a follow ended inside that nothing read again (the kubelet
        // no longer had it): but for its end, which may be its words.
        if std::mem::take(&mut self.unconfirmed) {
            self.trim_unconfirmed();
        }
        if self.held.is_empty() {
            return None;
        }
        let rest = std::mem::take(&mut self.held);
        Some(String::from_utf8_lossy(&rest).into_owned())
    }

    /// A line begins, at `at`: read, or passed over as one read before.
    ///
    /// What a follow opened again hands over is passed over by where it is
    /// in the kubelet's file, not by its time alone: every line up to the
    /// last read before, which is known by its time and by how many lines
    /// before it had the same; and that one, where it had ended, or on from
    /// where it got to. What comes after it is read, whatever its time.
    fn begin(&mut self, at: Timestamp, text: &mut Vec<u8>) {
        self.resumed = false;
        let Some(skip) = self.skip.as_mut() else {
            let nth = self.tie(at);
            self.last = Some(Begun {
                at,
                nth,
                into: 0,
                ended: false,
            });
            self.taking = Taking::All;
            return;
        };
        let upto = skip.upto;
        if at == upto.at {
            skip.seen += 1;
            if skip.seen < upto.nth {
                self.taking = Taking::None;
                return;
            }
            // The last line read before, where it had ended: passed over,
            // where it is as long. Else read on from where the follow was cut
            // inside it.
            if upto.ended {
                self.taking = Taking::Candidate(0);
                return;
            }
            self.skip = None;
            self.cut_open = false;
            self.last = Some(upto);
            self.taking = Taking::Again(0);
            return;
        }
        if at.as_nanosecond() <= upto.at.as_nanosecond() + OUT_OF_ORDER_NANOS {
            self.taking = Taking::None;
            return;
        }
        // The last line read before is not in the file the kubelet has. What
        // was held of it, where it had not ended, goes on as it is: the
        // rest of it may be what comes now, or not be there at all. But for
        // its end, where a follow ended there and nothing came again to say
        // that it was the task's.
        self.skip = None;
        self.cut_open = false;
        self.seam = true;
        if std::mem::take(&mut self.unconfirmed) {
            self.trim_unconfirmed();
        }
        text.append(&mut self.held);
        let nth = self.tie(at);
        self.last = Some(Begun {
            at,
            nth,
            into: 0,
            ended: false,
        });
        self.taking = Taking::All;
    }

    /// What is held of a line a follow ended inside, less its end, which
    /// may be the kubelet's words: no more of it than is kept of a long line.
    fn trim_unconfirmed(&mut self) {
        let upto = self.held.len().saturating_sub(KEPT);
        let upto = WholeLines::whole_characters(&self.held[..upto]);
        self.held.truncate(upto);
    }

    /// The line begun at `at` is the how-manyth begun at exactly that time:
    /// counted for the last few seconds, whether or not the lines between
    /// them had other times.
    fn tie(&mut self, at: Timestamp) -> usize {
        let at = at.as_nanosecond();
        self.latest = self.latest.max(at);
        while let Some(entry) = self.ties.first_entry() {
            if *entry.key() + TIES_NANOS >= self.latest {
                break;
            }
            entry.remove();
        }
        let n = self.ties.entry(at).or_default();
        *n += 1;
        *n
    }

    /// A line that has no time, which the kubelet does not write of the
    /// task's: passed over. The first a follow opened again hands over is
    /// the end of a line read before. Any other is the kubelet's own words.
    fn unstamped(&mut self) {
        if std::mem::take(&mut self.resumed) {
            self.in_tail = true;
        } else {
            self.foreign = true;
        }
        self.taking = Taking::None;
    }

    /// Bytes of the line being read, up to its newline where it `ended`.
    /// What is read into the log is held until the line ends, or is longer
    /// than is held; then handed on into `text`.
    fn read(&mut self, mut bytes: &[u8], ended: bool, text: &mut Vec<u8>) {
        match self.taking {
            Taking::None => return,
            Taking::Candidate(passed) => {
                let passed = passed + bytes.len();
                if !ended {
                    self.taking = Taking::Candidate(passed);
                    return;
                }
                if let Some(skip) = self.skip.as_mut() {
                    if passed == skip.upto.into {
                        self.last = Some(skip.upto);
                        self.skip = None;
                        self.cut_open = false;
                    } else {
                        skip.seen -= 1;
                    }
                }
                return;
            }
            Taking::Again(at) => {
                let Some(last) = self.last else {
                    self.taking = Taking::All;
                    return self.read(bytes, ended, text);
                };
                // What was read of it before: handed on, which is passed
                // over; then held, which what comes again is checked
                // against, as far as it is the same.
                let handed = last.into.saturating_sub(self.held.len());
                let mut at = at;
                let skipped = handed.saturating_sub(at).min(bytes.len());
                at += skipped;
                bytes = &bytes[skipped..];
                let same = bytes
                    .iter()
                    .zip(&self.held[(at.max(handed) - handed).min(self.held.len())..])
                    .take_while(|(a, b)| a == b)
                    .count();
                at += same;
                bytes = &bytes[same..];
                if bytes.is_empty() && !ended && at < last.into {
                    self.taking = Taking::Again(at);
                    return;
                }
                if at < last.into {
                    // It is not the same from here, or ends here: what was
                    // held past this was not the task's.
                    self.held.truncate(at.saturating_sub(handed));
                    if let Some(last) = self.last.as_mut() {
                        last.into = at.max(handed);
                    }
                }
                self.unconfirmed = false;
                self.taking = Taking::All;
            }
            Taking::All => {}
        }
        self.held.extend_from_slice(bytes);
        if let Some(last) = self.last.as_mut() {
            last.into += bytes.len();
            last.ended = ended;
        }
        if ended {
            self.unconfirmed = false;
            text.append(&mut self.held);
            text.push(b'\n');
        } else if self.held.len() > WholeLines::HELD {
            // A long line is handed on as it comes, but for its last part.
            let upto =
                WholeLines::whole_characters(&self.held[..self.held.len().saturating_sub(KEPT)]);
            text.extend(self.held.drain(..upto));
        }
    }
}

/// Follow the log of the task container `container` of pod `name`, from
/// when it has started (`started`) to when it has ended, handing each piece
/// of whole lines to `take` as it comes. Opened again, from where it got to,
/// whenever it ends while the pod runs. Once the pod is over (`over`), it is
/// read to its end, at most [`OPENS_AFTER_OVER`] times more, and not
/// followed: the container has stopped, and a follow is what the kubelet
/// watches the file for, and writes its words into where it cannot. A pod
/// whose container never started has no log, and is not asked for one.
/// Whether the log was read to its end: not where every read once the pod
/// was over was cut.
pub(crate) async fn follow(
    pods: &Api<Pod>,
    name: &str,
    container: &str,
    mut started: watch::Receiver<bool>,
    mut over: watch::Receiver<bool>,
    stamped: &mut Stamped,
    mut take: impl FnMut(String),
) -> bool {
    tokio::select! {
        _ = started.wait_for(|s| *s) => {}
        _ = over.wait_for(|o| *o) => {}
    }
    let mut again = AGAIN_FIRST;
    let mut opens_after_over = 0;
    loop {
        let is_over = *over.borrow();
        if is_over {
            if !*started.borrow() {
                return true;
            }
            if opens_after_over == OPENS_AFTER_OVER {
                return false;
            }
            opens_after_over += 1;
        }
        let params = LogParams {
            container: Some(container.to_string()),
            follow: !is_over,
            timestamps: true,
            since_time: stamped.resume_from(),
            ..LogParams::default()
        };
        let mut ended = match pods.log_stream(name, &params).await {
            Ok(stream) => {
                let mut stream = std::pin::pin!(stream);
                let mut buf = vec![0u8; 8 * 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) => break true,
                        // What was read before, read again, is not
                        // progress: the pace is kept.
                        Ok(n) => {
                            if let Some(whole) = stamped.feed(&buf[..n]) {
                                again = AGAIN_FIRST;
                                take(whole);
                            }
                        }
                        Err(e) => {
                            tracing::debug!(pod = %name, error = %e, "the task's log follow was cut; it is opened again");
                            break false;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(pod = %name, error = %e, "the task's log could not be followed; it is asked again");
                false
            }
        };
        if !is_over {
            stamped.ended(ended);
        }
        if stamped.foreign() || (ended && stamped.in_tail()) {
            tracing::warn!(pod = %name, "the kubelet ended a follow of the task's log with words of its own, which are not in the log; it is opened again");
            ended = false;
        }
        if stamped.seam() || (ended && stamped.lost_in_a_cut()) {
            tracing::warn!(pod = %name, "the last line read of the task's log is not in the file the kubelet has, which it rotated: what was in the file before after that line, and lines written a moment after it, may not be in the log, and a line written across the two files may be in it in part twice or in part not");
        }
        if *over.borrow() {
            if ended && is_over {
                return true;
            }
            continue;
        }
        // Ended before the pod: cut, or the container has stopped and the
        // pod is not seen to be over yet. Again shortly, or at once when it
        // is.
        let wait = if ended { AFTER_AN_END } else { again };
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            _ = over.wait_for(|o| *o) => {}
        }
        if !ended {
            again = (again * 2).min(AGAIN_MOST);
        }
    }
}

/// Read the log of the task container `container` of pod `name` once,
/// not followed, from where it got to, handing whole lines to `take`: what
/// the kubelet has of it now, at a deadline that ends the reading of it.
/// The kubelet writes no words of its own into a log it is not following,
/// and what was held of a line is checked against this. Whether it was read
/// to its end.
pub(crate) async fn read_now(
    pods: &Api<Pod>,
    name: &str,
    container: &str,
    stamped: &mut Stamped,
    mut take: impl FnMut(String),
) -> bool {
    let params = LogParams {
        container: Some(container.to_string()),
        timestamps: true,
        since_time: stamped.resume_from(),
        ..LogParams::default()
    };
    let stream = match pods.log_stream(name, &params).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::debug!(pod = %name, error = %e, "the task's log could not be read at its end");
            return false;
        }
    };
    let mut stream = std::pin::pin!(stream);
    let mut buf = vec![0u8; 8 * 1024];
    let read = loop {
        match stream.read(&mut buf).await {
            Ok(0) => break true,
            Ok(n) => {
                if let Some(whole) = stamped.feed(&buf[..n]) {
                    take(whole);
                }
            }
            Err(e) => {
                tracing::debug!(pod = %name, error = %e, "the task's log could not be read to its end");
                break false;
            }
        }
    };
    if stamped.seam() || (read && stamped.lost_in_a_cut()) {
        tracing::warn!(pod = %name, "the last line read of the task's log is not in the file the kubelet has, which it rotated: what was in the file before after that line, and lines written a moment after it, may not be in the log, and a line written across the two files may be in it in part twice or in part not");
    }
    read
}

#[cfg(test)]
mod tests {
    use super::*;

    const T1: &str = "2026-10-21T10:00:00.100000000Z";
    const T2: &str = "2026-10-21T10:00:00.200000000Z";
    const T3: &str = "2026-10-21T10:00:01.000000000Z";
    /// More than a second after the others.
    const T4: &str = "2026-10-21T10:00:02.000000000Z";
    const WORDS: &str = "failed to create fsnotify watcher: too many open files";

    /// The stream's bytes, fed in pieces of `size`, and what they made.
    fn fed(stamped: &mut Stamped, stream: impl AsRef<[u8]>, size: usize) -> String {
        let mut text = String::new();
        for piece in stream.as_ref().chunks(size) {
            text.extend(stamped.feed(piece));
        }
        text
    }

    fn stream(lines: &[(&str, &str)]) -> String {
        lines
            .iter()
            .map(|(at, line)| format!("{at} {line}\n"))
            .collect()
    }

    /// Each line's time is taken off, however the stream is cut: inside a
    /// time, at its space, inside a line.
    #[test]
    fn the_time_is_taken_off_each_line_wherever_the_stream_is_cut() {
        let s = stream(&[(T1, "one"), (T2, "two words"), (T3, ""), (T3, "four")]);
        for size in 1..=s.len() {
            let mut stamped = Stamped::default();
            let mut text = fed(&mut stamped, &s, size);
            text.extend(stamped.rest());
            assert_eq!(text, "one\ntwo words\n\nfour\n", "in pieces of {size}");
        }
    }

    /// A line is handed on once it ends; one longer than is held, as it
    /// comes but for its last part, cut on a character's boundary; and a last
    /// one with no newline at the end.
    #[test]
    fn a_line_is_handed_on_whole_or_once_it_is_long() {
        let mut stamped = Stamped::default();
        assert_eq!(stamped.feed(format!("{T1} begun").as_bytes()), None);
        assert_eq!(
            stamped.feed(b" and ended\n").as_deref(),
            Some("begun and ended\n")
        );
        // Its last part, kept, begins inside the character: it is kept whole.
        let before = WholeLines::HELD - KEPT;
        let kept = format!("\u{e9}{}", "a".repeat(KEPT - 1));
        let long = format!("{}{kept}", "a".repeat(before));
        let fed = stamped.feed(format!("{T2} {long}").as_bytes()).unwrap();
        assert_eq!(fed, "a".repeat(before), "all but its last part");
        assert_eq!(stamped.feed(b" end"), None);
        assert_eq!(stamped.rest(), Some(format!("{kept} end")));
        assert_eq!(stamped.rest(), None);
    }

    /// A time with an offset is the instant it names: a line written at the
    /// same instant in another zone is the same.
    #[test]
    fn a_time_with_an_offset_is_its_instant() {
        let mut stamped = Stamped::default();
        fed(&mut stamped, "2026-10-21T18:00:00.000000000+08:00 a\n", 7);
        let at: Timestamp = "2026-10-21T10:00:00Z".parse().unwrap();
        assert_eq!(stamped.last.unwrap().at, at);
    }

    /// Opened again from the second before the last line begun, a whole
    /// second: the kubelet rounds a time it is given.
    #[test]
    fn a_follow_is_opened_again_from_the_second_before_its_last_line() {
        let mut stamped = Stamped::default();
        assert_eq!(stamped.resume_from(), None, "nothing read: from the start");
        fed(&mut stamped, stream(&[(T1, "a"), (T2, "b")]), 100);
        let from = stamped.resume_from().unwrap();
        assert_eq!(from, "2026-10-21T09:59:59Z".parse::<Timestamp>().unwrap());
    }

    /// A line of the other stream written after the last read, timed a
    /// moment before it in the second before, comes again, and is read.
    #[test]
    fn a_line_timed_just_before_the_second_of_the_last_read_is_read() {
        let first = "2026-10-21T09:59:58.500000000Z";
        let before = "2026-10-21T09:59:59.999900000Z";
        let after = "2026-10-21T10:00:00.000100000Z";
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, stream(&[(first, "a"), (after, "b")]), 6);
        stamped.ended(false);
        let from = stamped.resume_from().unwrap();
        // The kubelet hands over every line from then.
        let lines = [(first, "a"), (after, "b"), (before, "c")];
        let again: Vec<_> = lines
            .into_iter()
            .filter(|(at, _)| at.parse::<Timestamp>().unwrap() >= from)
            .collect();
        text.push_str(&fed(&mut stamped, stream(&again), 5));
        assert_eq!(text, "a\nb\nc\n");
    }

    /// What a follow opened again hands over first was read already: the
    /// lines before the last are passed over, and so is the last where it
    /// had ended. Each line is in the log once.
    #[test]
    fn lines_read_before_a_follow_was_cut_are_not_read_again() {
        let first = stream(&[(T1, "a"), (T2, "b"), (T2, "c")]);
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, &first, 5);
        stamped.resume_from();
        // From the second's start: all three again, then what is new.
        let again = stream(&[(T1, "a"), (T2, "b"), (T2, "c"), (T2, "d"), (T3, "e")]);
        text.push_str(&fed(&mut stamped, &again, 4));
        text.extend(stamped.rest());
        assert_eq!(text, "a\nb\nc\nd\ne\n");
    }

    /// A line the follow was cut inside goes on from where it got to, a
    /// character cut in two included.
    #[test]
    fn a_line_cut_by_the_follow_goes_on_from_where_it_got_to() {
        let line = "caf\u{e9} \u{2713} and the rest";
        let whole = format!("{T1} before\n{T2} {line}\n{T3} after\n");
        let at_line = whole.find(line).unwrap();
        for cut in at_line..at_line + line.len() {
            let mut stamped = Stamped::default();
            let mut text = fed(&mut stamped, &whole.as_bytes()[..cut], 3);
            stamped.resume_from();
            text.push_str(&fed(&mut stamped, &whole, 6));
            text.extend(stamped.rest());
            assert_eq!(text, format!("before\n{line}\nafter\n"), "cut at {cut}");
        }
    }

    /// Cut anywhere, a time or its space included, and opened again, the log
    /// is each line once; and again where the follow opened again is cut
    /// anywhere too, before or after where the first was.
    #[test]
    fn a_follow_cut_anywhere_reads_each_line_once() {
        let whole = stream(&[(T1, "one"), (T2, "two"), (T2, "two again"), (T3, "three")]);
        for cut in 0..=whole.len() {
            for second in 0..=whole.len() {
                let mut stamped = Stamped::default();
                let mut text = fed(&mut stamped, &whole[..cut], 2);
                stamped.resume_from();
                text.push_str(&fed(&mut stamped, &whole[..second], 5));
                stamped.resume_from();
                text.push_str(&fed(&mut stamped, &whole, 7));
                text.extend(stamped.rest());
                assert_eq!(
                    text, "one\ntwo\ntwo again\nthree\n",
                    "cut at {cut}, then {second}"
                );
            }
        }
    }

    /// A line timed well after the last read ends what is passed over, where
    /// the kubelet no longer has the lines before it (it rotated the log
    /// meanwhile), and that is told.
    #[test]
    fn a_line_well_after_the_last_read_ends_what_is_passed_over() {
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, stream(&[(T1, "a"), (T2, "b")]), 100);
        assert!(!stamped.seam());
        stamped.resume_from();
        text.push_str(&fed(&mut stamped, stream(&[(T4, "c")]), 100));
        assert_eq!(text, "a\nb\nc\n");
        assert!(stamped.seam(), "the rotation is told");
        assert!(!stamped.seam(), "once");
    }

    /// What was read before is passed over by where it is in the file: a
    /// line written after the last read, timed a moment before it (the other
    /// stream's), is read; and one written before it, timed a moment after,
    /// is not read twice.
    #[test]
    fn lines_are_passed_over_by_where_they_are_not_by_their_time() {
        let early = "2026-10-21T10:00:00.150000000Z";
        let late = "2026-10-21T10:00:00.250000000Z";
        let mut stamped = Stamped::default();
        let mut text = fed(
            &mut stamped,
            stream(&[(T1, "a"), (late, "out of order"), (T2, "b")]),
            3,
        );
        stamped.resume_from();
        let again = stream(&[
            (T1, "a"),
            (late, "out of order"),
            (T2, "b"),
            (early, "c"),
            (T3, "d"),
        ]);
        text.push_str(&fed(&mut stamped, &again, 4));
        assert_eq!(text, "a\nout of order\nb\nc\nd\n");
        assert!(!stamped.seam());
    }

    /// Lines of the same time with another's between them are each known by
    /// how many of that time came before: the last read is found.
    #[test]
    fn lines_of_one_time_apart_are_counted_together() {
        let other = "2026-10-21T10:00:00.050000000Z";
        let lines = [(T1, "a"), (other, "b"), (T1, "c")];
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, stream(&lines), 3);
        assert_eq!(stamped.last.unwrap().nth, 2);
        stamped.resume_from();
        let mut again = lines.to_vec();
        again.push((T3, "d"));
        text.push_str(&fed(&mut stamped, stream(&again), 5));
        assert_eq!(text, "a\nb\nc\nd\n");
    }

    /// The first thing a follow opened again hands over can be the end of a
    /// line begun before the second it was opened from, with no time: it was
    /// read before, and is not read again.
    #[test]
    fn the_end_of_a_line_begun_before_a_follow_was_opened_again_is_not_read_again() {
        let before = "2026-10-21T09:59:59.900000000Z";
        let mut stamped = Stamped::default();
        let long = format!(
            "{before} {}{}\n{T2} saved\n",
            "x".repeat(100),
            "y".repeat(50)
        );
        let mut text = fed(&mut stamped, &long, 7);
        stamped.resume_from();
        let again = format!("{}\n{T2} saved\n{T3} new\n", "y".repeat(50));
        text.push_str(&fed(&mut stamped, &again, 9));
        assert_eq!(
            text,
            format!("{}{}\nsaved\nnew\n", "x".repeat(100), "y".repeat(50))
        );
        assert!(
            !stamped.foreign(),
            "the end of a line read before is not foreign"
        );
    }

    /// The kubelet's own words in a follow, with no time, are not the task's:
    /// they are not in the log, and the follow's end is a cut. Before the
    /// task's first line too, follow after follow.
    #[test]
    fn the_kubelet_s_own_words_are_not_in_the_log() {
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, format!("{T1} a\n{WORDS}"), 5);
        text.extend(stamped.rest());
        assert_eq!(text, "a\n");
        assert!(stamped.foreign());
        assert!(!stamped.foreign(), "said once");

        let mut stamped = Stamped::default();
        let mut text = String::new();
        for _ in 0..3 {
            stamped.resume_from();
            text.push_str(&fed(&mut stamped, WORDS, 6));
            assert!(stamped.foreign(), "each follow that ends so");
        }
        stamped.resume_from();
        text.push_str(&fed(&mut stamped, stream(&[(T1, "first")]), 4));
        text.extend(stamped.rest());
        assert_eq!(text, "first\n");
        assert!(!stamped.foreign());
    }

    /// The kubelet's words at the end of a part of a line, which is how a
    /// follow it cannot go on with ends where the task was writing a long
    /// one, are let go once the follow opened again hands the line over:
    /// what it hands over is the same as what was held, as far as the words.
    #[test]
    fn the_kubelet_s_words_inside_a_line_are_let_go_when_it_comes_again() {
        let part = "b".repeat(40);
        for rest in ["c".repeat(30), String::new()] {
            let mut stamped = Stamped::default();
            let mut text = fed(&mut stamped, format!("{T1} a\n{T2} {part}{WORDS}"), 9);
            assert_eq!(text, "a\n", "the part waits for its line's end");
            stamped.resume_from();
            let again = format!("{T1} a\n{T2} {part}{rest}\n{T3} after\n");
            text.push_str(&fed(&mut stamped, &again, 11));
            text.extend(stamped.rest());
            assert_eq!(text, format!("a\n{part}{rest}\nafter\n"));
        }
        // And where the log ends there, with no newline: what the last read
        // handed over, and not the words.
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, format!("{T1} a\n{T2} {part}{WORDS}"), 9);
        stamped.resume_from();
        text.push_str(&fed(&mut stamped, format!("{T1} a\n{T2} {part}"), 11));
        text.extend(stamped.rest());
        assert_eq!(text, format!("a\n{part}"));
    }

    /// A line handed on as it comes for its length keeps its last part held:
    /// the kubelet's words that end a part of it are let go when it comes
    /// again, and what comes after them is read, none of it passed over.
    #[test]
    fn the_kubelet_s_words_after_a_long_part_are_let_go() {
        let part = "b".repeat(WholeLines::HELD);
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, format!("{T1} {part}{WORDS}"), 4096);
        assert!(text.len() < part.len(), "its end is held");
        stamped.resume_from();
        let more = "c".repeat(100);
        text.push_str(&fed(&mut stamped, format!("{T1} {part}{more}\n"), 4096));
        text.extend(stamped.rest());
        assert_eq!(text, format!("{part}{more}\n"));
    }

    /// The first thing a follow opened again hands over, with no time, is
    /// the end of a line read before, which ends: one that is still going
    /// when the stream ends is the kubelet's words.
    #[test]
    fn a_stream_that_ends_inside_its_first_untimed_part_is_told() {
        let mut stamped = Stamped::default();
        fed(&mut stamped, stream(&[(T1, "a")]), 9);
        stamped.resume_from();
        fed(
            &mut stamped,
            "unable to retrieve container logs for containerd://0",
            9,
        );
        assert!(stamped.in_tail());
        assert!(!stamped.foreign());
        stamped.resume_from();
        assert!(!stamped.in_tail(), "a new stream");
        fed(&mut stamped, format!("tail\n{T1} a\n"), 9);
        assert!(!stamped.in_tail(), "a tail that ended");
    }

    /// Where the kubelet rotated the log, and the last line read before was
    /// being written across the two files, what was held of it goes on as it
    /// is, and the log opened again inside the line after it reads on from
    /// where it got to, its bytes counted from that line's start.
    #[test]
    fn a_line_cut_across_a_rotation_is_read_on_after_it() {
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, format!("{T1} a\n{T2} xxxx"), 3);
        stamped.resume_from();
        // The current file: the line's rest, timed by the kubelet as a line.
        let current = format!("{T4} yyyyzz\n{T4} after\n");
        text.push_str(&fed(
            &mut stamped,
            &current[..current.find("zz").unwrap()],
            2,
        ));
        assert!(stamped.seam());
        stamped.resume_from();
        text.push_str(&fed(&mut stamped, &current, 5));
        text.extend(stamped.rest());
        assert_eq!(text, "a\nxxxxyyyyzz\nafter\n");
    }

    /// Where a follow ended inside a line with the kubelet's words, and the
    /// log opened again no longer has the line (it was rotated), the end of
    /// what was held of it goes, the words with it.
    #[test]
    fn the_kubelet_s_words_held_across_a_rotation_go() {
        let mut stamped = Stamped::default();
        let part = "x".repeat(KEPT * 2);
        let mut text = fed(&mut stamped, format!("{T1} a\n{T2} {part}{WORDS}"), 64);
        stamped.ended(true);
        stamped.resume_from();
        text.push_str(&fed(&mut stamped, format!("{T4} after\n"), 5));
        assert!(stamped.seam());
        assert!(!text.contains(WORDS), "{text}");
        assert!(text.starts_with(&format!("a\n{}", "x".repeat(KEPT))));
        assert!(text.ends_with("after\n"));
    }

    /// On CRI-O the lines of one read of the container's output all have
    /// one time. Where the kubelet rotated the log inside a line, the rest of
    /// it, at the top of the file it serves, has the time of the lines after
    /// it: the last line read before is known among them by its length too,
    /// and is not read twice.
    #[test]
    fn the_last_line_read_is_known_by_its_length_among_lines_of_its_time() {
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, format!("{T1} a\n{T2} xxxx"), 7);
        // One follow across the rotation: the rest has no time in it.
        text.push_str(&fed(
            &mut stamped,
            format!("yy\n{T3} Tests: 120 passed\n"),
            7,
        ));
        stamped.ended(true);
        // The read once the pod is over: the current file, from its top.
        stamped.resume_from();
        text.push_str(&fed(
            &mut stamped,
            stream(&[(T3, "yy"), (T3, "Tests: 120 passed")]),
            5,
        ));
        text.extend(stamped.rest());
        assert_eq!(text, "a\nxxxxyy\nTests: 120 passed\n");
        assert!(!stamped.lost_in_a_cut());
    }

    /// A read at a deadline that is cut while it hands over again the line
    /// being read keeps what was held of it: it is not taken for the log's
    /// end.
    #[test]
    fn a_line_handed_over_again_in_part_is_kept_whole_where_the_read_was_cut() {
        let line = "x".repeat(2_000);
        let mut stamped = Stamped::default();
        let mut text = fed(&mut stamped, format!("{T1} a\n{T2} {line}"), 512);
        stamped.resume_from();
        let again = format!("{T1} a\n{T2} {line}");
        text.push_str(&fed(&mut stamped, &again[..again.len() - 1_000], 512));
        stamped.cut();
        text.extend(stamped.rest());
        assert_eq!(text, format!("a\n{line}"));
    }

    /// A follow cut inside a line, and the log opened again handing over
    /// nothing but the kubelet's words: what was held of the line was never
    /// near them, and is kept.
    #[test]
    fn a_line_cut_by_the_network_is_kept_where_only_words_come_after() {
        let mut stamped = Stamped::default();
        let mut text = fed(
            &mut stamped,
            format!("{T1} a\n{T2} step 3: waiting for db"),
            7,
        );
        stamped.ended(false);
        stamped.resume_from();
        text.push_str(&fed(
            &mut stamped,
            "unable to retrieve container logs for containerd://0",
            9,
        ));
        stamped.ended(true);
        assert!(stamped.in_tail());
        text.extend(stamped.rest());
        assert_eq!(text, "a\nstep 3: waiting for db");
    }

    /// A stream that ends cleanly with what it had to pass over still to pass
    /// over: the kubelet no longer has the last line read.
    #[test]
    fn a_log_that_no_longer_has_the_last_line_read_is_told() {
        let mut stamped = Stamped::default();
        fed(&mut stamped, stream(&[(T1, "a")]), 9);
        // Opened again with no cut (the read once the pod is over): a file
        // rotated after the last line read is not a loss.
        stamped.resume_from();
        assert_eq!(fed(&mut stamped, "", 9), "");
        assert!(!stamped.lost_in_a_cut());
        // After a cut it is.
        stamped.ended(false);
        stamped.resume_from();
        let text = fed(&mut stamped, stream(&[(T2, "b")]), 9);
        assert_eq!(text, "", "within a second of it: taken for one read before");
        assert!(stamped.lost_in_a_cut());
    }

    /// Lines that each have a time of their own, as many as a busy task
    /// writes, are counted as they come, not each against all the others.
    #[test]
    fn many_lines_are_counted_in_their_time() {
        let mut stamped = Stamped::default();
        let base: Timestamp = T1.parse().unwrap();
        let mut log = String::new();
        for n in 0..100_000i64 {
            let at = base
                .checked_add(k8s_openapi::jiff::SignedDuration::from_micros(n * 10))
                .unwrap();
            log.push_str(&format!("{at} line {n}\n"));
        }
        let began = std::time::Instant::now();
        let text = fed(&mut stamped, &log, 8 * 1024);
        assert_eq!(text.lines().count(), 100_000);
        assert!(
            began.elapsed() < Duration::from_secs(20),
            "{:?} for 100,000 lines",
            began.elapsed()
        );
    }

    /// With the kubelet's words, cut anywhere and opened again, each line is
    /// in the log once and the words in none.
    #[test]
    fn a_follow_with_the_kubelet_s_words_cut_anywhere_reads_each_line_once() {
        let lines = [
            (T1, "one"),
            (T2, "a line long enough to be cut in"),
            (T3, "three"),
        ];
        let whole = stream(&lines);
        let at_two = whole.find("a line").unwrap();
        for cut in at_two..whole.len() {
            let mut stamped = Stamped::default();
            let mut text = fed(&mut stamped, format!("{}{WORDS}", &whole[..cut]), 4);
            stamped.foreign();
            stamped.resume_from();
            text.push_str(&fed(&mut stamped, &whole, 6));
            text.extend(stamped.rest());
            assert_eq!(
                text, "one\na line long enough to be cut in\nthree\n",
                "cut at {cut}"
            );
        }
    }
}
