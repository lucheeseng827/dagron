//! A task's output as whole lines, from frames that need not end where a
//! line does: what the Docker and Kubernetes executors read of a task's
//! streams is joined here before it is sent to the task's log.

/// A container's stdout and stderr, joined so that a line of one is never
/// cut in two by a line of the other.
///
/// The daemon hands the two streams over as frames, in the order it read
/// them, and a frame need not end where a line does. A frame of an attached
/// container's streams ends wherever the daemon's read of the container's
/// pipe happened to end, on Docker as on Podman; Docker's log, read back,
/// ends one short of a newline only in a line longer than its buffer
/// (16 KiB). The kubelet hands a pod's log over as one stream, which the
/// Kubernetes follow (`kube_log`) joins here as stdout. Joined as they
/// came, a part-line of stdout followed by a line of stderr is one wrong
/// line and a second half with no beginning: a job's `line 27515` read
/// `line 275`, a marker of dagron-ci's, and `15`.
///
/// So what a stream has sent of a line that has not ended waits here for its
/// end, as bytes: a character cut in two by a frame is whole again when it
/// is read as text. A line is placed where it ended. More than [`Self::HELD`]
/// of one is not waited for, and goes on as it is, up to its last whole
/// character: a runtime's own log cuts a line that long itself.
#[derive(Default)]
pub(crate) struct WholeLines {
    out: Vec<u8>,
    err: Vec<u8>,
}

impl WholeLines {
    /// The most of an unfinished line that waits for its end.
    pub(crate) const HELD: usize = 64 * 1024;

    /// A frame of stdout. The whole lines that stream now has, if any.
    pub(crate) fn stdout(&mut self, frame: &[u8]) -> Option<String> {
        Self::take(&mut self.out, frame)
    }

    /// A frame of stderr, likewise.
    pub(crate) fn stderr(&mut self, frame: &[u8]) -> Option<String> {
        Self::take(&mut self.err, frame)
    }

    fn take(held: &mut Vec<u8>, frame: &[u8]) -> Option<String> {
        held.extend_from_slice(frame);
        let ended = held
            .iter()
            .rposition(|b| *b == b'\n')
            .map_or(0, |at| at + 1);
        let upto = if held.len() - ended > Self::HELD {
            Self::whole_characters(held)
        } else {
            ended
        };
        if upto == 0 {
            return None;
        }
        let whole: Vec<u8> = held.drain(..upto).collect();
        Some(String::from_utf8_lossy(&whole).into_owned())
    }

    /// How much of `bytes` can be read as text without cutting its last
    /// character in two: all of it, or all but the bytes of a character
    /// whose others have not come yet. A character is four bytes at most, so
    /// a cut one begins in the last three. Bytes that are no text at all are
    /// not waited on for long: once more follows them they are not last.
    pub(crate) fn whole_characters(bytes: &[u8]) -> usize {
        for back in 1..=bytes.len().min(3) {
            let at = bytes.len() - back;
            let first = bytes[at];
            if first & 0b1100_0000 == 0b1000_0000 {
                // The middle of a character: its first byte is further back.
                continue;
            }
            let needs = match first {
                0xF0.. => 4,
                0xE0.. => 3,
                0xC0.. => 2,
                _ => 1,
            };
            return if needs > back { at } else { bytes.len() };
        }
        bytes.len()
    }

    /// What is left when the streams have ended: a last line that had no
    /// newline, of either. Stdout's first, and each read as text on its own:
    /// half a character at the end of one is not made whole by the other's.
    pub(crate) fn rest(&mut self) -> Option<String> {
        let out = std::mem::take(&mut self.out);
        let err = std::mem::take(&mut self.err);
        (!out.is_empty() || !err.is_empty()).then(|| {
            let mut left = String::from_utf8_lossy(&out).into_owned();
            left.push_str(&String::from_utf8_lossy(&err));
            left
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frames as a daemon hands them over, joined as the executor joins them.
    fn joined(frames: &[(bool, &[u8])]) -> String {
        let mut lines = WholeLines::default();
        let mut log = String::new();
        for (stderr, frame) in frames {
            let whole = if *stderr {
                lines.stderr(frame)
            } else {
                lines.stdout(frame)
            };
            log.extend(whole);
        }
        log.extend(lines.rest());
        log
    }

    /// What the rig saw: a read of a job's stdout ended in the middle of a
    /// line, and the lines dagron-ci's exec had written to stderr came next.
    /// The line is whole, and is where it ended.
    #[test]
    fn a_line_of_one_stream_is_not_cut_in_two_by_a_line_of_the_other() {
        let frames: &[(bool, &[u8])] = &[
            (false, b"line 27514\nline 275"),
            (true, b"::dagron-ci::step::1::end::0\n"),
            (true, b"::dagron-ci::result::{}\n"),
            (false, b"15\nline 27516\n"),
        ];
        assert_eq!(
            joined(frames),
            "line 27514\n::dagron-ci::step::1::end::0\n::dagron-ci::result::{}\n\
             line 27515\nline 27516\n"
        );
    }

    /// Frames that end with their lines are joined exactly as they came,
    /// which is all a daemon that cuts at newlines ever sends.
    #[test]
    fn frames_that_end_with_a_line_are_joined_as_they_came() {
        let frames: &[(bool, &[u8])] = &[
            (false, b"one\n"),
            (true, b"oops\n"),
            (false, b"two\nthree\n"),
            (true, b"\n"),
        ];
        assert_eq!(joined(frames), "one\noops\ntwo\nthree\n\n");
    }

    /// A character cut in two by a frame is one character, where joining
    /// each frame as text made two marks of it.
    #[test]
    fn a_character_cut_in_two_by_a_frame_is_whole() {
        let text = "caf\u{e9} \u{2713}\n".as_bytes();
        for cut in 1..text.len() {
            let frames: &[(bool, &[u8])] = &[(false, &text[..cut]), (false, &text[cut..])];
            assert_eq!(joined(frames), "caf\u{e9} \u{2713}\n", "cut at {cut}");
        }
    }

    /// A last line with no newline is kept, of both streams, stdout's first;
    /// and nothing is made up where nothing was left.
    #[test]
    fn a_last_line_with_no_newline_is_kept() {
        let frames: &[(bool, &[u8])] = &[
            (true, b"warning: "),
            (false, b"done\nno newline"),
            (true, b"at the end"),
        ];
        assert_eq!(joined(frames), "done\nno newlinewarning: at the end");
        assert_eq!(joined(&[(false, b"whole\n")]), "whole\n");
        assert_eq!(WholeLines::default().rest(), None);
    }

    /// A line passed on for its length is not cut inside a character: the
    /// bytes of one whose others have not come yet wait for them, wherever
    /// in the character the frame ended.
    #[test]
    fn a_line_passed_on_for_its_length_is_not_cut_inside_a_character() {
        for (character, bytes) in [("\u{e9}", 2), ("\u{2713}", 3), ("\u{1f600}", 4)] {
            assert_eq!(character.len(), bytes);
            for cut in 1..bytes {
                let mut lines = WholeLines::default();
                let mut first = vec![b'a'; WholeLines::HELD];
                first.extend(&character.as_bytes()[..cut]);
                let mut log = String::new();
                log.extend(lines.stdout(&first));
                assert_eq!(
                    log.len(),
                    WholeLines::HELD,
                    "{bytes} bytes cut at {cut}: the rest waits"
                );
                log.extend(lines.stdout(&character.as_bytes()[cut..]));
                log.extend(lines.stdout(b" end\n"));
                log.extend(lines.rest());
                assert_eq!(
                    log,
                    format!("{}{character} end\n", "a".repeat(WholeLines::HELD)),
                    "{bytes} bytes cut at {cut}"
                );
            }
        }
        // A whole character at the end is passed on with the rest, and so
        // are bytes that begin no character: nothing waits for what cannot
        // come.
        assert_eq!(WholeLines::whole_characters("a\u{2713}".as_bytes()), 4);
        assert_eq!(WholeLines::whole_characters(b"ab"), 2);
        assert_eq!(
            WholeLines::whole_characters(&[b'a', 0x93, 0x93, 0x93, 0x93]),
            5
        );
        assert_eq!(WholeLines::whole_characters(b""), 0);
        // A byte that could begin one waits, and goes on when more follows.
        assert_eq!(WholeLines::whole_characters(&[b'a', 0xFF]), 1);
        assert_eq!(WholeLines::whole_characters(&[b'a', 0xFF, b'b']), 3);
    }

    /// What is left of each stream at the end is read as text on its own:
    /// the first byte of a character at the end of one and the last byte of
    /// it at the end of the other are two bytes that are no text, not the
    /// character.
    #[test]
    fn what_is_left_of_each_stream_is_read_on_its_own() {
        let frames: &[(bool, &[u8])] = &[(false, &[b'o', 0xC3]), (true, &[0xA9])];
        assert_eq!(joined(frames), "o\u{fffd}\u{fffd}");
        let frames: &[(bool, &[u8])] = &[(true, b"warn"), (false, b"out")];
        assert_eq!(joined(frames), "outwarn");
    }

    /// A line is waited for up to a bound and no further: one that goes on
    /// past it is passed on as it is, so a stream with no newline at all is
    /// not held in memory until it ends.
    #[test]
    fn a_line_longer_than_what_is_held_goes_on_as_it_is() {
        let mut lines = WholeLines::default();
        let most = vec![b'a'; WholeLines::HELD];
        assert_eq!(lines.stdout(&most), None, "as much as is held, waits");
        assert_eq!(
            lines.stdout(b"a").map(|s| s.len()),
            Some(WholeLines::HELD + 1),
            "one byte more does not"
        );
        assert_eq!(lines.rest(), None);
        // What ended is passed on, and a long rest after it goes with it.
        let mut frame = b"ended\n".to_vec();
        frame.extend(vec![b'b'; WholeLines::HELD + 1]);
        assert_eq!(lines.stderr(&frame).map(|s| s.len()), Some(frame.len()));
    }
}
