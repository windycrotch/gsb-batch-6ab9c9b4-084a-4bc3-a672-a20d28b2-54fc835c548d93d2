//! Speculative multi-candidate probing with fully isolated trial contexts.
//!
//! [`probe`] runs each candidate parser against its own fresh
//! [`FileAnalyze`] over the same read-only buffer. The first candidate to
//! report a match wins; every losing trial is discarded wholesale — cursor
//! position, truncation flag, stream fields, and access counters from a
//! trial can never leak into later candidates or into the final result.
//!
//! Once a winner is found it is **re-run from a clean state** (offset 0,
//! untruncated, empty stream collection) and the returned result comes
//! exclusively from that re-run:
//!
//! - [`ProbeMatch::streams`] holds exactly the fields the winner's re-run
//!   produced — including partial fields when the buffer ran out mid-parse.
//! - [`ProbeMatch::incomplete`] is set only when the *winner's re-run* hit
//!   a truncated read. Overruns during losing trials merely eliminate those
//!   candidates; they never mark the final result incomplete.
//! - [`ProbeMatch::access_stats`] counts only the winner's re-run. Trial
//!   overhead is not included.
//!
//! Probing is read-only with respect to the input buffer, so any number of
//! probes may run concurrently against the same bytes, and repeating a
//! probe on the same input yields the same winner and the same fields.

use revelo_core::{AccessStats, FileAnalyze, StreamCollection};

/// A candidate parser — the same signature used by the dispatch [`table`](crate::table).
pub type ParserFn = fn(&mut FileAnalyze) -> bool;

/// Outcome of a [`probe`] run.
#[derive(Debug)]
pub enum ProbeOutcome {
    /// No candidate recognized the buffer (or the candidate list or buffer
    /// was empty). Nothing any candidate wrote survives — callers observe a
    /// clean "no match" with no residual state.
    NoMatch,
    /// A candidate matched and was re-run from a clean state.
    Match(Box<ProbeMatch>),
}

/// Full result of the winning parser's clean re-run.
#[derive(Debug)]
pub struct ProbeMatch {
    /// Index of the winning candidate in the list passed to [`probe`].
    pub candidate_index: usize,
    /// The winning parser.
    pub parser: ParserFn,
    /// Fields produced by the winner's re-run. When [`Self::incomplete`] is
    /// true this holds the partial fields collected before the buffer ran
    /// out, so callers can still display partial metadata.
    pub streams: StreamCollection,
    /// True when the winner's re-run hit a truncated (out-of-bounds) read.
    /// Never set because of overruns in losing trial runs.
    pub incomplete: bool,
    /// Access statistics (request counts, requested bytes, max single
    /// request length) for the winner's re-run only.
    pub access_stats: AccessStats,
}

/// Probe `bytes` with `candidates`, returning the winner and its complete
/// parse result.
///
/// Each candidate runs in an isolated [`FileAnalyze`]; the first one to
/// return `true` wins and is re-run from offset 0 with an untruncated,
/// empty stream collection. Returns [`ProbeOutcome::NoMatch`] when the
/// candidate list or the buffer is empty, or when every candidate declines.
pub fn probe(bytes: &[u8], candidates: &[ParserFn]) -> ProbeOutcome {
    if bytes.is_empty() || candidates.is_empty() {
        return ProbeOutcome::NoMatch;
    }

    // Trial phase: each candidate gets a fresh, isolated read context over
    // the shared buffer. Nothing from a trial — cursor, truncation flag,
    // stream fields, access counts — escapes this loop.
    let mut winner = None;
    for (index, &parser) in candidates.iter().enumerate() {
        let mut trial = FileAnalyze::new(bytes);
        if parser(&mut trial) {
            winner = Some((index, parser));
            break;
        }
    }
    let Some((candidate_index, parser)) = winner else {
        return ProbeOutcome::NoMatch;
    };

    // Re-run phase: the winner parses again from a pristine state. The
    // final result — fields, truncation flag, access stats — comes only
    // from this run.
    let mut fa = FileAnalyze::new(bytes);
    parser(&mut fa);
    ProbeOutcome::Match(Box::new(ProbeMatch {
        candidate_index,
        parser,
        streams: fa.streams().clone(),
        incomplete: fa.truncated(),
        access_stats: fa.access_stats(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use revelo_core::StreamKind;

    /// Buffer layout for the test parsers below:
    /// `"RVLO" | version:u16be | payload...`
    const BUF: &[u8] = b"RVLO\x00\x2A\xDE\xAD\xBE\xEF";

    /// The "real" parser: matches the RVLO magic and fills two fields.
    fn rvlo_parser(fa: &mut FileAnalyze) -> bool {
        if !fa.peek_magic(b"RVLO") {
            return false;
        }
        fa.skip_b4("Magic");
        let version = fa.get_b2("Version");
        // Raw access so the re-run's access stats are observably non-zero.
        let _ = fa.peek_raw(4);
        let pos = fa.stream_prepare(StreamKind::General);
        fa.set_field(StreamKind::General, pos, "Format", "RVLO");
        fa.set_field(StreamKind::General, pos, "Version", version.to_string());
        true
    }

    /// A losing candidate that dirties everything it can before declining:
    /// advances the cursor, sets the truncation flag, writes stream fields,
    /// and performs raw accesses.
    fn messy_loser(fa: &mut FileAnalyze) -> bool {
        let pos = fa.stream_prepare(StreamKind::General);
        fa.set_field(StreamKind::General, pos, "Format", "NOT-ME");
        fa.set_field(StreamKind::General, pos, "Leak", "trial-residue");
        let _ = fa.read_raw(1024); // overruns: truncated = true, cursor at EOF
        false
    }

    /// A quiet loser that declines without touching anything.
    fn quiet_loser(_fa: &mut FileAnalyze) -> bool {
        false
    }

    fn matched(outcome: ProbeOutcome) -> ProbeMatch {
        match outcome {
            ProbeOutcome::Match(m) => *m,
            ProbeOutcome::NoMatch => panic!("expected a match"),
        }
    }

    #[test]
    fn single_candidate_hit_returns_full_result() {
        let m = matched(probe(BUF, &[rvlo_parser]));
        assert_eq!(m.candidate_index, 0);
        assert!(!m.incomplete);
        assert_eq!(
            m.streams.retrieve(StreamKind::General, 0, "Format").map(|z| z.as_str()),
            Some("RVLO")
        );
        assert_eq!(
            m.streams.retrieve(StreamKind::General, 0, "Version").map(|z| z.as_str()),
            Some("42")
        );
        // Access stats come from the winner's re-run: one peek_raw(4).
        assert_eq!(m.access_stats.peek_raw_calls, 1);
        assert_eq!(m.access_stats.bytes_requested, 4);
        assert_eq!(m.access_stats.max_request_len, 4);
    }

    #[test]
    fn loser_state_does_not_leak_into_winner_rerun() {
        // The messy loser runs first, dirties its context, and declines.
        // The winner's fields must be exactly what a clean solo run yields.
        let m = matched(probe(BUF, &[messy_loser, quiet_loser, rvlo_parser]));
        assert_eq!(m.candidate_index, 2);
        assert!(!m.incomplete, "loser's overrun must not mark the result incomplete");
        assert_eq!(m.streams.stream_count(StreamKind::General), 1, "loser's stream leaked");
        assert_eq!(
            m.streams.retrieve(StreamKind::General, 0, "Format").map(|z| z.as_str()),
            Some("RVLO")
        );
        assert!(
            m.streams.retrieve(StreamKind::General, 0, "Leak").is_none(),
            "loser's field leaked into the final result"
        );
        // Stats count only the winner's re-run — the loser's read_raw(1024)
        // and peek attempts must not appear.
        assert_eq!(m.access_stats.read_raw_calls, 0);
        assert_eq!(m.access_stats.peek_raw_calls, 1);
        assert_eq!(m.access_stats.bytes_requested, 4);
        assert_eq!(m.access_stats.max_request_len, 4);
    }

    #[test]
    fn candidate_trial_overrun_only_eliminates_that_candidate() {
        // messy_loser overruns during its trial; the buffer is in fact
        // complete for the winner, so the final result is not incomplete.
        let m = matched(probe(BUF, &[messy_loser, rvlo_parser]));
        assert!(!m.incomplete);
        assert_eq!(
            m.streams.retrieve(StreamKind::General, 0, "Format").map(|z| z.as_str()),
            Some("RVLO")
        );
    }

    #[test]
    fn winner_overrun_returns_partial_fields_and_marks_incomplete() {
        // Parser claims a 4-byte magic then reads a u64 that isn't there.
        fn greedy(fa: &mut FileAnalyze) -> bool {
            if !fa.peek_magic(b"RVLO") {
                return false;
            }
            fa.skip_b4("Magic");
            let pos = fa.stream_prepare(StreamKind::General);
            fa.set_field(StreamKind::General, pos, "Format", "RVLO-Truncated");
            let _ = fa.get_b8("Missing"); // overruns a 6-byte buffer
            true
        }
        let short = b"RVLO\x00\x2A";
        let m = matched(probe(short, &[greedy]));
        assert!(m.incomplete, "winner's own overrun must mark the result incomplete");
        // Partial fields parsed before the overrun are still exposed.
        assert_eq!(
            m.streams.retrieve(StreamKind::General, 0, "Format").map(|z| z.as_str()),
            Some("RVLO-Truncated")
        );
    }

    #[test]
    fn all_candidates_fail_leaves_no_residue() {
        match probe(BUF, &[messy_loser, quiet_loser]) {
            ProbeOutcome::NoMatch => {}
            ProbeOutcome::Match(_) => panic!("expected no match"),
        }
    }

    #[test]
    fn empty_inputs_return_no_match_without_panicking() {
        assert!(matches!(probe(&[], &[rvlo_parser]), ProbeOutcome::NoMatch));
        assert!(matches!(probe(BUF, &[]), ProbeOutcome::NoMatch));
        assert!(matches!(probe(&[], &[]), ProbeOutcome::NoMatch));
    }

    #[test]
    fn concurrent_repeated_probes_are_identical() {
        fn summarize(m: &ProbeMatch) -> Vec<(String, String)> {
            let mut out: Vec<(String, String)> = m
                .streams
                .iter()
                .flat_map(|(kind, pos, stream)| {
                    stream
                        .iter()
                        .map(move |(k, v)| (format!("{kind:?}/{pos}/{k}"), v.to_string()))
                })
                .collect();
            out.sort();
            out
        }

        let candidates: &[ParserFn] = &[messy_loser, rvlo_parser];
        let reference = matched(probe(BUF, candidates));
        let expected_fields = summarize(&reference);
        let expected_stats = reference.access_stats;

        std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|_| s.spawn(|| matched(probe(BUF, candidates))))
                .collect();
            for h in handles {
                let m = h.join().expect("probe thread panicked");
                assert_eq!(m.candidate_index, reference.candidate_index);
                assert_eq!(m.incomplete, reference.incomplete);
                assert_eq!(summarize(&m), expected_fields);
                assert_eq!(m.access_stats, expected_stats);
            }
        });
    }
}
