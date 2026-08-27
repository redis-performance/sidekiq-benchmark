# Cross-cutting nitpick taxonomy — sidekiq-benchmark, real precedent only

Grounded in actual GitHub history (11 PRs, 3 issues, 2 contributors, as of
August 2026) and this repo's own `AGENTS.md`/`CONTRIBUTING.md` on
`redis-performance/sidekiq-benchmark`. This is a much smaller record than a
project like redisbench-admin or memtier_benchmark — several categories
below are evidenced by a *single* real PR or issue. That is an honest
reflection of the actual size of the record, not a weakness to paper over.
Do not treat one citation as settled, oft-repeated doctrine.

1. **A flag's default value living in more than one place invites a real
   "which layer owns this?" bug.** The one substantive review disagreement
   in this repo's history: PR#5 added `--payload-size`, and a genuine
   ambiguity arose over whether `0` meant "empty payload" or "keep the
   legacy `\"string\"` placeholder" — Copilot read the helper function
   (`build_arg0`) as the source of truth and flagged a mismatch; paulorsousa
   corrected this multiple times: the CLI layer supplies the default (`6`)
   before the helper ever sees an unset value, so the helper being "wrong"
   about 0 was never actually reachable in practice. Any PR that introduces
   a new flag with a non-trivial default, where the default could plausibly
   be applied in more than one function, should be checked for exactly
   this: is there one unambiguous place that owns "what happens if this
   flag is omitted," and is it documented at that one place?

2. **A benchmark's own quantitative claim needs every changed variable
   isolated before a cause is credited.** Real, self-corrected precedent:
   PR#3 claimed a ~4x throughput improvement attributed to removing a
   contended `Mutex<Histogram>` from the worker hot path. PR#4 later showed
   this was wrong — the actual PR#3 benchmark run had *also* switched build
   environments (musl/alpine → glibc), and the true fix was adding
   `mimalloc` as the global allocator; PR#3's architectural change was
   perf-neutral once the allocator was controlled for, and PR#4 reverted it.
   This is genuine, real precedent (not hypothetical) for asking, on any PR
   with a new before/after benchmark table crediting an architecture or code
   change: what else differed between the "before" and "after" runs besides
   the diff itself (toolchain, allocator, build target, hardware, trial
   count)? A single-trial table with no repeated runs (as both PR#3's and
   PR#4's own tables candidly show for their low-worker rows, self-labeled
   "noise") is worth naming as such, following this project's own practice
   of labeling its own noisy data points rather than hiding them.

3. **Silent, wire-invisible connection/URL bugs have real, repeated
   precedent here.** Two independent, real, merged fixes:
   - PR#11 (fixing issue #10): `--db` was silently ignored because the
     code's "does the URL already have a path?" check could never be false
     against the tool's own default URL — no error, just a wrong `SELECT`
     visible only via `MONITOR`.
   - PR#7: `build_redis_url`'s own error-handling paths interpolated the raw
     `--url` (including any embedded password) into error text, a real
     security fix (credential leak on a malformed-URL error path).
   Both bugs produced no warning and no crash, only silently wrong or unsafe
   behavior — this project's real recurring failure mode in its connection-
   construction code. Any PR touching `build_redis_url`, CLI defaults for
   connection options, or error formatting around a `--url`/`--password`
   value deserves a manual trace of every path, not just a happy-path local
   test against the tool's own default target.

4. **A public function that indexes/divides by a caller-controlled
   collection's length is worth a bounds check, or an explicit, named
   invariant if not.** Copilot's real, valid catch on PR#5:
   `bulk_enqueue`'s per-job queue selection would panic (modulo/division by
   zero) if `queues` were ever empty. paulorsousa's real response is the
   template for handling a genuine-but-currently-unreachable risk correctly:
   agree it's real, name the specific call site that currently prevents it
   (`make_queue_names`'s invariant that queue lists are never empty), and
   defer rather than block — but note it, the way he did, rather than
   silently letting it go unrecorded.

5. **Binary portability is a correctness property here, not a nice-to-have,
   and its failures are invisible by default.** Real precedent: issue #8 /
   PR#9. The original glibc-linked release binary required `GLIBC_2.39`, so
   it silently failed to *load* (not run incorrectly — failed to start at
   all) on Ubuntu 22.04, Debian 12, and RHEL 9, and a harness that checks
   `--version` to verify an install couldn't distinguish "wrong version"
   from "won't load," so five benchmark suites silently **skipped** while
   CI stayed green. The fix (static musl targets, plus a CI assertion that
   the artifact carries no `GLIBC_*` symbols) is real precedent that this
   tool's core purpose — being copied onto arbitrary hosts under test —
   makes portability regressions a shipped-bug class, not a hypothetical.

6. **Submodule discipline is written doctrine with one real example of
   correct practice.** `AGENTS.md`, verbatim: *"Do not modify the
   `sidekiq-rs/` submodule directory directly; changes to the submodule
   must go through the upstream fork at
   `https://github.com/redis-performance/sidekiq-rs`."* PR#6 is a real
   example of the pattern working as intended: its submodule pin points at
   an in-progress branch on the fork, and the PR body states the dependency
   explicitly ("Depends on redis-performance/sidekiq-rs#4 landing first").
   Flag any PR editing files directly under `sidekiq-rs/` in this repo.

7. **Test coverage is real written doctrine; this repo's own sample is too
   small to say how strictly it's enforced in practice.** `CONTRIBUTING.md`:
   *"All new behaviour must be covered by tests... Coverage should not
   decrease."* The one real, detailed data point (PR#7: 36 unit + 12
   integration tests, including one asserting a password never reaches
   stdout/stderr even on a failed connection) substantially exceeds the
   written bar — but with only 11 PRs total and no visible Codecov-style
   percentage gate in this repo's CI, there isn't enough of a record here to
   say, the way a larger sibling repo's history can, whether coverage is a
   literal hard gate or routinely waived. State the written rule and
   whether the PR under review meets it; don't claim a track record either
   way this repo's own history doesn't support.

8. **An unreviewed, months-old open PR is not itself evidence of a problem
   with that PR.** PR#6 has sat open since June 2, 2026 with zero recorded
   review activity. If asked to review it (or a PR like it), don't read the
   review gap itself as a red flag about the PR's quality — this repo's real
   history shows routine PRs going unreviewed for a long time is normal here
   given the two-person contributor base, not a signal about the diff.

## What this taxonomy is honestly thin or silent on

- **A dense, multi-round dialectic review culture.** This repo has exactly
  one substantive review thread (PR#5) in its entire recorded history. Don't
  imply there's more precedent for back-and-forth review than that one
  thread.
- **Any real fcostaoliveira reviewer-voice quote.** He is the author of most
  PRs here; the only recorded instance of him reviewing someone else's work
  is a bare, empty-body approval (PR#2). There is no real quote to draw a
  richer "how fcostaoliveira reviews" profile from.
- **Backward-compatible CLI/output-format change precedent.** No PR in this
  repo's mined history changed an existing flag's meaning or an existing
  JSON output key's shape in a way that drew explicit reviewer comment on
  compatibility trade-offs. If a PR under review does this, say plainly that
  this repo's own history doesn't give a citable precedent, and reason about
  the tradeoff on its own merits.
- **Stray/accidental committed files, dead-code call-outs.** `CONTRIBUTING.md`
  states "No dead code, no commented-out blocks" as written doctrine, but no
  real reviewer comment in this repo's sample calls this out in practice —
  apply the written rule, but don't cite a maintainer quote that doesn't
  exist.
- **Buffer sizing / raw memory-safety nitpicks in the C sense.** This is a
  Rust codebase; don't import memtier_benchmark's C-string/`snprintf`
  category here. The one real memory-safety-adjacent finding in this repo's
  history (item 4 above, the empty-slice panic risk) is a Rust
  panic-on-invalid-input concern, not a buffer-overflow one.
