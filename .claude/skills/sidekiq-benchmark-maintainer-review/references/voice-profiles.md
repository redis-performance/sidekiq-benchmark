# Voice profiles — real sidekiq-benchmark contributors

Mined from actual GitHub history on `redis-performance/sidekiq-benchmark`
(`gh pr list`, `gh pr view --json comments,reviews`, and
`gh api .../pulls/<n>/comments` for inline review comments) as of August
2026: **11 PRs total, 3 issues total, 2 human contributors.** Read this
alongside `nitpick-taxonomy.md` before writing anything, and read the
honesty note below before assuming there is more voice here than there is.

## The honest baseline: this record is thinner than a typical mined-review skill

Of the 11 PRs:

- #1, #2, #9, #11 — merged same-day or within minutes, no review comments
  (or a bare empty-body `APPROVED`).
- #3, #4, #7 — merged with **zero** reviews recorded at all (in #3's case, a
  Copilot review that itself failed to run: *"Copilot encountered an error
  and was unable to review this pull request"*).
- #6 — opened June 2, 2026 by paulorsousa, **still open as of this mining**
  (`reviewDecision` empty), no review activity recorded.
- #5 — the **one** PR with a real, substantive, multi-comment inline review
  thread. This is the entire evidentiary basis for "what does a real
  sidekiq-benchmark review look like when someone engages with it."

There is no equivalent here to a project with dozens of real reviewer
quotes. Do not write as if there were.

## fcostaoliveira — Filipe Oliveira (primary maintainer, author of most PRs)

Author of 7 of the 11 PRs (#1, #2, #7, #9, #11, and both issues #8 and #10
that those last two fix). His PR descriptions are the most substantial real
writing in this repo's history — highly structured, quantitative, and
several carry a `🤖 Generated with Claude Code` footer (PR#1, #3, #4, #5),
meaning this is AI-assisted description text, not necessarily hand-written
prose. Real, recurring structure across his PRs (#7, #9, #11):

- A one-line root cause stated plainly, often with the exact code snippet
  that was wrong (PR#11: *"`build_redis_url` applied `--db` only when `--url`
  carried no path... The default `--url` is `redis://127.0.0.1:6379/13`,
  which **always** has a path, so the branch never fired."*).
- A "why this matters beyond looking cosmetic" section connecting the bug to
  real deployment targets, not just the local dev case (PR#11's table of
  which Redis topologies have db 13 available; PR#9's table of which distros
  the old glibc binary could run on).
- An explicit "verified on the wire" or "verified locally" section with
  concrete commands/output, not just "tested."
- A "Test plan" checklist as the closing section.

As a *reviewer* (not author), the only recorded instance is his empty-body
`APPROVED` on PR#2 (a Claude-Code-drafted GitHub Actions node24 bump, itself
noted in the PR body as `"Reviewed by 3 independent agents (opus/sonnet/
haiku) before opening"`). **What this means for the bot's voice**: there is
no real fcostaoliveira reviewer-voice quote to imitate beyond "silent
approval of a routine, well-described PR." Don't invent one.

## paulorsousa — Paulo Sousa (second contributor; the one real review voice)

Author of PRs #3, #4, #5, #6 (perf work and a payload-size feature). His one
substantive block of review text in this repo's history is the inline
comment thread on **PR#5** (`--payload-size` flag), reviewing a PR from the
same repo — not fcostaoliveira's — which is itself worth noting: the one
real review thread found is not the primary-maintainer-reviews-everyone-else
pattern seen in larger sibling repos, it's the second contributor reviewing
a change to their own earlier feature area.

Real quotes, verbatim, from that thread (a Copilot bot repeatedly flagged
what it read as a documentation/behavior mismatch around whether
`--payload-size 0` should mean "empty" or "the legacy `\"string\"`
placeholder"; paulorsousa corrected the same underlying misunderstanding
several times, in slightly different words, across different files):

- *"the default is `6` to match previous `\"string\"` placeholder (the docs
  put it clear)"*
- *"you're mixing layers. CLI default fills `payload_size` correctly (`6`)
  if none is passed"*
- *"default handled on the CLI layer"*
- *"default value handled on the layer above (CLI)"* (repeated near-verbatim
  three times across different files/lines)
- *"Test looks just fine. If no payload size is given, we should assume `6`
  (previous behaviour)"*

And, on a genuinely valid but out-of-scope Copilot catch (a public function
that would panic on an empty `queues` slice via modulo/division):

- *"Good comment! Not related to this PR, will add on another. Not high
  severity tho, because current exec path guarantees queues len is always
  >= 1 (see `make_queue_names`)"*

And closing warmly once a point was addressed:

- *"Makes sense! TY 🤖 😃"*

**What this means for the bot's voice**: paulorsousa's real pattern is
short, declarative, names the actual mechanism (which layer owns a default,
which call site provides an invariant) rather than restating the symptom,
and is comfortable telling an automated reviewer (Copilot) it's simply
wrong about the codebase's design rather than hedging. When he agrees a
point is valid but doesn't block on it, he says so plainly and cites the
concrete reason (an invariant, a call site) rather than a vague "not a
blocker." He does not write multi-sentence paragraphs — every quote above
is one sentence or a fragment.

## GitHub Copilot review bot — real signal, but not maintainer voice, and not always right

Runs automatically on PRs (evidenced on #3, #5) and posts both a summary and
inline comments. On PR#5 it made two real, substantive catches:

1. A genuine documentation/behavior split around what `--payload-size 0`
   means (empty string vs. the legacy `"string"` placeholder) — though its
   own framing of the fix (change the code to match its reading of the docs)
   was the wrong direction; paulorsousa's replies show the correct fix was
   understanding that the CLI layer, not the helper, owns the default.
2. A real panic risk in `bulk_enqueue` on an empty `queues` slice (modulo by
   zero) — valid, but paulorsousa correctly identified it as currently
   unreachable given `make_queue_names`'s invariant, and deferred it.

It also posted the **same** comment text multiple times across re-review
rounds on PR#5 (visible as literal duplicates in the mined thread) and, on
PR#3, failed outright (*"Copilot encountered an error and was unable to
review this pull request"*). Treat it as a real, sometimes-correct
automated signal worth not duplicating when it already caught something —
not as evidence of this project's own reviewer culture, and not a model for
how to phrase things (it writes multi-sentence explanatory paragraphs;
paulorsousa's real corrections are one line).
