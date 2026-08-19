---
id: 10
title: "Do the identically-named average columns need renaming or only documentation?"
type: grilling
status: closed
assignee: claude
blocked-by: []
---

## Question

The prior-family datasets and the study-period datasets both publish average
columns under identical names (`spill_count_weekly_avg` and friends), but the
quantities differ: the prior family averages over a transaction-anchored
lookback window, while the study-period family averages over the fixed study
window, and the denominators differ accordingly. Now that the event-based
study-period branch has landed (ticket 04), both families measure from the
same event basis, so the remaining collision is window and denominator only.

Should the plan rename these columns in one family (breaking the frozen
literal schemas the contract tests pin, which the charter ruled against
reopening), or keep the names and resolve the collision through
documentation — and if documentation, where does that documentation live so
an analysis author joining both datasets cannot miss it?

This is a plan decision, so it blocks ticket 06 (assemble and lock the
plan). Run through /grilling with a recommended answer attached.

## Resolution (2026-08-17)

Settled in a live grilling session with Jacopo, one question at a time. Each
decision below was put to him explicitly and confirmed.

Facts established before the round, from the code and the consumer
inventory: the collision covers four columns, not two
(`spill_count_daily_avg`, `spill_hrs_daily_avg`, `spill_count_weekly_avg`,
`spill_hrs_weekly_avg`), published by all six datasets. The formula is
identical in both families — the whole-period total divided by
`n_days_in_window`, times seven for the weekly pair — and both families
publish `n_days_in_window` itself, so every row states its own denominator.
No live consumer joins a prior-family dataset to a study-period dataset,
and on `main` no analysis script reads the study-period averages at all,
so the risk is entirely about future analysis code.

- **Keep the names; no rename in either family.** Jacopo's reasoning,
  confirmed: the differences are defined by the dataset being used —
  conceptually it is the same variable, an average over the dataset's own
  stated exposure window, which is exactly how `CONCEPTS.md` already
  defines Average Daily and Average Weekly Spill Exposure. Renaming would
  reopen the frozen literal schemas the charter ruled untouchable, force
  contract-test and consumer edits, and (if done in one family only) make
  the two families' schemas diverge on their most-used measure names
  against the shared-core story of the refactor. The self-describing
  `n_days_in_window` column is a stronger guard than a name suffix because
  it survives into every derived data frame.
- **The documentation lives in two places: `CONCEPTS.md` and a named
  subsection of the plan document.** The glossary is the durable home,
  following the ticket 05 precedent of recording reading conventions
  there; the plan document repeats the statement in its schema section
  because it is the one document guaranteed to pass in front of Jacopo
  and coauthors at sign-off. Embedding Arrow field-level metadata in the
  parquet schemas was rejected: it changes the serialized schema form the
  contract tests pin, and no R workflow surfaces it.
- **The locked `CONCEPTS.md` wording**, approved verbatim after one
  revision for plainness, to be appended to the "Average Daily Spill
  Exposure" entry when the plan executes (the "Average Weekly Spill
  Exposure" entry is unchanged):

  > All exposure datasets publish these averages under the same column
  > names: `spill_count_daily_avg`, `spill_hrs_daily_avg`,
  > `spill_count_weekly_avg`, `spill_hrs_weekly_avg`. The window differs
  > by family. The prior-to-transaction datasets average over each
  > transaction's own lookback window; the study-period datasets average
  > over the fixed study window. Each row records its window length in
  > `n_days_in_window`. When joining datasets from the two families,
  > rename these columns first.

  The prescriptive final sentence stays in the glossary deliberately: the
  cross-family join is the one operation the warning exists for, and the
  glossary already carries one prescriptive convention (the ticket 05
  NA-propagation rule).
