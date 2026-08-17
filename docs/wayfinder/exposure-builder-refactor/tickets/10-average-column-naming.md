---
id: 10
title: "Do the identically-named average columns need renaming or only documentation?"
type: grilling
status: open
assignee:
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
