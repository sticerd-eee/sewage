---
id: 04
title: "Land the event-based study-period branch"
type: task
status: open
assignee:
blocked-by: []
---

## Question

Branch `jo/cross-section-individual-edm` (event-based study-period split:
`study_period` from clipped events, `study_period_ea` from Annual Returns,
shared evidence grid, source-comparison verification script) must merge to
main before the plan locks, because the plan is written against the
post-merge state of `cross_section_study_period_utils.R` and the split
builders.

This is Jacopo's call: review the branch, open or finish its PR, merge it,
and confirm the two study-period datasets regenerate. The resolution records
the merge commit and anything that changed during review that the plan
should know about.
