---
id: 11
title: "Sign off the run-2 spec as launch-ready"
type: grilling
status: closed
assignee: jacopoolivieri
resolved: 2026-07-09
blocked-by: [08]
---

## Question

Jacopo reads `docs/plans/2026-07-09-001-research-ideation-funnel-v2.md` and either
signs it off as launch-ready — which completes this map — or requests changes,
which are applied and re-confirmed in the same session. Two spec-level defaults
were added during assembly and deserve an explicit look (details in the
[assembly ticket's resolution](08-assemble-lock-spec.md)):

1. The quarantine also moves this planning trail
   (`docs/wayfinder/ideation-funnel-v2/`) out of the repo for the duration of the
   run, returning it in the restore commit.
2. Refiners may not run data probes (run 1 allowed it; the later probe-protocol
   decision restricts probing to the two feasibility roles, and the spec follows
   the stricter rule).

Also worth confirming: the concrete quarantine path
(`/Users/jacopoolivieri/sewage-run1-quarantine`) and the run branch name
(`jo/ideation-run2`).

## Resolution (2026-07-09)

Jacopo signed the spec off as launch-ready. All four review points were put to
him individually and confirmed without changes:

1. The quarantine also moves this planning trail
   (`docs/wayfinder/ideation-funnel-v2/`) to the holding folder for the duration
   of the run, returning it in the restore commit — confirmed.
2. Refiners run no data probes; probing stays exclusive to the feasibility
   engineer and the feasibility refuter per the locked probe protocol —
   confirmed.
3. The quarantine holding path is `/Users/jacopoolivieri/sewage-run1-quarantine`
   — confirmed.
4. The run branch is `jo/ideation-run2` — confirmed.

No changes were requested. The spec at
`docs/plans/2026-07-09-001-research-ideation-funnel-v2.md` is final, and this
sign-off completes the map: the run itself is launched from the spec in a fresh
session, per the out-of-scope boundary.
