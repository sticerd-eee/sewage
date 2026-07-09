---
id: 09
title: "Freeze main as-is or land the repeat-transactions rebuild first?"
type: grilling
status: closed
assignee: jacopo
blocked-by: []
---

## Question

The context delta inventory ([resolution](01-context-delta-inventory.md)) found
that the repeat-transactions rebuild is only a plan on the unmerged branch
`jo/repeat-transactions-rebuild` — on `main` the repeat IDs are still positional,
the summary stage still writes, and the rental spill lookup is still at the
temporary 5 km. Decide with Jacopo which state run 2's context pack describes:

- **Freeze main as-is.** The spec and context pack describe the current
  positional-ID repeat artifacts, flagging the rebuild as a pending proposal.
  Fastest path to launch; risk is that ideation candidates touching repeat
  transactions reason from (and any feasibility probes read) artifacts already
  known to be misaligned and slated for replacement.
- **Land the rebuild first.** Execute the locked plan in
  `docs/plans/2026-07-07-001` before the run launches, so the context pack and
  probe surface describe stable hashed IDs at 10 km. Cleaner facts; delays the
  run by the rebuild's execution time.

The answer feeds the spec-assembly ticket directly (what the context pack says
about repeat transactions, and whether the quarantine snapshot waits for a merge)
and colors the data-probe protocol's touchable-dataset list, since
`03_repeat_sales` reads the repeat artifacts directly.

## Resolution (2026-07-09)

Freeze main as-is; launch is never gated on the rebuild. Fact-finding for this
decision established that the branch `jo/repeat-transactions-rebuild` contains
only the plan document — no code has been executed — and that execution means
five chunks with a mid-way human gate, a 10 km rentals spatial join roughly four
times the current pair-table area, and regeneration of most of layer 06 plus
about twenty layer-09 consumers. Jacopo chose not to delay the run on that work.

On disclosure, Jacopo went further than either drafted option: the context pack
says **nothing** about the pending rebuild. It is a plain description of the
repeat artifacts exactly as they stand on main at the moment the quarantine
snapshot is taken, with no forward-looking engineering flags. If the rebuild
happens to have landed by snapshot time, the pack simply describes the new
state (hashed content-stable IDs at 10 km) instead — but nothing waits for it.

Consequences for downstream tickets: the spec-assembly ticket writes the repeat
entry of the context pack from main at snapshot time and adds no rebuild caveat;
the data-probe protocol treats the repeat artifacts as ordinary touchable
datasets with no special warning.
