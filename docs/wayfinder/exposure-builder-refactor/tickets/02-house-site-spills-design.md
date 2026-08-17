---
id: 02
title: "Design house_site_spills / rental_site_spills"
type: grilling
status: open
assignee:
blocked-by: [01]
---

## Question

What exactly are `house_site_spills` and `rental_site_spills`? To settle:

1. **Columns.** The raw measures (clipped `spill_hrs`, 12/24 `spill_count`),
   `distance_m`, `n_days_in_window`, and which evidence columns
   (`site_missing`, event-evidence verdict, EA-evidence verdict,
   `annual_returns_na_then_absent`) ride on each row, at what grain
   (transaction–Site Group, with or without the radius replication).
2. **Edge rows.** How transactions with zero nearby sites and
   coordinate-ineligible transactions are represented — sentinel rows in the
   artifact versus rejoining the transaction ledger at derivation time.
3. **Location and partitioning.** Where the artifacts live under
   `data/processed/`, and how they are partitioned.
4. **Publication machinery.** Whether the internal artifacts publish through
   the full staged-validation machinery (`dataset_publication_utils.R`) like
   public datasets, or through a lighter gate, and what their validation
   contract checks.
5. **Windows.** Confirm the artifact carries the prior-family per-transaction
   window only (the study family needs no transaction-grain layer because its
   site totals are transaction-independent), or whether a study-window
   variant earns its place.
