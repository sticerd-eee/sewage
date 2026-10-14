---
id: 02
title: "Design house_site_spills / rental_site_spills"
type: grilling
status: closed
assignee: jacopo
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

## Resolution (2026-08-17)

Settled in a live grilling session with Jacopo. Every decision below was put
to him explicitly and confirmed.

- **Grain.** One row per eligible transaction × nearby Site Group within the
  maximum radius threshold, with the pair's actual `distance_m` and **no
  `radius` column**. The per-radius replication that today produces three
  copies of each pair in the published site-grain datasets becomes a
  mechanical step inside the site-grain derivation; the radius-grain
  derivation keeps its distance-ordered cumulative aggregation. The engine
  already computes exactly this intermediate once per chunk
  (`prior_exposure_transaction_site_metrics`,
  `scripts/R/utils/prior_exposure_utils.R:411-468`), so the artifact
  materializes an existing stage rather than inventing a new one.
- **Measure and key columns.** Keys `house_id`/`rental_id` and `site_id`;
  measures `distance_m`, clipped `spill_hrs`, and `spill_count` (a single
  column produced by the 12/24-hour block rule in `count_spills()` — the
  ticket's "12/24 spill_count" is one column, not two). Transaction-level
  metadata (`price`/`listing_price`, `n_days_in_window`) is **excluded**:
  those are facts about the transaction, replicated wastefully at pair
  grain, and both derivations already load the transaction ledger and join
  metadata exactly as `prior_exposure_metadata` does today.
- **Evidence flags: atomic conditions, not verdicts.** Jacopo rejected
  carrying the composite `has_unknown_event_evidence` because it bundles
  distinct conditions. The artifact instead carries four atomic flags, each
  true when its condition holds in at least one year of the transaction's
  lookback window:
  1. `annual_returns_absent` — no Annual Return filed (`absent`); this is
     today's `site_missing`, renamed because nothing about the site is
     missing and "missing" collides with `reported_na`.
  2. `annual_returns_na` — a return was filed but its metrics are missing
     (`reported_na`).
  3. `reported_positive_without_matched_events` — the return reports spills
     but zero events matched (the finding-11 case). The name keeps
     "matched" deliberately: the events feed is positives-only, so what
     fails is the matching.
  4. `annual_returns_na_then_absent` — unchanged; pinned by the public
     radius schema and already a named CONCEPTS.md concept.
  No verdict column is stored. The masks become one-line ORs in the
  derivation layer: the event-evidence mask is the OR of flags 1–3, and the
  EA-evidence mask is the OR of flags 1–2 — so both harmonized verdicts
  fall out of the same columns and the earlier question of carrying a
  dedicated EA verdict is dissolved rather than answered. Stage 1
  derivations reproduce today's masks exactly (site grain: today's
  `site_missing` plus unknown-event-evidence semantics; radius grain: the
  `annual_returns_absent`-based `has_missing_site` only); Stage 2 moves
  every event-based derivation onto the full three-flag OR, making the
  policy change visible as a one-expression diff.
- **Public names stay frozen.** The site-grain derivation renames
  `annual_returns_absent` back to `site_missing` at publication, because
  the published schemas are byte-compatible in Stage 1. The clearer names
  are internal until a later schema revision. CONCEPTS.md receives glossary
  entries for the four flags when the plan executes, alongside the
  charter's planned `house_site_spills` line.
- **Edge rows.** Real pairs only — no sentinel rows, no NA keys. A
  transaction with zero nearby sites (which in this engine subsumes
  coordinate-ineligible transactions, since the concept does not exist
  here) simply has no rows. The radius-grain derivation re-enumerates the
  transaction universe by rejoining the eligible-transaction ledger, as
  today's `CJ(transaction_ids, radii)` grid does; the site-grain derivation
  continues to omit such transactions. Stage-1 reconciliation proves the
  universe is preserved.
- **Location and format.** `data/processed/cross_section/sales/house_site_spills`
  and `data/processed/cross_section/rentals/rental_site_spills`, beside the
  datasets derived from them. Chunked parquet directories using the
  existing streaming `chunk-%010d` pattern, with **no Hive partitioning** —
  the natural partition key today is `radius`, which the artifact
  deliberately lacks.
- **Publication machinery.** The full staged machinery
  (`publish_validated_dataset`: hidden sibling stage, pre- and
  post-promotion validation, atomic rename) with a hand-written Arrow
  schema and expected-key validation derived from the lookup, same as
  public datasets — the artifact feeds both published prior datasets, so a
  half-validated write here poisons everything downstream. It stays **off**
  the public enumerated list (it is internal by charter), but its schema is
  pinned in the contract test file so drift is caught by tests, not only at
  build time.
- **Windows.** Prior-family per-transaction windows only. The study-period
  family's site totals are transaction-independent (one fixed calendar
  window shared by all transactions), so it needs no transaction–site
  measurement table; its connection to this refactor is the shared
  measurement core (ticket 03), not this artifact.
