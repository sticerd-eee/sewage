# ADR 0002: Content-Stable Transaction Identity as Hex Strings

- **Status:** Accepted
- **Date:** 2026-08-14
- **Implementation:** Completed on `jo/repeat-transactions-rebuild` on 2026-08-15;
  all declared downstream ID artifacts passed the 100% source-match gate.

## Context

`rental_id` and `house_id` were positional row numbers assigned at cleaning time. Any upstream change to row count or order silently re-labelled every transaction, and downstream joins attached derived data to the wrong rows with no error. This produced two independent corruption episodes within thirteen months (rentals, July 2026; sales, August 2026). The repeat-group identifier (`repeat_id`) had the same defect one level up: sequential assignment made it unstable across runs and gave the rentals and sales mappings overlapping ranges.

## Decision

Transaction identity derives from content, not position. `rental_id` hashes the transaction's post-cleaning natural key; `house_id` hashes the Land Registry `transaction_id`; `repeat_id` hashes the normalised address key that defines the repeat group. All three are xxhash64 rendered as 16-character hex **strings**.

Strings, not integers, deliberately: R silently coerces 64-bit integers to doubles above 2^53, corrupting joins invisibly, and a 32-bit space guarantees collisions at these row counts. String keys make every stale-generation join fail loudly (near-zero match rate) instead of succeeding wrongly. The Arrow schema contracts merged in PRs #28/#29 that pinned the ID columns to int32 now enforce `utf8`, consistent with this decision and the 2026-08-14 rebuild plan.

## Consequences

- Regeneration discipline is mandatory: any change to the hashed fields' cleaning transforms churns IDs, and every ID-keyed artifact must regenerate together. The failure mode for missing this is loud, by design.
- Old positional-ID artifacts are retired by full regeneration, never bridged with crosswalks.
- The same transaction carries the same ID in every artifact generation and in both the long-run superset and study-window subset tables, which is what makes the superset/subset design safe.
- Considered and rejected: positional IDs guarded by provenance checks (leaves the root cause; guards rot), integer64 (silent double coercion), int32 (collision-certain).
