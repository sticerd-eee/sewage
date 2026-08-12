# ADR 0001: Separate Canonical Spill Sites from Site Groups

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

Annual Return EDM rows describe monitored discharge points, while event records often identify only a shared site name, permit, or location. Several independently reported monitored points can therefore receive one event identity. The former “Works” label suggested that this rule-based grouping had been verified as one physical wastewater works, which the source evidence does not establish.

Using one table grain for both identities also mixed incompatible metadata. Commissioning, availability, operation, and closure evidence belongs to individual monitored points; event counts, event hours, representative locations, and annual status can be observed only for the shared group in some records.

## Decision

`site_id_canonical` identifies a **Canonical Spill Site**, the stable monitored-point identity and the key of `unique_spill_sites`. `site_id` identifies its containing **Site Group** and may repeat in the canonical inventory.

The Site Group Register and `site_group_crosswalk.parquet` own membership, group-year annual status, group spill totals, representative location, and event-match evidence. Canonical commissioning and other member metadata are resolved independently from annual-return histories before Site Group membership is attached.

Group-keyed consumers obtain one row per `site_id` from the Site Group crosswalk. They must not deduplicate repeated canonical rows. Canonical consumers use `site_id_canonical` and must not aggregate member histories through the group key.

## Consequences

- The canonical inventory contains one row per Annual Return Lookup ID and can repeat `site_id` for multi-member groups.
- Event, rainfall, property-distance, mapping, and exposure products remain Site Group keyed.
- Commissioning figures count Canonical Spill Sites and disclose unresolved histories separately.
- The misleading “Works” abstraction and its output alias are retired from live contracts. Physical wastewater-works language remains valid only when it describes source data or real infrastructure, not the project-created grouping.
