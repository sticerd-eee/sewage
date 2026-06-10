# Concepts

Shared domain vocabulary for this project - entities, named processes, and status concepts with project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

## Annual Return Lookup Area

### Annual Return EDM
The yearly company-reported event-duration monitoring return for storm overflow assets, combining site identifiers, permit/activity references, location fields, and reporting-year metadata.

### Monitored Discharge Point
A single monitored overflow asset at a wastewater works — a storm tank, inlet overflow, or network outfall — and the unit recorded by one Annual Return EDM row.

One works can carry several Monitored Discharge Points that share every identifying field (names, permits, location) while reporting different spill behavior. Per-monitor unique identifiers exist only from the 2023 return onward (re-keyed each year, with a bridge to the prior year's identifiers); earlier returns carry no monitor-level key, so monitor-multiples in those years cannot be tracked individually across years.

### Annual-Return Site
A reporting-year-specific record of one Monitored Discharge Point in the Annual Return EDM. It is not automatically a stable cross-year entity, because identifiers and reporting structures can change between returns.

### Canonical Spill Site
A stable project-level site identity used to connect records that refer to the same storm overflow asset across years and datasets.

### Annual-Return Lookup
The cross-year mapping that assigns year-specific Annual-Return Sites to Canonical Spill Sites.

### Record-Linkage Component
A connected group of Annual-Return Sites implied by pairwise matching evidence. A component is only valid as one canonical track when it satisfies the project's site-identity invariants.

### Same-Year Component Conflict
A Record-Linkage Component containing more than one Annual-Return Site from the same reporting year. Such a component cannot be collapsed into one Canonical Spill Site without an explicit resolution rule.
