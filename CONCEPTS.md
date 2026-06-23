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

## Property-Price Analysis Area

### Near-Overflow Radius
The straight-line distance threshold within which a property is treated as exposed to a storm overflow, used to build the near-property cross-sections. The main analyses are run at several such thresholds, with the others serving as robustness checks. In regression specifications it is written generically as the radius buffer **B**, which takes the values 250, 500, and 1000 m across the sweep; each individual result table still reports its specific radius.

### Directional Spill Exposure
Spill exposure split by whether the contributing overflow lies upstream or downstream of the property along the river network — used to separate same-river pollution transport from generic proximity. Estimated unweighted and in an inverse-river-distance-weighted variant.

### Nearest-Site Exposure
Spill exposure measured from the single nearest overflow to a property (rather than aggregated over all overflows within the Near-Overflow Radius), typically with an upstream indicator and a river-distance control. The *one-site* sample restricts attention to properties with exactly one overflow within the radius.

### Cross-Radius Robustness Summary
A compact table reporting the preferred specification's coefficient(s) of interest across all radius buffers (250/500/1000 m) side by side. Produced as a `*_radius_robustness` artifact for the headline hedonic and public-attention analyses; for analyses without such an artifact (e.g. Directional Spill Exposure, Nearest-Site Exposure), the results report synthesizes one from the per-radius tables.

### Intensive Margin
The effect of sewage-spill *intensity* — a continuous exposure measure such as average daily spill count or spill hours — on property values, estimated among properties within the Near-Overflow Radius of an overflow.

### Extensive Margin
The effect of *proximity itself* — being near versus far from an overflow — on property values, and how the near-versus-far price gap responds to public attention. Contrasts with the Intensive Margin, which varies realized spill intensity rather than proximity.

## Public Attention Area

### Public Attention
The project-level construct for how salient sewage spills are to the public over time, proxied by search interest and media coverage rather than by physical spill activity itself.

### Google Trends Search Interest
A normalized time-series proxy for public search attention to sewage spills, used as one component of Public Attention.

### UK Media Article Count
A time-series proxy for media coverage of sewage spills, used alongside Google Trends Search Interest to measure Public Attention.

### Windowed Article Count
A trailing-window salience measure: the UK Media Article Count summed over the past 3, 6, or 12 months up to and including a transaction's month. Contrasts with the cumulative article count (summed from the start of the sample), which rises mechanically over time; windowed counts fluctuate, capturing recent rather than accumulated attention. Built on a gap-filled monthly grid because the source stores only months with at least one article.
