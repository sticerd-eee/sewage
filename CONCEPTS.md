# Concepts

Shared domain vocabulary for this project - entities, named processes, and status concepts with project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

## Annual Return Lookup Area

### Annual Return EDM
The yearly company-reported event-duration monitoring return for storm overflow assets, combining site identifiers, permit/activity references, location fields, and reporting-year metadata.

### Monitored Discharge Point
A single monitored overflow asset reported in an Annual Return EDM row. Several points can share names, permits, and locations while reporting different spill behaviour.

### Annual-Return Site
A reporting-year-specific record of one Monitored Discharge Point in the Annual Return EDM. It is not automatically a stable cross-year entity, because identifiers and reporting structures can change between returns.

### Canonical Spill Site
A stable project-level identity for one Monitored Discharge Point across reporting years. Canonical metadata, availability, and commissioning history belong to this entity.

### Annual-Return Lookup
The cross-year mapping that assigns year-specific Annual-Return Sites to Canonical Spill Sites.

### Site Group
A project-created grouping of Canonical Spill Sites used to connect event, location, and annual-return evidence that cannot be assigned reliably to one member. A Site Group supports group-level spill summaries but is not evidence of a verified physical wastewater works.

### Site Group Register
The authoritative record of Site Group membership and reporting-year status. It links each Canonical Spill Site to exactly one Site Group and records group-level event and annual-return evidence.

### Annual Status
The per-Site-Group-year classification that disambiguates the absence of events in the positives-only event feed: `reported_zero` (return filed, both metrics zero), `reported_positive`, `reported_na` (return filed, metrics missing), or `absent` (no return that year).

### Record-Linkage Component
A connected group of Annual-Return Sites implied by pairwise matching evidence. A component is only valid as one canonical track when it satisfies the project's site-identity invariants.

### Same-Year Component Conflict
A Record-Linkage Component containing more than one Annual-Return Site from the same reporting year. Such a component cannot be collapsed into one Canonical Spill Site without an explicit resolution rule.

## Property-Price Analysis Area

### Near-Overflow Radius
The straight-line distance threshold within which a property is treated as exposed to a storm overflow, used to build the near-property cross-sections. The main analyses are run at several such thresholds, with the others serving as robustness checks. In regression specifications it is written generically as the radius buffer **B**, which takes the values 250, 500, and 1000 m across the sweep; each individual result table still reports its specific radius.

### Spill Exposure
A property's continuous measure of nearby storm-overflow activity — spill count and spill hours at the overflows within its Near-Overflow Radius — measured over a stated exposure window.

Spill Exposure may be expressed as a Whole-Period Spill Exposure or an Average Daily Spill Exposure. Directional Spill Exposure and Nearest-Site Exposure are variants that split or restrict which overflows contribute.

### Whole-Period Spill Exposure
The total spill count or spill hours accumulated within a property's Near-Overflow Radius over the stated exposure window.

### Study-Period Spill Exposure
A Whole-Period Spill Exposure measured from the EA-revised annual-return totals over the fixed 2021–2024 study window. It is a time-invariant property-area measure and may include spill activity after an individual sale or rental transaction; it is published as a whole-period total and as equivalent daily and weekly averages.

### Spatially Eligible Transaction
A sale or rental transaction with usable property coordinates, for which nearby Site Groups can be evaluated. A spatially eligible transaction with no Site Group inside its Near-Overflow Radius has zero Spill Exposure; a transaction without usable coordinates has unknown, not zero, exposure.

### Average Daily Spill Exposure
Spill Exposure divided by the number of days in its stated exposure window. Headline coefficients may be reported in per-week units for interpretability.

### Average Weekly Spill Exposure
Average Daily Spill Exposure multiplied by seven, used as the principal reporting scale in the project's analyses.

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

## Repeat-Transactions Area

### Content-Stable Transaction ID
The identity of one cleaned sale or rental transaction, derived from the transaction's own recorded values rather than from its position in the file. Regenerating an input cannot silently re-label transactions: identifiers across artifact generations either match exactly or fail loudly.

### Address Key
The normalised composite of postcode and address components that identifies one property unit for repeat matching. A transaction missing its postcode or its primary address component carries no Address Key and cannot participate in repeat identification.

### Keyable Transaction
A transaction that carries an Address Key. Only keyable transactions can be assigned to a Repeat Group.

### Repeat Group
All Keyable Transactions in one dataset that share an Address Key — the project's operational notion of "the same property transacting repeatedly". A group of size one is a single; singles still belong to a Repeat Group. Group identity derives from the Address Key itself, so it is stable across runs; distinct Address Keys are asserted to map to distinct group identifiers at build time, and a group identifier is meaningful only within its own market's mapping.

### Repeat-Transactions Mapping
The transaction-grain artifact assigning every Keyable Transaction to its Repeat Group, singles included, with the group size recorded. It is a census of keyable transactions — exclusions are auditable by subtraction — and the sole bridge between cleaned transactions and repeat-based analyses. Group membership and group size are defined over the Long-Run Transaction History; a consumer restricting to a narrower window must regroup rather than trust the recorded size.

### Long-Run Transaction History
The maximal retained span of cleaned transactions (from 2014, the earliest rental coverage), wider than the spill study window. Repeat identification runs over the long-run history; the study-window transaction tables are derived from it by filtering, never built independently, so both views agree row-for-row where they overlap.

## Public Attention Area

### Public Attention
The project-level construct for how salient sewage spills are to the public over time, proxied by search interest and media coverage rather than by physical spill activity itself.

### Google Trends Search Interest
A normalized time-series proxy for public search attention to sewage spills, used as one component of Public Attention.

### UK Media Article Count
A time-series proxy for media coverage of sewage spills, used alongside Google Trends Search Interest to measure Public Attention.

### Windowed Article Count
A trailing-window salience measure: the UK Media Article Count summed over the past 3, 6, or 12 months up to and including a transaction's month. Contrasts with the cumulative article count (summed from the start of the sample), which rises mechanically over time; windowed counts fluctuate, capturing recent rather than accumulated attention. Built on a gap-filled monthly grid because the source stores only months with at least one article.
