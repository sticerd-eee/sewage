## Sewage Spill Data Sources

There are two sources of data on sewage spills in England, both produced by the event duration monitoring (EDM) devices installed on storm overflows:

- **EDM event-level records (FOI):**
  - Obtained from the water companies through freedom of information (FOI) requests.
  - Data is at the individual discharge level: each observations tells the start and end time of a discharge to the minute. About `r round(n_events / 1e6, 2)` million rows, 2021 to 2024.
- **EDM annual returns (EA):**
  - Filed by every water company with the Environment Agency (EA) and published publicly once a year. One row per monitored overflow per year, about 14,500 a year. Spill count and hours for the year, plus permit, name, grid reference, treatment type, receiving water, bathing designation.

They overlap in what they describe, not in what they can say (@fig-sources).

**Event-level records**

- Knows *when*: any window, any clipping. Spill counts follow the Environment Agency's 12/24 convention: the first 12 hours of a discharge count as one spill, each further 24-hour block as one more.
- Positives only. No rows can mean no spills, a monitor offline, or a missing return. The event records cannot tell these apart.
- No site characteristics.

**Annual returns**

- Knows *whether*: explicit zero, explicit NA, and sites that never spilled.
- Carries the characteristics the heterogeneity work needs.
- Cannot say when in the year a spill happened.

**Rule.** Exposure is computed from the event-level records only. The returns do two jobs: decide whether a site-year's events can be trusted, and describe the site. Their own totals enter only as a check in the matching step.

**Rejected.** Spreading the returns' annual totals evenly over the year. It avoids matching but breaks the design, which needs exposure before a sale date.
