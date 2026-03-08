# Repository Examples For Table And Figure Notes

Use these examples as the primary style anchor. They show the preferred note density for this project: one compact paragraph that makes the object readable on its own, with clear sample definitions, variable construction, and enough detail on estimation or visual encoding to make the note self-contained.

## Regression Table Benchmark

Primary file:

- `scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior.R`

Generated tables:

- `tables/hedonic_count_continuous_prior.tex`
- `tables/hedonic_hrs_continuous_prior.tex`

Representative pattern:

- Open with the estimand: `This table presents hedonic estimates of the relationship between sewage spill exposure and property values.`
- State the sample early: properties within 250m of a storm overflow in England, 2021--2023.
- Map the dependent variable to column groups: sales columns versus rental columns.
- Define exposure precisely, including timing and radius:
  - count version: average number of spill events per day across overflows within 250m from January 2021 to the transaction date;
  - hours version: average total spill hours per day over the same window.
- Name the control sets separately for sales and rentals.
- End with the inference convention and star legend.

Why this works:

- The first sentence says what is estimated, not what the paper concludes.
- The note gives a referee enough information to interpret the coefficient magnitudes without paging back.
- Column-group differences are handled cleanly in one sentence.

## Summary-Statistics Table Benchmark

Primary file:

- `scripts/R/09_analysis/07_dry_spills/daily_rain_regimes_table.R`

Generated table:

- `tables/dry_spill_daily_rain_regimes.tex`

Representative pattern:

- Open with the object and grouping: `This table presents daily spill statistics by rainfall category ...`
- State geography and period.
- Define rainfall using the exact spatial and temporal rule: maximum daily rainfall within the surrounding 3 x 3 km grid, measured on that day or the previous day.
- Define each reported statistic in natural language:
  - annual total;
  - share;
  - spill hours across all days including zeroes;
  - spill hours conditional on a spill day.

Why this works:

- It distinguishes conditioning choices explicitly.
- It does not assume the reader knows how the rainfall categories were constructed.
- It explains each reported statistic without mechanically restating every column heading.

## Map Benchmark

Source files:

- `scripts/R/09_analysis/01_descriptive/spill_maps.R`
- `03_motivating_evidence.tex`

Representative pattern in manuscript:

- State the geographic unit, place, and period: sewage spill counts across MSOAs in England for 2021--2023.
- State the panel mapping: one panel for total spills and one for dry spills.
- Define colour intensity as `log(count + 1)`.
- State how zero-spill MSOAs are shown.
- State the winsorisation rule.

Why this works:

- The note tells the reader how to decode the map rather than just naming the topic.
- It surfaces the visual-construction choices that materially affect interpretation.

## Transition-Figure Benchmark

Source files:

- `scripts/R/09_analysis/01_descriptive/spill_phase_diagrams.R`
- `03_motivating_evidence.tex`

Representative pattern in manuscript:

- Define the object as conditional transition probabilities from year `t` to year `t+1`.
- Explain the five mutually exclusive states: `0`, `Q1`, `Q2`, `Q3`, `Q4`.
- State that quartiles are based on the distribution of positive spill counts.
- State the pooled sample window.
- Explain how to read each cell: the probability of being in state `y` at `t+1` conditional on state `x` at `t`.

Why this works:

- The note explains the mechanics of the heatmap instead of leaving the reader to infer them.
- The state definition is precise enough that the plot can stand alone.

## Additional Figure Pattern Nearby

Useful nearby example in the manuscript:

- `03_motivating_evidence.tex` four-panel figure on prices, rents, distance, and spill count.

What it adds:

- Clear mapping from panels to outcomes.
- Explicit statement that plotted lines are fitted values from bivariate linear regressions.
- Sample trimming rule.
- Separate explanation for the two exposure dimensions: physical distance and cumulative spill count.

## House-Style Rules Distilled From These Examples

- Start with a direct sentence naming the object and what it reports.
- Put sample, geography, period, and spatial restriction near the front.
- Define key variables in prose, not code labels.
- Explain transformations, pooling, conditioning, trimming, or winsorisation whenever they are not obvious from the caption alone.
- Use one dense paragraph.
- End regression-table notes with the uncertainty convention and, when used, the significance-star legend.
- Keep the note descriptive and mechanically informative rather than interpretive.
