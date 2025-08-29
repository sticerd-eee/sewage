# Assumptions Analysis: Dry Spill Statistics Aggregation

## Executive Summary

This report analyzes the assumptions made in the R script `aggregate_dry_spill_stats.R` and its dependencies for academic research on sewage spills and house prices. The analysis identified **12 major assumptions** that could significantly impact research validity, prioritized from most to least critical.

**Key Findings:**
- The most critical assumptions relate to methodological choices (rainfall threshold, spatial/temporal representativeness)
- Several assumptions lack scientific justification and could bias econometric estimates
- Data quality assumptions may introduce systematic errors in spill classification
- Implementation assumptions generally have lower impact but could affect reproducibility

## Detailed Assumptions Analysis

### 1. HIGHEST PRIORITY - Causal/Methodological Assumptions

#### A. Rainfall Threshold Defines "Dry" Spills (0.25mm)
- **Location:** `CONFIG$dry_threshold_mm = 0.25` (lines 48, 441-456)
- **Significance:** This is the fundamental assumption driving the entire analysis. The script assumes spills occurring when rainfall < 0.25mm are "inappropriate" or "dry spills" that shouldn't happen during low precipitation.
- **Risk:** This threshold appears arbitrary and lacks scientific justification. Different sewer systems may have varying capacity thresholds, and 0.25mm may not reflect actual hydraulic triggers.
- **Impact on Research:** Critical - misclassification of spills could bias estimates of sewage system performance and house price effects.

#### B. Spatial Rainfall Representativeness
- **Location:** `get_required_grid_indices()` and `create_spill_site_lookup()` functions (lines 98-308 in clean_rainfall_data.R)
- **Significance:** Assumes rainfall measured at 1km² grid cells or 9-cell neighborhoods accurately represents precipitation conditions at spill sites.
- **Risk:** Ignores micro-scale variation and localized weather events that could affect spill causation.
- **Impact on Research:** High - measurement error in the key explanatory variable could attenuate or bias econometric estimates.

#### C. Temporal Rainfall Causation Windows
- **Location:** Time offset calculations (lines 132-137 in identify_dry_spills.R) and temporal aggregation (lines 50-57)
- **Significance:** Assumes specific time windows (day 0, day-1, or days 0-3) capture the relevant precipitation period for determining spill legitimacy.
- **Risk:** Ignores soil saturation, groundwater levels, and cumulative rainfall effects that may influence sewer capacity over longer periods.
- **Impact on Research:** High - incorrect temporal specification could misclassify spills and bias causal inference.

### 2. HIGH PRIORITY - Data Quality Assumptions

#### D. Complete Rainfall Data Coverage
- **Location:** `na.rm = TRUE` handling throughout (lines 213-219 in identify_dry_spills.R)
- **Significance:** Assumes NetCDF rainfall data provides complete temporal and spatial coverage, with missing data handled appropriately.
- **Risk:** Systematic gaps in rainfall data could bias identification of dry spills, particularly if missingness correlates with weather patterns.
- **Impact on Research:** Medium-High - could introduce selection bias in the sample of analyzed spills.

#### E. Accurate Spill Timing and Location
- **Location:** Spill data loading and processing (lines 89-97 in identify_dry_spills.R)
- **Significance:** Assumes start_time/end_time accurately reflect actual spill periods and NGR coordinates correctly locate spill points.
- **Risk:** Reporting delays or coordinate errors would misclassify spills as dry/wet.
- **Impact on Research:** High - measurement error in the dependent variable classification could bias results.

#### F. 12/24 Hour Counting Methodology Validity
- **Location:** `count_spills()` function (lines 130-189 in spill_aggregation_utils.R)
- **Significance:** Assumes the Environment Agency's 12/24 hour counting method accurately aggregates overlapping spill events.
- **Risk:** This methodology may not capture true environmental impact or duration of contamination events.
- **Impact on Research:** Medium - affects interpretation of spill frequency and severity measures.

### 3. MEDIUM PRIORITY - Statistical/Technical Assumptions

#### G. Grid Cell Spatial Homogeneity
- **Location:** Grid cell processing throughout clean_rainfall_data.R
- **Significance:** Treats 1km² grid cells as spatially homogeneous for rainfall.
- **Risk:** Ignores sub-grid variation that could affect local catchment behavior.
- **Impact on Research:** Medium - may introduce measurement error but likely averaged out across large samples.

#### H. Independent Classification Across Indicators
- **Location:** Multiple rainfall indicators creation (lines 50-57 in aggregate_dry_spill_stats.R)
- **Significance:** Creates 6 rainfall indicator variants but doesn't account for their correlation structure.
- **Risk:** Could lead to pseudo-replication in subsequent analyses if multiple correlated indicators are used simultaneously.
- **Impact on Research:** Medium - may affect standard error calculations and hypothesis testing.

#### I. Temporal Boundary Handling
- **Location:** `prepare_spill_data()` function (lines 85-116 in spill_aggregation_utils.R)
- **Significance:** Cross-year and cross-month spills are truncated to calendar boundaries.
- **Risk:** May split genuine continuous events and affect duration calculations.
- **Impact on Research:** Medium - could introduce measurement error in spill duration variables.

### 4. LOWER PRIORITY - Implementation Assumptions

#### J. Data Merging Completeness
- **Location:** `integrate_with_main_aggregations()` function (lines 207-258)
- **Significance:** Assumes main aggregation files exist and have compatible structure for joining.
- **Risk:** Failed joins could result in incomplete datasets.
- **Impact on Research:** Low-Medium - primarily affects reproducibility rather than validity.

#### K. Memory/Performance Scaling
- **Location:** `CONFIG$sites_per_chunk = 500` and `CONFIG$n_cores = 6` (lines 37, 70 in identify_dry_spills.R)
- **Significance:** Assumes this configuration optimizes performance without overwhelming system resources.
- **Risk:** Sub-optimal performance or memory issues on different systems.
- **Impact on Research:** Low - affects computational efficiency but not results validity.

#### L. File Format Consistency
- **Location:** Parquet file I/O throughout all scripts
- **Significance:** Assumes consistent file formats and column structures across processing stages.
- **Risk:** Format inconsistencies could cause processing failures.
- **Impact on Research:** Low - primarily affects reproducibility.

## Recommendations

### Critical Actions Required

1. **Justify Rainfall Threshold:** Provide scientific rationale for the 0.25mm threshold or conduct sensitivity analysis across multiple thresholds.

2. **Validate Spatial Representativeness:** Test correlation between grid-cell rainfall and local weather station data near spill sites.

3. **Expand Temporal Analysis:** Consider longer lookback periods and cumulative precipitation effects.

### Suggested Improvements

1. **Document Data Quality:** Add explicit checks for missing data patterns and their potential impact.

2. **Robustness Checks:** Implement multiple classification schemes and test sensitivity of results.

3. **Correlation Analysis:** Examine relationships between the 6 rainfall indicators before using in downstream analysis.

## Technical Implementation Notes

- Most critical assumptions are embedded in configuration parameters and can be modified for sensitivity testing
- The modular script structure facilitates testing alternative methodological approaches
- Comprehensive logging provides audit trail for assumption validation

## Conclusion

While the script demonstrates good software engineering practices, several methodological assumptions require careful justification or sensitivity analysis to ensure robust research conclusions. The rainfall threshold and spatial/temporal representativeness assumptions are particularly critical for the validity of econometric analysis of sewage spills' impact on house prices.

---

**Report Generated:** August 28, 2025  
**Reviewer:** Senior Data Scientist Review  
**Scripts Analyzed:** 
- `scripts/R/03_data_enrichment/aggregate_dry_spill_stats.R`
- `scripts/R/utils/spill_aggregation_utils.R`
- `scripts/R/03_data_enrichment/identify_dry_spills.R`
- `scripts/R/02_data_cleaning/clean_rainfall_data.R`