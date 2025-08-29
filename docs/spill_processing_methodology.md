# Sewage Spill Processing Methodology: Technical Documentation

## Overview

This document explains the two-stage methodology used to process raw sewage spill data for academic research on the impact of spills on house prices. The methodology involves:

1. **12/24 Hour Counting Logic**: Standardizes variable-length spill events into consistent time blocks
2. **Dry Spill Identification**: Enriches spill data with rainfall information to classify spills occurring during dry weather

## Part 1: 12/24 Hour Spill Counting Logic

### 1.1 Problem Statement

Raw spill data contains individual discharge events with start and end times that can vary dramatically:
- Some spills last minutes, others last weeks
- Multiple short spills may occur close together
- Raw data is difficult to compare across sites or time periods

**Goal**: Convert variable-length spill events into standardized counting units for consistent analysis.

### 1.2 Methodology Explanation

The 12/24 counting logic groups spills into "blocks" following this algorithm:

#### **Step 1: Initial Block Creation**
For the first spill at a location:
1. **Block starts** at the spill start time
2. **Initial block duration** is 12 hours from spill start
3. **Block extends** in 24-hour increments until it covers the spill end time

#### **Step 2: Subsequent Spill Processing**
For each additional spill at the same location:

**Case A: Spill Within Current Block**
- If spill end ≤ current block end → Skip (already counted)

**Case B: Spill Extends Current Block** 
- If spill start ≤ (current block end + 24 hours) → Extend current block in 24-hour increments

**Case C: New Block Required**
- If spill start > (current block end + 24 hours) → Start new independent block

#### **Step 3: Block Splitting**
Variable-length blocks are split into standard intervals:
- **First interval**: Always 12 hours
- **Subsequent intervals**: 24 hours each

### 1.3 Worked Example

**Raw Data:**
```
Site A: Spill 1: 2023-01-01 10:00 → 2023-01-01 14:00 (4 hours)
Site A: Spill 2: 2023-01-01 16:00 → 2023-01-01 18:00 (2 hours)  
Site A: Spill 3: 2023-01-03 08:00 → 2023-01-03 10:00 (2 hours)
```

**Processing:**

1. **Spill 1** (Jan 1, 10:00-14:00):
   - Create Block 1: Jan 1 10:00 → Jan 1 22:00 (12 hours covers 4-hour spill)

2. **Spill 2** (Jan 1, 16:00-18:00):
   - Spill start (16:00) ≤ Block 1 end (22:00) → Already covered, skip

3. **Spill 3** (Jan 3, 08:00-10:00):
   - Spill start (Jan 3 08:00) > Block 1 end + 24h (Jan 2 22:00) → Create Block 2
   - Create Block 2: Jan 3 08:00 → Jan 3 20:00 (12 hours covers 2-hour spill)

**Final Intervals:**
```
Site A: Interval 1: Jan 1 10:00 → Jan 1 22:00 (12 hours)
Site A: Interval 2: Jan 3 08:00 → Jan 3 20:00 (12 hours)
```

### 1.4 Daylight Saving Time (DST) Handling

**Problem**: Adding hours to datetime can result in `NA` during DST transitions (e.g., 2:00 AM doesn't exist when clocks spring forward).

**Solution**: The `handle_dst_extension()` function:
1. Attempts to add the specified hours
2. If result is `NA`, adds one additional hour to skip the DST gap
3. Ensures continuous time progression

### 1.5 Advantages and Limitations

#### **Advantages:**
- **Standardization**: Creates comparable units across sites and time periods
- **Handles consecutive spills**: Groups related discharge events logically  
- **Robust to data quality**: Works with imprecise timing data
- **DST-safe**: Handles timezone transitions correctly

#### **Limitations:**
- **Arbitrary thresholds**: 12/24 hour choices are somewhat arbitrary
- **Information loss**: Loses exact timing and duration details of original spills
- **Complexity**: Multi-step algorithm can be difficult to validate
- **Computational cost**: Requires sequential processing by location

#### **Alternative Approaches:**
1. **Fixed time windows**: Aggregate spills into calendar days/weeks/months
2. **Duration-based**: Group by total spill duration regardless of timing  
3. **Event-based**: Count number of discrete spill events
4. **Weighted approaches**: Weight by duration, volume, or severity

---

## Part 2: Dry Spill Identification

### 2.1 Problem Statement

Not all sewage spills are caused by heavy rainfall overwhelming treatment systems. "Dry spills" - those occurring during periods of low rainfall - may indicate:
- Equipment failures
- Poor maintenance practices  
- Illegal discharges
- System capacity problems

**Goal**: Identify spills that occurred during dry weather conditions using gridded rainfall data.

### 2.2 Data Integration Approach

#### **Spatial Matching:**
1. **NGR Conversion**: Convert National Grid Reference codes to easting/northing coordinates
2. **Grid Mapping**: Map coordinates to 1km x 1km rainfall grid cells
3. **9-Cell Analysis**: Extract rainfall data for the target cell plus 8 surrounding cells

#### **Temporal Matching:**
- **Day 0**: Spill start date
- **Day -1**: Day before spill
- **Day -2**: Two days before spill  
- **Day -3**: Three days before spill

#### **Rainfall Data Structure:**
For each spill, extract 36 rainfall values (9 cells × 4 days) labeled as:
- `rain_t1_s1` to `rain_t1_s9`: Day 0 (spill date), cells 1-9
- `rain_t2_s1` to `rain_t2_s9`: Day -1, cells 1-9  
- `rain_t3_s1` to `rain_t3_s9`: Day -2, cells 1-9
- `rain_t4_s1` to `rain_t4_s9`: Day -3, cells 1-9

### 2.3 Dry Spill Classification Methods

All methods use a **0.25mm rainfall threshold** - spills are "dry" if rainfall is below this level.

#### **Method 1: Exact Grid Cell (`dry_day_1`)**
- **Logic**: Rainfall in exact spill location grid cell < 0.25mm on spill date
- **Pros**: Most precise spatial matching
- **Cons**: Sensitive to NGR coordinate precision errors

#### **Method 2: 9-Cell Area (`dry_day_2`)**  
- **Logic**: Maximum rainfall across all 9 surrounding cells < 0.25mm on spill date
- **Pros**: More robust to coordinate imprecision
- **Cons**: May be too strict (one wet cell makes entire area "wet")

#### **Method 3: EA Methodology (`ea_dry_spill`)**
- **Logic**: Maximum rainfall across 9 cells < 0.25mm on spill date AND day before
- **Rationale**: Allows for drainage delays from previous day's rainfall
- **Pros**: Accounts for system lag effects
- **Cons**: Very conservative definition

#### **Method 4: BBC Methodology (`bbc_dry_spill`)**
- **Logic**: Maximum rainfall across 9 cells < 0.25mm for 4 days prior to spill
- **Rationale**: Accounts for longer-term soil moisture and system conditions
- **Pros**: Most comprehensive dry period assessment
- **Cons**: May exclude many spills that are genuinely rainfall-independent

### 2.4 Statistical Output

The algorithm calculates summary statistics for analysis:

- **`mean_rain_1`**: Average rainfall across 9 cells on spill date
- **`mean_rain_2`**: Average rainfall across all 36 measurements  
- **`max_rain_1`**: Maximum rainfall across 9 cells on spill date
- **`max_rain_2`**: Maximum rainfall across 9 cells on spill date or day before
- **`max_rain_3`**: Maximum rainfall across all 36 measurements
- **`any_miss`**: Flag indicating if any rainfall data is missing

### 2.5 Performance Optimizations

#### **Original Implementation Issues:**
- `rowwise()` operations on 1M+ observations
- Manual indexing into large rainfall arrays
- Repetitive extraction code for each time period

#### **Refactored Improvements:**
- **Matrix-based extraction**: Vectorized operations instead of row-by-row processing
- **Chunked data loading**: Memory-efficient NetCDF file handling
- **Vectorized coordinate conversion**: Batch processing of NGR codes
- **Efficient grid mapping**: Uses optimized boundary matching

### 2.6 Advantages and Limitations

#### **Advantages:**
- **Multiple methodologies**: Provides sensitivity analysis across approaches
- **High spatial resolution**: 1km grid captures local rainfall variation
- **Temporal flexibility**: Can adjust lookback periods for different analyses
- **Robust data handling**: Manages missing data and coordinate errors

#### **Limitations:**
- **Arbitrary threshold**: 0.25mm cutoff lacks strong theoretical justification
- **Spatial aggregation**: 9-cell approach may smooth important variation
- **Temporal assumptions**: Assumes immediate rainfall-spill relationships
- **Data quality dependency**: Results depend on NGR coordinate accuracy

#### **Alternative Approaches:**
1. **Antecedent precipitation indices**: Weighted rainfall over longer periods
2. **Soil moisture modeling**: Account for ground saturation levels
3. **Catchment-based analysis**: Match spills to upstream rainfall catchments
4. **Machine learning approaches**: Let data determine optimal thresholds and time windows
5. **Multi-threshold analysis**: Test sensitivity to different rainfall cutoffs

---

## Part 3: Critical Assessment

### 3.1 Is This the Best Methodology?

#### **Strengths of Current Approach:**
1. **Established precedent**: Builds on methodologies used by EA and BBC
2. **Conservative classification**: Multiple strict criteria reduce false positives  
3. **Comprehensive data use**: Leverages high-resolution spatial and temporal data
4. **Multiple definitions**: Enables sensitivity analysis

#### **Potential Improvements:**

##### **For 12/24 Counting:**
1. **Data-driven thresholds**: Use statistical analysis to optimize 12/24 hour parameters
2. **Site-specific calibration**: Adjust thresholds based on treatment plant capacity
3. **Severity weighting**: Weight blocks by spill volume or pollution load
4. **Validation against outcomes**: Test whether 12/24 blocks predict house price impacts better than alternatives

##### **For Dry Spill Classification:**  
1. **Catchment-based rainfall**: Match spills to upstream drainage areas rather than point locations
2. **Antecedent moisture modeling**: Include soil moisture and evapotranspiration
3. **Machine learning thresholds**: Use supervised learning to optimize classification
4. **Multi-scale analysis**: Test different spatial scales (1km, 5km, 10km)

### 3.2 Alternative Methodologies to Consider

#### **Option 1: Duration-Weighted Approach**
Instead of 12/24 blocks, weight spills by actual duration and severity:
```r
spill_impact = duration_hours * pollution_load * proximity_factor
```

#### **Option 2: Event Density Analysis**
Count spill frequency within time windows:
```r  
spill_density = count_spills_in_window / window_length
```

#### **Option 3: Machine Learning Classification**
Use features like:
- Rainfall (multiple time scales)
- Soil moisture
- Previous spill history
- Treatment plant capacity
- Seasonal patterns

Train models to predict "problematic" vs "weather-related" spills.

#### **Option 4: Hydrological Modeling**
Implement physical models of:
- Catchment runoff response
- Treatment plant capacity
- Combined sewer overflow thresholds

### 3.3 Validation Recommendations

To assess whether this is the optimal methodology:

1. **Ground truth comparison**: Manually classify a sample of spills and compare to algorithmic classification
2. **Outcome validation**: Test which classification method best predicts house price impacts
3. **Expert review**: Have water industry professionals assess classification accuracy
4. **Sensitivity analysis**: Test robustness to parameter choices (thresholds, time windows)
5. **Alternative comparison**: Implement and compare performance of alternative approaches

### 3.4 Recommended Next Steps

1. **Short-term**: Proceed with current methodology while documenting assumptions and limitations
2. **Medium-term**: Implement sensitivity analysis with different thresholds (0.1mm, 0.5mm, 1.0mm)
3. **Long-term**: Develop machine learning approach using current methodology as baseline

---

## Part 4: Code Architecture

### 4.1 Script 1: 12/24 Spill Counting (`apply_1224_spill_counting.R`)

#### **Function Hierarchy:**
```
apply_1224_counting() [Main workflow]
├── validate_spill_data() [Input validation]
├── create_spill_blocks() [Core algorithm]
│   └── handle_dst_extension() [DST handling]
└── split_blocks_to_intervals() [Output formatting]
    └── handle_dst_extension() [DST handling]
```

#### **Data Flow:**
```
Raw EDM Data → Validation → Block Creation → Block Splitting → Merge Attributes → Output
```

#### **Key Functions:**

**`validate_spill_data(spill_data)`**
- Checks for required columns: `outlet_discharge_ngr`, `start_time`, `end_time`
- Validates datetime formats (POSIXct)
- Warns about missing NGR values

**`create_spill_blocks(spill_data)`**
- Processes each location independently
- Applies 12/24 hour grouping logic
- Returns data.frame with `id`, `block_start`, `block_end`

**`split_blocks_to_intervals(blocks)`**
- Splits variable-length blocks into standard intervals
- First interval: 12 hours, subsequent: 24 hours each
- Returns data.frame with added `duration` column

**`handle_dst_extension(datetime, hours_to_add)`**
- Safely adds hours to datetime values
- Detects and handles DST transition NAs
- Returns DST-adjusted datetime vector

### 4.2 Script 2: Dry Spill Identification (`identify_dry_spills.R`)

#### **Function Hierarchy:**
```
enrich_spills_with_rainfall() [Main workflow]
├── clean_spill_ngr() [Data validation]
├── load_rainfall_data() [NetCDF processing]
├── convert_ngr_to_coordinates() [Spatial conversion]
├── map_coordinates_to_grid() [Grid mapping]  
├── extract_rainfall_matrix() [Data extraction]
├── calculate_rainfall_statistics() [Statistical summary]
└── classify_dry_spills() [Classification]
```

#### **Data Flow:**
```
1224 Spills → NGR Cleaning → Coordinate Conversion → Grid Mapping → 
Rainfall Extraction → Statistics → Classification → Output
```

#### **Key Functions:**

**`load_rainfall_data(file_paths)`**
- Combines multiple NetCDF files along time dimension
- Memory-efficient processing with garbage collection
- Returns 3D array [x, y, time]

**`convert_ngr_to_coordinates(ngr_vector)`**
- Vectorized NGR to easting/northing conversion
- Uses `rnrfa::osg_parse()` with error handling
- Returns data.frame with coordinate columns

**`extract_rainfall_matrix(spills_df, rainfall_array, time_offsets)`**
- Extracts 9-cell × 4-day rainfall data for each spill
- Matrix-based operations instead of rowwise
- Returns data.frame with 36 rainfall columns per spill

**`classify_dry_spills(rainfall_df, threshold)`**
- Applies four different dry spill methodologies
- Uses 0.25mm default threshold
- Returns data.frame with classification columns

### 4.3 Error Handling and Edge Cases

#### **Common Issues Handled:**
1. **Missing NGR data**: Filtered out with warnings
2. **Invalid NGR formats**: Length validation and cleaning
3. **DST transitions**: Automatic hour adjustment
4. **Missing rainfall data**: Flagged in `any_miss` column
5. **Grid boundary issues**: Coordinate bounds checking
6. **Memory limitations**: Chunked processing and garbage collection

#### **Input Validation:**
- File existence checks
- Required column validation  
- Data type verification
- Coordinate range validation

#### **Output Quality Assurance:**
- Progress reporting throughout processing
- Summary statistics for validation
- Consistent column naming and types
- Comprehensive documentation

---

## Conclusion

The current methodology provides a systematic, reproducible approach to sewage spill analysis with clear traceability from raw data to final classifications. While there are opportunities for improvement, particularly in threshold optimization and alternative modeling approaches, the current implementation serves as a solid foundation for academic research.

The modular code architecture enables easy testing of alternative approaches while maintaining compatibility with existing analyses. For immediate use, this methodology is appropriate, with recommendations for future enhancements based on validation results and domain expert feedback.