library(dplyr)
library(lubridate)
library(sf)
library(here)  # for relative paths

# Define paths relative to the project root
dp_path <- here("data", "processed")
output_path <- here("data", "processed", "dry_spills_aggregated")

# Load data
load(file.path(dp_path, "merged_edm_1224_dry_spill_data.RData"))

# Prepare and convert dry indicators
spill_data <- dry_spills_defined %>%
  mutate(
    year = year(block_start),
    month = month(block_start),
    quarter = quarter(block_start),
    dry_d1 = dry_day_1 == "yes",
    dry_d2 = dry_day_2 == "yes",
    dry_ea = ea_dry_spill == "yes",
    dry_bbc = bbc_dry_spill == "yes"
  )

# Monthly aggregation
monthly_ds <- spill_data %>%
  group_by(site_id, water_company, year, month, easting, northing) %>%
  summarise(
    mean_rain_1 = mean(mean_rain_1, na.rm = TRUE),
    mean_rain_2 = mean(mean_rain_2, na.rm = TRUE),
    max_rain_1 = max(max_rain_1, na.rm = TRUE),
    max_rain_2 = max(max_rain_2, na.rm = TRUE),
    max_rain_3 = max(max_rain_3, na.rm = TRUE),
    sum_dry_d1 = sum(dry_d1, na.rm = TRUE),
    sum_dry_d2 = sum(dry_d2, na.rm = TRUE),
    sum_dry_ea = sum(dry_ea, na.rm = TRUE),
    sum_dry_bbc = sum(dry_bbc, na.rm = TRUE),
    total_spills = n(),
    .groups = "drop"
  )

# Quarterly aggregation
quarterly_ds <- spill_data %>%
  group_by(site_id, water_company, year, quarter, easting, northing) %>%
  summarise(
    mean_rain_1 = mean(mean_rain_1, na.rm = TRUE),
    mean_rain_2 = mean(mean_rain_2, na.rm = TRUE),
    max_rain_1 = max(max_rain_1, na.rm = TRUE),
    max_rain_2 = max(max_rain_2, na.rm = TRUE),
    max_rain_3 = max(max_rain_3, na.rm = TRUE),
    sum_dry_d1 = sum(dry_d1, na.rm = TRUE),
    sum_dry_d2 = sum(dry_d2, na.rm = TRUE),
    sum_dry_ea = sum(dry_ea, na.rm = TRUE),
    sum_dry_bbc = sum(dry_bbc, na.rm = TRUE),
    total_spills = n(),
    .groups = "drop"
  )

# Save aggregated outputs
saveRDS(monthly_ds, file = file.path(output_path, "monthly_ds.rds"))
saveRDS(quarterly_ds, file = file.path(output_path, "quarterly_ds.rds"))
