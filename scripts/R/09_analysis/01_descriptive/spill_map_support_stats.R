# ==============================================================================
# Spill Map Support Statistics
# ==============================================================================
#
# Purpose: Recompute the descriptive statistics used to interpret the MSOA spill
#          maps and related London/non-London manuscript text.
#
# Author: Jacopo Olivieri
# Date: 2026-03-18
# Date Modified: 2026-10-13
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet
#   - data/processed/agg_spill_stats/agg_spill_dry_yr.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#   - data/raw/shapefiles/msoa_bcg_2021/
#   - data/raw/shapefiles/msoa_population_2021/sapemsoasyoatablefinal.xlsx
#   - data/raw/shapefiles/local_authorities_uk_buc/
#
# Outputs:
#   - Printed report to stdout only
#
# Notes:
#   - Uses the same site-to-MSOA assignment logic as the spill map scripts.
#   - Missing years are excluded (see
#     scripts/R/utils/spill_missing_utils.R). Light-grey MSOAs have zero storm
#     overflows or zero spills. No-data MSOAs have only overflows with no
#     usable year.
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "dplyr",
  "tidyr",
  "stringr",
  "sf",
  "here",
  "rio",
  "readxl"
)

# ==============================================================================
# 1. Configuration
# ==============================================================================
TARGET_YEARS <- 2021:2024
DRY_SPILL_COUNT_COL <- "dry_spill_count_yr_r1_d01_weak"
LONDON_LAD_PREFIX <- "^E09"


# ==============================================================================
# 2. Package Management
# ==============================================================================
check_required_packages(REQUIRED_PACKAGES)

initialise_environment <- function() {
  invisible(
    lapply(
      REQUIRED_PACKAGES,
      function(pkg) suppressPackageStartupMessages(
        library(pkg, character.only = TRUE)
      )
    )
  )
  invisible(suppressMessages(sf::sf_use_s2(FALSE)))
}

initialise_environment()
source(here::here("scripts", "R", "utils", "site_group_utils.R"))
source(here::here("scripts", "R", "utils", "spill_missing_utils.R"))


# ==============================================================================
# 3. Helper Functions
# ==============================================================================
safe_divide <- function(numerator, denominator) {
  ifelse(denominator > 0, numerator / denominator, NA_real_)
}

fmt_int <- function(x) {
  formatC(as.numeric(x), format = "d", big.mark = ",")
}

fmt_num <- function(x, digits = 1) {
  formatC(as.numeric(x), format = "f", digits = digits, big.mark = ",")
}

fmt_pct <- function(numerator, denominator, digits = 1) {
  paste0(fmt_num(100 * safe_divide(numerator, denominator), digits = digits), "%")
}

print_section <- function(title) {
  cat("\n", title, "\n", strrep("-", nchar(title)), "\n", sep = "")
}

print_zero_summary <- function(label, zero_total, zero_no_site, zero_with_site,
                               no_data) {
  cat(
    label, ": ", fmt_int(zero_total), " light-grey (zero-spill) MSOAs; ",
    fmt_int(zero_no_site), " with no mapped overflow site (",
    fmt_pct(zero_no_site, zero_total), "); ",
    fmt_int(zero_with_site), " with mapped site(s) but zero spills in all ",
    "non-missing years (", fmt_pct(zero_with_site, zero_total), "). ",
    "Separately, ", fmt_int(no_data),
    " MSOAs have mapped site(s) but no non-missing group-year (no data)\n",
    sep = ""
  )
}

print_class_counts <- function(label, map_data) {
  counts <- map_data |>
    dplyr::count(spill_class, .drop = FALSE)
  cat(
    label, ": ",
    paste0(counts$spill_class, " = ", fmt_int(counts$n), collapse = "; "),
    " (total ", fmt_int(sum(counts$n)), ")\n",
    sep = ""
  )
}

print_area_summary <- function(area_label, row, digits_per_area = 1) {
  cat(area_label, "\n", sep = "")
  cat("  Area (km^2): ", fmt_num(row$area_km2, 1), "\n", sep = "")
  cat("  Population: ", fmt_int(row$population), "\n", sep = "")
  cat("  Mapped overflow sites: ", fmt_int(row$mapped_sites), "\n", sep = "")
  cat(
    "  Mapped overflows with spill data (>= 1 non-missing year): ",
    fmt_int(row$sites_with_data), "\n",
    sep = ""
  )
  cat(
    "  Total spills (non-missing group-years): ",
    fmt_int(row$spill_count_total),
    " (excludes ", fmt_int(row$excluded_spills),
    " spills from ", fmt_int(row$excluded_group_years),
    " missing-status group-years with matched events)\n",
    sep = ""
  )
  cat("  Total dry spills: ", fmt_int(row$dry_spill_count_total), "\n", sep = "")
  cat(
    "  Site density (sites per 100 km^2): ",
    fmt_num(row$sites_per_100km2, digits_per_area), "\n",
    sep = ""
  )
  cat(
    "  Spill density (spills per 100 km^2): ",
    fmt_num(row$spills_per_100km2, digits_per_area), "\n",
    sep = ""
  )
  cat(
    "  Dry spill density (dry spills per 100 km^2): ",
    fmt_num(row$dry_spills_per_100km2, digits_per_area), "\n",
    sep = ""
  )
  cat(
    "  Spills per overflow with spill data: ",
    fmt_num(row$spills_per_site, 1), "\n",
    sep = ""
  )
  cat(
    "  Spills per 100,000 residents: ",
    fmt_num(row$spills_per_100k_people, 1), "\n",
    sep = ""
  )
  cat(
    "  Dry spills per 100,000 residents: ",
    fmt_num(row$dry_spills_per_100k_people, 2), "\n",
    sep = ""
  )
  cat(
    "  Population-density-weighted spills (spill * km^2 / population): ",
    fmt_num(row$spills_pop_density_weighted, 3), "\n",
    sep = ""
  )
  cat(
    "  Population-density-weighted dry spills (dry spill * km^2 / population): ",
    fmt_num(row$dry_spills_pop_density_weighted, 4), "\n",
    sep = ""
  )
}


# ==============================================================================
# 4. Data Loading
# ==============================================================================
cat("Loading inputs...\n")

regular_spills <- rio::import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_yr.parquet")
)

dry_spills <- rio::import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_dry_yr.parquet")
)

spill_sites <- read_site_group_projection(
  here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  years = 2021:2024
)

msoa_boundaries <- sf::st_read(
  here::here("data", "raw", "shapefiles", "msoa_bcg_2021"),
  quiet = TRUE
) |>
  dplyr::filter(stringr::str_detect(MSOA21CD, "^E")) |>
  dplyr::select(MSOA21CD, MSOA21NM, geometry) |>
  dplyr::rename(msoa_code = MSOA21CD, msoa_name = MSOA21NM) |>
  dplyr::mutate(area_km2 = as.numeric(sf::st_area(geometry)) / 1000000)

msoa_population <- readxl::read_excel(
  here::here(
    "data", "raw", "shapefiles", "msoa_population_2021",
    "sapemsoasyoatablefinal.xlsx"
  ),
  sheet = "Mid-2022 MSOA 2021",
  range = "A4:E7268"
) |>
  dplyr::select(msoa_code = 3, msoa_population = 5) |>
  dplyr::filter(!is.na(msoa_code), !is.na(msoa_population))

london_lads <- sf::st_read(
  here::here(
    "data", "raw", "shapefiles", "local_authorities_uk_buc",
    "Local_Authority_Districts_(May_2025)_Boundaries_UK_BUC.shp"
  ),
  quiet = TRUE
) |>
  dplyr::filter(stringr::str_detect(LAD25CD, LONDON_LAD_PREFIX)) |>
  dplyr::select(LAD25CD, LAD25NM, geometry)

cat("Inputs loaded.\n")


# ==============================================================================
# 5. Spatial Lookups
# ==============================================================================
msoa_london_lookup <- suppressWarnings(sf::st_point_on_surface(msoa_boundaries)) |>
  sf::st_join(london_lads, join = sf::st_within, left = TRUE) |>
  sf::st_drop_geometry() |>
  dplyr::transmute(msoa_code, is_london = !is.na(LAD25CD))

msoa_meta <- msoa_boundaries |>
  sf::st_drop_geometry() |>
  dplyr::left_join(msoa_london_lookup, by = "msoa_code") |>
  dplyr::left_join(msoa_population, by = "msoa_code") |>
  dplyr::mutate(is_london = tidyr::replace_na(is_london, FALSE))

site_lookup <- spill_sites |>
  dplyr::filter(!is.na(easting), !is.na(northing)) |>
  sf::st_as_sf(coords = c("easting", "northing"), crs = 27700, remove = FALSE) |>
  sf::st_join(
    msoa_boundaries |> dplyr::select(msoa_code, msoa_name),
    join = sf::st_within,
    left = TRUE
  ) |>
  sf::st_drop_geometry() |>
  dplyr::left_join(
    msoa_meta |> dplyr::select(msoa_code, is_london),
    by = "msoa_code"
  ) |>
  dplyr::mutate(
    is_london = tidyr::replace_na(is_london, FALSE),
    matched_msoa = !is.na(msoa_code)
  )


# ==============================================================================
# 6. Aggregate to MSOA
# ==============================================================================
missing_flags <- flag_missing_group_years(regular_spills)

site_msoa_lookup <- site_lookup |>
  dplyr::select(site_id, msoa_code, msoa_name, matched_msoa, is_london)

# Aggregate to MSOA and classify (helpers in spill_missing_utils.R)
build_msoa_map <- function(joined, count_col) {
  site_averages <- joined |>
    dplyr::filter(matched_msoa) |>
    summarise_overflow_annual_averages(count_col) |>
    dplyr::left_join(site_msoa_lookup, by = "site_id")

  # Reclassified below once MSOAs with no overflow are filled in
  by_msoa <- aggregate_overflow_averages_to_msoa(site_averages, count_col) |>
    dplyr::select(-spill_class)

  msoa_meta |>
    dplyr::left_join(by_msoa, by = c("msoa_code", "msoa_name")) |>
    dplyr::mutate(
      site_count = tidyr::replace_na(site_count, 0L),
      sites_with_data = tidyr::replace_na(sites_with_data, 0L),
      summed_average = tidyr::replace_na(summed_average, 0),
      spill_class = classify_msoa_spill_status(
        site_count, sites_with_data, summed_average
      )
    )
}

regular_in_scope <- regular_spills |>
  dplyr::filter(year %in% TARGET_YEARS) |>
  dplyr::select(-dplyr::any_of("ngr")) |>
  attach_missing_group_year_flag(missing_flags)
regular_joined <- left_join_site_group_projection(
  regular_in_scope,
  site_msoa_lookup,
  context = "Regular map-support Site Group join"
) |>
  dplyr::mutate(
    matched_msoa = tidyr::replace_na(matched_msoa, FALSE),
    is_london = tidyr::replace_na(is_london, FALSE)
  )

regular_map <- build_msoa_map(regular_joined, "spill_count_yr")

# Dry columns are zero-filled upstream, so the missing flag comes from the
# main yearly aggregates
dry_in_scope <- dry_spills |>
  dplyr::filter(year %in% TARGET_YEARS) |>
  dplyr::select(-dplyr::any_of("ngr")) |>
  dplyr::mutate(dry_spill_count_yr = .data[[DRY_SPILL_COUNT_COL]]) |>
  attach_missing_group_year_flag(missing_flags)
dry_joined <- left_join_site_group_projection(
  dry_in_scope,
  site_msoa_lookup,
  context = "Dry map-support Site Group join"
) |>
  dplyr::mutate(
    matched_msoa = tidyr::replace_na(matched_msoa, FALSE),
    is_london = tidyr::replace_na(is_london, FALSE)
  )

dry_map <- build_msoa_map(dry_joined, "dry_spill_count_yr")


# ==============================================================================
# 7. Summary Statistics
# ==============================================================================
summarise_classes <- function(map_data) {
  map_data |>
    dplyr::summarise(
      zero_total = sum(spill_class %in% c("no_overflow", "zero")),
      zero_no_site = sum(spill_class == "no_overflow"),
      zero_with_site = sum(spill_class == "zero"),
      no_data = sum(spill_class == "no_data"),
      positive = sum(spill_class == "positive")
    )
}

overall_zero_regular <- summarise_classes(regular_map)
overall_zero_dry <- summarise_classes(dry_map)
london_zero_regular <- summarise_classes(dplyr::filter(regular_map, is_london))
london_zero_dry <- summarise_classes(dplyr::filter(dry_map, is_london))

area_summary <- msoa_meta |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(
    msoas = dplyr::n(),
    area_km2 = sum(area_km2, na.rm = TRUE),
    population = sum(msoa_population, na.rm = TRUE),
    .groups = "drop"
  )

site_summary <- site_lookup |>
  dplyr::filter(matched_msoa) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(mapped_sites = dplyr::n_distinct(site_id), .groups = "drop")

# Overflows with spill data (denominator for per-overflow statistics) and
# spills excluded as missing group-years
sites_with_data_summary <- regular_joined |>
  dplyr::filter(matched_msoa) |>
  dplyr::group_by(is_london, site_id) |>
  dplyr::summarise(
    has_data = any(!is_missing_group_year & !is.na(spill_count_yr)),
    .groups = "drop"
  ) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(sites_with_data = sum(has_data), .groups = "drop")

excluded_totals <- regular_joined |>
  dplyr::filter(matched_msoa, is_missing_group_year, !is.na(spill_count_yr)) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(
    excluded_group_years = dplyr::n(),
    excluded_spills = sum(spill_count_yr),
    .groups = "drop"
  )

regular_totals <- regular_joined |>
  dplyr::filter(matched_msoa, !is_missing_group_year) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(
    spill_count_total = sum(spill_count_yr, na.rm = TRUE),
    .groups = "drop"
  )

dry_totals <- dry_joined |>
  dplyr::filter(matched_msoa, !is_missing_group_year) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(
    dry_spill_count_total = sum(dry_spill_count_yr, na.rm = TRUE),
    .groups = "drop"
  )

comparison_summary <- area_summary |>
  dplyr::left_join(site_summary, by = "is_london") |>
  dplyr::left_join(sites_with_data_summary, by = "is_london") |>
  dplyr::left_join(excluded_totals, by = "is_london") |>
  dplyr::left_join(regular_totals, by = "is_london") |>
  dplyr::left_join(dry_totals, by = "is_london") |>
  dplyr::mutate(
    excluded_group_years = tidyr::replace_na(excluded_group_years, 0L),
    excluded_spills = tidyr::replace_na(excluded_spills, 0),
    area_label = dplyr::if_else(is_london, "London", "Non-London"),
    site_share_of_england = safe_divide(mapped_sites, sum(mapped_sites)),
    msoa_share_of_england = safe_divide(msoas, sum(msoas)),
    sites_per_100km2 = safe_divide(mapped_sites, area_km2) * 100,
    spills_per_100km2 = safe_divide(spill_count_total, area_km2) * 100,
    dry_spills_per_100km2 = safe_divide(dry_spill_count_total, area_km2) * 100,
    spills_per_site = safe_divide(spill_count_total, sites_with_data),
    spills_per_100k_people = safe_divide(spill_count_total, population) * 100000,
    dry_spills_per_100k_people =
      safe_divide(dry_spill_count_total, population) * 100000,
    spills_pop_density_weighted =
      safe_divide(spill_count_total * area_km2, population),
    dry_spills_pop_density_weighted =
      safe_divide(dry_spill_count_total * area_km2, population)
  ) |>
  dplyr::arrange(is_london)


# ==============================================================================
# 8. Printed Report
# ==============================================================================
cat("\n")
cat("Spill Map Support Statistics\n")
cat("===========================\n")
cat("Years: ", min(TARGET_YEARS), "-", max(TARGET_YEARS), "\n", sep = "")
cat(
  "London definition: MSOAs whose point-on-surface falls within LAD codes ",
  "matching ", LONDON_LAD_PREFIX, ".\n",
  sep = ""
)
cat(
  "Dry spill series: ", DRY_SPILL_COUNT_COL,
  " (same definition used in the spill map workflow).\n",
  sep = ""
)
cat(
  "Missing group-years (absent/reported_na returns, or reported_positive ",
  "with no matched events) are excluded; spill totals sum non-missing ",
  "group-years only.\n",
  sep = ""
)

print_section("MSOA Classes (per-overflow averages over non-missing years)")
print_class_counts("All spills", regular_map)
print_class_counts("Dry spills", dry_map)
print_class_counts("London all spills", dplyr::filter(regular_map, is_london))
print_class_counts("London dry spills", dplyr::filter(dry_map, is_london))

print_section("Overall Zero-Spill Interpretation")
print_zero_summary(
  label = "All spills",
  zero_total = overall_zero_regular$zero_total,
  zero_no_site = overall_zero_regular$zero_no_site,
  zero_with_site = overall_zero_regular$zero_with_site,
  no_data = overall_zero_regular$no_data
)
print_zero_summary(
  label = "Dry spills",
  zero_total = overall_zero_dry$zero_total,
  zero_no_site = overall_zero_dry$zero_no_site,
  zero_with_site = overall_zero_dry$zero_with_site,
  no_data = overall_zero_dry$no_data
)

print_section("London-Specific Zero-Spill Interpretation")
print_zero_summary(
  label = "London all spills",
  zero_total = london_zero_regular$zero_total,
  zero_no_site = london_zero_regular$zero_no_site,
  zero_with_site = london_zero_regular$zero_with_site,
  no_data = london_zero_regular$no_data
)
print_zero_summary(
  label = "London dry spills",
  zero_total = london_zero_dry$zero_total,
  zero_no_site = london_zero_dry$zero_no_site,
  zero_with_site = london_zero_dry$zero_with_site,
  no_data = london_zero_dry$no_data
)

print_section("London Versus Non-London")
cat(
  "London share of English MSOAs: ",
  fmt_int(comparison_summary$msoas[comparison_summary$is_london]),
  " / ",
  fmt_int(sum(comparison_summary$msoas)),
  " (",
  fmt_pct(
    comparison_summary$msoas[comparison_summary$is_london],
    sum(comparison_summary$msoas)
  ),
  ")\n",
  sep = ""
)
cat(
  "London share of mapped overflow sites: ",
  fmt_int(comparison_summary$mapped_sites[comparison_summary$is_london]),
  " / ",
  fmt_int(sum(comparison_summary$mapped_sites)),
  " (",
  fmt_pct(
    comparison_summary$mapped_sites[comparison_summary$is_london],
    sum(comparison_summary$mapped_sites)
  ),
  ")\n",
  sep = ""
)

london_row <- comparison_summary |>
  dplyr::filter(is_london) |>
  dplyr::slice(1)

non_london_row <- comparison_summary |>
  dplyr::filter(!is_london) |>
  dplyr::slice(1)

print_area_summary("London", london_row)
print_area_summary("Non-London", non_london_row)

print_section("Manuscript Ready Comparisons")
cat(
  "Overflow sites per 100 km^2: London ",
  fmt_num(london_row$sites_per_100km2, 1),
  " versus non-London ",
  fmt_num(non_london_row$sites_per_100km2, 1),
  "\n",
  sep = ""
)
cat(
  "Spills per 100 km^2: London ",
  fmt_num(london_row$spills_per_100km2, 1),
  " versus non-London ",
  fmt_num(non_london_row$spills_per_100km2, 1),
  "\n",
  sep = ""
)
cat(
  "Dry spills per 100 km^2: London ",
  fmt_num(london_row$dry_spills_per_100km2, 1),
  " versus non-London ",
  fmt_num(non_london_row$dry_spills_per_100km2, 1),
  "\n",
  sep = ""
)
cat(
  "Spills per overflow with spill data: London ",
  fmt_num(london_row$spills_per_site, 1),
  " versus non-London ",
  fmt_num(non_london_row$spills_per_site, 1),
  "\n",
  sep = ""
)
cat(
  "Spills per 100,000 residents: London ",
  fmt_num(london_row$spills_per_100k_people, 1),
  " versus non-London ",
  fmt_num(non_london_row$spills_per_100k_people, 1),
  "\n",
  sep = ""
)
cat(
  "Dry spills per 100,000 residents: London ",
  fmt_num(london_row$dry_spills_per_100k_people, 2),
  " versus non-London ",
  fmt_num(non_london_row$dry_spills_per_100k_people, 2),
  "\n",
  sep = ""
)
cat(
  "Population-density-weighted spills: London ",
  fmt_num(london_row$spills_pop_density_weighted, 3),
  " versus non-London ",
  fmt_num(non_london_row$spills_pop_density_weighted, 3),
  "\n",
  sep = ""
)
cat(
  "Population-density-weighted dry spills: London ",
  fmt_num(london_row$dry_spills_pop_density_weighted, 4),
  " versus non-London ",
  fmt_num(non_london_row$dry_spills_pop_density_weighted, 4),
  "\n",
  sep = ""
)
