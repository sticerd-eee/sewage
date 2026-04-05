# ==============================================================================
# Spill Map Support Statistics
# ==============================================================================
#
# Purpose: Recompute the descriptive statistics used to interpret the MSOA spill
#          maps and related London/non-London manuscript text.
#
# Author: Jacopo Olivieri
# Date: 2026-03-18
# Date Modified: 2026-03-18
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet
#   - data/processed/agg_spill_stats/agg_spill_dry_yr.parquet
#   - data/processed/unique_spill_sites.parquet
#   - data/raw/shapefiles/msoa_bcg_2021/
#   - data/raw/shapefiles/msoa_population_2021/sapemsoasyoatablefinal.xlsx
#   - data/raw/shapefiles/local_authorities_uk_buc/
#
# Outputs:
#   - Printed report to stdout only
#
# Notes:
#   - Uses the same site-to-MSOA assignment logic as the spill map scripts.
#   - Grey MSOAs are defined as MSOAs with zero aggregated spill count after
#     assigning monitored storm overflows to MSOAs.
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
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
TARGET_YEARS <- 2021:2023
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

print_zero_summary <- function(label, zero_total, zero_no_site, zero_with_site) {
  cat(
    label, ": ", fmt_int(zero_total), " zero-spill MSOAs; ",
    fmt_int(zero_no_site), " with no mapped overflow site (",
    fmt_pct(zero_no_site, zero_total), "); ",
    fmt_int(zero_with_site), " with mapped site(s) but zero spills (",
    fmt_pct(zero_with_site, zero_total), ")\n",
    sep = ""
  )
}

print_area_summary <- function(area_label, row, digits_per_area = 1) {
  cat(area_label, "\n", sep = "")
  cat("  Area (km^2): ", fmt_num(row$area_km2, 1), "\n", sep = "")
  cat("  Population: ", fmt_int(row$population), "\n", sep = "")
  cat("  Mapped overflow sites: ", fmt_int(row$mapped_sites), "\n", sep = "")
  cat("  Total spills: ", fmt_int(row$spill_count_total), "\n", sep = "")
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
    "  Spills per mapped overflow site: ",
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

spill_sites <- rio::import(
  here::here("data", "processed", "unique_spill_sites.parquet"),
  trust = TRUE
) |>
  dplyr::distinct(water_company, site_id, .keep_all = TRUE)

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
regular_joined <- regular_spills |>
  dplyr::filter(year %in% TARGET_YEARS) |>
  dplyr::select(-dplyr::any_of("ngr")) |>
  dplyr::left_join(
    site_lookup |>
      dplyr::select(
        water_company, site_id, msoa_code, msoa_name, matched_msoa, is_london
      ),
    by = c("water_company", "site_id")
  ) |>
  dplyr::mutate(
    matched_msoa = tidyr::replace_na(matched_msoa, FALSE),
    is_london = tidyr::replace_na(is_london, FALSE)
  )

regular_by_msoa <- regular_joined |>
  dplyr::filter(matched_msoa) |>
  dplyr::group_by(msoa_code, msoa_name) |>
  dplyr::summarise(
    spill_count_total = sum(spill_count_yr, na.rm = TRUE),
    distinct_sites = dplyr::n_distinct(site_id),
    .groups = "drop"
  )

regular_map <- msoa_meta |>
  dplyr::left_join(regular_by_msoa, by = c("msoa_code", "msoa_name")) |>
  dplyr::mutate(
    spill_count_total = tidyr::replace_na(spill_count_total, 0),
    distinct_sites = tidyr::replace_na(distinct_sites, 0L)
  )

dry_joined <- dry_spills |>
  dplyr::filter(year %in% TARGET_YEARS) |>
  dplyr::select(-dplyr::any_of("ngr")) |>
  dplyr::mutate(dry_spill_count_total = .data[[DRY_SPILL_COUNT_COL]]) |>
  dplyr::left_join(
    site_lookup |>
      dplyr::select(
        water_company, site_id, msoa_code, msoa_name, matched_msoa, is_london
      ),
    by = c("water_company", "site_id")
  ) |>
  dplyr::mutate(
    matched_msoa = tidyr::replace_na(matched_msoa, FALSE),
    is_london = tidyr::replace_na(is_london, FALSE)
  )

dry_by_msoa <- dry_joined |>
  dplyr::filter(matched_msoa, !is.na(dry_spill_count_total)) |>
  dplyr::group_by(msoa_code, msoa_name) |>
  dplyr::summarise(
    dry_spill_count_total = sum(dry_spill_count_total, na.rm = TRUE),
    distinct_sites = dplyr::n_distinct(site_id),
    .groups = "drop"
  )

dry_map <- msoa_meta |>
  dplyr::left_join(dry_by_msoa, by = c("msoa_code", "msoa_name")) |>
  dplyr::mutate(
    dry_spill_count_total = tidyr::replace_na(dry_spill_count_total, 0),
    distinct_sites = tidyr::replace_na(distinct_sites, 0L)
  )


# ==============================================================================
# 7. Summary Statistics
# ==============================================================================
overall_zero_regular <- regular_map |>
  dplyr::summarise(
    zero_total = sum(spill_count_total == 0),
    zero_no_site = sum(spill_count_total == 0 & distinct_sites == 0),
    zero_with_site = sum(spill_count_total == 0 & distinct_sites > 0)
  )

overall_zero_dry <- dry_map |>
  dplyr::summarise(
    zero_total = sum(dry_spill_count_total == 0),
    zero_no_site = sum(dry_spill_count_total == 0 & distinct_sites == 0),
    zero_with_site = sum(dry_spill_count_total == 0 & distinct_sites > 0)
  )

london_zero_regular <- regular_map |>
  dplyr::filter(is_london) |>
  dplyr::summarise(
    zero_total = sum(spill_count_total == 0),
    zero_no_site = sum(spill_count_total == 0 & distinct_sites == 0),
    zero_with_site = sum(spill_count_total == 0 & distinct_sites > 0)
  )

london_zero_dry <- dry_map |>
  dplyr::filter(is_london) |>
  dplyr::summarise(
    zero_total = sum(dry_spill_count_total == 0),
    zero_no_site = sum(dry_spill_count_total == 0 & distinct_sites == 0),
    zero_with_site = sum(dry_spill_count_total == 0 & distinct_sites > 0)
  )

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

regular_totals <- regular_joined |>
  dplyr::filter(matched_msoa) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(
    spill_count_total = sum(spill_count_yr, na.rm = TRUE),
    .groups = "drop"
  )

dry_totals <- dry_joined |>
  dplyr::filter(matched_msoa, !is.na(dry_spill_count_total)) |>
  dplyr::group_by(is_london) |>
  dplyr::summarise(
    dry_spill_count_total = sum(dry_spill_count_total, na.rm = TRUE),
    .groups = "drop"
  )

comparison_summary <- area_summary |>
  dplyr::left_join(site_summary, by = "is_london") |>
  dplyr::left_join(regular_totals, by = "is_london") |>
  dplyr::left_join(dry_totals, by = "is_london") |>
  dplyr::mutate(
    area_label = dplyr::if_else(is_london, "London", "Non-London"),
    site_share_of_england = safe_divide(mapped_sites, sum(mapped_sites)),
    msoa_share_of_england = safe_divide(msoas, sum(msoas)),
    sites_per_100km2 = safe_divide(mapped_sites, area_km2) * 100,
    spills_per_100km2 = safe_divide(spill_count_total, area_km2) * 100,
    dry_spills_per_100km2 = safe_divide(dry_spill_count_total, area_km2) * 100,
    spills_per_site = safe_divide(spill_count_total, mapped_sites),
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

print_section("Overall Zero-Spill Interpretation")
print_zero_summary(
  label = "All spills",
  zero_total = overall_zero_regular$zero_total,
  zero_no_site = overall_zero_regular$zero_no_site,
  zero_with_site = overall_zero_regular$zero_with_site
)
print_zero_summary(
  label = "Dry spills",
  zero_total = overall_zero_dry$zero_total,
  zero_no_site = overall_zero_dry$zero_no_site,
  zero_with_site = overall_zero_dry$zero_with_site
)

print_section("London-Specific Zero-Spill Interpretation")
print_zero_summary(
  label = "London all spills",
  zero_total = london_zero_regular$zero_total,
  zero_no_site = london_zero_regular$zero_no_site,
  zero_with_site = london_zero_regular$zero_with_site
)
print_zero_summary(
  label = "London dry spills",
  zero_total = london_zero_dry$zero_total,
  zero_no_site = london_zero_dry$zero_no_site,
  zero_with_site = london_zero_dry$zero_with_site
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
  "Spills per mapped overflow site: London ",
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
