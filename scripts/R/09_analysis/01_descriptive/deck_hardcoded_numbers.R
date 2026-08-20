# ==============================================================================
# Deck Hardcoded-Number Audit
# ==============================================================================
#
# Purpose: Recompute the descriptive counts quoted directly in short_pres.tex
#          and the corresponding manuscript data prose.
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet
#   - data/processed/cross_section/sales/study_period/
#   - data/processed/cross_section/rentals/study_period/
#
# Outputs:
#   - Printed report to stdout only
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

Sys.setenv(ARROW_USER_SIMD_LEVEL = "NONE")

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c("arrow", "dplyr", "here")

TARGET_YEARS <- 2021:2024
SALES_WINDOW_DAYS <- 1461L
RENTAL_WINDOW_DAYS <- 1095L
RADIUS_M <- 1000L
STAGE_2_CUTOFF <- as.POSIXct(
  "2026-08-19 15:00:00",
  tz = "Europe/Rome"
)

PATH_SPILLS <- here::here(
  "data", "processed", "agg_spill_stats", "agg_spill_yr.parquet"
)
PATH_SALES <- here::here(
  "data", "processed", "cross_section", "sales", "study_period"
)
PATH_RENTALS <- here::here(
  "data", "processed", "cross_section", "rentals", "study_period"
)

check_required_packages(REQUIRED_PACKAGES)
suppressPackageStartupMessages(library(dplyr))

fmt_int <- function(x) {
  formatC(as.numeric(x), format = "d", big.mark = ",")
}

fmt_million <- function(x) {
  formatC(as.numeric(x) / 1e6, format = "f", digits = 3)
}

check_stage_2_input <- function(path) {
  input_mtime <- file.info(path)$mtime

  if (is.na(input_mtime) || input_mtime < STAGE_2_CUTOFF) {
    stop(
      "Stage-2 input is missing or predates the canonical rebuild cutoff: ",
      path,
      call. = FALSE
    )
  }

  cat("  Stage-2 input mtime: ", format(input_mtime), " (", path, ")\n", sep = "")
}

summarise_property_panel <- function(path, id_col, expected_days, label) {
  dat <- arrow::open_dataset(path) |>
    dplyr::filter(.data$radius == RADIUS_M) |>
    dplyr::select(
      dplyr::all_of(id_col),
      "n_days_in_window",
      "n_spill_sites",
      "spatially_eligible",
      "has_missing_site",
      "spill_count"
    ) |>
    dplyr::collect()

  if (anyDuplicated(dat[[id_col]]) > 0L) {
    stop(label, " has duplicate transaction IDs at radius ", RADIUS_M, "m.")
  }
  if (!identical(unique(dat$n_days_in_window), expected_days)) {
    stop(label, " does not use the expected ", expected_days, "-day window.")
  }

  near <- dat$spatially_eligible & dat$n_spill_sites > 0L
  complete_exposure <- near & !dat$has_missing_site & !is.na(dat$spill_count)

  tibble::tibble(
    panel = label,
    window_days = expected_days,
    total_transactions = nrow(dat),
    within_1km = sum(near),
    within_1km_complete_exposure = sum(complete_exposure),
    within_1km_incomplete_reporting = sum(near & !complete_exposure)
  )
}

cat("Stage-2 provenance checks\n")
check_stage_2_input(PATH_SALES)
check_stage_2_input(PATH_RENTALS)

cat("\nSpill totals, 2021--2024\n")
spills <- arrow::read_parquet(PATH_SPILLS) |>
  dplyr::filter(.data$year %in% TARGET_YEARS)

spill_summary <- spills |>
  dplyr::summarise(
    site_groups = dplyr::n_distinct(.data$site_id),
    site_years = dplyr::n(),
    complete_site_years = sum(
      .data$annual_status %in% c("reported_positive", "reported_zero")
    ),
    reporting_gap_site_years = sum(
      .data$annual_status %in% c("reported_na", "absent")
    ),
    spill_events = sum(.data$spill_count_yr, na.rm = TRUE),
    spill_hours = sum(.data$spill_hrs_yr, na.rm = TRUE)
  )

print(spill_summary)
cat(
  "  Deck scale: ", fmt_million(spill_summary$spill_events),
  " million events; ", fmt_million(spill_summary$spill_hours),
  " million spill-hours\n",
  sep = ""
)

cat("\nProperty counts from canonical study-period cross-sections\n")
property_summary <- dplyr::bind_rows(
  summarise_property_panel(PATH_SALES, "house_id", SALES_WINDOW_DAYS, "Sales"),
  summarise_property_panel(PATH_RENTALS, "rental_id", RENTAL_WINDOW_DAYS, "Rentals")
)
print(property_summary)

for (i in seq_len(nrow(property_summary))) {
  row <- property_summary[i, ]
  cat(
    "  ", row$panel, ": ", fmt_int(row$total_transactions),
    " total; ", fmt_int(row$within_1km), " within 1km; ",
    fmt_int(row$within_1km_complete_exposure),
    " within 1km with a complete reporting window; ",
    fmt_int(row$within_1km_incomplete_reporting),
    " within 1km excluded for incomplete reporting\n",
    sep = ""
  )
}
