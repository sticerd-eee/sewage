# ==============================================================================
# Canonical unique_spill_sites Contract Tests
# ==============================================================================
#
# Runnable standalone via plain Rscript; exits non-zero on the first failure.
# The public seam is `build_unique_spill_sites()`, with focused checks of its
# fail-closed lookup and Site Group membership boundaries.
#
# ==============================================================================

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(
      paste0(
        message,
        "\nExpected: ", paste(capture.output(str(expected)), collapse = " "),
        "\nActual: ", paste(capture.output(str(actual)), collapse = " ")
      ),
      call. = FALSE
    )
  }
}

assert_error <- function(expression, pattern, message) {
  error <- tryCatch(
    {
      force(expression)
      NULL
    },
    error = identity
  )
  if (is.null(error) || !grepl(pattern, conditionMessage(error), perl = TRUE)) {
    stop(
      paste0(
        message,
        if (is.null(error)) "\nNo error was raised." else
          paste0("\nActual error: ", conditionMessage(error))
      ),
      call. = FALSE
    )
  }
}

suppressPackageStartupMessages({
  library(dplyr)
  library(here)
  library(tibble)
  library(tidyr)
})

script_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "03_data_enrichment", "create_unique_spill_sites.R"),
  local = script_env
)

years <- 2021:2024

make_lookup <- function(ids = c(100L, 101L, 200L)) {
  tibble(
    site_id = ids,
    component = seq_along(ids),
    site_id_2021 = ids + 1000L,
    site_id_2022 = ids + 2000L,
    site_id_2023 = ids + 3000L,
    site_id_2024 = ids + 4000L
  )
}

make_crosswalk <- function() {
  bind_rows(
    tibble(
      site_id = 100L,
      year = years,
      water_company = "Test Water",
      site_id_canonical_members = "100;101"
    ),
    tibble(
      site_id = 200L,
      year = years,
      water_company = "Other Water",
      site_id_canonical_members = "200"
    )
  )
}

annual_row <- function(canonical_id,
                       year,
                       water_company,
                       ngr = "TQ 30000 80000",
                       commission = NA_character_,
                       operation_percent = NA_real_,
                       operation_reason = NA_character_,
                       spill_hrs = NA_real_,
                       spill_count = NA_real_,
                       lookup = make_lookup()) {
  row <- tibble(
    year = as.integer(year),
    water_company = water_company,
    outlet_discharge_ngr = ngr,
    edm_commission_date = commission,
    edm_operation_percent = operation_percent,
    edm_operation_reason = operation_reason,
    spill_hrs_ea = spill_hrs,
    spill_count_ea = spill_count,
    site_id_2021 = NA_integer_,
    site_id_2022 = NA_integer_,
    site_id_2023 = NA_integer_,
    site_id_2024 = NA_integer_
  )
  lookup_row <- lookup |> filter(site_id == canonical_id)
  row[[paste0("site_id_", year)]] <- lookup_row[[paste0("site_id_", year)]]
  row
}

lookup <- make_lookup()
crosswalk <- make_crosswalk()

annual <- bind_rows(
  # Multi-member group: metadata and commission evidence remain independent.
  annual_row(
    100L, 2021L, "Test Water", "TQ 30000 80000", "Mar 2021",
    95, "Operating", 1, 1
  ),
  annual_row(
    101L, 2021L, "Test Water", "TQ 40000 80000",
    "To be installed by December 2023", 80, "Operating", NA, NA
  ),
  # Reported row with missing spill metrics is still available.
  annual_row(
    101L, 2023L, "Test Water", "not a grid reference", NA,
    90, "Reduced coverage", NA, NA
  ),
  # Explicit NLO history: absent 2023 is not carried forward; 2024 may return.
  annual_row(
    200L, 2022L, "Other Water", "TQ 50000 80000", NA,
    50, "No longer operational", 0, 0
  ),
  annual_row(
    200L, 2024L, "Renamed Water", "TQ 70000 80000", NA,
    75, "Operating", 2, 1
  ),
  # Same-year operation contradictions fail closed, including NLO detection
  # from the raw reason history before the conflict is collapsed.
  annual_row(
    200L, 2022L, "Other Water", "TQ 50000 80000", NA,
    55, "Maintenance", 0, 0
  )
)

result <- script_env$build_unique_spill_sites(
  annual_data = annual,
  lookup_data = lookup,
  crosswalk_data = crosswalk,
  years = years
)

# ----------------------------------------------------------------------------
# Identity, grain, ordering, and exact lookup coverage (R1-R4; AE1)
# ----------------------------------------------------------------------------

assert_identical(nrow(result), 3L, "Output must contain one row per lookup ID.")
assert_identical(
  result$site_id_canonical,
  c(100L, 101L, 200L),
  "Output must be ordered by Site Group ID, then canonical ID."
)
assert_identical(
  result$site_id,
  c(100L, 100L, 200L),
  "Multi-member groups must repeat only the Site Group ID."
)
assert_true(
  !anyNA(result$site_id_canonical) && !anyDuplicated(result$site_id_canonical),
  "site_id_canonical must be complete and unique."
)
assert_identical(
  sort(result$site_id_canonical),
  sort(as.integer(lookup$site_id)),
  "Canonical output coverage must exactly equal the lookup universe."
)

site_100 <- result |> filter(site_id_canonical == 100L)
site_101 <- result |> filter(site_id_canonical == 101L)
site_200 <- result |> filter(site_id_canonical == 200L)

assert_identical(
  as.character(site_100$edm_commission_date),
  "2021-03-01",
  "Canonical member 100 must retain its own resolved commission history."
)
assert_identical(
  site_100$edm_commission_resolution_status,
  "resolved",
  "Canonical member 100 should resolve independently."
)
assert_identical(
  site_101$edm_commission_resolution_status,
  "future_only",
  "Canonical member 101 must not inherit another group member's date."
)

# ----------------------------------------------------------------------------
# Row-presence availability and first explicit NLO year (R5-R6; AE2-AE3)
# ----------------------------------------------------------------------------

assert_identical(
  as.logical(site_101$available_year_2023),
  TRUE,
  "A reported row with missing spill metrics must be available."
)
assert_identical(
  as.logical(site_101$available_year_2024),
  FALSE,
  "An absent canonical site-year must be unavailable."
)
assert_identical(
  unname(as.logical(site_200[paste0("available_year_", 2022:2024)])),
  c(TRUE, FALSE, TRUE),
  "NLO must not fill absent years or suppress a later return."
)
assert_identical(
  site_200$no_longer_operational_year,
  2022L,
  "The first explicit raw NLO observation must be retained."
)
assert_true(
  !"nlo_carryforward_year" %in% names(result),
  "The removed carry-forward field must not remain in the canonical schema."
)

# ----------------------------------------------------------------------------
# Canonical operation conflicts and most-recent parseable location (R4, R17)
# ----------------------------------------------------------------------------

assert_true(
  is.na(site_200$edm_operation_percent_2022) &&
    isTRUE(site_200$edm_operation_percent_conflict_2022),
  "Conflicting operation percentages must become NA plus a conflict flag."
)
assert_true(
  is.na(site_200$edm_operation_reason_2022) &&
    isTRUE(site_200$edm_operation_reason_conflict_2022),
  "Conflicting operation reasons must become NA plus a conflict flag."
)
assert_identical(
  site_101$ngr,
  "TQ4000080000",
  "A newer unparseable NGR must not shadow the most recent parseable location."
)
assert_true(
  !is.na(site_101$easting) && !is.na(site_101$northing),
  "The retained canonical NGR must have parsed coordinates."
)

validation <- script_env$build_canonical_metadata_validation(
  script_env$map_annual_to_canonical_sites(annual, lookup, years),
  large_coordinate_movement_m = 1000
)
validation_101 <- validation |> filter(site_id_canonical == 101L)
validation_200 <- validation |> filter(site_id_canonical == 200L)
assert_identical(
  validation_101$n_parseable_locations,
  1L,
  "Missing coordinates must not count as a distinct parseable location."
)
assert_true(
  isTRUE(validation_200$water_company_changed),
  "Company changes must enter canonical validation evidence."
)
assert_true(
  isTRUE(validation_200$large_coordinate_movement),
  "Large coordinate movement must enter canonical validation evidence."
)

# ----------------------------------------------------------------------------
# Fail-closed membership and lookup coverage
# ----------------------------------------------------------------------------

membership <- script_env$build_canonical_membership(crosswalk, lookup)
assert_identical(
  membership$site_id_canonical,
  c(100L, 101L, 200L),
  "Membership must include multi-member and singleton canonical sites."
)

missing_membership <- crosswalk |> filter(site_id != 200L)
assert_error(
  script_env$build_canonical_membership(missing_membership, lookup),
  "coverage.*lookup|lookup.*coverage",
  "A lookup ID with no Site Group membership must stop."
)

duplicate_membership <- bind_rows(
  crosswalk,
  tibble(
    site_id = 101L,
    year = years,
    water_company = "Test Water",
    site_id_canonical_members = "101"
  )
)
assert_error(
  script_env$build_canonical_membership(duplicate_membership, lookup),
  "multiple Site Groups|representative",
  "A canonical site mapped to multiple Site Groups must stop."
)

cross_company_membership <- bind_rows(
  crosswalk,
  tibble(
    site_id = 100L,
    year = 2024L,
    water_company = "Wrong Water",
    site_id_canonical_members = "100;101"
  )
)
assert_error(
  script_env$build_canonical_membership(cross_company_membership, lookup),
  "multiple companies|company",
  "Cross-company membership evidence must stop."
)

unknown_annual <- annual_row(
  100L, 2021L, "Test Water", lookup = lookup
) |>
  mutate(site_id_2021 = 999999L)
assert_error(
  script_env$map_annual_to_canonical_sites(unknown_annual, lookup, years),
  "without lookup coverage",
  "Annual rows without lookup coverage must not fall back to raw IDs."
)

duplicated_lookup <- lookup
duplicated_lookup$site_id_2021[2] <- duplicated_lookup$site_id_2021[1]
assert_error(
  script_env$map_annual_to_canonical_sites(annual, duplicated_lookup, years),
  "duplicated.*year-site|unique",
  "Ambiguous lookup year IDs must stop rather than select the first match."
)

# ----------------------------------------------------------------------------
# Exported formats retain the declared schema and row count
# ----------------------------------------------------------------------------

temp_dir <- tempfile("canonical-site-contract-")
dir.create(temp_dir)
parquet_path <- file.path(temp_dir, "unique_spill_sites.parquet")
excel_path <- file.path(temp_dir, "unique_spill_sites.xlsx")
script_env$export_data(result, excel_path, parquet_path)
parquet_result <- arrow::read_parquet(parquet_path)
excel_result <- rio::import(excel_path, trust = TRUE)
assert_identical(
  names(parquet_result),
  names(result),
  "Parquet must retain the declared canonical schema."
)
assert_identical(
  names(excel_result),
  names(result),
  "Excel must retain the declared canonical schema."
)
assert_identical(
  c(nrow(parquet_result), nrow(excel_result)),
  c(nrow(result), nrow(result)),
  "Both output formats must contain the complete canonical inventory."
)

cat("All canonical unique_spill_sites contract tests passed.\n")
