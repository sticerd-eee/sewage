# ==============================================================================
# Cleaning Rebuild Contract Tests
# ==============================================================================

# Runnable standalone via plain Rscript; exits non-zero on the first failure.

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

assert_error_matching <- function(expr, pattern, message) {
  err <- tryCatch({ force(expr); NULL }, error = identity)
  assert_true(
    inherits(err, "error") && grepl(pattern, conditionMessage(err), ignore.case = TRUE),
    paste0(
      message,
      if (inherits(err, "error")) paste0("\nGot error: ", conditionMessage(err)) else "\nNo error was raised."
    )
  )
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
  library(tibble)
})

cleaner_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "02_data_cleaning", "clean_lr_house_price_data.R"),
  envir = cleaner_env
)

zoopla_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "02_data_cleaning", "clean_zoopla_data.R"),
  envir = zoopla_env
)

reconcile_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "testing", "reconcile_cleaning_rebuild.R"),
  envir = reconcile_env
)

test_dir <- tempfile("cleaning-rebuild-contracts-")
dir.create(test_dir, recursive = TRUE)
on.exit(unlink(test_dir, recursive = TRUE), add = TRUE)

years <- 2014:2024
raw_dir <- file.path(test_dir, "raw")
dir.create(raw_dir)

# LR input discovery must reject the incomplete vintage before any file is read.
invisible(file.create(file.path(raw_dir, sprintf("pp-%d.csv", years[-1]))))
assert_error_matching(
  cleaner_env$validate_expected_lr_files(raw_dir, years),
  "pp-2014.csv",
  "The exact eleven-file LR vintage must be present before loading starts."
)
invisible(file.create(file.path(raw_dir, "pp-2014.csv")))
expected_paths <- cleaner_env$validate_expected_lr_files(raw_dir, years)
assert_identical(
  basename(expected_paths),
  sprintf("pp-%d.csv", years),
  "LR input discovery must return exact annual filenames in configured order."
)
assert_error_matching(
  cleaner_env$validate_expected_lr_files(raw_dir, 2021:2024),
  "exact 2014-2024",
  "The LR cleaner must not silently shrink its locked long-run window."
)

raw_fixture <- tibble(
  transaction_id = c("{A}", "{B}", "{C}"),
  price = c(100000, 120000, 130000),
  date_of_transfer = as.POSIXct(
    c("2014-02-01 00:00:00", "2021-07-15 00:00:00", "2024-12-31 00:00:00"),
    tz = "UTC"
  ),
  postcode = c("AB12CD", "AB12CD", "XY98ZT"),
  property_type = c("T", "T", "F"),
  old_new = "N",
  duration = "F",
  paon = c("1", "1", "2"),
  saon = NA_character_,
  street = c("HIGH STREET", "HIGH STREET", "LOW STREET"),
  locality = "",
  town_city = "TOWN",
  district = "DISTRICT",
  county = "COUNTY",
  ppd_category = "A",
  record_status = "A",
  qtr_id = c(-27, 3, 16),
  month_id = c(-83, 7, 48)
)

assert_error_matching(
  cleaner_env$validate_lr_transactions(bind_rows(raw_fixture, raw_fixture[1, ]), years),
  "duplicate",
  "Duplicate transaction_id values must abort before outputs are built."
)
missing_id <- raw_fixture
missing_id$transaction_id[1] <- NA_character_
assert_error_matching(
  cleaner_env$validate_lr_transactions(missing_id, years),
  "missing",
  "Missing transaction_id values must abort."
)
empty_id <- raw_fixture
empty_id$transaction_id[1] <- ""
assert_error_matching(
  cleaner_env$validate_lr_transactions(empty_id, years),
  "empty",
  "Empty transaction_id values must abort."
)
outside_window <- raw_fixture
outside_window$date_of_transfer[1] <- as.POSIXct("2013-12-31", tz = "UTC")
assert_error_matching(
  cleaner_env$validate_lr_transactions(outside_window, years),
  "2014.*2024",
  "Dates outside the configured long-run window must abort."
)

postcode_lookup <- tibble(
  postcode = c("AB12CD", "XY98ZT"),
  longitude = c(-1, -2),
  latitude = c(51, 52)
)
assert_error_matching(
  cleaner_env$enrich_lr_postcodes(raw_fixture, bind_rows(postcode_lookup, postcode_lookup[1, ])),
  "duplicate",
  "Postcode enrichment must reject a non-unique lookup."
)

enriched <- cleaner_env$enrich_lr_postcodes(raw_fixture, postcode_lookup)
assert_identical(nrow(enriched), nrow(raw_fixture), "Postcode enrichment must preserve LR row count.")

pair <- cleaner_env$build_lr_output_pair(
  enriched,
  long_run_years = years,
  study_years = 2021:2024
)
assert_identical(nrow(pair$long_run), 3L, "The long-run output must retain every valid LR row.")
assert_identical(nrow(pair$study), 2L, "The study output must be a 2021-2024 filter of the long-run output.")
assert_true(!("transaction_id" %in% names(pair$long_run)), "transaction_id must be absent from the long-run schema.")
assert_true(!("transaction_id" %in% names(pair$study)), "transaction_id must be absent from the study schema.")
assert_true(all(grepl("^[0-9a-f]{16}$", pair$long_run$house_id)), "house_id must be lowercase xxhash64 hex.")
assert_identical(
  pair$long_run$house_id,
  cleaner_env$hash_transaction_id(raw_fixture$transaction_id),
  "house_id must be the deterministic hash of source transaction_id."
)
assert_identical(
  pair$study$house_id,
  pair$long_run$house_id[pair$long_run$house_id %in% pair$study$house_id],
  "Study rows must carry the house_id assigned in the superset."
)
assert_identical(
  pair$study,
  filter(pair$long_run, lubridate::year(.data$date_of_transfer) %in% 2021:2024),
  "The study output must equal a pure filter of the long-run output."
)

# Candidate publication must refuse canonical-looking paths and stamp both files
# with one shared run identifier.
assert_error_matching(
  cleaner_env$write_cleaning_candidate(pair$study, file.path(test_dir, "house_price.parquet"), list()),
  "candidate",
  "The cleaner must refuse writes to canonical output paths."
)

long_path <- file.path(test_dir, "house_price_long_run_candidate.parquet")
study_path <- file.path(test_dir, "house_price_candidate.parquet")
run_stamp <- "2026-08-14T12:34:56Z"
cleaner_env$write_lr_candidates(pair, long_path, study_path, run_stamp = run_stamp)
drifted_pair <- pair
drifted_pair$study$price[1] <- drifted_pair$study$price[1] + 1
assert_error_matching(
  cleaner_env$write_lr_candidates(
    drifted_pair,
    file.path(test_dir, "drifted_long_run_candidate.parquet"),
    file.path(test_dir, "drifted_candidate.parquet"),
    run_stamp = run_stamp
  ),
  "unchanged filter",
  "Candidate publication must reject independently built or drifted study data."
)

long_table <- arrow::read_parquet(long_path, as_data_frame = FALSE)
study_table <- arrow::read_parquet(study_path, as_data_frame = FALSE)
required_metadata <- c(
  "cleaning_manifest_version", "cleaning_run_stamp", "cleaning_market",
  "cleaning_artifact_role", "cleaning_year_min", "cleaning_year_max",
  "cleaning_source_row_count", "cleaning_parent_role"
)
assert_true(all(required_metadata %in% names(long_table$metadata)), "The LR superset must carry the cleaning metadata contract.")
assert_true(all(required_metadata %in% names(study_table$metadata)), "The LR subset must carry the cleaning metadata contract.")
assert_identical(long_table$metadata$cleaning_run_stamp, run_stamp, "The LR superset run stamp must round-trip.")
assert_identical(study_table$metadata$cleaning_run_stamp, run_stamp, "Superset and subset must share one run stamp.")
assert_identical(long_table$metadata$cleaning_artifact_role, "long_run", "The superset metadata must declare its role.")
assert_identical(study_table$metadata$cleaning_parent_role, "long_run", "The subset metadata must declare derivation from the superset.")

# LR reconciliation permits identity/date-period/membership transitions only.
old <- raw_fixture[2:3, ] |>
  mutate(house_id = seq_len(n()), .before = transaction_id)
candidate_long <- pair$long_run
candidate_study <- pair$study

reconciliation <- reconcile_env$reconcile_lr_allowed_deltas(
  old,
  candidate_long,
  candidate_study,
  study_years = 2021:2024
)
assert_identical(nrow(reconciliation$unexpected_value_deltas), 0L, "Unchanged fixture rows must satisfy the LR allowed-delta contract.")
assert_identical(nrow(reconciliation$unexpected_membership_deltas), 0L, "Valid date-based membership must satisfy the LR contract.")

corrected_long <- candidate_long
corrected_id <- cleaner_env$hash_transaction_id("{B}")
corrected_long$date_of_transfer[corrected_long$house_id == corrected_id] <-
  as.POSIXct("2020-12-31", tz = "UTC")
corrected_long$qtr_id[corrected_long$house_id == corrected_id] <- 0L
corrected_long$month_id[corrected_long$house_id == corrected_id] <- 0L
corrected_study <- filter(
  corrected_long,
  lubridate::year(.data$date_of_transfer) %in% 2021:2024
)
corrected <- reconcile_env$reconcile_lr_allowed_deltas(
  old,
  corrected_long,
  corrected_study,
  2021:2024
)
corrected_transition <- filter(corrected$transitions, .data$house_id == corrected_id)
assert_true(corrected_transition$date_changed[[1]], "A same-vintage date correction must be reported.")
assert_true(corrected_transition$membership_changed[[1]], "A date-driven study-membership transition must be reported.")
assert_identical(nrow(corrected$unexpected_value_deltas), 0L, "Date and implied period changes are allowed LR deltas.")
assert_identical(nrow(corrected$unexpected_membership_deltas), 0L, "Date-implied study membership changes are allowed LR deltas.")

changed_price <- candidate_long
changed_price$price[changed_price$house_id == cleaner_env$hash_transaction_id("{B}")] <- 999999
unexpected <- reconcile_env$reconcile_lr_allowed_deltas(old, changed_price, candidate_study, 2021:2024)
assert_true(nrow(unexpected$unexpected_value_deltas) == 1L, "A non-allowed LR value change must be reported.")
assert_true("price" %in% unexpected$unexpected_value_deltas$column, "The reconciliation must name the drifted column.")

duplicate_old <- bind_rows(old, old[1, ] |> mutate(date_of_transfer = as.POSIXct("2020-12-31", tz = "UTC")))
duplicates <- reconcile_env$reconcile_lr_allowed_deltas(duplicate_old, candidate_long, candidate_study, 2021:2024)
assert_true(nrow(duplicates$old_duplicate_ids) == 1L, "Historical duplicate transaction ids must be called out for the gate.")

vintage_report <- reconcile_env$build_file_vintage_report(c(long_path, study_path), "fixture")
assert_true(all(c("vintage", "path", "basename", "size_bytes", "mtime", "sha256") %in% names(vintage_report)), "Vintage reports must include checksums and file facts.")
assert_true(all(grepl("^[0-9a-f]{64}$", vintage_report$sha256)), "Vintage reports must carry SHA-256 checksums.")

message("Cleaning rebuild contract tests passed (LR/U2 slice).")

# Zoopla/U3: rented_est is the sole window field, exact duplicates disappear
# before identity assignment, and the study candidate is a pure superset filter.
zoopla_raw <- tibble(
  zp.Address1 = c("1 HIGH STREET", "1 HIGH STREET", "2 LOW STREET", "3 OLD ROAD", "4 NEW ROAD"),
  zp.Address2 = c(NA, NA, "FLAT A", NA, NA),
  zp.Address3 = NA_character_,
  zp.Postcode = c("AB1 2CD", "AB1 2CD", "XY9 8ZT", "ZZ1 1ZZ", "XY9 8ZT"),
  zp.PropertyType = c("Terraced", "Terraced", "Flat", "Detached", "Flat"),
  zp.Bedrooms = c(2, 2, 1, 3, 2),
  zp.Bathrooms = 1,
  zp.Receptions = 1,
  zp.Floors = 1,
  zp.ListingCreated = as.Date(c("2020-12-01", "2020-12-01", "2020-11-01", "2014-01-01", "2023-01-01")),
  zp.ListingPageViews = c(10, 10, 20, 30, 40),
  zp.ListingPrice = c(1000, 1000, 1100, 900, 1200),
  zp.LatestToRent = as.Date(c("2020-12-31", "2020-12-31", "2021-01-05", "2014-02-01", NA)),
  zp.Rented = as.Date(c("2021-01-02", "2021-01-02", "2020-12-30", NA, "2023-03-01")),
  epc.EnergyEfficiency = c(70, 70, 65, 60, 75),
  epc.EnergyRating = c("C", "C", "D", "D", "C")
)

zoopla_cleaned <- zoopla_env$clean_zoopla_data(
  zoopla_raw,
  years = 2014:2023,
  track_raw_origin = TRUE
)
assert_identical(
  lubridate::year(zoopla_cleaned$rented_est),
  c(2021, 2021, 2020, 2014, 2023),
  "Zoopla long-run selection must be based exclusively on coalesce(rented, latest_to_rent)."
)
assert_identical(
  zoopla_cleaned$rented_est,
  dplyr::coalesce(zoopla_cleaned$rented, zoopla_cleaned$latest_to_rent),
  "rented_est must retain the existing coalesce(rented, latest_to_rent) rule exactly."
)

deduped <- zoopla_env$deduplicate_zoopla_transactions(zoopla_cleaned, zoopla_raw)
assert_identical(deduped$removed_count, 1L, "Zoopla exact-duplicate removal must report removed rows.")
assert_identical(deduped$duplicate_group_count, 1L, "Zoopla exact-duplicate removal must report duplicate groups.")
assert_identical(nrow(deduped$data), 4L, "Zoopla exact duplicates must be removed before ID assignment.")
assert_true(!(".raw_origin_row" %in% names(deduped$data)), "Raw-origin helper columns must not leak into outputs.")
assert_true(
  nrow(deduped$origin_spot_check) == 2L &&
    identical(deduped$origin_spot_check$.raw_origin_row, c(1L, 2L)),
  "The duplicate report must retain a raw-origin spot check for one duplicate group."
)

zoopla_lookup <- tibble(
  postcode = c("AB12CD", "XY98ZT", "ZZ11ZZ"),
  longitude = c(-1, -2, -3),
  latitude = c(51, 52, 53)
)
assert_error_matching(
  zoopla_env$enrich_zoopla_postcodes(
    deduped$data,
    bind_rows(zoopla_lookup, zoopla_lookup[1, ])
  ),
  "duplicate",
  "Zoopla postcode enrichment must reject a non-unique lookup."
)
zoopla_enriched <- zoopla_env$enrich_zoopla_postcodes(deduped$data, zoopla_lookup)
assert_identical(nrow(zoopla_enriched), nrow(deduped$data), "Zoopla postcode enrichment must preserve row count.")

zoopla_pair <- zoopla_env$build_zoopla_output_pair(
  zoopla_enriched,
  long_run_years = 2014:2023,
  study_years = 2021:2023
)
assert_identical(nrow(zoopla_pair$long_run), 4L, "The Zoopla superset must retain every valid deduplicated row.")
assert_identical(nrow(zoopla_pair$study), 2L, "The Zoopla study output must use rented_est, not the old OR filter.")
assert_true(all(grepl("^[0-9a-f]{16}$", zoopla_pair$long_run$rental_id)), "rental_id must be lowercase xxhash64 hex.")
assert_true(!anyDuplicated(zoopla_pair$long_run$rental_id), "The long-run rental_id must be unique.")
assert_true(all(zoopla_pair$study$qtr_id > 0), "The 2021-2023 study candidate cannot contain negative qtr_id values.")
assert_identical(
  zoopla_pair$study,
  filter(zoopla_pair$long_run, lubridate::year(.data$rented_est) %in% 2021:2023),
  "The Zoopla study output must equal a pure filter of the long-run output."
)
assert_identical(
  zoopla_pair$long_run$rental_id,
  zoopla_env$hash_rental_identity(zoopla_pair$long_run),
  "rental_id must hash the locked seven-field post-cleaning composite."
)
assert_true(
  any(is.na(zoopla_pair$long_run$rented)) && any(is.na(zoopla_pair$long_run$latest_to_rent)),
  "The rental identity fixture must exercise both missing date fields."
)
second_pair <- zoopla_env$build_zoopla_output_pair(zoopla_enriched, 2014:2023, 2021:2023)
assert_identical(
  second_pair$long_run$rental_id,
  zoopla_pair$long_run$rental_id,
  "Two Zoopla builds must produce byte-identical rental_id vectors."
)

composite_collision <- bind_rows(
  zoopla_enriched,
  zoopla_enriched[1, ] |> mutate(property_type = "F")
)
assert_error_matching(
  zoopla_env$build_zoopla_output_pair(composite_collision, 2014:2023, 2021:2023),
  "composite",
  "A non-unique seven-field rental identity composite must abort."
)

original_hash_function <- zoopla_env$hash_rental_identity
zoopla_env$hash_rental_identity <- function(data) rep("0000000000000000", nrow(data))
assert_error_matching(
  zoopla_env$build_zoopla_output_pair(zoopla_enriched, 2014:2023, 2021:2023),
  "hash",
  "A rental_id hash collision must abort independently of composite uniqueness."
)
zoopla_env$hash_rental_identity <- original_hash_function

zoopla_long_path <- file.path(test_dir, "zoopla_rentals_long_run_candidate.parquet")
zoopla_study_path <- file.path(test_dir, "zoopla_rentals_candidate.parquet")
zoopla_run_stamp <- "2026-08-14T12:35:57Z"
zoopla_env$write_zoopla_candidates(
  zoopla_pair,
  zoopla_long_path,
  zoopla_study_path,
  run_stamp = zoopla_run_stamp,
  source_row_count = nrow(zoopla_raw),
  removed_duplicate_count = deduped$removed_count
)
zoopla_long_table <- arrow::read_parquet(zoopla_long_path, as_data_frame = FALSE)
zoopla_study_table <- arrow::read_parquet(zoopla_study_path, as_data_frame = FALSE)
assert_identical(zoopla_long_table$metadata$cleaning_run_stamp, zoopla_run_stamp, "The Zoopla superset run stamp must round-trip.")
assert_identical(zoopla_study_table$metadata$cleaning_run_stamp, zoopla_run_stamp, "Zoopla outputs must share one run stamp.")
assert_identical(zoopla_long_table$metadata$cleaning_market, "rentals", "Zoopla metadata must declare the rentals market.")
assert_identical(zoopla_study_table$metadata$cleaning_parent_role, "long_run", "The Zoopla study candidate must declare derivation from the superset.")
assert_identical(zoopla_long_table$metadata$cleaning_removed_duplicate_rows, "1", "Zoopla metadata must record the dedupe count.")

# Reconstruct the historical OR-selected study file from the same cleaned raw
# fixture. It contains one exact duplicate and one row selected solely because
# latest_to_rent is in-window while rented_est (rented) is not.
old_zoopla <- zoopla_cleaned |>
  select(-".raw_origin_row") |>
  filter(
    lubridate::year(.data$latest_to_rent) %in% 2021:2023 |
      lubridate::year(.data$rented) %in% 2021:2023
  ) |>
  left_join(zoopla_lookup, by = "postcode") |>
  mutate(rental_id = row_number(), .before = 1)

zoopla_reconciliation <- reconcile_env$reconcile_zoopla_allowed_deltas(
  old_zoopla,
  zoopla_pair$long_run,
  zoopla_pair$study,
  study_years = 2021:2023
)
assert_identical(zoopla_reconciliation$dedupe_summary$removed_rows[[1]], 1L, "Rental reconciliation must quantify exact-duplicate removal.")
assert_identical(nrow(zoopla_reconciliation$selection_removed), 1L, "Rental reconciliation must quantify the OR-to-rented_est selection shift.")
assert_identical(nrow(zoopla_reconciliation$unexpected_value_deltas), 0L, "Allowed rental rebuild changes must preserve stable columns.")
assert_identical(nrow(zoopla_reconciliation$unexpected_membership_deltas), 0L, "Only dedupe and rented_est selection may change rental membership.")

drifted_zoopla <- zoopla_pair$study
drifted_zoopla$listing_page_views[1] <- drifted_zoopla$listing_page_views[1] + 1
zoopla_value_drift <- reconcile_env$reconcile_zoopla_allowed_deltas(
  old_zoopla,
  zoopla_pair$long_run,
  drifted_zoopla,
  2021:2023
)
assert_true(
  "listing_page_views" %in% zoopla_value_drift$unexpected_value_deltas$column,
  "Rental reconciliation must name non-allowed stable-column drift."
)

missing_zoopla <- zoopla_pair$study[-1, ]
zoopla_member_drift <- reconcile_env$reconcile_zoopla_allowed_deltas(
  old_zoopla,
  zoopla_pair$long_run,
  missing_zoopla,
  2021:2023
)
assert_true(
  nrow(zoopla_member_drift$unexpected_membership_deltas) > 0L,
  "Rental reconciliation must reject membership changes beyond dedupe and rented_est selection."
)

message("Cleaning rebuild contract tests passed (Zoopla/U3 slice).")
