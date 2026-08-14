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
