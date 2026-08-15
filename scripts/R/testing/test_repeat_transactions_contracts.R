# ==============================================================================
# Repeat Transaction Pipeline Contract Tests
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
    inherits(err, "error") && grepl(pattern, conditionMessage(err)),
    paste0(
      message,
      if (inherits(err, "error")) {
        paste0("\nGot error: ", conditionMessage(err))
      } else {
        "\nNo error was raised."
      }
    )
  )
}

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(here)
})

source(here::here("scripts", "R", "utils", "hash_utils.R"))
source(here::here("scripts", "R", "utils", "repeat_transactions_utils.R"))

test_dir <- tempfile("repeat-contracts-")
dir.create(test_dir, recursive = TRUE)
on.exit(unlink(test_dir, recursive = TRUE), add = TRUE)

fixture <- data.table(
  house_id = c("01abc", "02def", "03ghi"),
  postcode = c("SW1A 1AA", "SW1A 1AA", "E1 6AN"),
  paon = c("10", "10", "7"),
  saon = c(NA_character_, NA_character_, "FLAT 2"),
  street = c("ST. JOHN'S ROAD", "ST JOHNS ROAD", "BRICK LANE"),
  date_of_transfer = as.Date(c("2018-01-01", "2022-01-01", "2023-03-15")),
  price = c(100000, 120000, 450000),
  property_type = c("T", "T", "F")
)

make_config <- function(name = "base", coverage_floor = 0) {
  list(
    id_col = "house_id",
    date_col = "date_of_transfer",
    price_col = "price",
    postcode_col = "postcode",
    address_cols = c("postcode", "paon", "saon", "street"),
    primary_address_col = "paon",
    property_type_col = "property_type",
    input_path = file.path(test_dir, paste0(name, "-input.parquet")),
    output_path = file.path(test_dir, paste0(name, "-mapping.parquet")),
    large_group_review_path = file.path(test_dir, paste0(name, "-large.parquet")),
    price_ratio_review_path = file.path(test_dir, paste0(name, "-ratios.parquet")),
    market = "sales",
    log_name = paste0("repeat-contract-", name),
    year_min = 2014L,
    year_max = 2024L,
    key_coverage_floor = coverage_floor,
    repeat_share_floor = 0,
    large_group_size = 12L,
    extreme_annualized_price_ratio = 4
  )
}

# Shared hash serialization is deterministic and type-stable.
hash_input <- data.table(
  postcode = c("AB12CD", "AB12CD"),
  address_line_01 = c("1 HIGH STREET", "1 HIGH STREET"),
  address_line_02 = c(NA_character_, NA_character_),
  address_line_03 = c("", ""),
  listing_price = c(1200, 1200),
  latest_to_rent = as.Date(c("2022-01-03", "2022-01-03")),
  rented = as.Date(c(NA, NA))
)
hashes <- hash_rental_identity(hash_input)
assert_identical(hashes[[1]], hashes[[2]], "Equal rental composites must hash equally.")
assert_true(all(grepl("^[0-9a-f]{16}$", hashes)), "Hashes must be lowercase 16-character hex.")
expected_rental_serialization <- paste(
  "AB12CD", "1 HIGH STREET", HASH_NA_TOKEN, "", "1200", "2022-01-03",
  HASH_NA_TOKEN,
  sep = HASH_FIELD_SEPARATOR
)
assert_identical(
  serialize_hash_fields(hash_input[1], names(hash_input)),
  expected_rental_serialization,
  "Rental identity serialization is a locked public-ID preimage contract."
)
assert_identical(
  hashes[[1]],
  "61ec4f1ca85dfdb2",
  "The locked rental identity fixture must retain its xxhash64 value."
)
assert_identical(
  hash_transaction_id("{ABC}"),
  "094a16add8cb5eb0",
  "The locked Land Registry transaction fixture must retain its xxhash64 value."
)
assert_identical(
  hash_transaction_id(c("{ABC}", "{ABC}")),
  rep(hash_transaction_id("{ABC}"), 2L),
  "Transaction-id hashing must be deterministic and vectorised."
)
reserved_input <- data.table(value = paste0("unsafe", HASH_FIELD_SEPARATOR))
assert_error_matching(
  serialize_hash_fields(reserved_input, "value"),
  "reserved",
  "Hash serialization must reject reserved tokens in source fields."
)

# Happy path, punctuation normalization, missing-component handling, and singles.
base_result <- build_repeat_mapping(fixture, make_config())
assert_identical(nrow(base_result$mapping), 3L, "All keyable transactions, including singles, must be mapped.")
assert_identical(sort(unique(base_result$mapping$repeat_count)), c(1L, 2L), "Repeat counts must include groups of one and two.")
assert_identical(uniqueN(base_result$mapping$repeat_id), 2L, "Equivalent punctuated addresses must share a repeat id.")
assert_true(!grepl("NA", base_result$keyed_data$address_key[[1]], fixed = TRUE), "A missing address component must not inject literal NA into the key.")
assert_identical(
  base_result$keyed_data$repeat_id,
  hash_serialized_values(base_result$keyed_data$address_key),
  "Each repeat id must equal the hash of its normalized address key."
)
assert_identical(
  base_result$keyed_data$address_key[[1]],
  "SW1A 1AA|10||ST JOHNS ROAD",
  "The normalized repeat-address preimage is a locked contract."
)
assert_identical(
  base_result$keyed_data$repeat_id[[1]],
  "89f5f526a23b82ad",
  "The locked repeat-address fixture must retain its xxhash64 value."
)

sorted_mapping <- function(x) setorder(copy(x), house_id)[]
shuffled_result <- build_repeat_mapping(fixture[c(3, 1, 2)], make_config("shuffled"))
second_result <- build_repeat_mapping(copy(fixture), make_config("second"))
assert_identical(sorted_mapping(base_result$mapping), sorted_mapping(shuffled_result$mapping), "Row order must not affect mappings.")
assert_identical(sorted_mapping(base_result$mapping), sorted_mapping(second_result$mapping), "Repeated runs must yield value-identical mappings.")

missing_fixture <- rbind(
  fixture,
  data.table(
    house_id = "04jkl", postcode = "N1 1AA", paon = NA_character_,
    saon = NA_character_, street = "UPPER STREET",
    date_of_transfer = as.Date("2023-06-01"), price = 500000,
    property_type = "F"
  )
)
missing_result <- build_repeat_mapping(missing_fixture, make_config("missing"))
assert_identical(missing_result$metrics$excluded_count, 1L, "Exactly one missing-primary row must be excluded.")
assert_true(!("04jkl" %in% missing_result$mapping$house_id), "Rows without a primary address must not enter the mapping.")

singles <- copy(fixture)
singles[, `:=`(postcode = c("A1", "B1", "C1"), paon = c("1", "2", "3"))]
singles_result <- build_repeat_mapping(singles, make_config("singles"))
assert_true(all(singles_result$mapping$repeat_count == 1L), "Zero-repeat input must retain every single with repeat_count one.")
assert_true(all(grepl("^[0-9a-f]{16}$", singles_result$mapping$repeat_id)), "Singles must have hashed repeat ids without sentinels.")

duplicate_ids <- rbind(fixture, fixture[1])
assert_error_matching(
  build_repeat_mapping(duplicate_ids, make_config("duplicate-ids")),
  "unique",
  "Duplicate input ids must abort."
)
duplicate_rows <- rbind(fixture, fixture[1])
duplicate_rows[.N, house_id := "04jkl"]
duplicate_config <- make_config("duplicate-rows")
duplicate_config$duplicate_check_cols <- setdiff(names(fixture), "house_id")
assert_error_matching(
  build_repeat_mapping(duplicate_rows, duplicate_config),
  "exact duplicates",
  "Full-field duplicate checks must abort even when transaction ids differ."
)
assert_error_matching(
  build_repeat_mapping(missing_fixture, make_config("coverage", coverage_floor = 0.9)),
  "coverage",
  "Coverage below the configured floor must abort."
)

original_hash_serialized_values <- hash_serialized_values
assign(
  "hash_serialized_values",
  function(x) rep("0000000000000000", length(x)),
  envir = .GlobalEnv
)
assert_error_matching(
  build_repeat_mapping(fixture, make_config("collision")),
  "one-to-one",
  "Distinct address keys that collide must abort."
)
assign(
  "hash_serialized_values",
  original_hash_serialized_values,
  envir = .GlobalEnv
)

large_fixture <- fixture[rep(1, 15)]
large_fixture[, `:=`(
  house_id = sprintf("large-%02d", seq_len(.N)),
  date_of_transfer = as.Date("2010-01-01") + seq_len(.N) * 365,
  price = 100000 + seq_len(.N) * 1000
)]
large_config <- make_config("large")
large_config$year_min <- 2010L
large_config$year_max <- 2030L
large_result <- build_repeat_mapping(large_fixture, large_config)
assert_identical(nrow(large_result$large_groups), 1L, "A 15-transaction group must be routed to review without failing.")

mixed_fixture <- copy(fixture)
mixed_fixture[2, property_type := "F"]
warnings <- character()
mixed_result <- withCallingHandlers(
  build_repeat_mapping(mixed_fixture, make_config("mixed-types")),
  warning = function(w) {
    warnings <<- c(warnings, conditionMessage(w))
    invokeRestart("muffleWarning")
  }
)
assert_true(any(grepl("property type", warnings, ignore.case = TRUE)), "Mixed property types must warn rather than fail.")
assert_identical(nrow(mixed_result$mapping), 3L, "Warn-only diagnostics must not remove mapped rows.")

# Arrow integration: literal schema, review files, manifest round-trip, and first-generation log.
logs <- character()
integration_config <- make_config("integration")
arrow::write_parquet(fixture, integration_config$input_path)
run_result <- run_repeat_transactions(
  integration_config,
  log_fn = function(level, message) logs <<- c(logs, paste(level, message))
)
assert_true(any(grepl("first generation", logs, ignore.case = TRUE)), "The first run must log that manifest diffing was skipped.")
assert_true(file.exists(integration_config$output_path), "The mapping parquet must be written.")
assert_true(file.exists(integration_config$large_group_review_path), "The large-group review parquet must be written even when empty.")
assert_true(file.exists(integration_config$price_ratio_review_path), "The price-ratio review parquet must be written even when empty.")

written <- arrow::read_parquet(integration_config$output_path, as_data_frame = FALSE)
assert_identical(names(written), c("house_id", "repeat_id", "repeat_count"), "The mapping must have exactly the declared three columns.")
assert_identical(written$schema$field(0)$type$ToString(), "string", "The transaction id Arrow type must be utf8/string.")
assert_identical(written$schema$field(1)$type$ToString(), "string", "repeat_id Arrow type must be utf8/string.")
assert_identical(written$schema$field(2)$type$ToString(), "int32", "repeat_count Arrow type must be int32.")

manifest <- written$metadata
expected_manifest_keys <- c(
  "repeat_manifest_version", "input_path", "input_row_count", "input_mtime",
  "run_timestamp", "keyed_count", "excluded_count", "key_coverage",
  "repeat_share", "largest_group_size", "market", "repeat_count_semantics"
)
assert_true(all(expected_manifest_keys %in% names(manifest)), "Every required manifest key must round-trip through Arrow metadata.")
assert_identical(manifest$input_path, normalizePath(integration_config$input_path), "Manifest input_path must identify the production-read source.")
assert_identical(manifest$input_row_count, "3", "Manifest input row count must round-trip as text.")
assert_identical(manifest$keyed_count, "3", "Manifest keyed count must round-trip as text.")
assert_identical(manifest$excluded_count, "0", "Manifest excluded count must round-trip as text.")

rerun_logs <- character()
run_repeat_transactions(
  integration_config,
  log_fn = function(level, message) rerun_logs <<- c(rerun_logs, paste(level, message))
)
assert_true(any(grepl("manifest delta", rerun_logs, ignore.case = TRUE)), "A compatible prior manifest must produce a logged delta.")

corrupt_manifest_path <- file.path(test_dir, "corrupt-previous.parquet")
writeBin(charToRaw("not parquet"), corrupt_manifest_path)
assert_error_matching(
  read_repeat_manifest(corrupt_manifest_path),
  "unreadable",
  "An existing unreadable repeat mapping must fail closed."
)

# Both thin entry scripts must accept an in-memory fixture and isolated outputs.
sales_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "06_analysis_datasets", "repeat_sales.R"),
  envir = sales_env
)
sales_entry_config <- sales_env$repeat_sales_config(test_dir)
sales_entry_config$input_path <- file.path(test_dir, "sales-entry-input.parquet")
sales_entry_config$log_file <- file.path(test_dir, "sales-entry.log")
assert_identical(
  c(sales_entry_config$key_coverage_floor, sales_entry_config$repeat_share_floor),
  c(0.99, 0.35),
  "Sales production floors must remain locked below the accepted baseline."
)
sales_env$main(sales_entry_config, data = fixture)
assert_true(
  file.exists(sales_entry_config$output_path),
  "The repeat-sales entry script must run a fixture end-to-end."
)

rentals_fixture <- data.table(
  rental_id = c("rental-01", "rental-02", "rental-03"),
  postcode = c("SW1A 1AA", "SW1A 1AA", "E1 6AN"),
  address_line_01 = c("10 High Street", "10 HIGH STREET", "7 Brick Lane"),
  address_line_02 = c(NA_character_, NA_character_, "Flat 2"),
  address_line_03 = NA_character_,
  listing_price = c(1200, 1300, 1800),
  latest_to_rent = as.Date(c("2019-01-01", "2022-01-01", "2023-01-01")),
  rented = as.Date(c("2019-01-15", "2022-01-15", "2023-01-15")),
  rented_est = as.Date(c("2019-01-15", "2022-01-15", "2023-01-15")),
  property_type = c("T", "T", "F")
)
rentals_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "06_analysis_datasets", "repeat_rentals.R"),
  envir = rentals_env
)
rentals_entry_config <- rentals_env$repeat_rentals_config(test_dir)
rentals_entry_config$input_path <- file.path(test_dir, "rentals-entry-input.parquet")
rentals_entry_config$log_file <- file.path(test_dir, "rentals-entry.log")
assert_identical(
  c(rentals_entry_config$key_coverage_floor, rentals_entry_config$repeat_share_floor),
  c(0.99, 0.70),
  "Rental production floors must remain locked below the accepted baseline."
)
rentals_env$main(rentals_entry_config, data = rentals_fixture)
assert_true(
  file.exists(rentals_entry_config$output_path),
  "The repeat-rentals entry script must run a fixture end-to-end."
)

cat("Repeat transaction contract tests passed.\n")
