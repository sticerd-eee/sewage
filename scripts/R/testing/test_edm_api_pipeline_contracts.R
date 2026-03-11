assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
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

read_text <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

source(here::here("scripts", "config", "api_config.R"), local = TRUE)
contract <- get_edm_api_contract()

assert_identical(
  contract$scope,
  "england_only",
  "The live EDM API contract should be explicitly England-only."
)
assert_true(
  length(contract$company_ids) == 9L,
  "The live EDM API contract should define nine in-scope companies."
)
assert_true(
  !"welsh_water" %in% contract$company_ids,
  "Welsh Water should remain out of scope for the live API contract."
)
assert_identical(
  contract$raw_directory,
  here::here("data", "raw", "edm_data", "raw_api_responses"),
  "The live EDM API raw snapshot path should match the declared contract."
)
assert_identical(
  contract$processed_directory,
  here::here("data", "processed", "edm_api_data"),
  "The processed EDM API path should match the declared contract."
)
assert_identical(
  contract$combined_file,
  here::here("data", "processed", "edm_api_data", "combined_api_data.parquet"),
  "The combined EDM API Parquet path should match the declared contract."
)

fetch_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "01_data_ingestion", "fetch_edm_api_data_2024_onwards.R"),
  local = fetch_env
)

process_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "02_data_cleaning", "process_edm_api_json_to_parquet_2024_onwards.R"),
  local = process_env
)

combine_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "02_data_cleaning", "combine_api_edm_data_2024_onwards.R"),
  local = combine_env
)

integration_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "05_data_integration", "combine_2021-2023_and_api_edm_data.R"),
  local = integration_env
)

assert_identical(
  fetch_env$CONFIG$base_save_directory,
  contract$raw_directory,
  "The fetch script raw snapshot path should match the EDM API contract."
)
assert_identical(
  process_env$CONFIG$input_dir,
  contract$raw_directory,
  "The process script input path should match the EDM API contract."
)
assert_identical(
  process_env$CONFIG$output_dir,
  contract$processed_directory,
  "The process script output path should match the EDM API contract."
)
assert_identical(
  combine_env$CONFIG$input_dir,
  contract$processed_directory,
  "The combine script input path should match the EDM API contract."
)
assert_identical(
  combine_env$CONFIG$output_file,
  contract$combined_file,
  "The combine script output path should match the EDM API contract."
)
assert_identical(
  integration_env$CONFIG$api_data_file,
  contract$combined_file,
  "The 2021-2023/API integration script should consume the contracted combined API file."
)

raw_dir <- tempfile("edm-api-raw-")
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(raw_dir, recursive = TRUE, force = TRUE), add = TRUE)

dir.create(file.path(raw_dir, "anglian_water"))
dir.create(file.path(raw_dir, "thames_water"))
dir.create(file.path(raw_dir, "welsh_water"))

raw_resolution <- process_env$resolve_company_directories(raw_dir, contract)
assert_identical(
  basename(raw_resolution$company_dirs),
  c("anglian_water", "thames_water"),
  "Raw directory resolution should keep only in-scope company folders."
)
assert_identical(
  raw_resolution$status$unexpected_company_ids,
  "welsh_water",
  "Raw directory resolution should flag Welsh Water as an out-of-scope artifact."
)
assert_true(
  "northumbrian_water" %in% raw_resolution$status$missing_company_ids,
  "Raw directory resolution should report missing in-scope companies explicitly."
)

processed_dir <- tempfile("edm-api-processed-")
dir.create(processed_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(processed_dir, recursive = TRUE, force = TRUE), add = TRUE)

example_company_data <- dplyr::tibble(
  water_company = "anglian_water",
  attributes_id = "site-1",
  attributes_latest_event_start = as.character(
    as.numeric(as.POSIXct("2024-01-02 00:00:00", tz = "UTC")) * 1000
  ),
  attributes_latest_event_end = as.character(
    as.numeric(as.POSIXct("2024-01-02 01:00:00", tz = "UTC")) * 1000
  ),
  attributes_latitude = 51.5,
  attributes_longitude = -0.1,
  attributes_receiving_water_course = "River Test"
)

arrow::write_parquet(
  example_company_data,
  file.path(processed_dir, "anglian_water.parquet")
)
arrow::write_parquet(
  example_company_data,
  file.path(processed_dir, "welsh_water.parquet")
)

resolved_parquets <- combine_env$resolve_input_parquet_files(
  input_dir = processed_dir,
  file_pattern = combine_env$CONFIG$input_file_pattern,
  exclude_pattern = combine_env$CONFIG$exclude_pattern,
  contract = contract
)

assert_identical(
  basename(resolved_parquets$parquet_files),
  "anglian_water.parquet",
  "Parquet resolution should keep only in-scope company files."
)
assert_identical(
  resolved_parquets$status$unexpected_company_ids,
  "welsh_water",
  "Parquet resolution should flag Welsh Water as an out-of-scope artifact."
)

schema_test_dir <- tempfile("edm-api-schema-")
dir.create(schema_test_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(schema_test_dir, recursive = TRUE, force = TRUE), add = TRUE)

arrow::write_parquet(
  dplyr::mutate(example_company_data, irrelevant_col = 1),
  file.path(schema_test_dir, "anglian_water.parquet")
)
arrow::write_parquet(
  dplyr::mutate(
    example_company_data,
    water_company = "thames_water",
    irrelevant_col = I(list(c("x", "y")))
  ),
  file.path(schema_test_dir, "thames_water.parquet")
)

schema_combined <- combine_env$read_and_combine_parquet_files(
  input_dir = schema_test_dir,
  file_pattern = combine_env$CONFIG$input_file_pattern,
  exclude_pattern = combine_env$CONFIG$exclude_pattern,
  contract = contract
)

assert_true(
  !is.null(schema_combined),
  "The combine step should ignore irrelevant extra columns when reading in-scope Parquet files."
)
assert_identical(
  names(schema_combined),
  combine_env$REQUIRED_INPUT_COLUMNS,
  "The combine step should read only the required Parquet columns before binding."
)
assert_identical(
  sort(unique(schema_combined$water_company)),
  c("anglian_water", "thames_water"),
  "The combine step should still combine multiple in-scope company Parquet files."
)

clean_input <- dplyr::tibble(
  water_company = "anglian_water",
  attributes_id = "site-1",
  attributes_latest_event_start = as.character(
    as.numeric(as.POSIXct("2023-12-31 23:30:00", tz = "UTC")) * 1000
  ),
  attributes_latest_event_end = as.character(
    as.numeric(as.POSIXct("2024-01-01 01:30:00", tz = "UTC")) * 1000
  ),
  attributes_latitude = 51.5,
  attributes_longitude = -0.1,
  attributes_receiving_water_course = "River Test"
)

cleaned <- combine_env$clean_combined_data(clean_input)
assert_identical(
  cleaned$water_company[[1]],
  "Anglian Water",
  "The combine step should normalise snake_case company identifiers to display names."
)
assert_identical(
  cleaned$year[[1]],
  2024L,
  "The combine step should derive year with an explicit lubridate dependency."
)
assert_identical(
  as.numeric(cleaned$start_time[[1]]),
  as.numeric(as.POSIXct("2024-01-01 00:00:00", tz = "UTC")),
  "The combine step should truncate pre-2024 starts to the live API contract start date."
)

timestamp_log_file <- tempfile("edm-api-combine-log-", fileext = ".log")
combine_env$setup_logging(timestamp_log_file, console = FALSE, threshold = "INFO")

timestamp_input <- dplyr::tibble(
  water_company = c("anglian_water", "thames_water"),
  attributes_id = c("site-1", "site-2"),
  attributes_latest_event_start = c(
    as.character(as.numeric(as.POSIXct("2024-01-02 00:00:00", tz = "UTC")) * 1000),
    "not-a-timestamp"
  ),
  attributes_latest_event_end = c(
    as.character(as.numeric(as.POSIXct("2024-01-02 01:00:00", tz = "UTC")) * 1000),
    as.character(as.numeric(as.POSIXct("2024-01-02 03:00:00", tz = "UTC")) * 1000)
  ),
  attributes_latitude = c(51.5, 51.6),
  attributes_longitude = c(-0.1, -0.2),
  attributes_receiving_water_course = c("River Test", "River Thames")
)

timestamp_cleaned <- combine_env$clean_combined_data(timestamp_input)
timestamp_log_text <- read_text(timestamp_log_file)

assert_identical(
  nrow(timestamp_cleaned),
  1L,
  "Malformed timestamp rows should still be dropped from the cleaned combine output."
)
assert_true(
  grepl(
    "Timestamp parse summary for attributes_latest_event_start: 0 missing values, 1 unparseable values.",
    timestamp_log_text,
    fixed = TRUE
  ),
  "The combine step should log malformed start-time counts explicitly."
)
assert_true(
  grepl(
    "Dropping 1 rows with missing or unparseable timestamps and 0 rows ending before",
    timestamp_log_text,
    fixed = TRUE
  ),
  "The combine step should log timestamp-driven row drops explicitly."
)

failure_script <- tempfile("combine-main-failure-", fileext = ".R")
writeLines(
  c(
    paste0(
      "source(",
      dQuote(here::here(
        "scripts",
        "R",
        "02_data_cleaning",
        "combine_api_edm_data_2024_onwards.R"
      )),
      ", local = TRUE)"
    ),
    "LOG_FILE <- tempfile('combine-failure-', fileext = '.log')",
    "CONFIG$input_dir <- tempfile('missing-edm-dir-')",
    "main()"
  ),
  failure_script
)
failure_run <- suppressWarnings(system2(
  "Rscript",
  c("--vanilla", failure_script),
  stdout = TRUE,
  stderr = TRUE
))
failure_status <- attr(failure_run, "status")

assert_true(
  !is.null(failure_status) && failure_status != 0L,
  "The combine script should exit non-zero when main() encounters a fatal error."
)

existing_output <- dplyr::tibble(
  water_company = "Anglian Water",
  unique_id = "existing-site",
  latitude = 51.5,
  longitude = -0.1,
  receiving_water_course = "River Test",
  start_time = as.POSIXct("2024-01-02 00:00:00", tz = "UTC"),
  end_time = as.POSIXct("2024-01-02 01:00:00", tz = "UTC"),
  year = 2024L
)

no_input_dir <- tempfile("edm-api-empty-")
dir.create(no_input_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(no_input_dir, recursive = TRUE, force = TRUE), add = TRUE)

existing_output_path <- file.path(tempdir(), "combined_api_existing.parquet")
arrow::write_parquet(existing_output, existing_output_path)

combine_no_input_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "02_data_cleaning", "combine_api_edm_data_2024_onwards.R"),
  local = combine_no_input_env
)
combine_no_input_env$CONFIG$input_dir <- no_input_dir
combine_no_input_env$CONFIG$output_file <- existing_output_path
combine_no_input_env$LOG_FILE <- tempfile("combine-no-input-", fileext = ".log")

no_input_error <- tryCatch(
  {
    combine_no_input_env$main()
    NULL
  },
  error = identity
)

assert_true(
  inherits(no_input_error, "error"),
  "The combine step should fail instead of publishing when no in-scope Parquet files are available."
)
assert_identical(
  arrow::read_parquet(existing_output_path),
  existing_output,
  "The combine step should leave the existing canonical output untouched on the no-input path."
)

zero_row_dir <- tempfile("edm-api-zero-row-")
dir.create(zero_row_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(zero_row_dir, recursive = TRUE, force = TRUE), add = TRUE)

zero_row_input <- dplyr::tibble(
  water_company = "anglian_water",
  attributes_id = "site-3",
  attributes_latest_event_start = as.character(
    as.numeric(as.POSIXct("2023-12-30 00:00:00", tz = "UTC")) * 1000
  ),
  attributes_latest_event_end = as.character(
    as.numeric(as.POSIXct("2023-12-30 01:00:00", tz = "UTC")) * 1000
  ),
  attributes_latitude = 51.7,
  attributes_longitude = -0.3,
  attributes_receiving_water_course = "River Test"
)
arrow::write_parquet(
  zero_row_input,
  file.path(zero_row_dir, "anglian_water.parquet")
)

zero_row_output_path <- file.path(tempdir(), "combined_api_zero_row_existing.parquet")
arrow::write_parquet(existing_output, zero_row_output_path)

combine_zero_row_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "02_data_cleaning", "combine_api_edm_data_2024_onwards.R"),
  local = combine_zero_row_env
)
combine_zero_row_env$CONFIG$input_dir <- zero_row_dir
combine_zero_row_env$CONFIG$output_file <- zero_row_output_path
combine_zero_row_env$LOG_FILE <- tempfile("combine-zero-row-", fileext = ".log")

zero_row_error <- tryCatch(
  {
    combine_zero_row_env$main()
    NULL
  },
  error = identity
)

assert_true(
  inherits(zero_row_error, "error"),
  "The combine step should fail instead of publishing when cleaning removes every row."
)
assert_identical(
  arrow::read_parquet(zero_row_output_path),
  existing_output,
  "The combine step should leave the existing canonical output untouched on the zero-row path."
)

readme_text <- read_text(here::here("README.md"))
assert_true(
  grepl("data/raw/edm_data/raw_api_responses", readme_text, fixed = TRUE),
  "README should document the raw API snapshot path."
)
assert_true(
  grepl("England-only", readme_text, fixed = TRUE),
  "README should describe the live API scope explicitly."
)
assert_true(
  !grepl("all 10 WaSCs", readme_text, fixed = TRUE),
  "README should not retain the stale ten-company live API claim."
)

ingestion_text <- read_text(here::here("book", "data_clean_documentation", "02_ingestion.qmd"))
assert_true(
  grepl("England-only", ingestion_text, fixed = TRUE),
  "The ingestion chapter should describe the live API scope explicitly."
)
assert_true(
  grepl("data/raw/edm_data/raw_api_responses", ingestion_text, fixed = TRUE),
  "The ingestion chapter should document the raw API snapshot path."
)
assert_true(
  !grepl("appquarto publish gh-pagesended", ingestion_text, fixed = TRUE),
  "The ingestion chapter should not retain garbled prose."
)

test_api_rmd_text <- read_text(here::here("scripts", "R", "testing", "test_api.Rmd"))
assert_true(
  grepl("test_fetch_edm_api_data_2024_onwards.R", test_api_rmd_text, fixed = TRUE),
  "The fetch notebook should wrap the current fetch regression script."
)
assert_true(
  !grepl("install.packages", test_api_rmd_text, fixed = TRUE),
  "The fetch notebook should not preserve runtime package installation logic."
)

test_combine_rmd_text <- read_text(
  here::here("scripts", "R", "testing", "test_06_combine_api_edm_data_2024_onwards.Rmd")
)
assert_true(
  grepl("test_edm_api_pipeline_contracts.R", test_combine_rmd_text, fixed = TRUE),
  "The combine notebook should wrap the current contract regression script."
)
assert_true(
  !grepl("Welsh Water", test_combine_rmd_text, fixed = TRUE),
  "The combine notebook should not retain the stale ten-company assumption."
)

integration_notebook_text <- read_text(
  here::here("scripts", "R", "testing", "test_combine_2021-2023_and_api_edm_data.rmd")
)
assert_true(
  !grepl("seq(10)", integration_notebook_text, fixed = TRUE),
  "The exploratory 2021-2023/API integration notebook should not hard-code a ten-company loop."
)

cat("All EDM API pipeline contract tests passed.\n")
