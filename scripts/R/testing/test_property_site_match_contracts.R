# ==============================================================================
# Property-Site Match Producer Contract Tests
# ==============================================================================
#
# Quick deterministic contracts shared by the house-sale and rental producers.
# Run from the repository root with plain Rscript.
#
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(glue)
  library(here)
  library(logger)
  library(purrr)
  library(sf)
  library(tibble)
})

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(
      message,
      "\nActual: ", paste(capture.output(str(actual)), collapse = " "),
      "\nExpected: ", paste(capture.output(str(expected)), collapse = " "),
      call. = FALSE
    )
  }
}

assert_error_contains <- function(expression, expected, message) {
  error_message <- tryCatch(
    {
      force(expression)
      NA_character_
    },
    error = function(error) conditionMessage(error)
  )
  if (is.na(error_message) || !grepl(expected, error_message, fixed = TRUE)) {
    stop(message, "\nActual error: ", error_message, call. = FALSE)
  }
}

capture_warning_messages <- function(expression) {
  messages <- character()
  value <- withCallingHandlers(
    expression,
    warning = function(warning) {
      messages <<- c(messages, conditionMessage(warning))
      invokeRestart("muffleWarning")
    }
  )
  list(value = value, messages = messages)
}

read_text <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

source_producer <- function(path) {
  producer_env <- new.env(parent = globalenv())
  sys.source(
    here::here("scripts", "R", "utils", "site_group_utils.R"),
    envir = producer_env
  )
  sys.source(here::here(path), envir = producer_env)
  producer_env
}

producer_specs <- list(
  house_sales = list(
    env = source_producer(
      file.path(
        "scripts", "R", "04_feature_engineering",
        "site_house_sale_match.R"
      )
    ),
    id_column = "house_id",
    prepare_property = "prepare_house_data",
    match_chunk = "match_house_chunk",
    data_key = "house",
    schema_function = "house_lookup_schema"
  ),
  rentals = list(
    env = source_producer(
      file.path(
        "scripts", "R", "04_feature_engineering",
        "site_rental_match.R"
      )
    ),
    id_column = "rental_id",
    prepare_property = "prepare_rental_data",
    match_chunk = "match_rental_chunk",
    data_key = "rentals",
    schema_function = "rental_lookup_schema"
  )
)

site_fixture <- tibble(
  site_id = c(101L, 202L),
  easting = c(500000, 500000),
  northing = c(200000, 210000)
)

property_fixture <- tibble(
  property_id = 1:5,
  easting = c(500001, 510000, 510100, 500000, 600000),
  northing = c(200000, 200000, 200000, 205000, 300000)
)

normalise_lookup <- function(lookup, id_column) {
  lookup |>
    arrange(.data[[id_column]], .data$site_id) |>
    as.data.frame()
}

run_match <- function(spec, properties = property_fixture) {
  id_column <- spec$id_column
  names(properties)[names(properties) == "property_id"] <- id_column
  property_sf <- spec$env[[spec$prepare_property]](properties)
  sites <- spec$env$prepare_spill_sites(site_fixture)
  do.call(spec$env[[spec$match_chunk]], list(
    property_sf,
    sites$spill_sf,
    sites$lookup,
    radius_km = 10
  ))
}

run_stream <- function(spec, properties = property_fixture, chunk_size = 2L,
                       output_path = tempfile(fileext = ".parquet"),
                       fail_at = NULL) {
  names(properties)[names(properties) == "property_id"] <- spec$id_column
  data <- setNames(list(properties), spec$data_key)
  data$spill <- site_fixture
  spec$env$process_spatial_data(
    data,
    output_path = output_path,
    radius_km = 10,
    chunk_size = chunk_size,
    fail_at = fail_at
  )
  output_path
}

stage_siblings <- function(output_path) {
  candidates <- list.files(
    dirname(output_path),
    all.files = TRUE,
    full.names = TRUE,
    no.. = TRUE
  )
  candidates[startsWith(
    basename(candidates),
    paste0(".", basename(output_path), ".stage-")
  )]
}

for (producer_name in names(producer_specs)) {
  spec <- producer_specs[[producer_name]]
  id_column <- spec$id_column

  assert_identical(
    spec$env$CONFIG$radius_km,
    10,
    paste(producer_name, "must configure a 10 km radius.")
  )
  assert_identical(
    sum(names(spec$env$CONFIG) == "radius_km"),
    1L,
    paste(producer_name, "must expose exactly one radius configuration value.")
  )
  assert_true(
    identical(
      formals(spec$env[[spec$match_chunk]])["radius_km"],
      alist(radius_km = )
    ),
    paste(producer_name, "matching must require an explicit radius.")
  )
  assert_true(
    identical(
      formals(spec$env$process_spatial_data)["radius_km"],
      alist(radius_km = )
    ),
    paste(producer_name, "orchestration must require an explicit radius.")
  )

  result <- run_match(spec)

  assert_true(
    any(result[[id_column]] == 1L & result$site_id == 101L),
    paste(producer_name, "must match a property inside 10 km.")
  )
  assert_true(
    any(result[[id_column]] == 2L & result$site_id == 101L),
    paste(producer_name, "must include a property exactly 10 km away.")
  )
  assert_true(
    !any(result[[id_column]] == 3L & result$site_id == 101L, na.rm = TRUE),
    paste(producer_name, "must exclude a property beyond 10 km.")
  )

  multi_match <- result |> filter(.data[[id_column]] == 4L)
  assert_identical(
    multi_match$site_id,
    c(101L, 202L),
    paste(producer_name, "must retain both Site Group matches.")
  )
  assert_identical(
    multi_match$n_site_groups,
    c(2L, 2L),
    paste(producer_name, "must report the property-level Site Group count.")
  )
  assert_true(
    !anyDuplicated(result[c(id_column, "site_id")]),
    paste(producer_name, "must produce unique property-Site Group keys.")
  )

  unmatched <- result |> filter(.data[[id_column]] == 5L)
  assert_identical(
    nrow(unmatched),
    1L,
    paste(producer_name, "must retain one sentinel row for an unmatched property.")
  )
  assert_true(
    is.na(unmatched$site_id) && is.na(unmatched$distance_m) &&
      is.na(unmatched$distance_km) && unmatched$n_site_groups == 0L,
    paste(producer_name, "must preserve the unmatched sentinel representation.")
  )
  assert_identical(
    vapply(result, typeof, character(1)),
    setNames(
      c("integer", "integer", "double", "double", "integer"),
      c(id_column, "site_id", "distance_m", "distance_km", "n_site_groups")
    ),
    paste(producer_name, "must preserve the five-column lookup types.")
  )

  duplicate_properties <- property_fixture[c(1L, 1L), ]
  assert_error_contains(
    run_match(spec, duplicate_properties),
    paste("unique on", id_column),
    paste(producer_name, "must reject duplicate property identifiers.")
  )

  one_chunk_path <- run_stream(spec, chunk_size = 100L)
  one_chunk <- normalise_lookup(arrow::read_parquet(one_chunk_path), id_column)
  single_row_path <- run_stream(spec, chunk_size = 1L)
  single_row_chunks <- normalise_lookup(
    arrow::read_parquet(single_row_path),
    id_column
  )
  assert_identical(
    single_row_chunks,
    one_chunk,
    paste(producer_name, "must be invariant to chunk boundaries.")
  )
  exact_multiple_path <- run_stream(spec, chunk_size = 5L)
  partial_chunk_path <- run_stream(spec, chunk_size = 2L)
  assert_identical(
    normalise_lookup(arrow::read_parquet(exact_multiple_path), id_column),
    one_chunk,
    paste(producer_name, "must preserve exact-multiple chunk output.")
  )
  assert_identical(
    normalise_lookup(arrow::read_parquet(partial_chunk_path), id_column),
    one_chunk,
    paste(producer_name, "must preserve final-partial-chunk output.")
  )

  unmatched_first <- property_fixture[c(5L, 1:4), ]
  unmatched_first_path <- run_stream(
    spec,
    properties = unmatched_first,
    chunk_size = 1L
  )
  unmatched_first_reader <- arrow::ParquetFileReader$create(unmatched_first_path)
  assert_identical(
    unmatched_first_reader$GetSchema()$ToString(),
    spec$env[[spec$schema_function]]()$ToString(),
    paste(producer_name, "must retain its schema when the first chunk is unmatched.")
  )

  canonical_path <- tempfile(paste0(producer_name, "-canonical-"), fileext = ".parquet")
  arrow::write_parquet(result, canonical_path)
  Sys.setFileTime(canonical_path, Sys.time() - 120)
  canonical_bytes <- unname(tools::md5sum(canonical_path))
  canonical_mtime <- file.info(canonical_path)$mtime
  for (failure_point in c(
    "after_first_row_group", "close", "validation", "sample_oracle", "promotion"
  )) {
    failure <- tryCatch(
      {
        run_stream(
          spec,
          chunk_size = 2L,
          output_path = canonical_path,
          fail_at = failure_point
        )
        NULL
      },
      error = identity
    )
    assert_true(
      inherits(failure, "error"),
      paste(producer_name, "must propagate", failure_point, "failures.")
    )
    assert_identical(
      unname(tools::md5sum(canonical_path)),
      canonical_bytes,
      paste(producer_name, "must preserve canonical bytes after", failure_point)
    )
    assert_identical(
      file.info(canonical_path)$mtime,
      canonical_mtime,
      paste(producer_name, "must preserve canonical mtime after", failure_point)
    )
    assert_identical(
      stage_siblings(canonical_path),
      character(),
      paste(producer_name, "must clean its stage after", failure_point)
    )
  }

  diagnostic_properties <- tibble(
    property_id = 1:4,
    easting = c(500000, NA_real_, 500000, NaN),
    northing = c(200000, 200000, Inf, 200000)
  )
  names(diagnostic_properties)[names(diagnostic_properties) == "property_id"] <-
    id_column
  log_path <- tempfile(paste0(producer_name, "-coordinate-log-"))
  logger::log_appender(logger::appender_file(log_path))
  diagnostic_result <- capture_warning_messages(
    spec$env[[spec$prepare_property]](diagnostic_properties)
  )
  logger::log_appender(logger::appender_console)

  assert_identical(
    length(diagnostic_result$messages),
    1L,
    paste(producer_name, "must warn once when coordinates are excluded.")
  )
  diagnostic_log <- read_text(log_path)
  assert_true(
    grepl("total=4", diagnostic_log, fixed = TRUE) &&
      grepl("eligible=1", diagnostic_log, fixed = TRUE) &&
      grepl("excluded=3", diagnostic_log, fixed = TRUE) &&
      grepl("75.00%", diagnostic_log, fixed = TRUE),
    paste(producer_name, "must log reconciled coordinate coverage.")
  )
  assert_identical(
    nrow(diagnostic_result$value),
    1L,
    paste(producer_name, "must retain only finite coordinate rows.")
  )

  missing_id_properties <- diagnostic_properties[1L, ]
  missing_id_properties[[id_column]] <- NA_integer_
  assert_error_contains(
    spec$env[[spec$prepare_property]](missing_id_properties),
    paste(id_column, "must not contain missing values"),
    paste(producer_name, "must reject missing property identifiers.")
  )

  all_ineligible <- diagnostic_properties[2:4, ]
  suppressWarnings(assert_error_contains(
    spec$env[[spec$prepare_property]](all_ineligible),
    "No coordinate-eligible",
    paste(producer_name, "must reject an all-ineligible property input.")
  ))
}

producer_paths <- c(
  here::here(
    "scripts", "R", "04_feature_engineering", "site_house_sale_match.R"
  ),
  here::here(
    "scripts", "R", "04_feature_engineering", "site_rental_match.R"
  )
)
producer_text <- paste(vapply(producer_paths, read_text, character(1)), collapse = "\n")
assert_true(
  !grepl("install.packages(", producer_text, fixed = TRUE),
  "Property-match producers must not install packages at runtime."
)
assert_true(
  !grepl("setup_logging <- function", producer_text, fixed = TRUE),
  "Property-match producers must delegate generic logger setup to script_setup.R."
)
assert_true(
  !grepl("10km_site_", producer_text, fixed = TRUE),
  "Property-match producers must use radius-neutral operational filenames."
)
assert_true(
  !grepl("split\\s*\\(", producer_text),
  "Property-match producers must iterate row-index ranges without split()."
)
assert_true(
  !grepl("bind_rows\\s*\\(chunks", producer_text),
  "Property-match producers must not accumulate and bind all result chunks."
)

cat("Property-site match producer contract tests passed.\n")
