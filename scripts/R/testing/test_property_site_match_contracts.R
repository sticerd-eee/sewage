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
        "10km_site_house_sale_match.R"
      )
    ),
    id_column = "house_id",
    prepare_property = "prepare_house_data"
  ),
  rentals = list(
    env = source_producer(
      file.path(
        "scripts", "R", "04_feature_engineering",
        "10km_site_rental_match.R"
      )
    ),
    id_column = "rental_id",
    prepare_property = "prepare_rental_data"
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

run_match <- function(spec, properties = property_fixture, chunk_size = NULL) {
  id_column <- spec$id_column
  names(properties)[names(properties) == "property_id"] <- id_column
  property_sf <- spec$env[[spec$prepare_property]](properties)
  sites <- spec$env$prepare_spill_sites(site_fixture)

  arguments <- list(
    property_sf,
    sites$spill_sf,
    sites$lookup,
    radius_km = 10
  )
  if (!is.null(chunk_size) &&
      "chunk_size" %in% names(formals(spec$env$perform_spatial_join))) {
    arguments$chunk_size <- chunk_size
  }
  do.call(spec$env$perform_spatial_join, arguments)
}

for (producer_name in names(producer_specs)) {
  spec <- producer_specs[[producer_name]]
  id_column <- spec$id_column
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

  one_chunk <- normalise_lookup(run_match(spec, chunk_size = 100L), id_column)
  single_row_chunks <- normalise_lookup(run_match(spec, chunk_size = 1L), id_column)
  assert_identical(
    single_row_chunks,
    one_chunk,
    paste(producer_name, "must be invariant to chunk boundaries.")
  )
}

cat("Property-site match producer contract tests passed.\n")
