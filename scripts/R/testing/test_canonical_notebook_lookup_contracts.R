# ==============================================================================
# Canonical notebook Annual Return lookup contracts
# ==============================================================================

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

suppressPackageStartupMessages({
  library(dplyr)
  library(here)
  library(tibble)
})

source(here::here(
  "scripts", "R", "utils", "annual_return_mapping_utils.R"
))

lookup_long <- tibble(
  year = c(2021L, 2022L),
  year_site_id = c(9001L, 9002L),
  site_id_canonical = c(101L, 101L)
)
annual_long <- tibble(
  year = c(2021L, 2022L),
  year_site_id = c(9001L, 9002L),
  spill_hrs_ea = c(1, 2)
)

mapped <- annual_long |>
  left_join(lookup_long, by = c("year", "year_site_id")) |>
  finalise_annual_lookup_mapping()

assert_identical(
  mapped$site_id_canonical,
  c(101L, 101L),
  "Matched notebook rows must use the lookup-provided canonical ID."
)
assert_true(
  !any(mapped$site_id_canonical == mapped$year_site_id),
  "The happy path must not replace canonical IDs with year-local IDs."
)

unmatched_annual <- bind_rows(
  annual_long,
  tibble(year = 2021L, year_site_id = 9999L, spill_hrs_ea = 3)
)
assert_error_contains(
  unmatched_annual |>
    left_join(lookup_long, by = c("year", "year_site_id")) |>
    finalise_annual_lookup_mapping(),
  "Annual Return rows for 2021 exist without lookup coverage",
  "An unmatched year-local ID must fail instead of becoming a canonical ID."
)

missing_id_annual <- tibble(
  year = 2022L,
  year_site_id = NA_integer_,
  spill_hrs_ea = 4
)
assert_error_contains(
  missing_id_annual |>
    left_join(lookup_long, by = c("year", "year_site_id")) |>
    finalise_annual_lookup_mapping(),
  "year/site examples: 2022/<missing>",
  "A missing year-local ID must remain visible and fail lookup coverage."
)

notebook_paths <- here::here(
  "scripts", "R", "testing",
  c(
    "investigate_partial_availability_missingness.qmd",
    "missing_observation_patterns_2021_2023.qmd"
  )
)

for (notebook_path in notebook_paths) {
  purl_path <- tempfile("canonical-notebook-", fileext = ".R")
  knitr::purl(notebook_path, output = purl_path, quiet = TRUE)
  parse(file = purl_path)
  notebook_code <- paste(readLines(purl_path, warn = FALSE), collapse = "\n")

  assert_true(
    grepl("finalise_annual_lookup_mapping()", notebook_code, fixed = TRUE),
    paste0(basename(notebook_path), " must use the shared fail-closed mapping seam.")
  )
  assert_true(
    !grepl("as.integer(year_site_id)", notebook_code, fixed = TRUE),
    paste0(basename(notebook_path), " must not fall back to a year-local ID.")
  )
}

cat("Canonical notebook lookup contracts passed.\n")
