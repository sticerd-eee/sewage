# ==============================================================================
# Site Group Characteristics Contract Tests
# ==============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(here)
  library(sf)
  library(tibble)
  library(tidyr)
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

producer_env <- new.env(parent = globalenv())
sys.source(
  here::here(
    "scripts", "R", "03_data_enrichment",
    "build_site_group_characteristics.R"
  ),
  envir = producer_env
)

# Annual-return field truth table: present blanks are known negatives, while
# absent rows are unknown. Matching is whitespace- and case-insensitive.
raw_values <- c(
  " Seaside Bay ", "", "0", " no ", "Not Applicable", "TBC",
  "to BE confirmed", NA_character_
)
present <- c(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE)
assert_identical(
  producer_env$classify_designation_value(raw_values, present),
  c(
    "designated", "not_designated", "not_designated", "not_designated",
    "not_designated", "unknown", "unknown", "unknown"
  ),
  "Designation values must follow the plan truth table."
)
assert_identical(
  producer_env$classify_designation_value(NA_character_, TRUE),
  "not_designated",
  "A blank field on a present row must be a known negative."
)

# Mapping fixture includes duplicate annual rows, mixed canonical members,
# cross-year changes, TBC, and a Site Group with no annual-return evidence.
years <- 2021:2024
lookup <- tibble(
  site_id = c(10L, 11L, 20L),
  site_id_2021 = c(101L, 111L, 201L),
  site_id_2022 = c(102L, 112L, 202L),
  site_id_2023 = c(103L, 113L, 203L),
  site_id_2024 = c(104L, 114L, 204L)
)
membership <- tibble(
  site_id_canonical = c(10L, 11L, 20L),
  site_id = c(1L, 1L, 2L)
)
annual <- tibble(
  year = c(2021L, 2021L, 2021L, 2022L, 2022L, 2023L, 2023L, 2024L),
  site_id_2021 = c(101L, 111L, 111L, rep(NA_integer_, 5L)),
  site_id_2022 = c(rep(NA_integer_, 3L), 102L, 112L, rep(NA_integer_, 3L)),
  site_id_2023 = c(rep(NA_integer_, 5L), 103L, 203L, NA_integer_),
  site_id_2024 = c(rep(NA_integer_, 7L), 104L),
  bathing_water = c("Named Bath", "", "", "", "", "TBC", "", "Named Bath"),
  shellfish_water = c("", "Named Shell", "Named Shell", "", "", "", "TBC", "")
)

histories <- producer_env$build_designation_histories(
  annual_data = annual,
  lookup_data = lookup,
  membership_data = membership,
  site_ids = 1:3,
  years = years
)

assert_identical(
  histories$site_id,
  1:3,
  "Designation histories must cover the complete Site Group universe."
)
assert_true(
  !anyDuplicated(histories$site_id),
  "Designation histories must be unique on site_id."
)
site_1 <- histories |> filter(site_id == 1L)
site_2 <- histories |> filter(site_id == 2L)
site_3 <- histories |> filter(site_id == 3L)
assert_identical(site_1$bath_status_21, "designated", "Any positive member must win.")
assert_identical(site_1$bath_mixed_21, TRUE, "Positive and negative members must be mixed.")
assert_identical(site_1$shell_status_21, "designated", "Duplicate named rows remain positive.")
assert_identical(site_1$bath_status_23, "unknown", "TBC must remain unknown.")
assert_identical(site_1$bath_ever_2124, TRUE, "Ever must retain positive evidence.")
assert_identical(site_1$bath_changed_2124, TRUE, "Known positive and negative years must flag change.")
assert_identical(site_1$bath_unknown_2124, TRUE, "An unknown year must flag uncertain history.")
assert_identical(site_1$bath_24, TRUE, "Known 2024 designation must be logical TRUE.")
assert_identical(site_2$shell_status_23, "unknown", "TBC must map through canonical membership.")
assert_true(is.na(site_2$shell_24), "Absent 2024 evidence must produce logical NA.")
assert_true(
  all(unlist(site_3[paste0("bath_status_", 21:24)]) == "unknown"),
  "A Site Group with no annual-return rows must be unknown in every year."
)

bad_lookup <- bind_rows(lookup, lookup[1, ])
assert_error_contains(
  producer_env$build_designation_histories(
    annual, bad_lookup, membership, 1:3, years
  ),
  "unique",
  "Duplicate canonical lookup mappings must fail closed."
)
bad_membership <- membership |> filter(site_id_canonical != 11L)
assert_error_contains(
  producer_env$build_designation_histories(
    annual, lookup, bad_membership, 1:3, years
  ),
  "Unmapped",
  "Unmapped canonical sites with annual evidence must fail closed."
)

# Dissolving adjacent land polygons removes their internal administrative
# border. Distances must be planar metres to the exterior Mean High Water line.
land_parts <- st_sf(
  country = c("England", "Wales"),
  geometry = st_sfc(
    st_polygon(list(rbind(c(0, 0), c(100, 0), c(100, 100), c(0, 100), c(0, 0)))),
    st_polygon(list(rbind(c(100, 0), c(200, 0), c(200, 100), c(100, 100), c(100, 0)))),
    crs = 27700
  )
)
points <- tibble(
  site_id = 1:3,
  easting = c(50, 100, NA_real_),
  northing = c(50, 50, NA_real_)
)
coast <- producer_env$calculate_site_coast_distance(points, land_parts)
assert_identical(
  round(coast$distance_to_coast_m, 6),
  c(50, 50, NA_real_),
  "The dissolved internal border must not be treated as coast."
)

site_output <- left_join(points |> select(site_id), coast, by = "site_id") |>
  left_join(histories, by = "site_id")
producer_env$validate_site_group_characteristics(site_output, 1:3)
assert_identical(
  names(site_output),
  producer_env$site_group_characteristics_columns(),
  "The Site Group output schema and order must be exact."
)
assert_error_contains(
  producer_env$validate_site_group_characteristics(
    bind_rows(site_output, site_output[1, ]), 1:3
  ),
  "unique",
  "Duplicate Site Group output keys must be rejected."
)
assert_error_contains(
  producer_env$validate_site_group_characteristics(
    mutate(site_output, bath_status_21 = "invalid"), 1:3
  ),
  "allowed",
  "Unexpected designation enums must be rejected."
)

cat("All Site Group characteristics contract tests passed.\n")
