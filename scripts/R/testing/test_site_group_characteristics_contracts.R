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
assert_error_contains(
  producer_env$build_designation_histories(
    annual, lookup, bind_rows(membership, membership[1, ]), 1:3, years
  ),
  "unique",
  "Duplicate canonical membership rows must fail closed."
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

# The shared sibling-file publisher validates before and after promotion and
# restores the previous generation when final validation fails.
publication_root <- tempfile("characteristics-file-publication-")
dir.create(publication_root)
canonical_file <- file.path(publication_root, "canonical.txt")
candidate_file <- file.path(publication_root, ".candidate.txt")
writeLines("old", canonical_file)
writeLines("new", candidate_file)
validate_generation <- function(path) {
  value <- readLines(path, warn = FALSE)
  if (!value %in% c("old", "new")) stop("invalid generation", call. = FALSE)
}
producer_env$publish_validated_file(
  candidate_file, canonical_file, validate_generation
)
assert_identical(
  readLines(canonical_file, warn = FALSE), "new",
  "The validated candidate file must replace the prior generation."
)
assert_true(
  !file.exists(paste0(canonical_file, ".prev")),
  "Successful file publication must remove its temporary backup."
)

writeLines("bad", candidate_file)
assert_error_contains(
  producer_env$publish_validated_file(
    candidate_file,
    canonical_file,
    function(path) {
      value <- readLines(path, warn = FALSE)
      if (identical(path, canonical_file) && value == "bad") {
        stop("injected final validation failure", call. = FALSE)
      }
    }
  ),
  "restored",
  "A failed final file validation must restore the prior generation."
)
assert_identical(
  readLines(canonical_file, warn = FALSE), "new",
  "File publication failure must leave the prior canonical readable."
)

writeLines("candidate", candidate_file)
assert_error_contains(
  producer_env$publish_validated_file(
    candidate_file,
    canonical_file,
    function(path) invisible(path),
    rename_path = function(from, to) {
      if (identical(from, candidate_file) && identical(to, canonical_file)) {
        return(FALSE)
      }
      file.rename(from, to)
    }
  ),
  "restored",
  "A failed file promotion must report successful restoration."
)
assert_identical(
  readLines(canonical_file, warn = FALSE), "new",
  "Promotion failure must restore the prior canonical file."
)

# Optional production read-back. The test remains a pure fixture test before
# the first build, then becomes the canonical contract gate once output exists.
canonical_path <- producer_env$CONFIG$output_path
if (file.exists(canonical_path)) {
  canonical <- arrow::read_parquet(canonical_path)
  projection <- producer_env$read_site_group_projection(
    producer_env$CONFIG$crosswalk_path, years = 2021:2024
  )
  refined <- "site_generation" %in% names(canonical)
  producer_env$validate_site_group_characteristics(canonical, projection$site_id, refined)
  assert_identical(
    arrow::open_dataset(canonical_path)$schema$names,
    producer_env$site_group_characteristics_columns(refined),
    "Canonical Site Group parquet must retain the exact physical schema."
  )
  assert_identical(
    producer_env$arrow_schema_signature(
      arrow::open_dataset(canonical_path)$schema
    ),
    producer_env$arrow_schema_signature(
      producer_env$site_group_characteristics_schema(refined)
    ),
    "Canonical Site Group parquet must retain the exact physical types."
  )

  for (lookup_path in c(
      here::here("data", "processed", "spill_house_lookup.parquet"),
      here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
    )) {
    pair_site_ids <- arrow::open_dataset(lookup_path) |>
      filter(.data$distance_m <= 1000) |>
      select("site_id") |>
      distinct() |>
      collect() |>
      pull("site_id")
    assert_true(
      all(pair_site_ids %in% canonical$site_id),
      paste("All real <=1 km property pairs must join Site Group characteristics:", lookup_path)
    )
  }

  # Nearest-site spot checks cover inland, coastal, Welsh, northern-English,
  # island, and both documented tidal-estuary semantics.
  located <- projection |>
    left_join(select(canonical, "site_id", "distance_to_coast_m"), by = "site_id")
  references <- tribble(
    ~label, ~easting, ~northing, ~minimum_m, ~maximum_m,
    "Birmingham inland", 407000, 287000, 50000, Inf,
    "Brighton coastal", 531000, 104000, 0, 5000,
    "Cardiff Welsh", 318000, 176000, 0, 5000,
    "Northern-English border guard", 393000, 568000, 10000, Inf,
    "Isle of Wight", 450000, 85000, 0, 10000,
    "Tidal Thames London", 530000, 180000, 0, 5000,
    "Severn Gloucester", 383000, 219000, 0, 5000
  )
  spot_checks <- bind_rows(lapply(seq_len(nrow(references)), function(index) {
    squared <- (located$easting - references$easting[[index]])^2 +
      (located$northing - references$northing[[index]])^2
    nearest <- located[which.min(squared), ]
    tibble(
      label = references$label[[index]],
      site_id = nearest$site_id,
      distance_to_reference_m = sqrt(min(squared, na.rm = TRUE)),
      distance_to_coast_m = nearest$distance_to_coast_m,
      minimum_m = references$minimum_m[[index]],
      maximum_m = references$maximum_m[[index]]
    )
  }))
  assert_true(
    all(spot_checks$distance_to_coast_m >= spot_checks$minimum_m &
      spot_checks$distance_to_coast_m <= spot_checks$maximum_m),
    paste(
      "Site Coast Distance spot checks failed:",
      paste(capture.output(print(spot_checks)), collapse = " ")
    )
  )
  print(spot_checks)
}

cat("All Site Group characteristics contract tests passed.\n")
