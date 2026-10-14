# ==============================================================================
# Grid Long-Difference Sales Contract Tests
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(here)
})

source(here::here(
  "scripts", "R", "06_analysis_datasets", "grid_long_difference_sales.R"
))

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

fixture_path <- tempfile(fileext = ".parquet")
on.exit(unlink(fixture_path), add = TRUE)

arrow::write_parquet(
  data.frame(
    house_id = c("001", "002", "003"),
    site_id = c(10L, 20L, 30L),
    distance_m = c(100, 250, 251),
    unused_payload = c("a", "b", "c")
  ),
  fixture_path
)

query_at_collect <- NULL
collect_spy <- function(query) {
  query_at_collect <<- query
  dplyr::collect(query)
}

result <- load_spill_lookup_within_radius(
  fixture_path,
  radius_m = 250,
  collect_fn = collect_spy
)

assert_true(
  inherits(query_at_collect, "arrow_dplyr_query"),
  "The lookup must remain an Arrow lazy query until collect()."
)
assert_true(
  identical(names(query_at_collect), c("house_id", "site_id", "distance_m")),
  "Projection must be pushed into Arrow before collect()."
)
assert_true(
  grepl(
    "Filter: (distance_m <= 250)",
    paste(capture.output(print(query_at_collect)), collapse = "\n"),
    fixed = TRUE
  ),
  "The radius predicate must be pushed into Arrow before collect()."
)
assert_true(
  identical(result$house_id, c("001", "002")),
  "The lazy loader must retain character IDs, including leading zeroes."
)
assert_true(
  identical(names(result), c("house_id", "site_id", "distance_m")),
  "The collected lookup must expose only the required columns."
)

cat("All grid long-difference sales contract tests passed.\n")
