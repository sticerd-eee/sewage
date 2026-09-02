# ==============================================================================
# Salience Strata Contract Tests
# Run from the repository root: Rscript scripts/R/testing/test_salience_strata_contracts.R
# ==============================================================================

suppressPackageStartupMessages(library(dplyr))
source(here::here("scripts", "R", "09_analysis", "utils_salience_strata.R"))

assert_equal <- function(actual, expected) {
  if (!isTRUE(all.equal(actual, expected, check.attributes = FALSE))) {
    stop("Unexpected result:\n", paste(capture.output(print(actual)), collapse = "\n"),
         "\nExpected:\n", paste(capture.output(print(expected)), collapse = "\n"),
         call. = FALSE)
  }
}

assert_error <- function(expression, pattern) {
  error <- tryCatch({ force(expression); NULL }, error = identity)
  stopifnot(inherits(error, "error"), grepl(pattern, conditionMessage(error)))
}

# Nearest selection must be invariant to pair order, include the 2 km boundary,
# and validate all eligible pairs (including sites that do not win the tie).
sites <- tibble::tibble(
  site_id = c(2L, 10L, 3L),
  distance_to_coast_m = c(100, 5000, NA_real_),
  bath_ever_2124 = c(TRUE, FALSE, FALSE),
  bath_unknown_2124 = c(FALSE, TRUE, FALSE)
)
pairs <- tibble::tibble(
  house_id = c("tie", "tie", "boundary", "outside", "missing_distance"),
  site_id = c(10L, 2L, 3L, 999L, 999L),
  distance_m = c(100, 100, 2000, 2000.1, NA_real_)
)
nearest <- nearest_salience_sites(pairs, sites, "house_id")
assert_equal(nearest$house_id, c("boundary", "tie"))
assert_equal(nearest$site_id, c(3L, 2L))
assert_equal(nearest$min_distance, c(2000, 100))
assert_equal(nearest_salience_sites(pairs[5:1, ], sites, "house_id"), nearest)
assert_equal(nearest_salience_sites(arrow::Table$create(pairs), sites, "house_id"), nearest)
assert_equal(attr(nearest, "coverage")$pair_characteristic_share, 1)
assert_error(
  nearest_salience_sites(pairs, filter(sites, site_id != 10L), "house_id"),
  "characteristics"
)
assert_error(nearest_salience_sites(pairs, bind_rows(sites, sites[1, ]), "house_id"),
             "unique")

# Coast boundaries, inland designated waters, missing coast and unknown evidence.
coast <- tibble::tibble(
  distance_to_coast_m = c(2000, 2000.1, 10000, 10000.1, 12000, NA, 100, 100, 100),
  bath_ever_2124 = c(FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, FALSE, NA, TRUE),
  bath_unknown_2124 = c(FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE)
)
headline <- classify_salience_coast(coast)
assert_equal(headline$salience_class, c("coastal_not_bathing", "inland_not_bathing", "inland_not_bathing",
  "inland_not_bathing", "inland_bathing", NA, "coastal_not_bathing", "coastal_not_bathing", "coastal_bathing"))
assert_equal(headline$coastal, c(TRUE, FALSE, FALSE, FALSE, FALSE, NA, TRUE, TRUE, TRUE))
assert_equal(headline$bathing, c(FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, FALSE, FALSE, TRUE))
assert_equal(classify_salience_coast(coast, coast_rule_m = 10000)$salience_class,
  c("coastal_not_bathing", "coastal_not_bathing", "coastal_not_bathing", "inland_not_bathing",
    "inland_bathing", NA, "coastal_not_bathing", "coastal_not_bathing", "coastal_bathing"))
assert_equal(classify_salience_coast(coast, unknown_policy = "exclude")$salience_class,
  c("coastal_not_bathing", "inland_not_bathing", "inland_not_bathing", "inland_not_bathing",
    "inland_bathing", NA, NA, NA, "coastal_bathing"))
assert_equal(headline$coast_rule_m, rep(2000, 9))
assert_equal(headline$bath_unknown, coast$bath_unknown_2124)
assert_equal(headline$bath_unresolved, c(FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, FALSE))
reclassified <- classify_salience_coast(headline, 10000, "exclude")
assert_equal(reclassified$coast_rule_m, rep(10000, 9))
assert_equal(reclassified$unknown_policy, rep("exclude", 9))
radius_coast <- rename(coast, min_coast_dist_m = distance_to_coast_m,
                       any_bath_2124 = bath_ever_2124)
assert_equal(classify_salience_coast(radius_coast, source = "radius")$salience_class,
             headline$salience_class)
assert_equal(classify_salience_coast(radius_coast, 10000, "exclude", "radius")$salience_class,
             c("coastal_not_bathing", "coastal_not_bathing", "coastal_not_bathing",
               "inland_not_bathing", "inland_bathing", NA, NA, NA, "coastal_bathing"))

# A positive observation resolves ever-designation even with unknown years or
# unknown neighbouring sites. Missing positive evidence never becomes designated.
uncertain <- tibble::tibble(
  distance_to_coast_m = rep(c(100, 12000), 4),
  bath_ever_2124 = c(TRUE, TRUE, FALSE, FALSE, NA, NA, TRUE, FALSE),
  bath_unknown_2124 = c(TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, NA, NA)
)
resolved <- classify_salience_coast(uncertain, unknown_policy = "exclude")
assert_equal(resolved$salience_class,
             c("coastal_bathing", "inland_bathing", NA, NA, NA, NA, "coastal_bathing", NA))
assert_equal(resolved$bath_unresolved, c(FALSE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, TRUE))
assert_equal(resolved$bath_unknown_2124, uncertain$bath_unknown_2124)
uncertain_radius <- rename(uncertain, min_coast_dist_m = distance_to_coast_m,
                           any_bath_2124 = bath_ever_2124)
assert_equal(classify_salience_coast(uncertain_radius, unknown_policy = "exclude",
                                    source = "radius")$salience_class, resolved$salience_class)
assert_equal(flag_salience_london(tibble::tibble(region = c("London", "South East", NA)))$london,
             c(TRUE, FALSE, FALSE))

# All four coast x bathing classes are disjoint and exhaust eligible rows.
coast_filters <- salience_strata("coast_bathing")
assert_equal(names(coast_filters), c("coastal_bathing", "coastal_not_bathing",
                                   "inland_bathing", "inland_not_bathing"))
masks <- lapply(coast_filters, function(f) f(headline))
assert_equal(Reduce(`+`, lapply(masks, as.integer)), as.integer(!is.na(headline$salience_class)))
assert_equal(masks$coastal_bathing | masks$coastal_not_bathing, headline$coastal %in% TRUE)
assert_equal(masks$inland_bathing, c(FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE))
for (rule in c(2000, 10000)) {
  for (policy in c("not_designated", "exclude")) {
    classified <- classify_salience_coast(coast, rule, policy)
    membership <- lapply(coast_filters, function(f) as.integer(f(classified)))
    assert_equal(Reduce(`+`, membership), as.integer(!is.na(classified$salience_class)))
  }
}

intensity <- tibble::tibble(
  spill_count_band = c("spill_le_p50", "spill_gt_p50", "unknown", "zero", "no_site", NA),
  near_bin = c(1L, 1L, 1L, 1L, 0L, 0L)
)
bands <- salience_strata("intensity")
assert_equal(names(bands), c("spill_le_p50", "spill_gt_p50"))
assert_equal(bands$spill_le_p50(intensity), c(TRUE, FALSE, FALSE, FALSE, FALSE, FALSE))
extensive_bands <- salience_strata("intensity", include_far = TRUE)
assert_equal(extensive_bands$spill_le_p50(intensity), c(TRUE, FALSE, FALSE, FALSE, TRUE, TRUE))
assert_equal(extensive_bands$spill_gt_p50(intensity), c(FALSE, TRUE, FALSE, FALSE, TRUE, TRUE))
assert_error(extensive_bands$spill_gt_p50(mutate(intensity, spill_count_band = "zero")),
             "Far-group")
for (band in c("unknown", "zero", "spill_le_p50", "spill_gt_p50")) {
  bad_far <- intensity
  bad_far$spill_count_band[bad_far$near_bin == 0L] <- band
  assert_error(extensive_bands$spill_le_p50(bad_far), "Far-group")
}
no_site_near <- bind_rows(intensity, tibble::tibble(spill_count_band = "no_site", near_bin = 1L))
assert_equal(extensive_bands$spill_le_p50(no_site_near),
             c(TRUE, FALSE, FALSE, FALSE, TRUE, TRUE, FALSE))
assert_equal(extensive_bands$spill_gt_p50(no_site_near),
             c(FALSE, TRUE, FALSE, FALSE, TRUE, TRUE, FALSE))

# Joining property evidence must preserve repeated transactions and their order.
transactions <- tibble::tibble(house_id = c("tie", "tie", "boundary", "unmatched"),
                              region = c("London", "South East", "North East", NA))
joined <- join_nearest_salience(transactions, nearest, "house_id")
assert_equal(joined$house_id, transactions$house_id)
assert_equal(joined$salience_class, c("coastal_bathing", "coastal_bathing", NA, NA))
assert_equal(joined$london, c(TRUE, FALSE, FALSE, FALSE))

companion <- tibble::tibble(
  house_id = rep(c("tie", "boundary"), 2), radius = rep(c(250L, 500L), each = 2),
  min_coast_dist_m = c(5000, 100, 50, 50), any_bath_2124 = c(FALSE, TRUE, TRUE, TRUE),
  bath_unknown_2124 = FALSE,
  spill_count_band = c("spill_le_p50", "no_site", "spill_gt_p50", "spill_le_p50")
)
radius_joined <- join_radius_salience(transactions, companion, "house_id", 250L)
assert_equal(radius_joined$salience_class, c("inland_not_bathing", "inland_not_bathing", "coastal_bathing", NA))
assert_equal(radius_joined$spill_count_band, c("spill_le_p50", "spill_le_p50", "no_site", NA))
assert_equal(join_radius_salience(transactions, companion, "house_id", 500L)$spill_count_band,
             c("spill_gt_p50", "spill_gt_p50", "spill_le_p50", NA))
assert_equal(join_radius_salience(transactions, arrow::Table$create(companion),
                                 "house_id", 500L)$spill_count_band,
             c("spill_gt_p50", "spill_gt_p50", "spill_le_p50", NA))
intensity_joined <- join_radius_salience(joined, companion, "house_id", 500L,
                                        classify_coast = FALSE)
assert_equal(intensity_joined$salience_class, joined$salience_class)
assert_equal(intensity_joined$spill_count_band,
             c("spill_gt_p50", "spill_gt_p50", "spill_le_p50", NA))
rentals <- rename(transactions, rental_id = house_id)
rental_nearest <- nearest_salience_sites(rename(pairs, rental_id = house_id), sites, "rental_id")
assert_equal(join_nearest_salience(rentals, rental_nearest, "rental_id")$london,
             c(TRUE, FALSE, FALSE, FALSE))
assert_equal(join_radius_salience(rentals, rename(companion, rental_id = house_id),
                                 "rental_id", 250L)$salience_class,
             c("inland_not_bathing", "inland_not_bathing", "coastal_bathing", NA))
assert_error(join_radius_salience(transactions, bind_rows(companion, companion), "house_id", 250L),
             "unique")

# Counts are transactions; the missing-coast share is over distinct nearest
# Site Groups, so repeat transactions must not change its denominator.
count_data <- tibble::tibble(
  house_id = c("a", "a", "b", "c", "e", "d", "d"),
  site_id = c(1L, 1L, 2L, 3L, 3L, 4L, 4L),
  salience_class = c("coastal_bathing", "coastal_bathing", "coastal_not_bathing",
                     "inland_bathing", "inland_not_bathing", NA, NA),
  coast_rule_m = 2000, unknown_policy = "not_designated",
  london = c(TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE)
)
count_nearest <- tibble::tibble(site_id = 1:4, distance_to_coast_m = c(100, 200, 9000, NA))
counts <- log_salience_cells(count_data, count_nearest, "sales", "coast_bathing")
assert_equal(counts$n_before_london_drop, c(2L, 1L, 1L, 1L))
assert_equal(counts$n_after_london_drop, c(1L, 1L, 1L, 1L))
assert_equal(counts$nearest_site_missing_coast_share, rep(0.25, 4))
assert_equal(counts$n_unclassified, rep(2L, 4))
assert_error(log_salience_cells(filter(count_data, salience_class != "inland_bathing"),
                               count_nearest, "rentals", "coast_bathing"),
             "rentals.*inland_bathing")

# Before estimation, every extensive stratum must retain near and far rows in
# both periods after dropping London. Totals alone can hide a missing cell.
support_data <- filter(count_data, !is.na(salience_class))
support_data <- support_data[rep(seq_len(nrow(support_data)), each = 4L), ] |>
  mutate(near_bin = rep(c(1L, 1L, 0L, 0L), 5), post = rep(c(0L, 1L, 0L, 1L), 5))
support_counts <- log_salience_cells(support_data, count_nearest, "sales", "coast_bathing")
assert_equal(support_counts$n_near_pre_after, rep(1L, 4))
assert_equal(support_counts$n_far_post_after, rep(1L, 4))
assert_error(log_salience_cells(mutate(support_data, near_bin = 1L),
                               count_nearest, "sales", "coast_bathing"),
             "support.*sales.*coastal_bathing")
assert_error(log_salience_cells(filter(support_data, !(near_bin == 0L & post == 0L)),
                               count_nearest, "rentals", "coast_bathing"),
             "support.*rentals.*coastal_bathing")
# Retaining London validates the actual estimation cells while still reporting
# the counterfactual counts after dropping London. Here far/pre exists only there.
london_support <- mutate(support_data, london = near_bin == 0L & post == 0L)
assert_error(log_salience_cells(london_support, count_nearest, "sales", "coast_bathing"),
             "support.*sales.*coastal_bathing")
london_counts <- log_salience_cells(london_support, count_nearest, "sales",
                                    "coast_bathing", drop_london = FALSE)
assert_equal(london_counts$n_estimation, c(8L, 4L, 4L, 4L))
assert_equal(london_counts$n_after_london_drop, c(6L, 3L, 3L, 3L))
assert_equal(london_counts$n_far_pre_estimation, c(2L, 1L, 1L, 1L))
assert_equal(london_counts$n_far_pre_after, rep(0L, 4))
assert_error(log_salience_cells(filter(london_support, !(near_bin == 0L & post == 0L)),
                               count_nearest, "rentals", "coast_bathing", drop_london = FALSE),
             "support.*rentals.*coastal_bathing")
intensity_counts <- intensity |>
  mutate(site_id = 1L, salience_class = "coastal_bathing", london = FALSE,
         coast_rule_m = 2000, unknown_policy = "not_designated") |>
  log_salience_cells(count_nearest, "sales", "intensity", include_far = TRUE)
assert_equal(intensity_counts$n_before_london_drop, c(3L, 3L))
assert_equal(intensity_counts$n_unclassified, c(2L, 2L))

cat("Salience strata contract tests passed.\n")
