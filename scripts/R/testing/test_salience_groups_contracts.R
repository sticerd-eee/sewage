# Overlapping paper groups: run from the root with Rscript in the rv environment.
source(here::here("scripts", "R", "utils", "salience_group_utils.R"))
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))

fixture <- tibble::tibble(
  distance_to_coast_m = c(100, 5000, 2000, 2000.1, NA, NA, 100, 5000),
  bath_ever_2124 = c(TRUE, TRUE, FALSE, FALSE, TRUE, NA, NA, FALSE),
  bath_unknown_2124 = c(TRUE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE),
  region = c("London", rep("South West", 7))
)
classified <- classify_salience_groups(fixture)
masks <- salience_group_masks(classified)
stopifnot(
  identical(names(masks), c("bathing", "coastal", "inland")),
  identical(masks$bathing, c(TRUE, TRUE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE)),
  identical(masks$coastal, c(TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, TRUE, FALSE)),
  identical(masks$inland, c(FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, TRUE)),
  identical(Reduce(`+`, masks), c(2L, 2L, 1L, 1L, 1L, 0L, 1L, 1L)),
  identical(classified$london, c(TRUE, rep(FALSE, 7)))
)
radius <- dplyr::rename(fixture, min_coast_dist_m = distance_to_coast_m,
                         any_bath_2124 = bath_ever_2124)
stopifnot(identical(salience_group_masks(classify_salience_groups(radius, source = "radius")), masks))

# Ties are deterministic; all eligible sites must join characteristic evidence.
sites <- tibble::tibble(site_id = c(2L, 10L), distance_to_coast_m = c(100, 5000),
                       bath_ever_2124 = c(TRUE, FALSE), bath_unknown_2124 = FALSE)
pairs <- tibble::tibble(house_id = c("a", "a", "b"), site_id = c(10L, 2L, 10L),
                       distance_m = c(100, 100, 2000))
nearest <- nearest_salience_sites(pairs, sites, "house_id")
duplicate_error <- tryCatch(nearest_salience_sites(dplyr::bind_rows(pairs, pairs[1, ]),
  sites, "house_id"), error = identity)
stopifnot(inherits(duplicate_error, "error"))
stopifnot(identical(nearest$site_id, c(2L, 10L)),
          identical(nearest_salience_sites(pairs[3:1, ], sites, "house_id"), nearest))
error <- tryCatch(nearest_salience_sites(pairs, sites[1, ], "house_id"), error = identity)
stopifnot(inherits(error, "error"))

# Radius evidence must not multiply transactions. Missing evidence leaves the
# row present but outside the groups supported by that evidence.
transactions <- tibble::tibble(house_id = c("a", "b", "c"), region = "South West")
companion <- tibble::tibble(house_id = c("a", "b", "a"), radius = c(250L, 250L, 500L),
  min_coast_dist_m = c(100, 5000, 5000), any_bath_2124 = c(TRUE, FALSE, FALSE),
  bath_unknown_2124 = FALSE)
joined <- join_salience_group_companion(transactions, companion, "house_id")
stopifnot(identical(joined$house_id, transactions$house_id),
          identical(joined$bathing, c(TRUE, FALSE, FALSE)),
          identical(joined$coastal, c(TRUE, FALSE, FALSE)),
          identical(joined$inland, c(FALSE, TRUE, FALSE)))
error <- tryCatch(join_salience_group_companion(transactions,
  dplyr::bind_rows(companion, companion[1, ]), "house_id"), error = identity)
stopifnot(inherits(error, "error"), grepl("unique", conditionMessage(error)))

# Sample counts are per group, before estimator removals; shared rows are counted
# in each selected group. All four extensive Post cells must be supported.
sample <- classified[rep(c(1L, 2L, 3L, 4L), each = 4L), ]
sample$near_bin <- rep(c(1L, 1L, 0L, 0L), 4)
sample$post <- rep(c(0L, 1L, 0L, 1L), 4)
audit <- audit_salience_groups(sample, "sales")
stopifnot(identical(audit$n_before_london_drop, c(8L, 8L, 8L)),
          identical(audit$n_estimation, c(4L, 4L, 8L)),
          identical(audit$n_near_pre, c(1L, 1L, 2L)))
error <- tryCatch(audit_salience_groups(dplyr::filter(sample, .data$post == 1L), "sales"),
                  error = identity)
stopifnot(inherits(error, "error"), grepl("support", conditionMessage(error)))

# Preserve the pre-existing formatting fix when reporting tiny negative effects.
synthetic <- structure(list(
  tidy = data.frame(term = c("small_negative", "rounded_zero"),
    estimate = c(-0.00026, -0.0000001), std.error = c(0.0001, 0.0001)),
  glance = data.frame(nobs = 100L)
), class = "modelsummary_list")
latex <- modelsummary::modelsummary(list(Test = synthetic), output = "latex",
  fmt = 5, stars = FALSE,
  gof_map = tibble::tribble(~raw, ~clean, ~fmt, "nobs", "Observations", 0))
formatted <- fit_tblr_latex(latex)
stopifnot(grepl("-0.00026", formatted, fixed = TRUE),
          !grepl("-0.00000", formatted, fixed = TRUE))
cat("Overlapping salience group contracts passed.\n")
