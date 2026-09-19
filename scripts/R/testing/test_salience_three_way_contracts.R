# Three-way classification and high-precision reporting; no data rebuilds.
source(here::here("scripts", "R", "testing", "salience_report_test_setup.R"))

# Bathing takes precedence, unresolved non-bathing stays eligible, missing coast
# stays excluded, and the London flag is preserved for the estimation filter.
fixture <- tibble::tibble(
  distance_to_coast_m = c(100, 5000, 100, 5000, 100, NA_real_),
  bath_ever_2124 = c(TRUE, TRUE, FALSE, FALSE, FALSE, TRUE),
  bath_unknown_2124 = c(TRUE, FALSE, FALSE, FALSE, TRUE, FALSE),
  region = c("London", rep("South West", 5))
)
classified <- fixture |>
  extensive$flag_salience_london() |>
  extensive$classify_salience_coast() |>
  classify_three_way()
stopifnot(identical(classified$three_way,
                    c("bathing", "bathing", "coastal", "inland", "coastal", NA_character_)),
          identical(classified$london, c(TRUE, rep(FALSE, 5))))

# Tiny negative estimates and CI limits must retain their sign at five decimals.
synthetic <- structure(list(
  tidy = data.frame(term = c("small_negative", "rounded_zero"),
    estimate = c(-0.00026, -0.0000001), std.error = c(0.0001, 0.0001),
    conf.low = c(-0.00046, -0.0002), conf.high = c(-0.00006, 0.0002)),
  glance = data.frame(nobs = 100L)
), class = "modelsummary_list")
latex <- modelsummary::modelsummary(list(Test = synthetic), output = "latex",
  statistic = "[{conf.low}, {conf.high}]", fmt = 5, stars = FALSE,
  gof_map = tibble::tribble(~raw, ~clean, ~fmt, "nobs", "Observations", 0))
formatted <- extensive$fit_tblr_latex(latex)
numbers <- regmatches(formatted, gregexpr("\\\\num\\{[^}]+\\}", formatted))[[1L]]
numbers <- as.numeric(sub(".*\\{([^}]+)\\}", "\\1", numbers))
stopifnot(isTRUE(all.equal(numbers, c(-0.00026, -0.00046, -0.00006, 0, -0.0002, 0.0002, 100))),
          !grepl("-0.00000", formatted, fixed = TRUE))

cat("Three-way classification and signed confidence-interval reporting passed.\n")
