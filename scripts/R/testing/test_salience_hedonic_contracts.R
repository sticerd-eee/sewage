# ==============================================================================
# Baseline Hedonic Salience Contracts -- run with plain Rscript from the root
# ==============================================================================
source(here::here("scripts", "R", "testing", "salience_report_test_setup.R"))

# The baseline retains zero exposure, London and missing coordinates. Both
# count and hours must be observed, as in the parent, even for count models.
transactions <- tibble::tibble(
  house_id = 1:9, price = 100 * (1:9), region = c("London", rep("South West", 8)),
  month_id = c(1L, 48L, rep(2L, 7)), lsoa = c(rep("A", 6), NA, "A", "A"),
  latitude = NA_real_, longitude = NA_real_,
  property_type = c(rep("D", 7), NA, "D"), old_new = "N", duration = "F"
)
exposure <- tibble::tibble(
  house_id = c(1:9, 1L), radius = c(rep(250L, 9), 500L), price = -999,
  n_spill_sites = c(rep(1L, 4), 0L, rep(1L, 5)),
  spill_count_weekly_avg = c(0, 1, NA, 1, 1, 2, 1, 1, 3, 999),
  spill_hrs_weekly_avg = c(0, 2, 2, NA, rep(2, 6))
)
sales <- prepare_salience_hedonic(transactions, exposure, "sales")
stopifnot(identical(sales$house_id, c(1L, 2L, 6L, 9L)),
          identical(sales$spill_count_weekly_avg, c(0, 1, 2, 3)),
          identical(sales$log_price, log(c(100, 200, 600, 900))),
          sales$region[1L] == "London")
rentals <- transactions |>
  dplyr::rename(rental_id = house_id, listing_price = price) |>
  dplyr::mutate(month_id = pmin(.data$month_id, 36L),
                bedrooms = c(rep(2, 8), NA), bathrooms = 1)
rent <- prepare_salience_hedonic(rentals, dplyr::rename(exposure, rental_id = house_id), "rentals")
stopifnot(identical(rent$rental_id, c(1L, 2L, 6L)))

# Only the shared nearest-site classifier determines baseline strata. Missing
# nearest sites and missing coast are distinct exclusions and must be logged.
nearest <- tibble::tibble(house_id = 1:6, site_id = 1:6, min_distance = 100,
  distance_to_coast_m = c(100, 100, 3000, 3000, NA, 100),
  bath_ever_2124 = c(TRUE, FALSE, TRUE, FALSE, FALSE, TRUE),
  bath_unknown_2124 = c(TRUE, TRUE, FALSE, FALSE, FALSE, FALSE))
classified <- join_nearest_salience(
  tibble::tibble(house_id = 1:7, region = c(rep("South West", 5), "London", "South West")),
  nearest, "house_id"
)
audit <- log_salience_cells(classified, nearest, "sales", "coast_bathing")
stopifnot(identical(audit$n_before_london_drop, c(2L, 1L, 1L, 1L)),
          all(audit$n_after_london_drop == 1L), all(audit$n_without_nearest == 1L),
          all(audit$n_unclassified == 2L), all(audit$n_nearest_sites_missing_coast == 1L))
excluded <- classify_salience_coast(classified, unknown_policy = "exclude")
stopifnot(excluded$salience_class[1L] == "coastal_bathing", is.na(excluded$salience_class[2L]))
error <- tryCatch(log_salience_cells(excluded, nearest, "sales", "coast_bathing"), error = identity)
stopifnot(inherits(error, "error"), grepl("sales / coastal_not_bathing", conditionMessage(error)))

# A balanced design identifies a known spill effect. Compare the full covariance
# with explicit heteroskedasticity-robust OLS + LSOA, without time fixed effects.
data <- expand.grid(lsoa = 1:6, month_id = 1:4, spill_count_weekly_avg = c(0, 1, 3),
                    property_type = 0:1, old_new = 0:1, duration = 0:1)
data$log_price <- with(data, 1 + 0.3 * spill_count_weekly_avg + 0.1 * property_type +
  0.05 * old_new + 0.04 * duration + 0.001 * lsoa * (month_id - 2.5))
for (market in c("sales", "rentals")) {
  prepared <- data
  if (market == "rentals") {
    prepared <- dplyr::rename(prepared, bedrooms = old_new, bathrooms = duration)
  }
  model <- fit_salience_hedonic(prepared, market)
  formula <- if (market == "sales") {
    log_price ~ spill_count_weekly_avg + property_type + old_new + duration | lsoa
  } else log_price ~ spill_count_weekly_avg + property_type + bedrooms + bathrooms | lsoa
  expected <- fixest::feols(formula, data = prepared, vcov = "hetero")
  stopifnot(abs(coef(model)["spill_count_weekly_avg"] - 0.3) < 1e-12,
            identical(model$fixef_vars, "lsoa"), nobs(model) == nrow(data),
            isTRUE(all.equal(vcov(model), vcov(expected), tolerance = 1e-12)))
}
# The reproduction gate must read the saturated columns in both panels and
# fail on a coefficient, standard-error or sample-size mismatch.
reference <- tempfile(fileext = ".tex")
reference_row <- function(label, value) {
  cells <- rep("\\num{-9.999}", 12L)
  cells[c(6L, 12L)] <- paste0("\\num{", value, "}")
  paste0(paste(c(label, cells), collapse = " & "), " \\\\")
}
rows <- c(reference_row("Spills per week (avg.)", "0.300"),
          reference_row("", fmt_table(fixest::se(model)["spill_count_weekly_avg"])),
          reference_row("Observations", as.character(nobs(model))))
writeLines(rows, reference)
for (market in c("sales", "rentals")) {
  stopifnot(verify_hedonic_reproduction(model, reference, market)$passed)
}
for (bad_rows in list(sub("0.300", "0.301", rows, fixed = TRUE),
                      replace(rows, 2L, reference_row("", "0.999")),
                      replace(rows, 3L, reference_row("Observations", "575")))) {
  writeLines(bad_rows, reference)
  error <- tryCatch(verify_hedonic_reproduction(model, reference, "sales"), error = identity)
  stopifnot(inherits(error, "error"), grepl("Reproduction failed", conditionMessage(error)))
}
unlink(reference)
# Exercise the real table exporter: eight saturated columns with their N and
# no time effects. Rendering in the publication harness is a separate gate.
table_path <- tempfile(fileext = ".tex")
fitted <- stats::setNames(rep(list(model), 4L), names(salience_strata("coast_bathing")))
variant <- tibble::tibble(coast_rule_m = 2000, unknown_policy = "not_designated", drop_london = TRUE)
export_salience_hedonic(list(sales = fitted, rentals = fitted), variant, table_path)
latex <- readLines(table_path)
observations <- strsplit(latex[startsWith(latex, "Observations &")], "&", fixed = TRUE)[[1L]][-1L]
stopifnot(length(observations) == 8L, all(grepl("\\num{576}", observations, fixed = TRUE)),
          any(grepl("Time FE & No & No & No & No & No & No & No & No", latex, fixed = TRUE)),
          any(grepl("Heteroskedasticity-robust", latex, fixed = TRUE)),
          any(grepl("\\begin{table}[H]", latex, fixed = TRUE)))
unlink(table_path)
data$spill_count_weekly_avg <- data$lsoa
error <- tryCatch(fit_salience_hedonic(data, "sales"), error = identity)
stopifnot(inherits(error, "error"), grepl("Unidentified spill effect", conditionMessage(error)))
cat("Baseline hedonic salience contracts passed.\n")
