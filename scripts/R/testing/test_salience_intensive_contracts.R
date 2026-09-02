# ==============================================================================
# Intensive-Margin Salience Contracts
# Run from the repository root with plain Rscript.
# ==============================================================================

source(here::here("scripts", "R", "09_analysis", "05_news",
                  "intensive_margin_salience_utils.R"))

# Preparation retains London until the stratum filter, takes the transaction
# price, restricts exposure to 250m, and keeps zero exposure but never unknowns.
transactions <- tibble::tibble(
  house_id = 1:11, price = c(100, 200, 300, 0, rep(100, 7)),
  region = c("London", rep("South West", 10)),
  month_id = c(1L, 36L, 48L, 2L, 2L, 2L, 2L, 2L, 49L, 0L, 2L),
  lsoa = "A", latitude = c(rep(51, 6), NA, rep(51, 4)), longitude = -1,
  property_type = c(rep("D", 7), NA, "D", "D", "D"),
  old_new = "N", duration = "F"
)
exposure <- tibble::tibble(
  house_id = c(1:11, 1L), radius = c(rep(250L, 11), 500L),
  price = -999, n_spill_sites = c(rep(1L, 4), 0L, rep(1L, 7)),
  spill_count_weekly_avg = c(0, 1, 2, 1, 1, NA, rep(1, 5), 999)
)
post <- tibble::tibble(month_id = 0:49, post = as.integer(month_id >= 20L))
sales <- prepare_salience_intensive(transactions, exposure, post, "sales", "post")
stopifnot(identical(sales$house_id, c(1L, 2L, 3L, 11L)),
          identical(sales$spill_count_weekly_avg, c(0, 1, 2, 1)),
          isTRUE(all.equal(sales$log_price, log(c(100, 200, 300, 100)))),
          sales$region[1L] == "London",
          identical(sales$post, c(0L, 1L, 1L, 0L)))
rentals <- transactions |>
  dplyr::rename(rental_id = house_id, listing_price = price, bedrooms = old_new, bathrooms = duration) |>
  dplyr::mutate(bedrooms = 2, bathrooms = 1)
rental_exposure <- dplyr::rename(exposure, rental_id = house_id)
rent <- prepare_salience_intensive(rentals, rental_exposure, post, "rentals", "post")
stopifnot(identical(rent$rental_id, c(1L, 2L, 11L)))
# Missing article months and non-finite log coverage do not enter the sample.
articles <- tibble::tibble(month_id = c(1L, 36L, 48L),
                           log_cumulative_articles = c(-Inf, log(5), log(20)))
sales_articles <- prepare_salience_intensive(
  transactions, exposure, articles, "sales", "log_cumulative_articles"
)
stopifnot(identical(sales_articles$house_id, c(2L, 3L)))

# Known effects in a balanced design, with location/month disturbances orthogonal
# to spill exposure. Both property-control sets and attention measures are used.
data <- expand.grid(lsoa = 1:6, month_id = 1:4, spill_count_weekly_avg = c(0, 1, 3),
                    property_type = 0:1, old_new = 0:1, duration = 0:1)
data$post <- as.integer(data$month_id >= 3L)
data$log_price <- with(data, 1 + 0.3 * spill_count_weekly_avg +
  0.02 * spill_count_weekly_avg * post + 0.1 * property_type +
  0.05 * old_new + 0.04 * duration + 0.001 * lsoa * (month_id - 2.5))
reference <- tempfile(fileext = ".tex")
reference_row <- function(label, value) {
  cells <- rep("\\num{-9.999}", 12L)
  cells[c(6L, 12L)] <- paste0("\\num{", value, "}")
  paste0(paste(c(label, cells), collapse = " & "), " \\\\")
}
rows <- c(
  reference_row("Spills per week (avg.)", "0.300"), reference_row("", "0.000"),
  reference_row("{Spills per week (avg.) \\\\ $\\times$ Attention}", "0.020"),
  reference_row("", "0.000"), reference_row("Observations", "576")
)
writeLines(sub("^ &", "&", rows), reference)
for (market in c("sales", "rentals")) {
  market_data <- data
  if (market == "rentals") {
    names(market_data)[names(market_data) == "old_new"] <- "bedrooms"
    names(market_data)[names(market_data) == "duration"] <- "bathrooms"
  }
  for (attention in c("post", "log_cumulative_articles")) {
    prepared <- market_data
    if (attention == "log_cumulative_articles") {
      prepared$log_cumulative_articles <- log(c(1, 3, 10, 50))[prepared$month_id]
      prepared$log_price <- with(prepared, log_price -
        0.02 * spill_count_weekly_avg * post +
        0.02 * spill_count_weekly_avg * log_cumulative_articles)
    }
    model <- fit_salience_intensive(prepared, market, attention)
    terms <- c("spill_count_weekly_avg", paste0("spill_count_weekly_avg:", attention))
    stopifnot(isTRUE(all.equal(unname(coef(model)[terms]), c(0.3, 0.02), tolerance = 1e-12)))
    result <- verify_salience_reproduction(
      model, reference, market, attention, exposure = "spill_count_weekly_avg"
    )
    stopifnot(all(result$passed), all(result$nobs == 576L))
  }
}
# A wrong sample size or interaction fails the published reproduction gate.
for (bad_rows in list(gsub("576", "575", rows, fixed = TRUE),
                      gsub("0.020", "0.025", rows, fixed = TRUE))) {
  writeLines(bad_rows, reference)
  error <- tryCatch(verify_salience_reproduction(
    model, reference, "rentals", "log_cumulative_articles", "spill_count_weekly_avg"
  ), error = identity)
  stopifnot(inherits(error, "error"), grepl("Reproduction failed", conditionMessage(error)))
}
unlink(reference)
# An unidentified spill-attention interaction must not silently become a blank
# table cell (Post constant within this sample).
data$post <- 1L
error <- tryCatch(fit_salience_intensive(data, "sales", "post"), error = identity)
stopifnot(inherits(error, "error"), grepl("Unidentified", conditionMessage(error)))
cat("Intensive-margin salience contracts passed.\n")
