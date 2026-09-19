# ==============================================================================
# Extensive-Margin Salience Reproduction Contracts
# Run from the repository root with plain Rscript.
# ==============================================================================

source(here::here("scripts", "R", "testing", "salience_report_test_setup.R"))

# A balanced design gives known Near and Near x Post effects. The disturbance
# varies only over location/month and is orthogonal to both treatment regressors.
data <- expand.grid(lsoa = 1:6, month_id = 1:4, near_bin = 0:1,
                    property_type = 0:1, old_new = 0:1, duration = 0:1)
data$post <- as.integer(data$month_id >= 3L)
data$log_price <- with(data, 1 + 0.3 * near_bin + 0.02 * near_bin * post +
  0.1 * property_type + 0.05 * old_new + 0.04 * duration +
  0.001 * lsoa * (month_id - 2.5))
model <- fit_salience_extensive(data, "sales", "post")

# Reference layout follows the published 12-column parent, including unlabeled
# SE rows beginning directly with '&'. Other columns deliberately differ.
reference <- tempfile(fileext = ".tex")
reference_row <- function(label, value) {
  cells <- rep("\\num{-9.999}", 12L)
  cells[c(6L, 12L)] <- paste0("\\num{", value, "}")
  paste0(paste(c(label, cells), collapse = " & "), " \\\\")
}
rows <- c(
  reference_row("Near bin", "0.300"), reference_row("", "0.000"),
  reference_row("{Near bin \\\\ $\\times$ Post}", "0.020"), reference_row("", "0.000"),
  reference_row("Observations", "384")
)
rows <- sub("^ &", "&", rows)
writeLines(rows, reference)
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
      prepared$log_price <- with(prepared, log_price - 0.02 * near_bin * post +
                                  0.02 * near_bin * log_cumulative_articles)
      prepared$post <- NULL
    }
    fitted <- fit_salience_extensive(prepared, market, attention)
    result <- verify_salience_reproduction(fitted, reference, market, attention)
    stopifnot(all(result$passed), all(result$nobs == 384L),
              identical(result$printed_estimate, c("0.300", "0.020")))
  }
}
# A mismatched reference must fail closed, even if the main coefficient matches.
writeLines(sub("384", "385", rows, fixed = TRUE), reference)
error <- tryCatch(verify_salience_reproduction(model, reference, "sales", "post"), error = identity)
stopifnot(inherits(error, "error"), grepl("Reproduction failed", conditionMessage(error)))
unlink(reference)
cat("Extensive-margin salience reproduction contracts passed.\n")
