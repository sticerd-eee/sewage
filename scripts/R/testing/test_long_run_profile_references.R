# Verify display contrasts against independently re-estimated reference-year models.
# Run from the project root: Rscript scripts/R/testing/test_long_run_profile_references.R

qmd <- here::here(
  "docs", "reports", "2026-09-19-001-long-run-public-attention-repeat-properties.qmd"
)
lines <- readLines(qmd, warn = FALSE)
labels <- c(
  "setup", "helpers-inputs", "helpers-models", "helpers-bundle", "helpers-display",
  "helpers-profile-references"
)
for (label in labels) {
  start <- match(paste0("#| label: ", label), lines)
  stopifnot(!is.na(start))
  end <- which(seq_along(lines) > start & lines == "```")[1L]
  eval(parse(text = lines[seq.int(start + 1L, end - 1L)]))
}

set.seed(20260920)
panel <- data.table::CJ(repeat_id = sprintf("p%03d", 1:120), year = 2014:2024)
panel[, property_number := as.integer(sub("p", "", repeat_id))]
panel[, `:=`(
  near = property_number %% 2L,
  lsoa = sprintf("l%03d", ceiling(property_number / 4)),
  msoa = sprintf("m%02d", ceiling(property_number / 12)),
  date = as.Date(sprintf("%d-%02d-15", year, sample.int(12, .N, replace = TRUE)))
)]
panel[, month_index := (year - 2014L) * 12L + lubridate::month(date)]
panel[, log_price := 10 + property_number / 100 + (year - 2014) / 30 +
  near * (year - 2019)^2 / 200 + stats::rnorm(.N, sd = 0.04)]
# Irregular observation histories, as in the report, rather than a balanced panel.
panel <- panel[stats::runif(nrow(panel)) > 0.25]

profiles <- lapply(names(MODEL_FE), function(fe) {
  profile <- fit_profile(panel, "sales", fe)$profile
  stopifnot(profile$status == "estimated")
  for (reference_year in c(2014L, 2021L, 2019L)) {
    rows <- profile_reference_rows(list(profile), reference_year)
    direct <- fixest::feols(
      stats::as.formula(sprintf(
        "log_price ~ i(year, near, ref = %d) | %s", reference_year, MODEL_FE[[fe]]
      )),
      data = panel, notes = FALSE
    ) |>
      summary(vcov = ~lsoa)
    contrasts <- rows[!rows$is_reference, ]
    terms <- profile_term(contrasts$year)
    stopifnot(
      isTRUE(all.equal(contrasts$estimate, unname(stats::coef(direct)[terms]), tolerance = 1e-6)),
      isTRUE(all.equal(contrasts$std_error, unname(sqrt(diag(direct$cov.scaled))[terms]), tolerance = 1e-6)),
      isTRUE(all.equal(contrasts$conf_low, unname(stats::confint(direct)[terms, 1]), tolerance = 1e-6)),
      isTRUE(all.equal(contrasts$conf_high, unname(stats::confint(direct)[terms, 2]), tolerance = 1e-6)),
      rows$estimate[rows$is_reference] == 0,
      is.na(rows$conf_low[rows$is_reference])
    )
  }
  profile
})

before <- serialize(profiles, NULL)
views <- dplyr::bind_rows(lapply(c(2014L, 2021L), function(year) {
  profile_reference_rows(profiles, year)
}))
stopifnot(identical(before, serialize(profiles, NULL)))
limits <- profile_reference_limits(views)
plots <- lapply(c(2014L, 2021L), function(year) {
  plot <- plot_profile_reference(views, year, limits)
  stopifnot(inherits(ggplot2::ggplotGrob(plot), "gtable"))
  ggplot2::ggplot_build(plot)
})
stopifnot(identical(
  lapply(plots[[1]]$layout$panel_params, function(p) p$y.range),
  lapply(plots[[2]]$layout$panel_params, function(p) p$y.range)
))

missing_reference <- profiles[[1]]
missing_reference$coefficients$available[missing_reference$coefficients$year == 2014] <- FALSE
missing_rows <- profile_reference_rows(list(missing_reference), 2014L)
stopifnot(all(!missing_rows$baseline_available), all(is.na(missing_rows$estimate)))

missing_covariance <- profiles[[1]]
missing_covariance$vcov <- NULL
missing_rows <- profile_reference_rows(list(missing_covariance), 2021L)
stopifnot(all(is.finite(missing_rows$estimate)), all(is.na(missing_rows$conf_low)))

cat("PASS: both FE structures match direct refits at three reference years; ",
    "shared scales, immutable profiles, unavailable reference and covariance checked.\n", sep = "")
