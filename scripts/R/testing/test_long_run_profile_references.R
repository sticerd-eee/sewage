# Verify display contrasts against independently re-estimated reference-year models,
# over the expanded 1995-2024 sales profile window and the unchanged 2014-2023
# rental window.
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

# Irregular observation histories, as in the report, rather than a balanced panel.
# Sales span the expanded 1995-2024 profile window; rentals keep 2014-2023.
synthetic_panel <- function(years, seed, n_properties = 120L, keep = 0.75) {
  set.seed(seed)
  panel <- data.table::CJ(repeat_id = sprintf("p%03d", seq_len(n_properties)), year = years)
  panel[, property_number := as.integer(sub("p", "", repeat_id))]
  panel[, `:=`(
    near = property_number %% 2L,
    lsoa = sprintf("l%03d", ceiling(property_number / 4)),
    msoa = sprintf("m%02d", ceiling(property_number / 12)),
    date = as.Date(sprintf("%d-%02d-15", year, sample.int(12, .N, replace = TRUE)))
  )]
  panel[, month_index := calendar_month_index(date)]
  panel[, log_price := 10 + property_number / 100 + (year - 2014) / 30 +
    near * (year - 2019)^2 / 200 + stats::rnorm(.N, sd = 0.04)]
  panel[stats::runif(nrow(panel)) < keep]
}
sales_panel <- synthetic_panel(1995:2024, seed = 20260920)
rental_panel <- synthetic_panel(2014:2023, seed = 20260921)
stopifnot(min(sales_panel$month_index) < 0L) # pre-2014 months carry nonpositive indices

check_against_refits <- function(panel, market) {
  lapply(names(MODEL_FE), function(fe) {
    profile <- fit_profile(panel, market, fe)$profile
    stopifnot(profile$status == "estimated")
    stopifnot(identical(profile$joint_test$years, CONFIG$profile_joint_years))
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
        identical(range(rows$year), range(panel$year)),
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
}

sales_profiles <- check_against_refits(sales_panel, "sales")
rental_profiles <- check_against_refits(rental_panel, "rentals")
profiles <- c(sales_profiles, rental_profiles)
names(profiles) <- vapply(profiles, function(p) p$model_key, character(1))

# Joint-test scope: the sales restriction stays 2014-2018 relative to 2019 on the
# expanded fit, and a missing restricted coefficient leaves it unavailable
# rather than replaced by a reduced test.
joint <- sales_profiles[[1]]$joint_test
stopifnot(joint$status == "available", identical(joint$years, 2014:2018), joint$df1 == 5L)
no_near_2016 <- sales_panel[!(year == 2016L & near == 1L)]
missing_2016 <- fit_profile(no_near_2016, "sales", "property_month")$profile
stopifnot(
  missing_2016$status == "estimated",
  !isTRUE(missing_2016$coefficients$available[missing_2016$coefficients$year == 2016L]),
  missing_2016$joint_test$status == "unavailable",
  grepl("2016", missing_2016$joint_test$reason),
  identical(missing_2016$joint_test$years, 2014:2018)
)
# An early year without near properties stays unavailable at every reference
# rather than appearing at zero; the supported years remain usable.
no_near_1996 <- sales_panel[!(year == 1996L & near == 1L)]
early_gap <- fit_profile(no_near_1996, "sales", "property_month")$profile
for (reference_year in c(2014L, 2021L)) {
  rows <- profile_reference_rows(list(early_gap), reference_year)
  stopifnot(
    is.na(rows$estimate[rows$year == 1996L]),
    is.na(rows$conf_low[rows$year == 1996L]),
    all(is.finite(rows$estimate[rows$year %in% c(1995L, 1997L, 2024L)])),
    all(rows$baseline_available)
  )
}

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
panel_ranges <- function(built, axis) {
  lapply(built$layout$panel_params, function(p) p[[paste0(axis, ".range")]])
}
stopifnot(
  length(plots[[1]]$layout$panel_params) == 4L,
  identical(panel_ranges(plots[[1]], "y"), panel_ranges(plots[[2]], "y")),
  identical(panel_ranges(plots[[1]], "x"), panel_ranges(plots[[2]], "x"))
)
# Market-specific year domains: sales panels span 1995-2024, rental panels 2014-2023.
layout <- plots[[1]]$layout$layout
x_ranges <- panel_ranges(plots[[1]], "x")
for (i in seq_len(nrow(layout))) {
  market <- as.character(layout$market_label[[i]])
  expected <- if (market == MARKET_LABELS[["sales"]]) c(1995, 2024) else c(2014, 2023)
  stopifnot(
    x_ranges[[i]][1] <= expected[1], x_ranges[[i]][1] > expected[1] - 2,
    x_ranges[[i]][2] >= expected[2], x_ranges[[i]][2] < expected[2] + 2
  )
}
# The 2019 figure keeps four panels with the same market-specific domains.
original <- ggplot2::ggplot_build(plot_profiles(profiles))
stopifnot(length(original$layout$panel_params) == 4L)
original_x <- panel_ranges(original, "x")
for (i in seq_len(nrow(original$layout$layout))) {
  market <- as.character(original$layout$layout$market_label[[i]])
  expected <- if (market == MARKET_LABELS[["sales"]]) c(1995, 2024) else c(2014, 2023)
  stopifnot(original_x[[i]][1] <= expected[1], original_x[[i]][2] >= expected[2], original_x[[i]][2] < expected[2] + 2)
}
breaks_long <- profile_year_breaks(2019L)(c(1994.4, 2024.6))
breaks_short <- profile_year_breaks(2021L)(c(2013.8, 2023.2))
stopifnot(
  identical(as.numeric(breaks_long), c(1995, 2000, 2005, 2010, 2015, 2019)),
  identical(as.numeric(breaks_short), c(2014, 2016, 2018, 2021)),
  2019 %in% profile_year_breaks(2019L)(c(2013.8, 2024.2))
)

missing_reference <- profiles[[1]]
missing_reference$coefficients$available[missing_reference$coefficients$year == 2014] <- FALSE
missing_rows <- profile_reference_rows(list(missing_reference), 2014L)
stopifnot(all(!missing_rows$baseline_available), all(is.na(missing_rows$estimate)))

missing_covariance <- profiles[[1]]
missing_covariance$vcov <- NULL
missing_rows <- profile_reference_rows(list(missing_covariance), 2021L)
stopifnot(all(is.finite(missing_rows$estimate)), all(is.na(missing_rows$conf_low)))

cat("PASS: both FE structures match direct refits at three reference years over 1995-2024 sales ",
    "and 2014-2023 rental histories; joint-test scope, unsupported early years, market-specific ",
    "domains, shared scales, immutable profiles, unavailable reference and covariance checked.\n", sep = "")
