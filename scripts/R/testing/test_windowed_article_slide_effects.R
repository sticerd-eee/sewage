# ==============================================================================
# Validate transformed effects for the windowed-article appendix slide
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required.", call. = FALSE)
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c("fixest", "here")
check_required_packages(REQUIRED_PACKAGES)

source(
  here::here(
    "scripts", "R", "09_analysis", "05_news",
    "windowed_article_effect_size_utils.R"
  ),
  local = TRUE
)

exact_effect <- exact_percent_effect(
  estimate = log(0.9974),
  std_error = 0.0016
)
stopifnot(
  isTRUE(all.equal(exact_effect[["estimate_pct"]], -0.26, tolerance = 1e-12)),
  isTRUE(all.equal(
    exact_effect[["std_error_pct"]],
    100 * 0.9974 * 0.0016,
    tolerance = 1e-12
  ))
)

source(
  here::here(
    "scripts", "R", "09_analysis", "05_news",
    "did_articles_windowed_prior_extensive.R"
  ),
  local = TRUE
)

set.seed(1601)
n <- 400L
test_data <- data.frame(
  location = rep(seq_len(20L), each = n / 20L),
  cluster = rep(seq_len(40L), each = n / 40L),
  near_bin = rep(c(0, 1), length.out = n),
  salience = stats::rnorm(n, mean = 4, sd = 0.8),
  salience_alt = stats::rnorm(n, mean = 2, sd = 0.5),
  error = stats::rnorm(n, sd = 0.15)
)
test_data$log_price <- with(
  test_data,
  10 - 0.03 * near_bin + 0.012 * near_bin * salience + error
)

preferred_model_names <- names(windowed_article_preferred_model_specs)
expected_model_specs <- list(
  sale_msoa = c(market = "sales", fixed_effects = "msoa"),
  sale_lsoa = c(market = "sales", fixed_effects = "lsoa"),
  rent_msoa = c(market = "rentals", fixed_effects = "msoa"),
  rent_lsoa = c(market = "rentals", fixed_effects = "lsoa")
)
stopifnot(identical(preferred_model_names, names(expected_model_specs)))

model_shifts <- stats::setNames(c(0.001, 0.002, 0.003, 0.004), preferred_model_names)
make_fixture_model <- function(salience_col, shift) {
  fixture_data <- test_data
  fixture_data$fixture_outcome <- fixture_data$log_price +
    shift * fixture_data$near_bin * fixture_data[[salience_col]]
  fixest::feols(
    stats::as.formula(paste0(
      "fixture_outcome ~ near_bin + near_bin:", salience_col, " | location"
    )),
    data = fixture_data,
    vcov = ~cluster
  )
}

salience_cols <- c(Cumulative = "salience", `3m` = "salience_alt")
models_by_measure <- lapply(salience_cols, function(salience_col) {
  lapply(model_shifts, function(shift) {
    make_fixture_model(salience_col, shift)
  })
})

effect_inputs_path <- tempfile(fileext = ".csv")
on.exit(unlink(effect_inputs_path), add = TRUE)
effect_inputs <- write_windowed_article_effect_sizes(
  models_by_measure = models_by_measure,
  salience_cols = salience_cols,
  interaction_term_fn = interaction_term,
  margin = "extensive",
  output_path = effect_inputs_path
)
summary_effects <- summarise_slide_effects(effect_inputs)

stopifnot(
  nrow(summary_effects) == 24L,
  identical(
    sort(unique(summary_effects$effect)),
    sort(c("interaction", "change_per_iqr", "change_per_sd"))
  )
)

for (measure in names(models_by_measure)) {
  salience_col <- salience_cols[[measure]]
  interaction <- interaction_term(salience_col)

  for (model_name in preferred_model_names) {
    model <- models_by_measure[[measure]][[model_name]]
    expected_spec <- expected_model_specs[[model_name]]
    coefficient <- extract_fixest_term(model, interaction)
    model_data <- fixest::fixest_data(model, sample = "estimation")
    salience_quantiles <- stats::quantile(
      model_data[[salience_col]], c(0.25, 0.75), names = FALSE
    )
    scales <- c(
      change_per_iqr = salience_quantiles[[2]] - salience_quantiles[[1]],
      change_per_sd = stats::sd(model_data[[salience_col]])
    )

    mapped_rows <- dplyr::filter(
      summary_effects,
      .data$market == expected_spec[["market"]],
      .data$fixed_effects == expected_spec[["fixed_effects"]],
      .data$measure == .env$measure
    )
    mapped_interaction <- dplyr::filter(
      mapped_rows, .data$effect == "interaction"
    )
    stopifnot(
      nrow(mapped_rows) == 3L,
      nrow(mapped_interaction) == 1L,
      isTRUE(all.equal(
        mapped_interaction$estimate_log[[1]],
        coefficient[["estimate"]],
        tolerance = 1e-10
      )),
      isTRUE(all.equal(
        mapped_interaction$std_error_log[[1]],
        coefficient[["std_error"]],
        tolerance = 1e-10
      )),
      is.na(mapped_interaction$estimate_pct[[1]]),
      is.na(mapped_interaction$std_error_pct[[1]])
    )

    for (effect in names(scales)) {
      scale <- scales[[effect]]
      expected_log_estimate <- coefficient[["estimate"]] * scale
      expected_log_se <- coefficient[["std_error"]] * scale
      expected_pct <- exact_percent_effect(expected_log_estimate, expected_log_se)
      mapped_effect <- dplyr::filter(
        mapped_rows, .data$effect == .env$effect
      )
      stopifnot(
        nrow(mapped_effect) == 1L,
        isTRUE(all.equal(
          mapped_effect$estimate_log[[1]], expected_log_estimate,
          tolerance = 1e-10
        )),
        isTRUE(all.equal(
          mapped_effect$std_error_log[[1]], expected_log_se,
          tolerance = 1e-10
        )),
        isTRUE(all.equal(
          mapped_effect$estimate_pct[[1]], expected_pct[["estimate_pct"]],
          tolerance = 1e-10
        )),
        isTRUE(all.equal(
          mapped_effect$std_error_pct[[1]], expected_pct[["std_error_pct"]],
          tolerance = 1e-10
        )),
        isTRUE(all.equal(mapped_effect$scale[[1]], scale, tolerance = 1e-10))
      )
    }
  }
}

stopifnot(
  identical(significance_stars(0.009), "***"),
  identical(significance_stars(0.01), "**"),
  identical(significance_stars(0.049), "**"),
  identical(significance_stars(0.05), "*"),
  identical(significance_stars(0.099), "*"),
  identical(significance_stars(0.1), "")
)

fake_effects <- expand.grid(
  market = c("sales", "rentals"),
  fixed_effects = c("msoa", "lsoa"),
  measure = names(windowed_article_salience_cols),
  effect = c("interaction", "change_per_iqr", "change_per_sd"),
  stringsAsFactors = FALSE
)
cell_id <- 10 * match(fake_effects$market, c("sales", "rentals")) +
  match(fake_effects$measure, names(windowed_article_salience_cols))
fake_effects$estimate_log <- cell_id / 1000
fake_effects$std_error_log <- 0.001
fake_effects$estimate_pct <- cell_id / 10
fake_effects$std_error_pct <- 0.01
fake_effects$p_value <- 1

selected_coefficients <- slide_effect_cells(
  fake_effects,
  fixed_effects = "msoa",
  effect = "interaction",
  statistic = "estimate",
  measure_order = names(windowed_article_salience_cols)
)
stopifnot(
  identical(
    selected_coefficients,
    c("0.011", "0.012", "0.013", "0.014", "0.021", "0.022", "0.023", "0.024")
  )
)

selected_effects <- slide_effect_cells(
  fake_effects,
  fixed_effects = "lsoa",
  effect = "change_per_iqr",
  statistic = "estimate",
  measure_order = names(windowed_article_salience_cols)
)
stopifnot(
  identical(
    selected_effects,
    c("1.10", "1.20", "1.30", "1.40", "2.10", "2.20", "2.30", "2.40")
  )
)

selected_coefficient_errors <- slide_effect_cells(
  fake_effects,
  fixed_effects = "lsoa",
  effect = "interaction",
  statistic = "std_error",
  measure_order = names(windowed_article_salience_cols)
)
selected_effect_errors <- slide_effect_cells(
  fake_effects,
  fixed_effects = "lsoa",
  effect = "change_per_sd",
  statistic = "std_error",
  measure_order = names(windowed_article_salience_cols)
)
stopifnot(
  identical(selected_coefficient_errors, rep("(0.001)", 8L)),
  identical(selected_effect_errors, rep("(0.01)", 8L))
)

starred_effects <- fake_effects
starred_effects$p_value[
  starred_effects$market == "sales" &
    starred_effects$fixed_effects == "msoa" &
    starred_effects$measure == "Cumulative"
] <- 0.049
for (effect in c("interaction", "change_per_iqr", "change_per_sd")) {
  starred_cells <- slide_effect_cells(
    starred_effects,
    fixed_effects = "msoa",
    effect = effect,
    statistic = "estimate",
    measure_order = names(windowed_article_salience_cols)
  )
  stopifnot(endsWith(starred_cells[[1]], "**"))
}

table_path <- tempfile(fileext = ".tex")
effect_path <- tempfile(fileext = ".csv")
on.exit(unlink(c(table_path, effect_path)), add = TRUE)
export_slide_table(
  effects = fake_effects,
  measure_order = names(windowed_article_salience_cols),
  table_path = table_path,
  effect_path = effect_path
)
table_text <- readLines(table_path, warn = FALSE)
exported_effects <- utils::read.csv(effect_path)
stopifnot(
  file.exists(table_path),
  file.exists(effect_path),
  nrow(exported_effects) == nrow(fake_effects),
  any(grepl("House Sales", table_text, fixed = TRUE)),
  any(grepl("Near bin", table_text, fixed = TRUE)),
  any(grepl("Implied price effect (\\%):", table_text, fixed = TRUE)),
  any(grepl("IQR increase in salience", table_text, fixed = TRUE)),
  any(grepl("1-SD increase in salience", table_text, fixed = TRUE)),
  any(grepl("row{6-8,12-14}={bg=blue!4}", table_text, fixed = TRUE)),
  sum(grepl("\\beamerrowcolor{blue!4}", table_text, fixed = TRUE)) == 6L,
  sum(grepl("(0.001)", table_text, fixed = TRUE)) == 2L,
  !any(grepl("(0.01)", table_text, fixed = TRUE)),
  !any(grepl("Near--far gap at mean salience", table_text, fixed = TRUE)),
  any(grepl("Property controls + LSOA FE", table_text, fixed = TRUE))
)

cat("Windowed-article slide-effect validation passed.\n")
