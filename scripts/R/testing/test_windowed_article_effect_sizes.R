#!/usr/bin/env Rscript

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required.", call. = FALSE)
}

source(here::here("scripts", "R", "09_analysis", "05_news", "windowed_article_effect_size_utils.R"))
source(here::here("scripts", "R", "09_analysis", "05_news", "extensive_margin_news_utils.R"))
source(here::here("scripts", "R", "09_analysis", "05_news", "windowed_article_analysis_config.R"))

required_packages <- c("dplyr", "fixest", "tibble")
missing_packages <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]
if (length(missing_packages) > 0L) {
  stop("Missing packages: ", paste(missing_packages, collapse = ", "), call. = FALSE)
}

comparison <- validate_comparison_config(
  list(near_min = 0L, near_max = 250L, far_min = 250L, far_max = 1000L),
  allow_adjacent = TRUE
)
stopifnot(comparison$near_band_label == "0-250m")
stopifnot(comparison$far_band_label == "250-1000m")

strict_error <- tryCatch(
  {
    validate_comparison_config(comparison)
    NULL
  },
  error = identity
)
stopifnot(inherits(strict_error, "error"))
stopifnot(identical(unname(windowed_article_intensive_radii), c(250L, 500L, 1000L)))
stopifnot(length(windowed_article_extensive_comparisons) == 4L)
stopifnot(identical(names(windowed_article_salience_cols), c("Cumulative", "3m", "6m", "12m")))

boundary_transactions <- tibble::tibble(id = 1:3)
boundary_lookup <- tibble::tibble(id = 1:3, min_distance = c(249.9, 250, 250.1))
boundary_sample <- build_extensive_margin_sample(
  boundary_transactions, boundary_lookup, "id", comparison
)
stopifnot(identical(boundary_sample$near_bin, c(1L, 1L, 0L)))

toy_data <- tibble::tibble(
  outcome = c(1, 2, 4, 3, 5, 8, 20),
  spill = c(0, 1, 2, 1, 3, 4, 100),
  salience = c(1, 3, 2, 4, 1, 5, 100),
  location = c("a", "a", "a", "b", "b", "b", "singleton")
)
model <- fixest::feols(
  outcome ~ spill * salience | location,
  data = toy_data,
  notes = FALSE
)
models_by_measure <- list(
  Cumulative = list(
    sale_msoa = model,
    sale_lsoa = model,
    rent_msoa = model,
    rent_lsoa = model
  )
)

output_path <- tempfile(fileext = ".csv")
on.exit(unlink(output_path), add = TRUE)
effect_sizes <- write_windowed_article_effect_sizes(
  models_by_measure = models_by_measure,
  salience_cols = c(Cumulative = "salience"),
  interaction_term_fn = function(column) paste0("spill:", column),
  margin = "intensive",
  output_path = output_path,
  spill_col = "spill",
  metadata = list(radius = 250L)
)

estimation_data <- fixest::fixest_data(model, sample = "estimation")
stopifnot(all(effect_sizes$effect_sample_n == nrow(estimation_data)))
stopifnot(all(effect_sizes$salience_sd == stats::sd(estimation_data$salience)))
stopifnot(all(effect_sizes$spill_sd == stats::sd(estimation_data$spill)))
stopifnot(!isTRUE(all.equal(effect_sizes$salience_sd[[1]], stats::sd(toy_data$salience))))

intensive_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "09_analysis", "05_news", "did_articles_windowed_prior.R"),
  envir = intensive_env
)
stopifnot(
  basename(intensive_env$legacy_intensive_table_path(250L, "Cumulative")) ==
    "did_articles_prior_250m.tex",
  basename(intensive_env$legacy_intensive_table_path(250L, "3m")) ==
    "did_articles_windowed_prior_3m.tex",
  is.null(intensive_env$legacy_intensive_table_path(500L, "3m"))
)

extensive_env <- new.env(parent = globalenv())
sys.source(
  here::here(
    "scripts", "R", "09_analysis", "05_news",
    "did_articles_windowed_prior_extensive.R"
  ),
  envir = extensive_env
)
default_comparison <- windowed_article_extensive_comparisons[["500 vs 1000-2000"]]
other_comparison <- windowed_article_extensive_comparisons[["250 vs 250-1000"]]
stopifnot(
  basename(extensive_env$legacy_extensive_table_path(
    default_comparison, "Cumulative"
  )) == "did_articles_prior_extensive.tex",
  basename(extensive_env$legacy_extensive_table_path(default_comparison, "12m")) ==
    "did_articles_windowed_prior_extensive_12m.tex",
  is.null(extensive_env$legacy_extensive_table_path(other_comparison, "12m"))
)

cat("Windowed article effect-size validation passed.\n")
