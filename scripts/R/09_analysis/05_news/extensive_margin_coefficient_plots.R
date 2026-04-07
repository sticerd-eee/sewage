# ==============================================================================
# Extensive-Margin News Coefficient Plots
# ==============================================================================
#
# Purpose: Estimate the near-far price gap for proximity to sewage overflow
#          sites across six distance-band comparisons, and plot the post-break
#          and news-coverage interaction coefficients for both the LSOA and
#          MSOA fixed-effects specifications. Treatment is extensive-margin
#          (near_bin = 1 for closer band, 0 for farther band).
#
# Author: Jacopo Olivieri
# Date: 2026-04-07
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - data/processed/lexis_nexis/search1_monthly.parquet
#   - data/raw/google_trends/google_trends_uk.xlsx
#
# Outputs:
#   - output/figures/extensive_margin_news_coefficients_lsoa.pdf
#   - output/figures/extensive_margin_news_coefficients_msoa.pdf
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "fixest",
  "forcats",
  "ggplot2",
  "glue",
  "here",
  "lubridate",
  "purrr",
  "readxl",
  "rio",
  "rlang",
  "scales",
  "showtext",
  "sysfonts",
  "stringr",
  "tibble"
)

check_required_packages(REQUIRED_PACKAGES)


# ==============================================================================
# 1. Configuration
# ==============================================================================
START_DATE <- as.Date("2021-01-01")
END_DATE   <- as.Date("2023-12-31")
LOOKUP_MAX_DISTANCE <- 10000
VCOV_MODE  <- "hetero"

PLOT_WIDTH  <- 18
PLOT_HEIGHT <- 10
PLOT_DPI    <- 300

MARKET_COLOURS <- c(Sales = "#1F78B4", Rentals = "#D95F02")
FAMILIES <- c("post", "articles")

PATH_OUTPUT_LSOA <- here::here(
  "output", "figures", "extensive_margin_news_coefficients_lsoa.pdf"
)
PATH_OUTPUT_MSOA <- here::here(
  "output", "figures", "extensive_margin_news_coefficients_msoa.pdf"
)

comparison_specs <- tibble::tribble(
  ~comparison_id,       ~comparison_label,        ~near_min, ~near_max, ~far_min, ~far_max,
  "250_vs_250_500",     "0-250m vs 250-500m",      0,  250,  250,   500,
  "250_vs_250_1000",    "0-250m vs 250-1000m",     0,  250,  250,  1000,
  "250_vs_500_1000",    "0-250m vs 500-1000m",     0,  250,  500,  1000,
  "500_vs_500_1000",    "0-500m vs 500-1000m",     0,  500,  500,  1000,
  "500_vs_1000_2000",   "0-500m vs 1000-2000m",    0,  500, 1000,  2000,
  "500_vs_500_2000",    "0-500m vs 500-2000m",     0,  500,  500,  2000
)

spec_grid <- tibble::tribble(
  ~spec_id,                ~spec_label,                  ~controls, ~fe_unit,
  "msoa_month_controls",   "MSOA + month FE + controls", TRUE,      "msoa",
  "lsoa_month_controls",   "LSOA + month FE + controls", TRUE,      "lsoa"
)


# ==============================================================================
# 2. Package Management & Environment
# ==============================================================================
initialise_environment <- function() {
  invisible(
    lapply(
      REQUIRED_PACKAGES,
      function(pkg) suppressPackageStartupMessages(
        library(pkg, character.only = TRUE)
      )
    )
  )
}

initialise_environment()


# ==============================================================================
# 3. Setup
# ==============================================================================

# 3.1 Font Setup ---------------------------------------------------------------
showtext::showtext_auto()
showtext::showtext_opts(dpi = 300)
sysfonts::font_add_google("Libertinus Serif", "Libertinus Serif", db_cache = FALSE)

# 3.2 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.3 ggplot Theme -------------------------------------------------------------
theme_pref <- ggplot2::theme_minimal() +
  ggplot2::theme(
    text = element_text(size = 10, family = "Libertinus Serif"),
    plot.title = element_text(
      face = "bold", size = 12, family = "Libertinus Serif",
      margin = ggplot2::margin(b = 9, unit = "pt")
    ),
    plot.subtitle = element_text(size = 10, family = "Libertinus Serif"),
    axis.title = element_text(
      face = "bold", size = 12, family = "Libertinus Serif"
    ),
    axis.text = element_text(size = 10, family = "Libertinus Serif"),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 10, family = "Libertinus Serif"),
    strip.text = element_text(
      face = "bold", size = 11, family = "Libertinus Serif"
    ),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )


# ==============================================================================
# 4. Helper Functions
# ==============================================================================

# 4.1 General Utilities --------------------------------------------------------

month_id_from_date <- function(x) {
  x_date <- as.Date(x)
  (lubridate::year(x_date) - 2021L) * 12L + lubridate::month(x_date)
}

controls_for_market <- function(market) {
  market <- tolower(as.character(market))
  switch(
    market,
    sales   = c("property_type", "old_new", "duration"),
    rentals = c("property_type", "bedrooms", "bathrooms"),
    character()
  )
}

pretty_family_short <- function(family) {
  dplyr::case_match(
    family,
    "post"     ~ "Near \u00d7 Post",
    "articles" ~ "Near \u00d7 log(Articles)",
    .default   = as.character(family)
  )
}

pretty_market <- function(market) {
  dplyr::case_match(
    market,
    "sales"   ~ "Sales",
    "rentals" ~ "Rentals",
    .default  = market
  )
}

# 4.2 Data Loading Functions ---------------------------------------------------

#' Load the peak Google Trends month_id (August 2022)
load_peak_month_id <- function() {
  google_trends <- readxl::read_excel(
    here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
    sheet = "united_kingdom"
  ) |>
    dplyr::filter(.data$Year >= 2021, .data$Year <= 2023)

  peak_row <- google_trends |>
    dplyr::slice_max(`'Sewage Spill' Google Searches`, n = 1, with_ties = FALSE)

  peak_year  <- peak_row$Year[[1]]
  peak_month <- as.integer(substr(peak_row$Date[[1]], 6, 7))

  (peak_year - 2021L) * 12L + peak_month
}

#' Load monthly LexisNexis article counts with cumulative totals
load_articles <- function() {
  arrow::read_parquet(
    here::here("data", "processed", "lexis_nexis", "search1_monthly.parquet")
  ) |>
    dplyr::filter(.data$month_id >= 1L, .data$month_id <= 36L) |>
    dplyr::arrange(.data$month_id) |>
    dplyr::mutate(
      cumulative_articles     = cumsum(.data$article_count),
      log_cumulative_articles = log(.data$cumulative_articles)
    )
}

#' Load nearest-site lookup and keep only the closest site per property
load_nearest_lookup <- function(path, id_col) {
  id_sym <- rlang::sym(id_col)

  lookup <- arrow::open_dataset(path) |>
    dplyr::select(!!id_sym, .data$site_id, .data$distance_m) |>
    dplyr::filter(
      !is.na(.data$site_id),
      !is.na(.data$distance_m),
      .data$distance_m <= LOOKUP_MAX_DISTANCE
    )

  min_distance <- lookup |>
    dplyr::group_by(!!id_sym) |>
    dplyr::summarise(min_distance = min(.data$distance_m, na.rm = TRUE), .groups = "drop")

  lookup |>
    dplyr::inner_join(min_distance, by = id_col) |>
    dplyr::filter(.data$distance_m == .data$min_distance) |>
    dplyr::group_by(!!id_sym) |>
    dplyr::summarise(
      min_distance     = min(.data$min_distance, na.rm = TRUE),
      nearest_site_id  = min(.data$site_id, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::collect()
}

#' Load Land Registry sales transactions (2021-2023)
load_sales_transactions <- function() {
  rio::import(
    here::here("data", "processed", "house_price.parquet"),
    trust = TRUE
  ) |>
    tibble::as_tibble() |>
    dplyr::select(dplyr::any_of(c(
      "house_id", "price", "date_of_transfer", "property_type",
      "old_new", "duration", "lsoa", "msoa", "latitude", "longitude"
    ))) |>
    dplyr::mutate(
      date_of_transfer = as.Date(.data$date_of_transfer),
      month_id      = month_id_from_date(.data$date_of_transfer),
      log_price     = log(.data$price),
      property_type = forcats::fct_drop(forcats::as_factor(.data$property_type)),
      old_new       = forcats::fct_drop(forcats::as_factor(.data$old_new)),
      duration      = forcats::fct_drop(forcats::as_factor(.data$duration))
    ) |>
    dplyr::filter(
      .data$date_of_transfer >= START_DATE,
      .data$date_of_transfer <= END_DATE,
      .data$month_id >= 1L,
      .data$month_id <= 36L,
      is.finite(.data$log_price)
    )
}

#' Load Zoopla rental listings (2021-2023)
load_rental_transactions <- function() {
  rio::import(
    here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    trust = TRUE
  ) |>
    tibble::as_tibble() |>
    dplyr::select(dplyr::any_of(c(
      "rental_id", "listing_price", "rented_est", "property_type",
      "bedrooms", "bathrooms", "lsoa", "msoa", "latitude", "longitude"
    ))) |>
    dplyr::mutate(
      rented_est    = as.Date(.data$rented_est),
      month_id      = month_id_from_date(.data$rented_est),
      log_price     = log(.data$listing_price),
      property_type = forcats::fct_drop(forcats::as_factor(.data$property_type))
    ) |>
    dplyr::filter(
      .data$rented_est >= START_DATE,
      .data$rented_est <= END_DATE,
      .data$month_id >= 1L,
      .data$month_id <= 36L,
      is.finite(.data$log_price)
    )
}

#' Prepare a market dataset by joining transactions to nearest-site lookups
#' and merging in article counts and the post-break indicator
prepare_market_data <- function(market, peak_month_id, articles) {
  if (market == "sales") {
    transactions   <- load_sales_transactions()
    nearest_lookup <- load_nearest_lookup(
      here::here("data", "processed", "spill_house_lookup.parquet"),
      "house_id"
    )
    id_col <- "house_id"
  } else {
    transactions   <- load_rental_transactions()
    nearest_lookup <- load_nearest_lookup(
      here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
      "rental_id"
    )
    id_col <- "rental_id"
  }

  transactions |>
    dplyr::inner_join(nearest_lookup, by = id_col) |>
    dplyr::left_join(
      articles |>
        dplyr::select(
          .data$month_id, .data$cumulative_articles, .data$log_cumulative_articles
        ),
      by = "month_id"
    ) |>
    dplyr::mutate(
      post   = as.integer(.data$month_id >= peak_month_id),
      market = market
    )
}

# 4.3 Sample Construction -----------------------------------------------------

#' Restrict data to a near/far comparison band and flag near_bin
build_comparison_sample <- function(data, comparison_row) {
  data |>
    dplyr::filter(
      (.data$min_distance >= comparison_row$near_min &
         .data$min_distance <= comparison_row$near_max) |
        (.data$min_distance > comparison_row$far_min &
           .data$min_distance <= comparison_row$far_max)
    ) |>
    dplyr::mutate(
      near_bin         = as.integer(.data$min_distance <= comparison_row$near_max),
      comparison_id    = comparison_row$comparison_id,
      comparison_label = comparison_row$comparison_label
    )
}

# 4.4 Estimation Functions -----------------------------------------------------

resolve_vcov <- function(data) {
  "hetero"
}

build_formula <- function(family, spec_row, market) {
  controls <- if (isTRUE(spec_row$controls)) controls_for_market(market) else character()
  rhs <- c("near_bin")

  if (family == "post") {
    rhs <- c(rhs, "near_bin:post")
  } else if (family == "articles") {
    rhs <- c(rhs, "near_bin:log_cumulative_articles")
  }

  rhs <- c(rhs, controls)
  rhs_text <- paste(rhs, collapse = " + ")

  stats::as.formula(
    glue::glue("log_price ~ {rhs_text} | {spec_row$fe_unit} + month_id")
  )
}

required_vars_for_family <- function(family, spec_row, market) {
  vars <- c("log_price", "near_bin")
  if (family == "post") {
    vars <- c(vars, "post")
  } else if (family == "articles") {
    vars <- c(vars, "log_cumulative_articles")
  }
  if (isTRUE(spec_row$controls)) {
    vars <- c(vars, controls_for_market(market))
  }
  if (spec_row$fe_unit != "none") {
    vars <- c(vars, spec_row$fe_unit, "month_id")
  }
  unique(vars)
}

target_term_for_family <- function(family) {
  dplyr::case_match(
    family,
    "post"     ~ "near_bin:post",
    "articles" ~ "near_bin:log_cumulative_articles"
  )
}

pick_term <- function(coef_names, target) {
  reverse_target <- paste(rev(strsplit(target, ":", fixed = TRUE)[[1]]), collapse = ":")
  hit <- coef_names[coef_names %in% c(target, reverse_target)]
  if (length(hit) > 0) hit[[1]] else NA_character_
}

#' Extract a single coefficient row from a fitted model
extract_model_term <- function(model, target_term, metadata) {
  status_row <- function(status, nobs = NA_integer_) {
    tibble::as_tibble(metadata) |>
      dplyr::mutate(
        term      = NA_character_,
        estimate  = NA_real_,
        std_error = NA_real_,
        statistic = NA_real_,
        p_value   = NA_real_,
        nobs      = nobs,
        adj_r2    = NA_real_,
        status    = status
      )
  }

  if (is.null(model)) return(status_row("model_null"))

  ct <- as.data.frame(fixest::coeftable(model))
  ct$term <- rownames(ct)
  chosen_term <- pick_term(ct$term, target_term)

  if (is.na(chosen_term)) {
    return(status_row("target_term_missing", nobs = stats::nobs(model)))
  }

  ct |>
    dplyr::filter(.data$term == chosen_term) |>
    dplyr::transmute(
      estimate  = Estimate,
      std_error = `Std. Error`,
      statistic = `t value`,
      p_value   = `Pr(>|t|)`
    ) |>
    dplyr::mutate(
      term   = chosen_term,
      nobs   = stats::nobs(model),
      adj_r2 = suppressWarnings(tryCatch(
        as.numeric(fixest::fitstat(model, "ar2"))[[1]],
        error = function(e) NA_real_
      ))
    ) |>
    dplyr::bind_cols(tibble::as_tibble(metadata)) |>
    dplyr::mutate(status = "ok")
}

#' Estimate a single model for one family x specification
estimate_one_model <- function(sample, market, comparison_row, family, spec_row) {
  metadata <- list(
    market           = market,
    comparison_id    = comparison_row$comparison_id,
    comparison_label = comparison_row$comparison_label,
    family           = family,
    spec_id          = spec_row$spec_id,
    spec_label       = spec_row$spec_label
  )

  needed   <- required_vars_for_family(family, spec_row, market)
  data_est <- sample |>
    dplyr::filter(stats::complete.cases(dplyr::pick(dplyr::all_of(needed))))

  if (nrow(data_est) == 0 || dplyr::n_distinct(data_est$near_bin) < 2) {
    return(
      tibble::as_tibble(metadata) |>
        dplyr::mutate(
          term = NA_character_, estimate = NA_real_, std_error = NA_real_,
          statistic = NA_real_, p_value = NA_real_,
          nobs = nrow(data_est), adj_r2 = NA_real_,
          status = "insufficient_variation"
        )
    )
  }

  model <- tryCatch(
    fixest::feols(
      fml  = build_formula(family, spec_row, market),
      data = data_est,
      vcov = resolve_vcov(data_est)
    ),
    error = function(e) NULL
  )

  extract_model_term(model, target_term_for_family(family), metadata)
}

# 4.5 Plot Function ------------------------------------------------------------

make_coefficient_plot <- function(data) {
  dodge <- ggplot2::position_dodge(width = 0.45)

  ggplot2::ggplot(
    data,
    ggplot2::aes(
      x    = .data$estimate,
      y    = .data$comparison_label,
      xmin = .data$estimate - 1.96 * .data$std_error,
      xmax = .data$estimate + 1.96 * .data$std_error,
      colour = .data$market,
      group  = .data$market
    )
  ) +
    ggplot2::geom_vline(xintercept = 0, linetype = 2, colour = "grey60") +
    ggplot2::geom_errorbarh(height = 0.16, alpha = 0.8, position = dodge) +
    ggplot2::geom_point(size = 2.3, position = dodge) +
    ggplot2::facet_wrap(~ family, scales = "free_x") +
    ggplot2::scale_colour_manual(values = MARKET_COLOURS, drop = FALSE) +
    ggplot2::labs(x = "Coefficient", y = NULL, colour = NULL) +
    theme_pref
}


# ==============================================================================
# 5. Data Loading
# ==============================================================================
cat("Loading reference data...\n")
peak_month_id <- load_peak_month_id()
articles      <- load_articles()

cat("Preparing sales data...\n")
sales_data <- prepare_market_data("sales", peak_month_id, articles)
cat("  ", nrow(sales_data), " matched sales transactions\n", sep = "")

cat("Preparing rentals data...\n")
rentals_data <- prepare_market_data("rentals", peak_month_id, articles)
cat("  ", nrow(rentals_data), " matched rental listings\n", sep = "")

market_data <- list(sales = sales_data, rentals = rentals_data)


# ==============================================================================
# 6. Estimation
# ==============================================================================
cat("Estimating models...\n")

result_rows <- list()
idx <- 1L

for (market in names(market_data)) {
  data_market <- market_data[[market]]

  for (i in seq_len(nrow(comparison_specs))) {
    comp   <- comparison_specs[i, ]
    sample <- build_comparison_sample(data_market, comp)

    for (family in FAMILIES) {
      for (j in seq_len(nrow(spec_grid))) {
        spec_row <- spec_grid[j, ]
        result_rows[[idx]] <- estimate_one_model(
          sample, market, comp, family, spec_row
        )
        idx <- idx + 1L
      }
    }
  }
}

main_results <- dplyr::bind_rows(result_rows) |>
  dplyr::mutate(
    market = pretty_market(.data$market),
    family = factor(.data$family, levels = FAMILIES)
  )

cat("  ", sum(main_results$status == "ok"), " / ", nrow(main_results),
    " models converged\n", sep = "")

# 6.1 Prepare Plot Data -------------------------------------------------------
prepare_plot_data <- function(results, target_spec_id) {
  results |>
    dplyr::filter(.data$spec_id == target_spec_id, .data$status == "ok") |>
    dplyr::mutate(
      comparison_label = factor(
        .data$comparison_label,
        levels = rev(comparison_specs$comparison_label)
      ),
      family = pretty_family_short(as.character(.data$family)),
      market = factor(.data$market, levels = c("Sales", "Rentals"))
    )
}

preferred_plot_data <- prepare_plot_data(main_results, "lsoa_month_controls")
msoa_plot_data      <- prepare_plot_data(main_results, "msoa_month_controls")


# ==============================================================================
# 7. Create and Save Figures
# ==============================================================================
cat("Creating figures...\n")

p_lsoa <- make_coefficient_plot(preferred_plot_data)

ggsave(
  filename = PATH_OUTPUT_LSOA,
  plot     = p_lsoa,
  width    = PLOT_WIDTH,
  height   = PLOT_HEIGHT,
  dpi      = PLOT_DPI,
  units    = "cm"
)
cat("  Saved: ", PATH_OUTPUT_LSOA, "\n", sep = "")

p_msoa <- make_coefficient_plot(msoa_plot_data)

ggsave(
  filename = PATH_OUTPUT_MSOA,
  plot     = p_msoa,
  width    = PLOT_WIDTH,
  height   = PLOT_HEIGHT,
  dpi      = PLOT_DPI,
  units    = "cm"
)
cat("  Saved: ", PATH_OUTPUT_MSOA, "\n", sep = "")

cat("Done.\n")
