# ==============================================================================
# Intensive-Margin Attention by Local Salience: 250m Estimation and Publication
# Inputs: Published prior exposure, transactions, attention, radius companions,
#   Site Group characteristics and property-Site Group lookups (audit only).
# Outputs: Five two-market tables per attention measure, models and audit CSVs.
# Run from the repository root with plain Rscript in the rv project environment.
# ==============================================================================

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
check_required_packages(c("arrow", "dplyr", "fixest", "forcats", "here",
                          "modelsummary", "readxl", "tibble"))
source(here::here("scripts", "R", "09_analysis", "utils_salience_strata.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "05_news",
                  "salience_attention_table_utils.R"), local = TRUE)

#' Prepare the unchanged parent sample before any salience or London filter
#' @param transactions Published transaction data, including region and controls.
#' @param exposure Published prior exposure (data frame or Arrow dataset).
#' @param attention_data Monthly post or log_cumulative_articles data.
#' @param market Sales (2021--2024) or rentals (2021--2023).
#' @param attention Parent attention regressor name.
#' @return Complete-case transactions within 250m; missing exposure stays excluded.
prepare_salience_intensive <- function(
  transactions, exposure, attention_data, market = c("sales", "rentals"),
  attention = c("post", "log_cumulative_articles")
) {
  market <- match.arg(market)
  attention <- match.arg(attention)
  id_col <- if (market == "sales") "house_id" else "rental_id"
  price_col <- if (market == "sales") "price" else "listing_price"
  end_month <- if (market == "sales") 48L else 36L
  controls <- if (market == "sales") c("property_type", "old_new", "duration") else
    c("property_type", "bedrooms", "bathrooms")
  factors <- if (market == "sales") controls else "property_type"
  transactions <- transactions |>
    dplyr::select(dplyr::all_of(c(id_col, price_col, "region", "month_id", "lsoa",
                                  "latitude", "longitude", controls))) |>
    dplyr::mutate(dplyr::across(dplyr::all_of(factors), forcats::as_factor))
  exposure |>
    dplyr::filter(.data$radius == 250L) |>
    dplyr::select(dplyr::all_of(c(id_col, "spill_count_weekly_avg", "n_spill_sites"))) |>
    dplyr::collect() |>
    dplyr::filter(.data$n_spill_sites > 0L) |>
    dplyr::inner_join(transactions, by = id_col, relationship = "one-to-one") |>
    dplyr::inner_join(attention_data, by = "month_id", relationship = "many-to-one") |>
    dplyr::mutate(log_price = log(.data[[price_col]])) |>
    dplyr::filter(
      .data$month_id >= 1L, .data$month_id <= .env$end_month,
      !is.na(.data$spill_count_weekly_avg), is.finite(.data[[attention]]),
      is.finite(.data$log_price),
      dplyr::if_all(dplyr::all_of(c("lsoa", "month_id", "latitude", "longitude", controls)),
                    ~ !is.na(.x))
    ) |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      dplyr::across(dplyr::all_of(factors), forcats::fct_drop)
    )
}

#' Fit the parent's saturated intensive-margin specification
#' @param data Prepared transactions, optionally restricted to a salience stratum.
#' @param market Sales or rentals.
#' @param attention Post or log cumulative articles.
#' @return Compact fixest model with LSOA-clustered inference already computed.
fit_salience_intensive <- function(
  data, market = c("sales", "rentals"),
  attention = c("post", "log_cumulative_articles")
) {
  market <- match.arg(market)
  attention <- match.arg(attention)
  controls <- if (market == "sales") "property_type + old_new + duration" else
    "property_type + bedrooms + bathrooms"
  terms <- c("spill_count_weekly_avg", paste0("spill_count_weekly_avg:", attention))
  formula <- stats::as.formula(paste(
    "log_price ~", paste(terms, collapse = " + "), "+", controls, "| lsoa + month_id"
  ))
  model <- fixest::feols(formula, data = data, vcov = ~lsoa, lean = TRUE)
  if (any(!is.finite(stats::coef(model)[terms])) || any(!is.finite(fixest::se(model)[terms]))) {
    stop("Unidentified spill/attention effect: ", market, " / ", attention, call. = FALSE)
  }
  model
}

#' Run the five intensive-margin salience tables or only the reproduction gate
#' @param measure Trends (Post) or articles (log cumulative coverage).
#' @param reproduce_only Disable strata and retain London for both markets.
#' @return Paths to tables, compact model bundle and measure-specific audits.
run_salience_intensive <- function(measure = c("trends", "articles"), reproduce_only = FALSE) {
  measure <- match.arg(measure)
  attention <- if (measure == "trends") "post" else "log_cumulative_articles"
  attention_data <- if (measure == "trends") {
    peak <- readxl::read_excel(
      here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
      sheet = "united_kingdom"
    ) |>
      dplyr::filter(.data$Year >= 2021, .data$Year <= 2024) |>
      dplyr::slice_max(.data[["'Sewage Spill' Google Searches"]], n = 1, with_ties = FALSE)
    peak_month <- (peak$Year - 2021) * 12 + as.integer(substr(peak$Date, 6, 7))
    if (length(peak_month) != 1L || is.na(peak_month) || peak_month != 20L) {
      stop("Expected the August 2022 attention peak.", call. = FALSE)
    }
    tibble::tibble(month_id = 1:48, post = as.integer(month_id >= peak_month))
  } else {
    arrow::read_parquet(here::here("data", "processed", "lexis_nexis", "search1_monthly.parquet")) |>
      dplyr::filter(.data$month_id >= 1L, .data$month_id <= 48L) |>
      dplyr::arrange(.data$month_id) |>
      dplyr::transmute(month_id = .data$month_id,
                       log_cumulative_articles = log(cumsum(.data$article_count)))
  }
  prefix <- paste0("did_", measure, "_prior_salience")
  variants <- tibble::tribble(
    ~variant, ~family, ~coast_rule_m, ~unknown_policy, ~drop_london,
    "coast_bathing", "coast_bathing", 2000, "not_designated", TRUE,
    "intensity", "intensity", 2000, "not_designated", TRUE,
    "robust_coast10km", "coast_bathing", 10000, "not_designated", TRUE,
    "robust_london", "coast_bathing", 2000, "not_designated", FALSE,
    "robust_dropunknown", "coast_bathing", 2000, "exclude", TRUE
  )
  paths <- list(
    tables = stats::setNames(here::here("output", "tables",
      paste0(prefix, "_", variants$variant, ".tex")), variants$variant),
    models = here::here("output", "regs", paste0(prefix, if (reproduce_only) "_reproduction", ".rds")),
    counts = here::here("output", "logs", paste0(prefix, "_cell_counts.csv")),
    coverage = here::here("output", "logs", paste0(prefix, "_nearest_site_coverage.csv")),
    reproduction = here::here("output", "logs", paste0(prefix, "_reproduction.csv"))
  )
  models <- stats::setNames(vector("list", nrow(variants)), variants$variant)
  unrestricted <- reproduction <- counts <- coverage <- list()
  if (!reproduce_only) {
    characteristics <- arrow::read_parquet(here::here(
      "data", "processed", "site_characteristics", "site_group_characteristics.parquet"
    ))
  }
  for (market in c("sales", "rentals")) {
    id_col <- if (market == "sales") "house_id" else "rental_id"
    transaction_path <- if (market == "sales") here::here("data", "processed", "house_price.parquet") else
      here::here("data", "processed", "zoopla", "zoopla_rentals.parquet")
    prior_dir <- if (market == "sales") "prior_to_sale" else "prior_to_rental"
    data <- prepare_salience_intensive(
      arrow::read_parquet(transaction_path),
      arrow::open_dataset(here::here("data", "processed", "cross_section", market, prior_dir)),
      attention_data, market, attention
    )
    unrestricted[[market]] <- fit_salience_intensive(data, market, attention)
    reproduction[[market]] <- verify_salience_reproduction(
      unrestricted[[market]],
      here::here("output", "tables", paste0("did_", measure, "_prior_250m.tex")),
      market, attention, exposure = "spill_count_weekly_avg"
    )
    if (reproduce_only) next
    lookup_path <- if (market == "sales") here::here("data", "processed", "spill_house_lookup.parquet") else
      here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
    nearest <- nearest_salience_sites(arrow::open_dataset(lookup_path), characteristics, id_col)
    coverage[[market]] <- dplyr::mutate(attr(nearest, "coverage"), market = market, .before = 1L)
    # Nearest Site Group is attached only for the missing-coast audit; every
    # regression stratum below uses the 250m property-radius companion.
    data <- data |>
      dplyr::left_join(dplyr::select(nearest, dplyr::all_of(c(id_col, "site_id"))),
                       by = id_col, relationship = "many-to-one") |>
      join_radius_salience(
        arrow::open_dataset(here::here("data", "processed", "cross_section",
                                       market, "prior_characteristics")),
        id_col, radius = 250L
      )
    for (i in seq_len(nrow(variants))) {
      variant <- variants[i, ]
      classified <- classify_salience_coast(
        data, variant$coast_rule_m, variant$unknown_policy, source = "radius"
      )
      cat("\n", prefix, " / ", variant$variant, "\n", sep = "")
      audit <- log_salience_cells(classified, nearest, market, variant$family,
                                  drop_london = variant$drop_london)
      filters <- salience_strata(variant$family)
      fitted <- lapply(names(filters), function(stratum) {
        selected <- filters[[stratum]](classified)
        if (variant$drop_london) selected <- selected & !classified$london
        tryCatch(
          fit_salience_intensive(classified[selected, ], market, attention),
          error = function(e) stop(market, " / ", variant$variant, " / ", stratum,
                                    ": ", conditionMessage(e), call. = FALSE)
        )
      })
      names(fitted) <- names(filters)
      models[[variant$variant]][[market]] <- fitted
      audit$attention <- attention
      audit$variant <- variant$variant
      audit$table_path <- paths$tables[[variant$variant]]
      audit$nobs <- vapply(fitted, stats::nobs, numeric(1))
      audit$n_removed_by_estimator <- audit$n_estimation - audit$nobs
      audit$n_missing_radius_companion <- sum(is.na(classified$radius))
      audit$n_missing_radius_coast <- sum(is.na(classified$min_coast_dist_m))
      audit$n_unresolved_bathing <- sum(classified$bath_unresolved)
      audit$n_unknown_band <- sum(classified$spill_count_band %in% "unknown")
      audit$n_zero_band <- sum(classified$spill_count_band %in% "zero")
      audit$n_no_site_band <- sum(classified$spill_count_band %in% "no_site")
      audit$n_missing_band <- sum(is.na(classified$spill_count_band))
      counts[[paste(market, variant$variant, sep = "_")]] <- audit
    }
    rm(data, classified, nearest)
    invisible(gc())
  }
  reproduction <- dplyr::bind_rows(reproduction)
  dir.create(dirname(paths$reproduction), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(reproduction, paths$reproduction, row.names = FALSE)
  if (!reproduce_only) {
    counts <- dplyr::bind_rows(counts)
    utils::write.csv(counts, paths$counts, row.names = FALSE)
    utils::write.csv(dplyr::bind_rows(coverage), paths$coverage, row.names = FALSE)
    for (i in seq_len(nrow(variants))) {
      variant <- variants[i, ]
      export_salience_attention(models[[variant$variant]], variant, attention,
                                 paths$tables[[variant$variant]], margin = "intensive")
    }
  }
  dir.create(dirname(paths$models), recursive = TRUE, showWarnings = FALSE)
  saveRDS(list(models = models, unrestricted = unrestricted,
               reproduction = reproduction, counts = counts, variants = variants), paths$models)
  cat("\n", prefix, ": ", if (reproduce_only) "reproduction passed" else "five tables and audits published",
      ".\n", sep = "")
  invisible(paths)
}
