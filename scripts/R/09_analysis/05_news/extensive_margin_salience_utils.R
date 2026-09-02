# ==============================================================================
# Extensive-Margin Attention by Local Salience: Shared Estimation and Publication
# Inputs: Unchanged parent samples; published nearest-site/radius characteristics.
# Outputs: Five two-market tables per attention measure, models and audit CSVs.
# Classification and cell validation belong to utils_salience_strata.R.
# ==============================================================================

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
check_required_packages(c("arrow", "dplyr", "fixest", "forcats", "here",
                          "modelsummary", "readxl", "rio", "tibble"))
source(here::here("scripts", "R", "09_analysis", "utils_salience_strata.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "05_news",
                  "salience_attention_table_utils.R"), local = TRUE)

#' Fit the parent's saturated specification on a supplied subsample
#' @param data Prepared transactions, optionally restricted to a stratum.
#' @param market Sales or rentals.
#' @param attention Post or log cumulative articles, as prepared by the parent.
#' @return Compact fixest model with LSOA-clustered inference already computed.
fit_salience_extensive <- function(
  data, market = c("sales", "rentals"),
  attention = c("post", "log_cumulative_articles")
) {
  market <- match.arg(market)
  attention <- match.arg(attention)
  controls <- if (market == "sales") "property_type + old_new + duration" else
    "property_type + bedrooms + bathrooms"
  term <- paste0("near_bin:", attention)
  formula <- stats::as.formula(paste(
    "log_price ~ near_bin +", term, "+", controls, "| lsoa + month_id"
  ))
  model <- fixest::feols(formula, data = data, vcov = ~lsoa, lean = TRUE)
  if (!is.finite(stats::coef(model)[term]) || !is.finite(fixest::se(model)[term])) {
    stop("Unidentified attention effect: ", market, " / ", term, call. = FALSE)
  }
  model
}

#' Run the five extensive-margin salience tables or only the reproduction gate
#' @param measure Trends (Post) or articles (log cumulative coverage).
#' @param reproduce_only Disable strata and retain London for both markets.
#' @return Paths to the tables, compact model bundle and measure-specific audits.
run_salience_extensive <- function(measure = c("trends", "articles"), reproduce_only = FALSE) {
  measure <- match.arg(measure)
  attention <- if (measure == "trends") "post" else "log_cumulative_articles"
  # Sourcing does not invoke the parent's main(), radius sweep or publication.
  parent <- new.env(parent = environment())
  sys.source(here::here("scripts", "R", "09_analysis", "05_news",
                       paste0("did_", measure, "_prior_extensive.R")), envir = parent)
  parent$initialise_environment()
  comparison <- parent$validate_comparison_config(parent$CONFIG$comparison)
  attention_data <- if (measure == "trends") {
    peak <- parent$load_google_trends_peak(
      parent$CONFIG$google_trends_path, parent$CONFIG$google_trends_sheet,
      parent$CONFIG$base_year
    )
    if (peak$peak_month_id != 20L) stop("Expected the August 2022 attention peak.", call. = FALSE)
    peak
  } else {
    parent$load_articles_data(parent$CONFIG$article_path,
                             parent$CONFIG$analysis_start_month_id, parent$CONFIG$sales_end_month_id)
  }
  prefix <- paste0("did_", measure, "_prior_extensive_salience")
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
    data <- if (market == "sales") parent$prepare_sales_analysis_data(comparison, attention_data) else
      parent$prepare_rental_analysis_data(comparison, attention_data)
    unrestricted[[market]] <- fit_salience_extensive(data, market, attention)
    reproduction[[market]] <- verify_salience_reproduction(
      unrestricted[[market]], parent$CONFIG$output_path, market, attention
    )
    if (reproduce_only) next
    id_col <- if (market == "sales") "house_id" else "rental_id"
    lookup_path <- if (market == "sales") parent$CONFIG$sales_lookup_path else parent$CONFIG$rental_lookup_path
    nearest <- nearest_salience_sites(arrow::open_dataset(lookup_path), characteristics, id_col)
    coverage[[market]] <- dplyr::mutate(attr(nearest, "coverage"), market = market, .before = 1L)
    data <- join_nearest_salience(data, nearest, id_col)
    data <- join_radius_salience(
      data, arrow::open_dataset(here::here("data", "processed", "cross_section",
                                          market, "prior_characteristics")),
      id_col, radius = 500L, classify_coast = FALSE
    )
    for (i in seq_len(nrow(variants))) {
      variant <- variants[i, ]
      classified <- classify_salience_coast(data, variant$coast_rule_m, variant$unknown_policy)
      include_far <- variant$family == "intensity"
      cat("\n", prefix, " / ", variant$variant, "\n", sep = "")
      audit <- log_salience_cells(classified, nearest, market, variant$family,
                                  include_far, drop_london = variant$drop_london)
      filters <- salience_strata(variant$family, include_far)
      fitted <- lapply(names(filters), function(stratum) {
        selected <- filters[[stratum]](classified)
        if (variant$drop_london) selected <- selected & !classified$london
        tryCatch(
          fit_salience_extensive(classified[selected, ], market, attention),
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
                                 paths$tables[[variant$variant]])
    }
  }
  dir.create(dirname(paths$models), recursive = TRUE, showWarnings = FALSE)
  saveRDS(list(models = models, unrestricted = unrestricted,
               reproduction = reproduction, counts = counts, variants = variants), paths$models)
  cat("\n", prefix, ": ", if (reproduce_only) "reproduction passed" else "five tables and audits published",
      ".\n", sep = "")
  invisible(paths)
}
