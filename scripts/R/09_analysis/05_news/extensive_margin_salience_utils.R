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

#' Compare the unrestricted model with the published parent's saturated column
#' @param model Unrestricted model retaining Greater London.
#' @param reference_path Existing parent LaTeX table; never overwritten here.
#' @param market Sales uses column 6; rentals uses column 12.
#' @param attention Attention regressor name.
#' @return Audit rows for Near and its attention interaction. Coefficients and
#'   standard errors must match at published precision and N must match exactly.
verify_salience_reproduction <- function(model, reference_path, market, attention) {
  reference <- readLines(reference_path, warn = FALSE)
  column <- if (market == "sales") 7L else 13L # includes the row label
  printed_cell <- function(prefix, offset = 0L) {
    row <- which(startsWith(reference, prefix))
    if (length(row) != 1L) stop("Cannot identify reference row: ", prefix, call. = FALSE)
    cells <- trimws(strsplit(reference[row + offset], "&", fixed = TRUE)[[1L]])
    if (length(cells) < column) stop("Missing saturated reference column.", call. = FALSE)
    cell <- cells[[column]]
    value <- sub(".*\\\\num\\{([^}]+)\\}.*", "\\1", cell)
    if (identical(value, cell)) stop("Cannot parse reference cell.", call. = FALSE)
    value
  }
  expected_n <- as.integer(printed_cell("Observations &"))
  terms <- c("near_bin", paste0("near_bin:", attention))
  prefixes <- c("Near bin &", "{Near bin")
  rows <- lapply(seq_along(terms), function(i) {
    term <- terms[i]
    estimate <- unname(stats::coef(model)[term])
    std_error <- unname(fixest::se(model)[term])
    observed <- sub("^-0\\.000$", "0.000", fmt_table(estimate))
    observed_se <- fmt_table(std_error)
    expected <- printed_cell(prefixes[i])
    expected_se <- printed_cell(prefixes[i], 1L)
    if (!is.finite(estimate) || observed != expected || observed_se != expected_se ||
        is.na(expected_n) || stats::nobs(model) != expected_n) {
      stop("Reproduction failed: ", market, " / ", term, " = ", observed,
           " (SE ", observed_se, ", N ", stats::nobs(model), ") vs ",
           expected, " (SE ", expected_se, ", N ", expected_n, ").", call. = FALSE)
    }
    tibble::tibble(
      market = market, attention = attention, stratum_filter = "disabled",
      london = "retained", term = term, estimate = estimate, std_error = std_error,
      printed_estimate = observed, reference_estimate = expected,
      printed_std_error = observed_se, reference_std_error = expected_se,
      nobs = stats::nobs(model), reference_nobs = expected_n,
      reference_path = reference_path, passed = TRUE
    )
  })
  cat("Reproduction passed: ", market, " / ", attention,
      ", N = ", stats::nobs(model), " (London retained).\n", sep = "")
  dplyr::bind_rows(rows)
}

#' Export one stratum family for both markets in the parent's table format
#' @param models Named sales/rentals lists of stratum models in utility order.
#' @param variant One row of the requested family/robustness settings.
#' @param attention Attention regressor name.
#' @param path Output LaTeX path.
#' @return Output path, invisibly.
export_salience_extensive <- function(models, variant, attention, path) {
  labels <- c(
    coastal_bathing = "{Coastal \\\\ bathing}",
    coastal_not_bathing = "{Coastal \\\\ not bathing}",
    inland_bathing = "{Inland \\\\ bathing}",
    inland_not_bathing = "{Inland \\\\ not bathing}",
    spill_le_p50 = "{Spill count \\\\ $\\leq$ median}",
    spill_gt_p50 = "{Spill count \\\\ $>$ median}"
  )
  panels <- lapply(models, function(market_models) {
    stats::setNames(market_models, unname(labels[names(market_models)]))
  })
  names(panels) <- c("House Sales", "House Rentals")
  # Unique positional names avoid duplicate stratum labels across market panels.
  extra <- tibble::tibble(term = c("Property controls", "Location FE", "Time FE"))
  for (i in seq_len(sum(lengths(models)))) extra[[paste0("(", i, ")")]] <- c("Yes", "LSOA", "Month")
  attr(extra, "position") <- "coef_end"
  interaction_label <- if (attention == "post") "{Near bin \\\\ $\\times$ Post}" else
    "{Near bin \\\\ $\\times$ $\\log (\\text{Articles})$}"
  latex <- modelsummary::modelsummary(
    panels, shape = "cbind", output = "latex",
    escape = FALSE, estimate = "{estimate}{stars}", statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01), fmt = fmt_table,
    coef_map = stats::setNames(c("Near bin", interaction_label),
                              c("near_bin", paste0("near_bin:", attention))),
    gof_map = tibble::tribble(~raw, ~clean, ~fmt,
                            "nobs", "Observations", 0,
                            "adj.r.squared", "Adj. R-squared", 3),
    add_rows = extra, notes = " ",
    title = paste0("Public Attention and Property Values by Local Salience: ",
                   if (attention == "post") "Post August 2022" else "Cumulative Articles")
  )
  family_notes <- if (variant$family == "intensity") {
    paste0(
      "Near properties are split by the published market-specific 500m spill-count ",
      "band: positive exposure at or below its positive-exposure median, or above ",
      "it. Unknown, zero, no-site and missing near bands are excluded. Each stratum ",
      "uses the full far group under the stated London policy; far controls are ",
      "shared between columns. Coast and bathing evidence do not restrict this family. "
    )
  } else {
    paste0(
      "Each column re-estimates the saturated model within its nearest-Site-Group ",
      "stratum. Coastal means coast distance at most ", variant$coast_rule_m,
      "m; inland means greater than ", variant$coast_rule_m,
      "m, independently of bathing designation. Bathing means ever designated in ",
      "2021--2024. Positive designation takes precedence over unknown evidence in ",
      "other years. The four strata are mutually exclusive within each market. ",
      if (variant$unknown_policy == "exclude") {
        "Unresolved designation (unknown or missing evidence without an observed positive) is excluded. "
      } else {
        "Not bathing includes unresolved designation evidence. "
      },
      "Missing coast distances are excluded. "
    )
  }
  notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} Sales in England, 2021--2024, ",
    "and rentals, 2021--2023, ",
    if (variant$drop_london) "excluding Greater London. " else "including Greater London. ",
    "Near denotes a nearest overflow within 0--500m; far is over 1000m and at most ",
    "2000m away. The dependent variable is log sale price or log weekly asking rent. ",
    if (attention == "post") "Post starts in August 2022. " else
      "Articles is cumulative UK news coverage from January 2021 through the transaction month, in natural logs. ",
    family_notes,
    "Controls are property type, new-build status and tenure for sales, and property ",
    "type, bedrooms and bathrooms for rentals, with LSOA and month fixed effects. ",
    "LSOA-clustered standard errors appear in parentheses. ",
    "*** $p<0.01$, ** $p<0.05$, * $p<0.1$.}},"
  )
  latex <- fit_tblr_latex(
    latex, label = paste0("tbl:", gsub("_", "-", tools::file_path_sans_ext(basename(path)))),
    notes = notes
  )
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeLines(latex, path)
  invisible(path)
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
      export_salience_extensive(models[[variant$variant]], variant, attention,
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
