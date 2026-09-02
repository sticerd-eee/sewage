# ==============================================================================
# Baseline Hedonic by Local Salience: Prior Spill Count within 250m
# Purpose: Re-estimate the parent's saturated LSOA hedonic within the four
#   nearest-Site-Group coast/bathing strata, for sales and rentals.
# Inputs: Published prior exposure, transactions, Site Group characteristics,
#   and property-Site Group lookups. No radius companions or data builds.
# Outputs: hedonic_count_continuous_prior_salience_coast_bathing.tex and
#   _robust_{coast10km,london,dropunknown}.tex; models and audit CSVs.
# Run from the root with plain Rscript (rv activates via .Rprofile).
# --reproduce disables strata, retains London and checks the parent table.
# ==============================================================================

# === 1. Setup ================================================================
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
check_required_packages(c("arrow", "dplyr", "fixest", "forcats", "here",
                          "modelsummary", "tibble"))
source(here::here("scripts", "R", "09_analysis", "utils_salience_strata.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)

# === 2. Parent sample and saturated model ====================================
#' Prepare the unchanged baseline sample before salience or London exclusions
#' @param transactions Published sales or rental transactions, including region.
#' @param exposure Published prior exposure (data frame or Arrow dataset).
#' @param market Sales or rentals.
#' @return Complete-case 250m sample using transaction prices and parent filters.
prepare_salience_hedonic <- function(transactions, exposure, market = c("sales", "rentals")) {
  market <- match.arg(market)
  id_col <- if (market == "sales") "house_id" else "rental_id"
  price_col <- if (market == "sales") "price" else "listing_price"
  controls <- if (market == "sales") c("property_type", "old_new", "duration") else
    c("property_type", "bedrooms", "bathrooms")
  factors <- if (market == "sales") controls else "property_type"
  transactions <- transactions |>
    dplyr::select(dplyr::all_of(c(id_col, price_col, "region", "lsoa", controls))) |>
    dplyr::mutate(dplyr::across(dplyr::all_of(factors), forcats::as_factor))
  exposure |>
    dplyr::filter(.data$radius == 250L, .data$n_spill_sites > 0L) |>
    dplyr::select(dplyr::all_of(c(id_col, "spill_count_weekly_avg", "spill_hrs_weekly_avg"))) |>
    dplyr::collect() |>
    dplyr::inner_join(transactions, by = id_col, relationship = "one-to-one") |>
    dplyr::mutate(log_price = log(.data[[price_col]])) |>
    # Preserve the parent's joint count/hours availability restriction. Its
    # published inputs already cover sales 2021--2024 and rentals 2021--2023;
    # the baseline does not impose the attention models' coordinate/month filters.
    dplyr::filter(dplyr::if_all(
      dplyr::all_of(c("spill_count_weekly_avg", "spill_hrs_weekly_avg", "lsoa", controls)),
      ~ !is.na(.x)
    )) |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      dplyr::across(dplyr::all_of(factors), forcats::fct_drop)
    )
}

#' Fit the parent's saturated hedonic with heteroskedasticity-robust inference
#' @param data Prepared transactions, optionally restricted to one stratum.
#' @param market Sales or rentals.
#' @return Compact fixest model; unidentified spill effects fail explicitly.
fit_salience_hedonic <- function(data, market = c("sales", "rentals")) {
  market <- match.arg(market)
  formula <- if (market == "sales") {
    log_price ~ spill_count_weekly_avg + property_type + old_new + duration | lsoa
  } else log_price ~ spill_count_weekly_avg + property_type + bedrooms + bathrooms | lsoa
  model <- fixest::feols(formula, data = data, vcov = "hetero", lean = TRUE)
  term <- "spill_count_weekly_avg"
  if (!is.finite(stats::coef(model)[term]) || !is.finite(fixest::se(model)[term])) {
    stop("Unidentified spill effect: ", market, call. = FALSE)
  }
  model
}

#' Check coefficient/SE at published precision and N against the parent table
#' @param model Unrestricted model with London retained.
#' @param reference_path Parent 250m LaTeX table (read-only).
#' @param market Sales uses column 6; rentals uses column 12.
#' @return One audit row; mismatch is a hard failure before publication.
verify_hedonic_reproduction <- function(model, reference_path, market) {
  reference <- readLines(reference_path, warn = FALSE)
  column <- if (market == "sales") 7L else 13L
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
  term <- "spill_count_weekly_avg"
  estimate <- unname(stats::coef(model)[term])
  std_error <- unname(fixest::se(model)[term])
  observed <- sub("^-0\\.000$", "0.000", fmt_table(estimate))
  observed_se <- fmt_table(std_error)
  expected <- printed_cell("Spills per week (avg.) &")
  expected_se <- printed_cell("Spills per week (avg.) &", 1L)
  expected_n <- as.integer(printed_cell("Observations &"))
  if (!is.finite(estimate) || !is.finite(std_error) || observed != expected ||
      observed_se != expected_se || is.na(expected_n) || stats::nobs(model) != expected_n) {
    stop("Reproduction failed: ", market, " = ", observed, " (SE ", observed_se,
         ", N ", stats::nobs(model), ") vs ", expected, " (SE ", expected_se,
         ", N ", expected_n, ").", call. = FALSE)
  }
  cat("Reproduction passed: ", market, ", N = ", stats::nobs(model), " (London retained).\n", sep = "")
  tibble::tibble(
    market = market, stratum_filter = "disabled", london = "retained", term = term,
    estimate = estimate, std_error = std_error,
    printed_estimate = observed, reference_estimate = expected,
    printed_std_error = observed_se, reference_std_error = expected_se,
    nobs = stats::nobs(model), reference_nobs = expected_n,
    reference_path = reference_path, passed = TRUE
  )
}

# === 3. Table publication ====================================================
#' Export the four stratum columns per market in the parent hedonic format
#' @param models Named sales/rentals lists of models in shared-utility order.
#' @param variant One row of coast, unknown-evidence and London settings.
#' @param path Output LaTeX path.
#' @return Output path, invisibly.
export_salience_hedonic <- function(models, variant, path) {
  labels <- c(coastal_bathing = "{Coastal \\\\ bathing}",
              coastal_not_bathing = "{Coastal \\\\ not bathing}",
              inland_bathing = "{Inland \\\\ bathing}",
              inland_not_bathing = "{Inland \\\\ not bathing}")
  panels <- lapply(models, function(fitted) stats::setNames(fitted, unname(labels[names(fitted)])))
  names(panels) <- c("House Sales", "House Rentals")
  extra <- tibble::tibble(term = c("Property controls", "Location FE", "Time FE"))
  for (i in seq_len(sum(lengths(models)))) extra[[paste0("(", i, ")")]] <- c("Yes", "LSOA", "No")
  attr(extra, "position") <- "coef_end"
  latex <- modelsummary::modelsummary(
    panels, shape = "cbind", output = "latex", escape = FALSE,
    estimate = "{estimate}{stars}", statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01), fmt = fmt_table,
    coef_map = c(spill_count_weekly_avg = "{Spills per week \\\\ (avg.)}"),
    gof_map = tibble::tribble(~raw, ~clean, ~fmt,
                            "nobs", "Observations", 0, "adj.r.squared", "Adj. R-squared", 3),
    add_rows = extra, notes = " ",
    title = "Effect of Sewage Spills (Count) on Property Values by Local Salience"
  )
  notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} Sales in England, 2021--2024, ",
    "and rentals, 2021--2023, ",
    if (variant$drop_london) "excluding Greater London. " else "including Greater London. ",
    "All properties are within 250m of an overflow. The dependent variable is log sale ",
    "price or log weekly asking rent. Spill exposure is average weekly spill count ",
    "(12/24 count) across all overflows within 250m from January 2021 to the transaction date. ",
    "The parent's joint count/hours availability restriction is retained. ",
    "Each column re-estimates the saturated LSOA model within its nearest-Site-Group ",
    "stratum. The nearest Site Group is selected within 2000m, with ties broken by Site Group ID. ",
    "Coastal means coast distance at most ", variant$coast_rule_m, "m; inland means greater than ",
    variant$coast_rule_m, "m, independently of bathing designation. Bathing means ever designated ",
    "in 2021--2024. Positive designation takes precedence over unknown evidence in other years. ",
    "The four strata are mutually exclusive within each market. ",
    if (variant$unknown_policy == "exclude") {
      "Unresolved designation (unknown or missing evidence without an observed positive) is excluded. "
    } else "Not bathing includes unresolved designation evidence. ",
    "Properties without a Site Group within 2000m or with missing nearest coast distance are excluded. ",
    "Controls are property type, new-build status and tenure for sales, and property type, bedrooms ",
    "and bathrooms for rentals. All columns include LSOA fixed effects and no time fixed effects. ",
    "Heteroskedasticity-robust standard errors appear in parentheses. ",
    "*** $p<0.01$, ** $p<0.05$, * $p<0.1$.}},"
  )
  latex <- fit_tblr_latex(latex,
    label = paste0("tbl:", gsub("_", "-", tools::file_path_sans_ext(basename(path)))), notes = notes)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeLines(latex, path)
  invisible(path)
}

# === 4. Estimation and audits ================================================
#' Run the four baseline hedonic tables, or only the unrestricted reproduction
#' @param reproduce_only Disable salience filters and retain London.
#' @return Paths to tables, compact models and audit CSVs, invisibly.
main <- function(reproduce_only = FALSE) {
  prefix <- "hedonic_count_continuous_prior_salience"
  variants <- tibble::tribble(
    ~variant, ~coast_rule_m, ~unknown_policy, ~drop_london,
    "coast_bathing", 2000, "not_designated", TRUE,
    "robust_coast10km", 10000, "not_designated", TRUE,
    "robust_london", 2000, "not_designated", FALSE,
    "robust_dropunknown", 2000, "exclude", TRUE
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
    data <- prepare_salience_hedonic(arrow::read_parquet(transaction_path),
      arrow::open_dataset(here::here("data", "processed", "cross_section", market, prior_dir)), market)
    unrestricted[[market]] <- fit_salience_hedonic(data, market)
    reproduction[[market]] <- verify_hedonic_reproduction(unrestricted[[market]],
      here::here("output", "tables", "hedonic_count_continuous_prior_250m.tex"), market)
    if (reproduce_only) next
    lookup_path <- if (market == "sales") here::here("data", "processed", "spill_house_lookup.parquet") else
      here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
    nearest <- nearest_salience_sites(arrow::open_dataset(lookup_path), characteristics, id_col)
    coverage[[market]] <- dplyr::mutate(attr(nearest, "coverage"), market = market, .before = 1L)
    data <- join_nearest_salience(data, nearest, id_col)
    for (i in seq_len(nrow(variants))) {
      variant <- variants[i, ]
      classified <- classify_salience_coast(data, variant$coast_rule_m, variant$unknown_policy)
      cat("\n", prefix, " / ", variant$variant, "\n", sep = "")
      audit <- log_salience_cells(classified, nearest, market, "coast_bathing",
                                  drop_london = variant$drop_london)
      filters <- salience_strata("coast_bathing")
      fitted <- lapply(names(filters), function(stratum) {
        selected <- filters[[stratum]](classified)
        if (variant$drop_london) selected <- selected & !classified$london
        tryCatch(fit_salience_hedonic(classified[selected, ], market),
          error = function(e) stop(market, " / ", variant$variant, " / ", stratum,
                                    ": ", conditionMessage(e), call. = FALSE))
      })
      names(fitted) <- names(filters)
      models[[variant$variant]][[market]] <- fitted
      audit$variant <- variant$variant
      audit$table_path <- paths$tables[[variant$variant]]
      audit$nobs <- vapply(fitted, stats::nobs, numeric(1))
      audit$n_removed_by_estimator <- audit$n_estimation - audit$nobs
      audit$n_missing_nearest_coast <- sum(!is.na(classified$site_id) & is.na(classified$distance_to_coast_m))
      audit$n_unresolved_bathing <- sum(!is.na(classified$site_id) & classified$bath_unresolved)
      cat("  Exclusions: no Site Group within 2km =", audit$n_without_nearest[1L],
          "; missing nearest coast =", audit$n_missing_nearest_coast[1L],
          "; all unclassified =", audit$n_unclassified[1L], "\n")
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
      export_salience_hedonic(models[[variant$variant]], variant, paths$tables[[variant$variant]])
    }
  }
  dir.create(dirname(paths$models), recursive = TRUE, showWarnings = FALSE)
  saveRDS(list(models = models, unrestricted = unrestricted, reproduction = reproduction,
               counts = counts, variants = variants), paths$models)
  cat("\n", prefix, ": ", if (reproduce_only) "reproduction passed" else "four tables and audits published",
      ".\n", sep = "")
  invisible(paths)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(setdiff(args, "--reproduce"))) stop("Only --reproduce is supported.", call. = FALSE)
  main(reproduce_only = "--reproduce" %in% args)
}
