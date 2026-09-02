# ==============================================================================
# Sales Post × Near Salience Tracer (Ticket 01)
# Purpose: Prove shared strata on the existing extensive-margin specification.
# Inputs: Published transactions, property-Site Group lookups, Site Group and
#   radius characteristics; existing did_trends_prior_extensive.tex reference.
# Outputs: Four-column sales table, fitted models, both-market cell/coverage logs.
# Run from the repository root with Rscript (rv activates through .Rprofile).
# --reproduce fits only the unrestricted sales model with London retained.
# ==============================================================================

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
check_required_packages(c("arrow", "dplyr", "fixest", "forcats", "here",
                          "modelsummary", "readxl", "rio", "tibble"))
source(here::here("scripts", "R", "09_analysis", "utils_salience_strata.R"), local = TRUE)

# Source the parent without running its main workflow. Its preparation functions
# retain the exact study windows, complete-case filters and proximity bands.
salience_parent <- new.env(parent = environment())
sys.source(here::here("scripts", "R", "09_analysis", "05_news",
                     "did_trends_prior_extensive.R"), envir = salience_parent)

#' Fit the unchanged saturated sales specification on a supplied subsample
#' @param data Prepared sales transactions, optionally restricted to a stratum.
#' @return LSOA-clustered fixest model with property controls and LSOA/month FE.
fit_salience_sales_post <- function(data) {
  fixest::feols(
    log_price ~ near_bin + near_bin:post + property_type + old_new + duration |
      lsoa + month_id,
    data = data, vcov = ~lsoa
  )
}

#' Check the unrestricted estimate against column 6 of the published parent
#' @param model Unrestricted saturated sales model, retaining Greater London.
#' @param reference_path Existing parent LaTeX table (never overwritten here).
#' @return One-row reproduction audit; mismatch or absent reference fails closed.
verify_salience_reproduction <- function(model, reference_path) {
  reference <- readLines(reference_path, warn = FALSE)
  printed_cell <- function(prefix) {
    row <- reference[startsWith(reference, prefix)]
    if (length(row) != 1L) stop("Cannot identify reference row: ", prefix, call. = FALSE)
    cells <- strsplit(row, " & ", fixed = TRUE)[[1L]]
    cell <- cells[[7L]] # label, then six sales models
    value <- sub(".*\\\\num\\{([^}]+)\\}.*", "\\1", cell)
    if (identical(value, cell)) stop("Cannot parse reference cell.", call. = FALSE)
    value
  }
  expected <- printed_cell("{Near bin")
  observed <- fmt_table(unname(stats::coef(model)["near_bin:post"]))
  expected_n <- as.integer(printed_cell("Observations &"))
  if (is.na(observed) || observed != expected || stats::nobs(model) != expected_n) {
    stop("Sales reproduction failed: Near x Post ", observed, " vs ", expected,
         "; N ", stats::nobs(model), " vs ", expected_n, call. = FALSE)
  }
  cat("Reproduction passed: sales Near x Post = ", observed,
      ", N = ", stats::nobs(model), " (London retained).\n", sep = "")
  tibble::tibble(
    market = "sales", stratum_filter = "disabled", london = "retained",
    term = "near_bin:post", estimate = unname(stats::coef(model)["near_bin:post"]),
    printed_estimate = observed, reference_estimate = expected,
    nobs = stats::nobs(model), reference_nobs = expected_n,
    reference_path = reference_path, passed = TRUE
  )
}

#' Write the tracer in the parent's modelsummary/tabularray format
#' @param models Ordered list of four coast/bathing sales models.
#' @param path Output LaTeX path.
#' @return Output path, invisibly.
export_salience_tracer <- function(models, path) {
  labels <- c("Bathing", "Coastal not bathing", "Coastal (all)", "Inland")
  names(models) <- labels
  extra <- tibble::tibble(term = c("Property controls", "Location FE", "Time FE"))
  for (label in labels) extra[[label]] <- c("Yes", "LSOA", "Month")
  attr(extra, "position") <- "coef_end"
  latex <- modelsummary::modelsummary(
    list("House Sales" = models), shape = "cbind", output = "latex",
    escape = FALSE, estimate = "{estimate}{stars}", statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01), fmt = fmt_table,
    coef_map = c("near_bin" = "Near bin", "near_bin:post" = "{Near bin \\\\ $\\times$ Post}"),
    gof_map = tibble::tribble(~raw, ~clean, ~fmt,
                            "nobs", "Observations", 0,
                            "adj.r.squared", "Adj. R-squared", 3),
    add_rows = extra, notes = " ",
    title = "Public Attention and House Sale Prices by Local Salience"
  )
  notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} Sales in England, 2021--2024, ",
    "excluding Greater London. Near denotes a nearest overflow within 0--500m; ",
    "the far group is over 1000m and at most 2000m away. Post starts in August ",
    "2022. The dependent variable is log sale price. Each column re-estimates ",
    "the saturated model within the stated nearest-Site-Group stratum. Bathing ",
    "means ever designated in 2021--2024. Coastal not bathing means coast ",
    "distance at most 2000m and no observed designation. Coastal (all) is the ",
    "union of those two columns; inland is the remainder. Missing coast ",
    "distances are excluded and unknown bathing evidence counts as not ",
    "designated. Controls are property type, new-build status and tenure, ",
    "with LSOA and month fixed effects. LSOA-clustered standard errors appear ",
    "in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )
  latex <- salience_parent$patch_modelsummary_latex(
    latex, label = "tbl:did-trends-prior-extensive-salience-coast-bathing", notes = notes
  )
  salience_parent$ensure_output_dir(path)
  writeLines(latex, path)
  invisible(path)
}

#' Run the tracer or only the reproduction gate
#' @param reproduce_only Disable strata and retain London; write only the
#'   reproduction audit and fitted unrestricted sales model.
#' @return Output paths, invisibly.
main <- function(reproduce_only = FALSE) {
  salience_parent$initialise_environment()
  comparison <- salience_parent$validate_comparison_config(salience_parent$CONFIG$comparison)
  peak <- salience_parent$load_google_trends_peak(salience_parent$CONFIG$google_trends_path)
  if (peak$peak_month_id != 20L) stop("Expected the August 2022 attention peak.", call. = FALSE)
  paths <- list(
    table = here::here("output", "tables", "did_trends_prior_extensive_salience_coast_bathing.tex"),
    models = here::here("output", "regs", "did_trends_prior_extensive_salience.rds"),
    counts = here::here("output", "logs", "salience_strata_cell_counts.csv"),
    coverage = here::here("output", "logs", "salience_nearest_site_coverage.csv"),
    reproduction = here::here("output", "logs", "salience_sales_post_reproduction.csv")
  )
  sales <- salience_parent$prepare_sales_analysis_data(comparison, peak)
  unrestricted <- fit_salience_sales_post(sales)
  reproduction <- verify_salience_reproduction(unrestricted, salience_parent$CONFIG$output_path)
  salience_parent$ensure_output_dir(paths$reproduction)
  utils::write.csv(reproduction, paths$reproduction, row.names = FALSE)
  if (reproduce_only) {
    path <- here::here("output", "regs", "salience_sales_post_reproduction.rds")
    salience_parent$ensure_output_dir(path)
    saveRDS(unrestricted, path)
    return(invisible(paths$reproduction))
  }

  characteristics <- arrow::read_parquet(here::here(
    "data", "processed", "site_characteristics", "site_group_characteristics.parquet"
  ))
  counts <- list()
  coverage <- list()
  models <- NULL
  for (market in c("sales", "rentals")) {
    id_col <- if (market == "sales") "house_id" else "rental_id"
    lookup_path <- if (market == "sales") salience_parent$CONFIG$sales_lookup_path else
      salience_parent$CONFIG$rental_lookup_path
    nearest <- nearest_salience_sites(arrow::open_dataset(lookup_path), characteristics, id_col)
    coverage[[market]] <- dplyr::mutate(attr(nearest, "coverage"), market = market, .before = 1L)
    data <- if (market == "sales") sales else
      salience_parent$prepare_rental_analysis_data(comparison, peak)
    data <- join_nearest_salience(data, nearest, id_col)
    # The radius band supplements (and must not replace) nearest-site coast data.
    data <- join_radius_salience(
      data, arrow::open_dataset(here::here("data", "processed", "cross_section",
                                          market, "prior_characteristics")),
      id_col, radius = 500L, classify_coast = FALSE
    )
    for (coast_rule in c(2000, 10000)) {
      for (policy in c("not_designated", "exclude")) {
        classified <- classify_salience_coast(data, coast_rule, policy)
        key <- paste(market, coast_rule, policy, sep = "_")
        counts[[key]] <- log_salience_cells(classified, nearest, market, "coast_bathing")
      }
    }
    counts[[paste0(market, "_intensity")]] <- log_salience_cells(
      data, nearest, market, "intensity", include_far = TRUE
    )
    if (market == "sales") {
      filters <- salience_strata("coast_bathing")
      models <- lapply(filters, function(select_stratum) {
        fit_salience_sales_post(data[select_stratum(data) & !data$london, ])
      })
      sales <- NULL
    }
  }
  utils::write.csv(dplyr::bind_rows(counts), paths$counts, row.names = FALSE)
  utils::write.csv(dplyr::bind_rows(coverage), paths$coverage, row.names = FALSE)
  export_salience_tracer(models, paths$table)
  salience_parent$ensure_output_dir(paths$models)
  saveRDS(list(models = models, unrestricted = unrestricted, reproduction = reproduction), paths$models)
  cat("Tracer and audits published.\n", paths$table, "\n", sep = "")
  invisible(paths)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(setdiff(args, "--reproduce"))) stop("Only --reproduce is supported.", call. = FALSE)
  main(reproduce_only = "--reproduce" %in% args)
}
