# ==============================================================================
# Salience Attention Tables: Shared Publication and Parent Reproduction Checks
# Used by the extensive and 250m intensive specifications for both markets.
# ==============================================================================

#' Compare the unrestricted model with the published parent's saturated column
#' @param model Unrestricted model retaining Greater London.
#' @param reference_path Existing parent LaTeX table; never overwritten here.
#' @param market Sales uses column 6; rentals uses column 12.
#' @param attention Attention regressor name.
#' @param exposure Parent regressor: near_bin or spill_count_weekly_avg.
#' @return Audit rows for exposure and its attention interaction. Coefficients and
#'   standard errors must match at published precision and N must match exactly.
verify_salience_reproduction <- function(
  model, reference_path, market, attention, exposure = "near_bin"
) {
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
  terms <- c(exposure, paste0(exposure, ":", attention))
  exposure_label <- if (exposure == "near_bin") "Near bin" else "Spills per week (avg.)"
  prefixes <- c(paste0(exposure_label, " &"), paste0("{", exposure_label))
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
#' @param margin Extensive (nearest-site strata) or intensive (250m radius strata).
#' @return Output path, invisibly.
export_salience_attention <- function(
  models, variant, attention, path, margin = c("extensive", "intensive")
) {
  margin <- match.arg(margin)
  intensive <- margin == "intensive"
  exposure <- if (intensive) "spill_count_weekly_avg" else "near_bin"
  exposure_label <- if (intensive) "Spills/week (avg.)" else "Near bin"
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
  interaction_label <- paste0(
    "{", exposure_label, " \\\\ $\\times$ ",
    if (attention == "post") "Post}" else "$\\log (\\text{Articles})$}"
  )
  latex <- modelsummary::modelsummary(
    panels, shape = "cbind", output = "latex",
    escape = FALSE, estimate = "{estimate}{stars}", statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01), fmt = fmt_table,
    coef_map = stats::setNames(c(exposure_label, interaction_label),
                              c(exposure, paste0(exposure, ":", attention))),
    gof_map = tibble::tribble(~raw, ~clean, ~fmt,
                            "nobs", "Observations", 0,
                            "adj.r.squared", "Adj. R-squared", 3),
    add_rows = extra, notes = " ",
    title = paste0("Public Attention and Property Values by Local Salience: ",
                   if (attention == "post") "Post August 2022" else "Cumulative Articles")
  )
  family_notes <- if (variant$family == "intensity" && intensive) {
    paste0(
      "Properties are split by their published market-specific 250m spill-count ",
      "band: positive exposure at or below its positive-exposure median, or above ",
      "it. Unknown, zero, no-site and missing bands are excluded. Coast and ",
      "bathing evidence do not restrict this family. "
    )
  } else if (variant$family == "intensity") {
    paste0(
      "Near properties are split by the published market-specific 500m spill-count ",
      "band: positive exposure at or below its positive-exposure median, or above ",
      "it. Unknown, zero, no-site and missing near bands are excluded. Each stratum ",
      "uses the full far group under the stated London policy; far controls are ",
      "shared between columns. Coast and bathing evidence do not restrict this family. "
    )
  } else {
    paste0(
      if (intensive) {
        paste0("Each column re-estimates the saturated model within its 250m ",
               "property-radius stratum. Coastal means minimum nearby-site coast distance at most ")
      } else {
        paste0("Each column re-estimates the saturated model within its nearest-Site-Group ",
               "stratum. Coastal means coast distance at most ")
      }, variant$coast_rule_m,
      "m; inland means greater than ", variant$coast_rule_m,
      "m, independently of bathing designation. ",
      if (intensive) {
        paste0("Bathing means any Site Group within 250m was ever designated in 2021--2024. ",
               "Positive designation takes precedence over unknown evidence at other sites or in other years. ")
      } else {
        paste0("Bathing means ever designated in 2021--2024. Positive designation takes precedence ",
               "over unknown evidence in other years. ")
      },
      "The four strata are mutually exclusive within each market. ",
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
    if (intensive) {
      paste0("All properties are within 250m of an overflow. Spill exposure is average ",
             "weekly spill count (12/24 count) across all overflows within 250m from ",
             "January 2021 to the transaction date. Missing exposure is excluded. ")
    } else {
      paste0("Near denotes a nearest overflow within 0--500m; far is over 1000m and at most ",
             "2000m away. ")
    },
    "The dependent variable is log sale price or log weekly asking rent. ",
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
