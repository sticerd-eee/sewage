# ==============================================================================
# Windowed Article Effect-Size Utilities
# ==============================================================================
#
# Purpose: Shared helpers for turning article-salience interaction coefficients
#          into comparable percentage effect sizes for the HTML summary report.
#
# ==============================================================================

extract_fixest_term <- function(model, term) {
  coef_table <- fixest::coeftable(model)

  if (!term %in% rownames(coef_table)) {
    stop("Required comparison coefficient is missing: ", term, call. = FALSE)
  }

  out <- c(
    estimate = unname(coef_table[term, "Estimate"]),
    std_error = unname(coef_table[term, "Std. Error"]),
    p_value = unname(coef_table[term, "Pr(>|t|)"])
  )

  if (!all(is.finite(out))) {
    stop("Required comparison coefficient is non-finite: ", term, call. = FALSE)
  }

  out
}

summarise_effect_inputs <- function(data, salience_col, spill_col = NULL) {
  if (!salience_col %in% names(data)) {
    stop("Missing salience column: ", salience_col, call. = FALSE)
  }

  salience <- data[[salience_col]]
  salience_quantiles <- stats::quantile(
    salience,
    probs = c(0.25, 0.75),
    na.rm = TRUE,
    names = FALSE
  )

  spill_sd <- NA_real_
  if (!is.null(spill_col)) {
    if (!spill_col %in% names(data)) {
      stop("Missing spill column: ", spill_col, call. = FALSE)
    }
    spill_sd <- stats::sd(data[[spill_col]], na.rm = TRUE)
  }

  out <- tibble::tibble(
    salience_sd = stats::sd(salience, na.rm = TRUE),
    salience_p25 = salience_quantiles[[1]],
    salience_p75 = salience_quantiles[[2]],
    salience_iqr = salience_quantiles[[2]] - salience_quantiles[[1]],
    spill_sd = spill_sd
  )

  numeric_values <- unlist(out, use.names = FALSE)
  finite_values <- numeric_values[!is.na(numeric_values)]
  if (!all(is.finite(finite_values))) {
    stop("Effect-size input statistics are non-finite.", call. = FALSE)
  }

  out
}

write_windowed_article_effect_sizes <- function(
  models_by_measure,
  salience_cols,
  sales_data,
  rental_data,
  interaction_term_fn,
  margin = c("intensive", "extensive"),
  output_path,
  spill_col = NULL
) {
  margin <- match.arg(margin)

  if (margin == "intensive" && is.null(spill_col)) {
    stop(
      "`spill_col` is required for intensive-margin effect sizes.",
      call. = FALSE
    )
  }

  missing_measures <- setdiff(names(salience_cols), names(models_by_measure))
  if (length(missing_measures) > 0L) {
    stop(
      "Missing models for salience measures: ",
      paste(missing_measures, collapse = ", "),
      call. = FALSE
    )
  }

  market_specs <- list(
    sales = list(data = sales_data, model_prefix = "sale"),
    rentals = list(data = rental_data, model_prefix = "rent")
  )
  fe_specs <- c(msoa = "MSOA", lsoa = "LSOA")

  rows <- list()
  row_id <- 0L

  for (measure in names(salience_cols)) {
    salience_col <- salience_cols[[measure]]
    term <- interaction_term_fn(salience_col)

    for (market in names(market_specs)) {
      market_spec <- market_specs[[market]]
      inputs <- summarise_effect_inputs(
        data = market_spec$data,
        salience_col = salience_col,
        spill_col = if (margin == "intensive") spill_col else NULL
      )
      scale <- if (margin == "intensive") inputs$spill_sd[[1]] else 1

      for (fe in names(fe_specs)) {
        model_name <- paste0(market_spec$model_prefix, "_", fe)
        model <- models_by_measure[[measure]][[model_name]]

        if (is.null(model)) {
          stop(
            "Missing model `", model_name, "` for measure `", measure, "`.",
            call. = FALSE
          )
        }

        coef_stats <- extract_fixest_term(model, term)

        row_id <- row_id + 1L
        rows[[row_id]] <- tibble::tibble(
          margin = margin,
          market = market,
          fixed_effects = fe,
          fixed_effects_label = fe_specs[[fe]],
          measure = measure,
          salience_col = salience_col,
          term = term,
          estimate = coef_stats[["estimate"]],
          std_error = coef_stats[["std_error"]],
          p_value = coef_stats[["p_value"]],
          spill_sd = inputs$spill_sd[[1]],
          salience_sd = inputs$salience_sd[[1]],
          salience_p25 = inputs$salience_p25[[1]],
          salience_p75 = inputs$salience_p75[[1]],
          salience_iqr = inputs$salience_iqr[[1]],
          effect_iqr_pct = 100 * coef_stats[["estimate"]] *
            scale * inputs$salience_iqr[[1]],
          effect_sd_pct = 100 * coef_stats[["estimate"]] *
            scale * inputs$salience_sd[[1]],
          effect_iqr_se_pct = 100 * coef_stats[["std_error"]] *
            scale * inputs$salience_iqr[[1]],
          effect_sd_se_pct = 100 * coef_stats[["std_error"]] *
            scale * inputs$salience_sd[[1]]
        )
      }
    }
  }

  out <- dplyr::bind_rows(rows)

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(out, output_path, row.names = FALSE, na = "")

  cat(sprintf("Effect-size summary exported to: %s\n", output_path))

  invisible(out)
}
