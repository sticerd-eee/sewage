# ==============================================================================
# Deterministic Summary of Lagged Public-Attention Results
# Purpose: Validate and consolidate the four lag-sweep component CSVs, then
#          create coefficient-path figures for the extensive-margin core,
#          extensive rental placebo, and intensive-margin extension.
# Inputs:  Four script-owned component effect-size CSVs in output/tables.
# Outputs: output/tables/did_news_lagged_sales_effect_sizes.csv
#          output/figures/did_news_lagged_sales_coefficient_paths.{pdf,png}
# ==============================================================================

required_columns <- c(
  "margin", "market", "measure", "radius", "lag", "sample",
  "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
)

result_key_columns <- c(
  "margin", "market", "measure", "radius", "lag", "sample"
)

allowed_lags <- c(0L, 3L, 6L, 12L)
allowed_radii <- c(250L, 500L, 1000L)

default_component_paths <- function() {
  c(
    extensive_post = here::here(
      "output", "tables",
      "did_trends_lagged_sales_extensive_effect_sizes.csv"
    ),
    extensive_articles = here::here(
      "output", "tables",
      "did_articles_lagged_sales_extensive_effect_sizes.csv"
    ),
    intensive_post = here::here(
      "output", "tables", "did_trends_lagged_sales_effect_sizes.csv"
    ),
    intensive_articles = here::here(
      "output", "tables", "did_articles_lagged_sales_effect_sizes.csv"
    )
  )
}

expected_component_keys <- function(component) {
  expand_keys <- function(...) {
    out <- expand.grid(..., stringsAsFactors = FALSE)
    rownames(out) <- NULL
    out
  }

  switch(
    component,
    extensive_post = expand_keys(
      margin = "extensive", market = c("sales", "rentals"),
      measure = "post", radius = 500L, lag = allowed_lags,
      sample = "full"
    ),
    extensive_articles = rbind(
      expand_keys(
        margin = "extensive", market = c("sales", "rentals"),
        measure = "articles", radius = 500L, lag = allowed_lags,
        sample = "common"
      ),
      expand_keys(
        margin = "extensive", market = c("sales", "rentals"),
        measure = "articles", radius = 500L, lag = 0L,
        sample = "full_reference"
      )
    ),
    intensive_post = rbind(
      expand_keys(
        margin = "intensive", market = "sales", measure = "post",
        radius = allowed_radii, lag = allowed_lags, sample = "full"
      ),
      expand_keys(
        margin = "intensive", market = "rentals", measure = "post",
        radius = allowed_radii, lag = 0L, sample = "full"
      )
    ),
    intensive_articles = rbind(
      expand_keys(
        margin = "intensive", market = "sales", measure = "articles",
        radius = allowed_radii, lag = allowed_lags, sample = "common"
      ),
      expand_keys(
        margin = "intensive", market = "sales", measure = "articles",
        radius = allowed_radii, lag = 0L, sample = "full_reference"
      ),
      expand_keys(
        margin = "intensive", market = "rentals", measure = "articles",
        radius = allowed_radii, lag = 0L, sample = "full_reference"
      )
    ),
    stop("Unknown component contract: ", component, call. = FALSE)
  )
}

format_keys <- function(data) {
  if (nrow(data) == 0L) return("<none>")
  apply(data[result_key_columns], 1L, paste, collapse = " / ") |>
    paste(collapse = "; ")
}

validate_component <- function(data, component) {
  if (!identical(names(data), required_columns)) {
    stop(
      component, " must contain exactly these columns in this order: ",
      paste(required_columns, collapse = ", "), call. = FALSE
    )
  }

  character_columns <- c("margin", "market", "measure", "sample")
  numeric_columns <- c(
    "radius", "lag", "estimate", "std_error", "conf_low", "conf_high",
    "p_value", "n"
  )
  if (!all(vapply(data[character_columns], is.character, logical(1)))) {
    stop(component, " has non-character label columns.", call. = FALSE)
  }
  if (!all(vapply(data[numeric_columns], is.numeric, logical(1)))) {
    stop(component, " has non-numeric result columns.", call. = FALSE)
  }
  if (anyNA(data) || !all(is.finite(as.matrix(data[numeric_columns])))) {
    stop(component, " contains missing or non-finite result values.", call. = FALSE)
  }
  if (anyDuplicated(data[result_key_columns])) {
    duplicates <- data[
      duplicated(data[result_key_columns]) |
        duplicated(data[result_key_columns], fromLast = TRUE),
      , drop = FALSE
    ]
    stop(
      component, " contains duplicate result keys: ", format_keys(duplicates),
      call. = FALSE
    )
  }
  if (any(data$std_error < 0) || any(data$conf_low > data$estimate) ||
      any(data$estimate > data$conf_high)) {
    stop(component, " contains invalid standard errors or confidence intervals.",
         call. = FALSE)
  }
  critical_value <- stats::qnorm(0.975)
  ci_tolerance <- 1e-8 * (1 + max(abs(c(data$conf_low, data$conf_high))))
  if (any(abs(data$conf_low -
      (data$estimate - critical_value * data$std_error)) > ci_tolerance) ||
      any(abs(data$conf_high -
        (data$estimate + critical_value * data$std_error)) > ci_tolerance)) {
    stop(
      component,
      " confidence intervals are not ordinary pointwise 95% intervals.",
      call. = FALSE
    )
  }
  if (any(data$p_value < 0 | data$p_value > 1) ||
      any(data$n <= 0 | data$n != floor(data$n))) {
    stop(component, " contains invalid p-values or observation counts.",
         call. = FALSE)
  }

  expected <- expected_component_keys(component)
  actual_key <- do.call(paste, c(data[result_key_columns], sep = "\r"))
  expected_key <- do.call(paste, c(expected[result_key_columns], sep = "\r"))
  missing <- expected[!expected_key %in% actual_key, , drop = FALSE]
  unexpected <- data[!actual_key %in% expected_key, result_key_columns, drop = FALSE]
  if (nrow(missing) > 0L || nrow(unexpected) > 0L) {
    stop(
      component, " has incorrect coverage. Missing: ", format_keys(missing),
      ". Unexpected: ", format_keys(unexpected), call. = FALSE
    )
  }

  invisible(data)
}

read_component <- function(path, component) {
  if (!file.exists(path)) {
    stop("Missing ", component, " component CSV: ", path, call. = FALSE)
  }
  data <- utils::read.csv(
    path, stringsAsFactors = FALSE, check.names = FALSE,
    colClasses = c(
      "character", "character", "character", "integer", "integer",
      "character", rep("numeric", 5L), "integer"
    )
  )
  validate_component(data, component)
  data
}

sort_results <- function(data) {
  margin_rank <- match(data$margin, c("extensive", "intensive"))
  measure_rank <- match(data$measure, c("post", "articles"))
  market_rank <- match(data$market, c("sales", "rentals"))
  sample_rank <- match(data$sample, c("full", "common", "full_reference"))
  data[order(
    margin_rank, measure_rank, market_rank, data$radius, sample_rank, data$lag
  ), required_columns, drop = FALSE]
}

consolidate_components <- function(component_paths) {
  expected_names <- c(
    "extensive_post", "extensive_articles", "intensive_post",
    "intensive_articles"
  )
  if (!identical(names(component_paths), expected_names)) {
    stop(
      "component_paths must be named in this fixed order: ",
      paste(expected_names, collapse = ", "), call. = FALSE
    )
  }
  components <- Map(
    read_component, unname(component_paths), names(component_paths),
    USE.NAMES = FALSE
  )
  out <- sort_results(do.call(rbind, components))
  rownames(out) <- NULL
  if (nrow(out) != 51L || anyDuplicated(out[result_key_columns])) {
    stop("Consolidated results must have 51 globally unique keys.", call. = FALSE)
  }
  out
}

atomic_write_csv <- function(data, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  temporary <- tempfile(
    pattern = paste0(".", basename(path), "."), tmpdir = dirname(path)
  )
  on.exit(if (file.exists(temporary)) unlink(temporary), add = TRUE)
  utils::write.csv(data, temporary, row.names = FALSE, na = "")
  if (!file.rename(temporary, path)) {
    stop("Could not atomically replace output: ", path, call. = FALSE)
  }
  invisible(path)
}

figure_data <- function(data) {
  is_path_sample <-
    (data$measure == "post" & data$sample == "full") |
    (data$measure == "articles" & data$sample == "common")
  is_included_panel <-
    data$margin == "extensive" |
    (data$margin == "intensive" & data$market == "sales")
  keep <- is_path_sample & is_included_panel
  out <- data[keep, , drop = FALSE]
  out$panel <- ifelse(
    out$margin == "intensive", "Intensive sales (extension)",
    ifelse(
      out$market == "rentals", "Extensive rentals (secondary placebo)",
      "Extensive sales (core)"
    )
  )
  out$series <- ifelse(
    out$margin == "intensive", paste0(out$radius, "m radius"),
    ifelse(out$market == "rentals", "Rentals", "Sales")
  )
  out$measure_label <- ifelse(
    out$measure == "post", "Post-peak indicator", "Cumulative articles"
  )
  out$panel <- factor(
    out$panel,
    levels = c(
      "Extensive sales (core)", "Extensive rentals (secondary placebo)",
      "Intensive sales (extension)"
    )
  )
  out$measure_label <- factor(
    out$measure_label,
    levels = c("Post-peak indicator", "Cumulative articles")
  )
  out
}

atomic_save_plot <- function(plot, path, width, height, dpi = 300) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  extension <- tools::file_ext(path)
  temporary <- tempfile(
    pattern = paste0(".", basename(path), "."), tmpdir = dirname(path),
    fileext = paste0(".", extension)
  )
  on.exit(if (file.exists(temporary)) unlink(temporary), add = TRUE)
  ggplot2::ggsave(
    temporary, plot = plot, width = width, height = height, units = "in",
    dpi = dpi, bg = "white"
  )
  if (!file.rename(temporary, path)) {
    stop("Could not replace figure: ", path, call. = FALSE)
  }
  invisible(path)
}

create_coefficient_path_figure <- function(data, pdf_path, png_path) {
  plot_data <- figure_data(data)
  expected_path_rows <- 4L + 4L + 4L + 4L + 12L + 12L
  stopifnot(nrow(plot_data) == expected_path_rows)

  plot <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x = .data$lag, y = .data$estimate, colour = .data$series,
      group = .data$series
    )
  ) +
    ggplot2::geom_hline(yintercept = 0, colour = "grey65", linewidth = 0.35) +
    ggplot2::geom_line(linewidth = 0.55) +
    ggplot2::geom_errorbar(
      ggplot2::aes(ymin = .data$conf_low, ymax = .data$conf_high),
      width = 0.55, linewidth = 0.45
    ) +
    ggplot2::geom_point(size = 1.8) +
    ggplot2::facet_grid(
      rows = ggplot2::vars(.data$panel),
      cols = ggplot2::vars(.data$measure_label), scales = "free_y"
    ) +
    ggplot2::scale_x_continuous(breaks = allowed_lags) +
    ggplot2::scale_colour_manual(values = c(
      "Sales" = "#1b6ca8", "Rentals" = "#a4512a",
      "250m radius" = "#4477aa", "500m radius" = "#228833",
      "1000m radius" = "#aa3377"
    )) +
    ggplot2::labs(
      x = "Attention lag (months)", y = "Interaction coefficient",
      colour = NULL,
      caption = paste(
        "Points are estimates; bars are ordinary pointwise 95% confidence",
        "intervals. Article paths use the common Jan 2022-Dec 2023 sample."
      )
    ) +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(
      legend.position = "bottom",
      panel.grid.minor = ggplot2::element_blank(),
      strip.text.y = ggplot2::element_text(angle = 0),
      plot.caption = ggplot2::element_text(hjust = 0)
    )

  atomic_save_plot(plot, pdf_path, width = 9.2, height = 8.2)
  atomic_save_plot(plot, png_path, width = 9.2, height = 8.2, dpi = 320)
  invisible(plot)
}

main <- function(
  component_paths = default_component_paths(),
  consolidated_path = here::here(
    "output", "tables", "did_news_lagged_sales_effect_sizes.csv"
  ),
  figure_pdf_path = here::here(
    "output", "figures", "did_news_lagged_sales_coefficient_paths.pdf"
  ),
  figure_png_path = here::here(
    "output", "figures", "did_news_lagged_sales_coefficient_paths.png"
  )
) {
  for (package in c("here", "ggplot2")) {
    if (!requireNamespace(package, quietly = TRUE)) {
      stop("Package required by summary script is not installed: ", package,
           call. = FALSE)
    }
  }
  results <- consolidate_components(component_paths)
  atomic_write_csv(results, consolidated_path)
  create_coefficient_path_figure(
    results, pdf_path = figure_pdf_path, png_path = figure_png_path
  )
  cat("Validated and consolidated ", nrow(results), " result rows.\n", sep = "")
  cat("Consolidated CSV: ", consolidated_path, "\n", sep = "")
  cat("Coefficient paths: ", figure_pdf_path, " and ", figure_png_path, "\n",
      sep = "")
  invisible(results)
}

if (sys.nframe() == 0L) main()
