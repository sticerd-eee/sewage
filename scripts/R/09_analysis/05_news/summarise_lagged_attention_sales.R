# ==============================================================================
# Deterministic Summary of Lagged Public-Attention Results
# Purpose: Validate and consolidate the four lag-sweep component CSVs, then
#          create separate coefficient-path figures for the extensive-margin
#          core/placebo and the intensive-margin extension.
# Inputs:  Four script-owned component effect-size CSVs in output/tables.
# Outputs: output/tables/did_news_lagged_sales_effect_sizes.csv
#          output/figures/did_news_lagged_sales_coefficient_paths.{pdf,png}
#          output/figures/did_news_lagged_sales_intensive_extension.{pdf,png}
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

make_synthetic_component <- function(component) {
  out <- expected_component_keys(component)
  row_id <- seq_len(nrow(out))
  out$estimate <- row_id / 100
  out$std_error <- rep(0.1, nrow(out))
  critical_value <- stats::qnorm(0.975)
  out$conf_low <- out$estimate - critical_value * out$std_error
  out$conf_high <- out$estimate + critical_value * out$std_error
  out$p_value <- rep(0.5, nrow(out))
  out$n <- as.integer(1000L + row_id)
  out[required_columns]
}

expect_validation_error <- function(expr) {
  error <- tryCatch(
    {
      force(expr)
      NULL
    },
    error = function(condition) condition
  )
  inherits(error, "error")
}

run_consolidation_sanity_checks <- function() {
  component_names <- c(
    "extensive_post", "extensive_articles", "intensive_post",
    "intensive_articles"
  )
  components <- setNames(
    lapply(component_names, make_synthetic_component),
    component_names
  )

  duplicate <- rbind(components$extensive_post, components$extensive_post[1L, ])
  missing <- components$extensive_articles[-1L, , drop = FALSE]
  stopifnot(
    expect_validation_error(validate_component(duplicate, "extensive_post")),
    expect_validation_error(validate_component(missing, "extensive_articles"))
  )

  temporary_directory <- tempfile("lagged-attention-sanity-")
  dir.create(temporary_directory)
  on.exit(unlink(temporary_directory, recursive = TRUE), add = TRUE)
  component_paths <- setNames(
    file.path(temporary_directory, paste0(component_names, ".csv")),
    component_names
  )
  Map(
    function(data, path) utils::write.csv(data, path, row.names = FALSE, na = ""),
    components,
    component_paths
  )

  first <- consolidate_components(component_paths)
  consolidated_path <- file.path(temporary_directory, "consolidated.csv")
  atomic_write_csv(first, consolidated_path)
  first_written <- readLines(consolidated_path, warn = FALSE)

  second <- consolidate_components(component_paths)
  atomic_write_csv(second, consolidated_path)
  second_written <- readLines(consolidated_path, warn = FALSE)

  stopifnot(
    nrow(first) == 51L,
    !anyDuplicated(first[result_key_columns]),
    identical(first, second),
    identical(first_written, second_written)
  )

  invisible(TRUE)
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

path_figure_data <- function(data) {
  is_path_sample <-
    (data$measure == "post" & data$sample == "full") |
    (data$measure == "articles" & data$sample == "common")
  is_included_panel <-
    data$margin == "extensive" |
    (data$margin == "intensive" & data$market == "sales")
  keep <- is_path_sample & is_included_panel
  out <- data[keep, , drop = FALSE]
  out$series <- ifelse(
    out$margin == "intensive", paste0(out$radius, "m radius"),
    ifelse(out$market == "rentals", "Rentals", "Sales")
  )
  out
}

core_figure_data <- function(data) {
  out <- path_figure_data(data)
  out <- out[out$margin == "extensive", , drop = FALSE]
  out$outcome_label <- ifelse(
    out$market == "sales",
    "Sales: log transaction price\nNear: 0-500m vs far: 1-2km",
    paste0(
      "Rentals: log weekly asking rent\n",
      "Near: 0-500m vs far: 1-2km (placebo)"
    )
  )
  out$interaction_label <- ifelse(
    out$measure == "post",
    "Binary attention\nNear × post indicator at t - L",
    "Article attention\nNear × log cumulative articles at t - L"
  )
  out$outcome_label <- factor(
    out$outcome_label,
    levels = c(
      "Sales: log transaction price\nNear: 0-500m vs far: 1-2km",
      paste0(
        "Rentals: log weekly asking rent\n",
        "Near: 0-500m vs far: 1-2km (placebo)"
      )
    )
  )
  out$interaction_label <- factor(
    out$interaction_label,
    levels = c(
      "Binary attention\nNear × post indicator at t - L",
      "Article attention\nNear × log cumulative articles at t - L"
    )
  )
  out
}

intensive_figure_data <- function(data) {
  out <- path_figure_data(data)
  out <- out[
    out$margin == "intensive" & out$market == "sales", , drop = FALSE
  ]
  out$series <- factor(
    out$series,
    levels = c("250m radius", "500m radius", "1000m radius")
  )
  out$interaction_label <- ifelse(
    out$measure == "post",
    paste0(
      "Binary attention\n",
      "Weekly spill exposure × post indicator at t - L"
    ),
    paste0(
      "Article attention\n",
      "Weekly spill exposure × log cumulative articles at t - L"
    )
  )
  out$interaction_label <- factor(
    out$interaction_label,
    levels = c(
      paste0(
        "Binary attention\n",
        "Weekly spill exposure × post indicator at t - L"
      ),
      paste0(
        "Article attention\n",
        "Weekly spill exposure × log cumulative articles at t - L"
      )
    )
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

create_core_coefficient_figure <- function(data, pdf_path, png_path) {
  plot_data <- core_figure_data(data)
  stopifnot(nrow(plot_data) == 16L)

  plot <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x = .data$lag, y = .data$estimate, colour = .data$market,
      group = .data$market
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
      rows = ggplot2::vars(.data$outcome_label),
      cols = ggplot2::vars(.data$interaction_label),
      scales = "free_y", switch = "y"
    ) +
    ggplot2::scale_x_continuous(breaks = allowed_lags) +
    ggplot2::scale_colour_manual(values = c(
      "sales" = "#1b6ca8", "rentals" = "#a4512a"
    ), guide = "none") +
    ggplot2::labs(
      title = paste(
        "Does earlier public attention change the near-far price or rent gap?"
      ),
      subtitle = paste(
        "Each point is the interaction coefficient from a separate LSOA and",
        "month fixed-effects regression."
      ),
      x = "Attention lag, L (months)",
      y = "Estimated near × attention coefficient (95% CI)",
      caption = paste(
        "Models include property controls and LSOA-clustered standard errors.",
        "\nPost models use Jan 2021-Dec 2023; article models use the common",
        "Jan 2022-Dec 2023 sample."
      )
    ) +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      strip.placement = "outside",
      strip.text = ggplot2::element_text(face = "bold", lineheight = 1.1),
      strip.text.y.left = ggplot2::element_text(angle = 0, hjust = 1),
      plot.caption = ggplot2::element_text(hjust = 0, lineheight = 1.05),
      plot.caption.position = "plot",
      plot.title.position = "plot"
    )

  atomic_save_plot(plot, pdf_path, width = 10.5, height = 6.6)
  atomic_save_plot(plot, png_path, width = 10.5, height = 6.6, dpi = 320)
  invisible(plot)
}

create_intensive_coefficient_figure <- function(data, pdf_path, png_path) {
  plot_data <- intensive_figure_data(data)
  stopifnot(nrow(plot_data) == 24L)

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
    ggplot2::facet_wrap(
      ggplot2::vars(.data$interaction_label), nrow = 1L, scales = "free_y"
    ) +
    ggplot2::scale_x_continuous(breaks = allowed_lags) +
    ggplot2::scale_colour_manual(values = c(
      "250m radius" = "#4477aa", "500m radius" = "#228833",
      "1000m radius" = "#aa3377"
    )) +
    ggplot2::labs(
      title = paste(
        "Does earlier public attention change the spill-intensity price gradient?"
      ),
      subtitle = paste(
        "Outcome: log transaction price. Exposure: weekly average spill count",
        "within the indicated radius. Each point is a separate regression."
      ),
      x = "Attention lag, L (months)",
      y = "Estimated spill exposure × attention coefficient (95% CI)",
      colour = "Exposure radius",
      caption = paste(
        "Models include property controls, LSOA and month fixed effects, and",
        "LSOA-clustered standard errors.\nPost models use Jan 2021-Dec 2023;",
        "article models use the common Jan 2022-Dec 2023 sample."
      )
    ) +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(
      legend.position = "bottom",
      panel.grid.minor = ggplot2::element_blank(),
      strip.text = ggplot2::element_text(face = "bold", lineheight = 1.1),
      plot.caption = ggplot2::element_text(hjust = 0, lineheight = 1.05),
      plot.caption.position = "plot",
      plot.title.position = "plot"
    )

  atomic_save_plot(plot, pdf_path, width = 10.5, height = 5.5)
  atomic_save_plot(plot, png_path, width = 10.5, height = 5.5, dpi = 320)
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
  ),
  extension_figure_pdf_path = here::here(
    "output", "figures", "did_news_lagged_sales_intensive_extension.pdf"
  ),
  extension_figure_png_path = here::here(
    "output", "figures", "did_news_lagged_sales_intensive_extension.png"
  )
) {
  for (package in c("here", "ggplot2")) {
    if (!requireNamespace(package, quietly = TRUE)) {
      stop("Package required by summary script is not installed: ", package,
           call. = FALSE)
    }
  }
  run_consolidation_sanity_checks()
  results <- consolidate_components(component_paths)
  atomic_write_csv(results, consolidated_path)
  create_core_coefficient_figure(
    results, pdf_path = figure_pdf_path, png_path = figure_png_path
  )
  create_intensive_coefficient_figure(
    results, pdf_path = extension_figure_pdf_path,
    png_path = extension_figure_png_path
  )
  stopifnot(
    file.exists(figure_pdf_path),
    file.info(figure_pdf_path)$size > 0L,
    file.exists(figure_png_path),
    file.info(figure_png_path)$size > 0L,
    file.exists(extension_figure_pdf_path),
    file.info(extension_figure_pdf_path)$size > 0L,
    file.exists(extension_figure_png_path),
    file.info(extension_figure_png_path)$size > 0L
  )
  cat("Validated and consolidated ", nrow(results), " result rows.\n", sep = "")
  cat("Consolidated CSV: ", consolidated_path, "\n", sep = "")
  cat("Core coefficient paths: ", figure_pdf_path, " and ", figure_png_path,
      "\n", sep = "")
  cat(
    "Intensive extension paths: ", extension_figure_pdf_path, " and ",
    extension_figure_png_path, "\n", sep = ""
  )
  invisible(results)
}

if (sys.nframe() == 0L) main()
