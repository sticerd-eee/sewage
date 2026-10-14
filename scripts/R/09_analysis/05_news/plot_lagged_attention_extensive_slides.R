# ==============================================================================
# Lagged Public-Attention Results — Extensive-Margin Slide Figure
# ==============================================================================
#
# Purpose: Create the appendix-slide figure showing how extensive-margin
#          near-far estimates vary with the assumed response lag.
#
# Input:
#   - output/tables/did_news_lagged_sales_effect_sizes.csv
#
# Output:
#   - output/figures/lagged_attention_extensive_response_slides.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================

INPUT_PATH <- here::here(
  "output", "tables", "did_news_lagged_sales_effect_sizes.csv"
)
OUTPUT_PATH <- here::here(
  "output", "figures", "lagged_attention_extensive_response_slides.pdf"
)

EXPECTED_LAGS <- c(0L, 3L, 6L, 12L)
EXPECTED_MARKETS <- c("sales", "rentals")
EXPECTED_MEASURES <- c("post", "articles")
MARKET_COLOURS <- c(sales = "#B63679FF", rentals = "#21908CFF")
FONT_FAMILY <- "libertinus"

# The deck includes this figure at 0.80\linewidth (about 11.7 cm at
# aspectratio=169 with 7mm margins). Export at that size so the text is shown
# 1:1 rather than shrunk by LaTeX.
FIGURE_WIDTH_IN <- 4.6
FIGURE_HEIGHT_IN <- 2.4


# ==============================================================================
# 2. Setup
# ==============================================================================

required_packages <- c(
  "dplyr", "ggplot2", "here", "showtext", "sysfonts"
)

missing_packages <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]

if (length(missing_packages) > 0L) {
  stop(
    "Install required packages before running this script: ",
    paste(missing_packages, collapse = ", "),
    call. = FALSE
  )
}

register_libertinus <- function() {
  font_dir <- path.expand("~/Library/Fonts")
  font_files <- c(
    regular = file.path(font_dir, "LibertinusSerif-Regular.ttf"),
    bold = file.path(font_dir, "LibertinusSerif-Bold.ttf"),
    italic = file.path(font_dir, "LibertinusSerif-Italic.ttf"),
    bolditalic = file.path(font_dir, "LibertinusSerif-BoldItalic.ttf")
  )

  if (!all(file.exists(font_files))) {
    stop(
      "Libertinus Serif font files are required in ~/Library/Fonts.",
      call. = FALSE
    )
  }

  do.call(
    sysfonts::font_add,
    c(list(family = FONT_FAMILY), as.list(font_files))
  )
  showtext::showtext_auto()
  showtext::showtext_opts(dpi = 300)
}


# ==============================================================================
# 3. Data
# ==============================================================================

load_figure_data <- function(path = INPUT_PATH) {
  if (!file.exists(path)) {
    stop("Missing consolidated lagged-attention results: ", path, call. = FALSE)
  }

  required_columns <- c(
    "margin", "market", "measure", "radius", "lag", "sample", "estimate",
    "conf_low", "conf_high"
  )
  results <- utils::read.csv(path, stringsAsFactors = FALSE)
  missing_columns <- setdiff(required_columns, names(results))

  if (length(missing_columns) > 0L) {
    stop(
      "Consolidated results are missing columns: ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  plot_data <- results |>
    dplyr::filter(
      .data$margin == "extensive",
      .data$market %in% EXPECTED_MARKETS,
      .data$measure %in% EXPECTED_MEASURES,
      .data$radius == 500,
      (.data$measure == "post" & .data$sample == "full") |
        (.data$measure == "articles" & .data$sample == "common")
    ) |>
    dplyr::mutate(
      lag = as.integer(.data$lag),
      estimate_pp = 100 * .data$estimate,
      conf_low_pp = 100 * .data$conf_low,
      conf_high_pp = 100 * .data$conf_high,
      market = factor(
        .data$market,
        levels = EXPECTED_MARKETS,
        labels = c("Sales", "Rentals")
      )
    ) |>
    dplyr::arrange(.data$measure, .data$market, .data$lag)

  expected_grid <- expand.grid(
    measure = EXPECTED_MEASURES,
    market = EXPECTED_MARKETS,
    lag = EXPECTED_LAGS,
    stringsAsFactors = FALSE
  )
  observed_grid <- plot_data |>
    dplyr::transmute(
      measure = .data$measure,
      market = tolower(as.character(.data$market)),
      lag = .data$lag
    )

  if (
    nrow(plot_data) != nrow(expected_grid) ||
      anyDuplicated(observed_grid) > 0L ||
      nrow(dplyr::anti_join(expected_grid, observed_grid, by = names(expected_grid))) > 0L ||
      nrow(dplyr::anti_join(observed_grid, expected_grid, by = names(expected_grid))) > 0L
  ) {
    stop(
      "Expected one extensive-margin estimate for every market, measure, and ",
      "response lag (16 rows total).",
      call. = FALSE
    )
  }

  if (
    any(!is.finite(plot_data$estimate_pp)) ||
      any(!is.finite(plot_data$conf_low_pp)) ||
      any(!is.finite(plot_data$conf_high_pp)) ||
      any(plot_data$conf_low_pp > plot_data$estimate_pp) ||
      any(plot_data$conf_high_pp < plot_data$estimate_pp)
  ) {
    stop("Estimates or confidence intervals are invalid.", call. = FALSE)
  }

  plot_data
}


# ==============================================================================
# 4. Figure
# ==============================================================================

theme_lagged_attention_slides <- function() {
  ggplot2::theme_minimal(base_family = FONT_FAMILY, base_size = 10) +
    ggplot2::theme(
      text = ggplot2::element_text(family = FONT_FAMILY),
      axis.title = ggplot2::element_text(face = "bold", size = 10),
      axis.text = ggplot2::element_text(size = 9),
      panel.spacing.x = grid::unit(12, "pt"),
      panel.spacing.y = grid::unit(9, "pt"),
      panel.grid.minor = ggplot2::element_blank(),
      panel.grid.major.x = ggplot2::element_line(colour = "grey95"),
      panel.grid.major.y = ggplot2::element_line(colour = "grey92"),
      panel.background = ggplot2::element_rect(fill = "white", colour = NA),
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      strip.text.x = ggplot2::element_text(
        face = "bold", size = 10, margin = ggplot2::margin(b = 3, unit = "pt")
      ),
      strip.text.y = ggplot2::element_text(face = "bold", size = 9.5),
      strip.background = ggplot2::element_blank(),
      legend.position = "none",
      plot.margin = ggplot2::margin(t = 2, r = 4, b = 1, l = 2, unit = "pt")
    )
}

MEASURE_LABELS <- c(
  post = "Post-attention indicator\nNear \u00d7 Post(t \u2212 L)",
  articles = "Cumulative media coverage\nNear \u00d7 log articles(t \u2212 L)"
)

#' Single faceted figure: measures in columns, markets in rows. Row scales are
#' free (shared across columns), so each market's zero line sits at the same
#' height in both columns while the rows fit their own estimates.
build_figure <- function(data) {
  plot_data <- data |>
    dplyr::mutate(
      measure = factor(
        .data$measure,
        levels = EXPECTED_MEASURES,
        labels = MEASURE_LABELS[EXPECTED_MEASURES]
      )
    )

  ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x = .data$lag,
      y = .data$estimate_pp,
      colour = .data$market,
      group = .data$market
    )
  ) +
    ggplot2::geom_hline(
      yintercept = 0, colour = "grey40", linewidth = 0.45
    ) +
    ggplot2::geom_line(linewidth = 0.55) +
    ggplot2::geom_errorbar(
      ggplot2::aes(ymin = .data$conf_low_pp, ymax = .data$conf_high_pp),
      width = 0.45,
      linewidth = 0.45
    ) +
    ggplot2::geom_point(size = 1.8) +
    ggplot2::facet_grid(
      rows = ggplot2::vars(.data$market),
      cols = ggplot2::vars(.data$measure),
      scales = "free_y"
    ) +
    ggplot2::scale_x_continuous(
      breaks = EXPECTED_LAGS,
      limits = range(EXPECTED_LAGS),
      expand = ggplot2::expansion(mult = c(0.06, 0.06))
    ) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = 0.1)) +
    ggplot2::scale_colour_manual(
      values = c(
        Sales = MARKET_COLOURS[["sales"]],
        Rentals = MARKET_COLOURS[["rentals"]]
      ),
      drop = FALSE
    ) +
    ggplot2::labs(x = "Response lag (months)", y = "Effect (pp)") +
    theme_lagged_attention_slides()
}

save_figure <- function(figure, path = OUTPUT_PATH) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  temporary <- tempfile(
    pattern = paste0(".", basename(path), "."),
    tmpdir = dirname(path),
    fileext = ".pdf"
  )
  on.exit(if (file.exists(temporary)) unlink(temporary), add = TRUE)

  grDevices::cairo_pdf(
    temporary, width = FIGURE_WIDTH_IN, height = FIGURE_HEIGHT_IN
  )
  print(figure)
  grDevices::dev.off()

  if (!file.rename(temporary, path)) {
    stop("Could not replace figure: ", path, call. = FALSE)
  }

  invisible(path)
}


# ==============================================================================
# 5. Run
# ==============================================================================

main <- function() {
  register_libertinus()
  figure_data <- load_figure_data()
  figure <- build_figure(figure_data)
  save_figure(figure)
  message("Saved slide figure: ", OUTPUT_PATH)
  invisible(figure)
}

if (sys.nframe() == 0L) {
  main()
}
