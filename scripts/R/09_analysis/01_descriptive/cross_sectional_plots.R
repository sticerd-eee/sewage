# ==============================================================================
# Cross-Sectional Bivariate Plots - Sales and Rentals Combined
# ==============================================================================
#
# Purpose: Generate bivariate plots showing relationships between prices
#          and sewage spill metrics (count, duration, distance) at different
#          distance thresholds (250m, 500m, 1000m) for both sales and rentals.
#
# Author: Jacopo Olivieri
# Date: 2025-11-22
#
# Inputs:
#   - data/processed/cross_section/sales/study_period/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/study_period/ - Cross-sectional rentals
#
# Outputs:
#   - output/figures/sales_{variable}_{method}.pdf - Sales bivariate plots
#   - output/figures/rental_{variable}_{method}.pdf - Rental bivariate plots
#   - output/figures/sales_{variable}_{method}_slides.pdf - Slide-optimized plots
#   - output/figures/rental_{variable}_{method}_slides.pdf - Slide-optimized plots
#   - output/figures/{sales,rental}_{variable}_{method}_nolegend.pdf
#   - output/figures/{sales,rental}_{variable}_{method}_slides_nolegend.pdf
#   - output/figures/cross_section_radius_legend{_slides}.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
# Sample size options:
#   Numeric value (e.g., 300000) - sample specified number of properties
#   NULL                         - use entire dataset (no sampling)
SAMPLE_SIZE <- NULL
RADII_TO_INCLUDE <- c(250, 500, 1000)
PLOT_WIDTH <- 16
PLOT_HEIGHT <- 12
PLOT_DPI <- 300
SLIDE_PLOT_WIDTH <- 6.8
SLIDE_PLOT_HEIGHT <- 2.5
SLIDE_PLOT_DPI <- 300

# Smoothing method options:
#   c("lm", "loess") - both linear and local regression (default)
#   "lm"            - linear regression only
#   "loess"         - local regression only
SMOOTHING_METHODS <- c("lm")

# ==============================================================================
# 2. Package Management
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required. Install project dependencies with `rv sync`.",
    call. = FALSE
  )
}
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "tidyverse",
  "purrr",
  "here",
  "viridis",
  "data.table",
  "scales",
  "showtext",
  "sysfonts"
)

# ==============================================================================
# 3. Setup
# ==============================================================================

# 3.1 Font Setup ---------------------------------------------------------------
add_libertinus_font <- function() {
  local_font_files <- c(
    regular = path.expand("~/Library/Fonts/LibertinusSerif-Regular.ttf"),
    bold = path.expand("~/Library/Fonts/LibertinusSerif-Bold.ttf"),
    italic = path.expand("~/Library/Fonts/LibertinusSerif-Italic.ttf"),
    bolditalic = path.expand("~/Library/Fonts/LibertinusSerif-BoldItalic.ttf")
  )

  if (all(file.exists(local_font_files))) {
    do.call(
      sysfonts::font_add,
      c(list(family = "libertinus"), as.list(local_font_files))
    )
    return("libertinus")
  }

  tryCatch(
    {
      sysfonts::font_add_google("Libertinus Serif", "libertinus", db_cache = TRUE)
      "libertinus"
    },
    error = function(e) {
      warning(
        "Libertinus Serif font unavailable; falling back to the default serif font."
      )
      "serif"
    }
  )
}

FONT_FAMILY <- "serif"

# 3.2 Output Directory ---------------------------------------------------------
OUTPUT_DIR <- here::here("output", "figures")

# 3.3 Publication-Quality Plot Theme ------------------------------------------
plot_variant_style <- function(variant = c("paper", "slides")) {
  variant <- match.arg(variant)

  if (variant == "slides") {
    return(list(
      base_size = 7.5,
      plot_title_size = 8.5,
      axis_title_size = 8,
      axis_text_size = 7.2,
      legend_title_size = 7.6,
      legend_text_size = 7.2,
      legend_key_width = 0.30,
      legend_key_height = 0.13,
      smooth_linewidth = 0.5,
      plot_margin = margin(t = 2, r = 6, b = 1, l = 8, unit = "pt")
    ))
  }

  list(
    base_size = 10,
    plot_title_size = 12,
    axis_title_size = 12,
    axis_text_size = 10,
    legend_title_size = 12,
    legend_text_size = 10,
    legend_key_width = 0.8,
    legend_key_height = 0.3,
    smooth_linewidth = 0.8,
    plot_margin = margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )
}

theme_cs_publication <- function(variant = c("paper", "slides")) {
  style <- plot_variant_style(variant)

  theme_minimal(base_family = FONT_FAMILY, base_size = style$base_size) +
    theme(
      text = element_text(family = FONT_FAMILY),
      plot.title = element_text(
        face = "bold",
        size = style$plot_title_size,
        margin = ggplot2::margin(b = 9, unit = "pt")
      ),
      axis.title = element_text(face = "bold", size = style$axis_title_size),
      axis.text = element_text(size = style$axis_text_size),
      panel.grid.minor = element_blank(),
      panel.grid.major.x = element_line(color = "gray95"),
      panel.grid.major.y = element_line(color = "gray95"),
      panel.background = element_rect(fill = "white", color = NA),
      plot.background = element_rect(fill = "white", color = NA),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.title = element_text(face = "bold", size = style$legend_title_size),
      legend.text = element_text(size = style$legend_text_size),
      legend.key.width = unit(style$legend_key_width, "cm"),
      legend.key.height = unit(style$legend_key_height, "cm"),
      legend.background = element_rect(fill = "white", color = NA),
      plot.margin = style$plot_margin
    )
}

slide_axis_label <- function(label) {
  replacements <- c(
    "Minimum Distance to Spill Site (m)" = "Distance to spill site (m)",
    "Spill Count (count)" = "Spill count",
    "Spill Duration (hours)" = "Spill duration (hours)",
    "Inverse Spill Count - Distance (count/m)" = "Spill count / distance",
    "Sale Price (£)" = "Sale (£)",
    "Rent Listing Price (£/month)" = "Rent (£)"
  )

  if (label %in% names(replacements)) {
    return(unname(replacements[[label]]))
  }

  label
}

# 3.4 Plotting Function --------------------------------------------------------
create_cs_plot <- function(
  plot_data,
  x_var,
  x_label,
  y_var,
  y_label,
  method = c("lm", "loess"),
  variant = c("paper", "slides"),
  show_legend = TRUE
) {
  variant <- match.arg(variant)
  style <- plot_variant_style(variant)

  if (variant == "slides") {
    x_label <- slide_axis_label(x_label)
    y_label <- slide_axis_label(y_label)
  }

  # Base plot
  p <- ggplot(
    plot_data,
    aes(y = .data[[y_var]], x = .data[[x_var]], color = radius)
  ) +
    scale_color_viridis_d(option = "magma", end = 0.8) +
    scale_y_continuous(labels = scales::comma) +
    labs(
      x = x_label,
      y = y_label,
      color = "Radius"
    ) +
    theme_cs_publication(variant)

  # Add smoothing lines
  if (length(method) == 1) {
    p <- p +
      geom_smooth(
        method = method,
        se = TRUE,
        linewidth = style$smooth_linewidth
      )
  } else {
    p <- p +
      geom_smooth(
        method = method[1],
        se = TRUE,
        linewidth = style$smooth_linewidth
      ) +
      geom_smooth(
        method = method[2],
        se = FALSE,
        linewidth = style$smooth_linewidth,
        linetype = 5
      )
  }

  if (!show_legend) {
    p <- p + theme(legend.position = "none")
  }

  return(p)
}

# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================

prepare_cross_section_sales <- function(
    data, radii = RADII_TO_INCLUDE, sample_size = SAMPLE_SIZE) {
  required <- c(
    "house_id", "price", "ppd_category", "spill_count", "spill_hrs",
    "min_distance", "spatially_eligible", "has_missing_site", "radius"
  )
  missing <- setdiff(required, names(data))
  if (length(missing) > 0L) {
    stop("Sales cross-section is missing: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  source_values <- data |>
    dplyr::distinct(.data$house_id, .data$price, .data$ppd_category)
  price_bounds <- stats::quantile(
    source_values$price, probs = c(0.05, 0.95), na.rm = TRUE
  )
  eligible_ids <- source_values |>
    dplyr::filter(dplyr::between(
      .data$price, price_bounds[[1L]], price_bounds[[2L]]
    )) |>
    dplyr::pull(.data$house_id)
  if (!is.null(sample_size)) {
    set.seed(123)
    eligible_ids <- sample(eligible_ids, size = sample_size)
  }
  data |>
    dplyr::filter(.data$house_id %in% eligible_ids, .data$radius %in% radii) |>
    dplyr::mutate(
      radius = factor(.data$radius, levels = radii, labels = paste0(radii, "m")),
      inverse_spill_count = .data$spill_count / .data$min_distance,
      inverse_spill_hrs = .data$spill_hrs / .data$min_distance,
      log_price = log(.data$price)
    )
}

prepare_cross_section_rentals <- function(
    data, radii = RADII_TO_INCLUDE, sample_size = SAMPLE_SIZE) {
  required <- c(
    "rental_id", "listing_price", "spill_count", "spill_hrs",
    "min_distance", "spatially_eligible", "has_missing_site", "radius"
  )
  missing <- setdiff(required, names(data))
  if (length(missing) > 0L) {
    stop("Rental cross-section is missing: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  source_values <- data |>
    dplyr::distinct(.data$rental_id, .data$listing_price)
  price_bounds <- stats::quantile(
    source_values$listing_price, probs = c(0.05, 0.95), na.rm = TRUE
  )
  eligible_ids <- source_values |>
    dplyr::filter(dplyr::between(
      .data$listing_price, price_bounds[[1L]], price_bounds[[2L]]
    )) |>
    dplyr::pull(.data$rental_id)
  if (!is.null(sample_size)) {
    set.seed(123)
    eligible_ids <- sample(eligible_ids, size = sample_size)
  }
  data |>
    dplyr::filter(.data$rental_id %in% eligible_ids, .data$radius %in% radii) |>
    dplyr::mutate(
      radius = factor(.data$radius, levels = radii, labels = paste0(radii, "m")),
      inverse_spill_count = .data$spill_count / .data$min_distance,
      inverse_spill_hrs = .data$spill_hrs / .data$min_distance,
      log_price = log(.data$listing_price)
    )
}

# ==============================================================================
# 5. Generate and Save Plots
# ==============================================================================

# Plot specifications
plot_specs <- list(
  distance = list(
    var = "min_distance",
    label = "Minimum Distance to Spill Site (m)"
  ),
  spill_count = list(
    var = "spill_count",
    label = "Spill Count (count)"
  ),
  spill_duration = list(
    var = "spill_hrs",
    label = "Spill Duration (hours)"
  ),
  inverse_spill_count = list(
    var = "inverse_spill_count",
    label = "Inverse Spill Count - Distance (count/m)"
  )
)

shared_legend_panel_vars <- c("distance", "spill_count")

# Determine suffix based on smoothing methods
method_suffix <- if (length(SMOOTHING_METHODS) == 2) {
  "_lm_loess"
} else if (SMOOTHING_METHODS[1] == "lm") {
  "_lm"
} else {
  "_loess"
}

save_plot_pdf <- function(plot, file_name, width, height, dpi = PLOT_DPI) {
  ggsave(
    filename = file.path(OUTPUT_DIR, file_name),
    plot = plot,
    width = width,
    height = height,
    dpi = dpi,
    units = "cm",
    device = cairo_pdf
  )
  cat("  Saved:", file_name, "\n")
}

no_legend_file_name <- function(file_name) {
  sub("\\.pdf$", "_nolegend.pdf", file_name)
}

radius_legend_colors <- function() {
  colors <- viridisLite::viridis(
    length(RADII_TO_INCLUDE),
    option = "magma",
    end = 0.8
  )
  stats::setNames(colors, paste0(RADII_TO_INCLUDE, "m"))
}

radius_legend_style <- function(variant = c("paper", "slides")) {
  variant <- match.arg(variant)

  if (variant == "slides") {
    return(list(
      title_size = 8.3,
      text_size = 7.7,
      line_width = 1.8,
      key_width = 0.5,
      key_height = 0.08,
      key_inset = 0.04,
      title_width = 1.2,
      label_gap = 0.08,
      label_widths = c(0.52, 0.52, 0.72),
      item_gap = 0.2,
      x_start = 0.15
    ))
  }

  list(
    title_size = 12,
    text_size = 10.5,
    line_width = 2.4,
    key_width = 0.8,
    key_height = 0.12,
    key_inset = 0.06,
    title_width = 1.9,
    label_gap = 0.12,
    label_widths = c(0.72, 0.72, 0.98),
    item_gap = 0.35,
    x_start = 0.25
  )
}

save_radius_legend_pdf <- function(file_name, variant, width, height) {
  variant <- match.arg(variant, c("paper", "slides"))
  legend_style <- radius_legend_style(variant)
  legend_colors <- radius_legend_colors()
  legend_labels <- names(legend_colors)
  output_path <- file.path(OUTPUT_DIR, file_name)

  cairo_pdf(
    filename = output_path,
    width = width / 2.54,
    height = height / 2.54
  )
  on.exit(dev.off(), add = TRUE)

  grid::grid.newpage()
  grid::pushViewport(grid::viewport(
    xscale = c(0, width),
    yscale = c(0, height),
    clip = "off"
  ))

  y_center <- height / 2
  x_pos <- legend_style$x_start

  grid::grid.text(
    "Radius",
    x = grid::unit(x_pos, "native"),
    y = grid::unit(y_center, "native"),
    just = "left",
    gp = grid::gpar(
      fontfamily = FONT_FAMILY,
      fontface = "bold",
      fontsize = legend_style$title_size
    )
  )

  x_pos <- x_pos + legend_style$title_width

  for (i in seq_along(legend_labels)) {
    grid::grid.rect(
      x = grid::unit(x_pos + legend_style$key_width / 2, "native"),
      y = grid::unit(y_center, "native"),
      width = grid::unit(legend_style$key_width, "native"),
      height = grid::unit(legend_style$key_height, "native"),
      gp = grid::gpar(fill = scales::alpha("grey80", 0.6), col = NA)
    )
    grid::grid.segments(
      x0 = grid::unit(x_pos + legend_style$key_inset, "native"),
      x1 = grid::unit(
        x_pos + legend_style$key_width - legend_style$key_inset,
        "native"
      ),
      y0 = grid::unit(y_center, "native"),
      y1 = grid::unit(y_center, "native"),
      gp = grid::gpar(
        col = unname(legend_colors[[i]]),
        lwd = legend_style$line_width,
        lineend = "round"
      )
    )

    x_pos <- x_pos + legend_style$key_width + legend_style$label_gap

    grid::grid.text(
      legend_labels[[i]],
      x = grid::unit(x_pos, "native"),
      y = grid::unit(y_center, "native"),
      just = "left",
      gp = grid::gpar(
        fontfamily = FONT_FAMILY,
        fontsize = legend_style$text_size
      )
    )

    x_pos <- x_pos + legend_style$label_widths[[i]] + legend_style$item_gap
  }

  grid::popViewport()
  cat("  Saved:", file_name, "\n")
}

# Generate Sales Plots
main <- function() {
  check_required_packages(REQUIRED_PACKAGES)
  invisible(lapply(REQUIRED_PACKAGES, library, character.only = TRUE))
  showtext::showtext_auto()
  showtext::showtext_opts(dpi = PLOT_DPI)
  FONT_FAMILY <<- add_libertinus_font()
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

  cat("Processing sales data...\n")
  plot_data_sales <- arrow::open_dataset(here::here(
    "data", "processed", "cross_section", "sales", "study_period"
  )) |>
    dplyr::collect() |>
    prepare_cross_section_sales()

  cat("Processing rentals data...\n")
  plot_data_rentals <- arrow::open_dataset(here::here(
    "data", "processed", "cross_section", "rentals", "study_period"
  )) |>
    dplyr::collect() |>
    prepare_cross_section_rentals()

  cat("Generating sales plots...\n")
  for (var_name in names(plot_specs)) {
  spec <- plot_specs[[var_name]]

  # Create paper plot
  p <- create_cs_plot(
    plot_data_sales,
    spec$var,
    spec$label,
    "price",
    "Sale Price (£)",
    method = SMOOTHING_METHODS,
    variant = "paper"
  )

  # Save paper plot
  file_name <- paste0("sales_", var_name, method_suffix, ".pdf")
  save_plot_pdf(
    p,
    file_name,
    width = PLOT_WIDTH,
    height = PLOT_HEIGHT
  )

  if (var_name %in% shared_legend_panel_vars) {
    p_no_legend <- create_cs_plot(
      plot_data_sales,
      spec$var,
      spec$label,
      "price",
      "Sale Price (£)",
      method = SMOOTHING_METHODS,
      variant = "paper",
      show_legend = FALSE
    )

    save_plot_pdf(
      p_no_legend,
      no_legend_file_name(file_name),
      width = PLOT_WIDTH,
      height = PLOT_HEIGHT
    )
  }

  # Create and save slide plot
  p_slides <- create_cs_plot(
    plot_data_sales,
    spec$var,
    spec$label,
    "price",
    "Sale Price (£)",
    method = SMOOTHING_METHODS,
    variant = "slides"
  )

  file_name_slides <- paste0("sales_", var_name, method_suffix, "_slides.pdf")
  save_plot_pdf(
    p_slides,
    file_name_slides,
    width = SLIDE_PLOT_WIDTH,
    height = SLIDE_PLOT_HEIGHT,
    dpi = SLIDE_PLOT_DPI
  )

  if (var_name %in% shared_legend_panel_vars) {
    p_slides_no_legend <- create_cs_plot(
      plot_data_sales,
      spec$var,
      spec$label,
      "price",
      "Sale Price (£)",
      method = SMOOTHING_METHODS,
      variant = "slides",
      show_legend = FALSE
    )

    save_plot_pdf(
      p_slides_no_legend,
      no_legend_file_name(file_name_slides),
      width = SLIDE_PLOT_WIDTH,
      height = SLIDE_PLOT_HEIGHT,
      dpi = SLIDE_PLOT_DPI
    )
  }
  }

# Generate Rental Plots
  cat("Generating rental plots...\n")
  for (var_name in names(plot_specs)) {
  spec <- plot_specs[[var_name]]

  # Create paper plot
  p <- create_cs_plot(
    plot_data_rentals,
    spec$var,
    spec$label,
    "listing_price",
    "Rent Listing Price (£/month)",
    method = SMOOTHING_METHODS,
    variant = "paper"
  )

  # Save paper plot
  file_name <- paste0("rental_", var_name, method_suffix, ".pdf")
  save_plot_pdf(
    p,
    file_name,
    width = PLOT_WIDTH,
    height = PLOT_HEIGHT
  )

  if (var_name %in% shared_legend_panel_vars) {
    p_no_legend <- create_cs_plot(
      plot_data_rentals,
      spec$var,
      spec$label,
      "listing_price",
      "Rent Listing Price (£/month)",
      method = SMOOTHING_METHODS,
      variant = "paper",
      show_legend = FALSE
    )

    save_plot_pdf(
      p_no_legend,
      no_legend_file_name(file_name),
      width = PLOT_WIDTH,
      height = PLOT_HEIGHT
    )
  }

  # Create and save slide plot
  p_slides <- create_cs_plot(
    plot_data_rentals,
    spec$var,
    spec$label,
    "listing_price",
    "Rent Listing Price (£/month)",
    method = SMOOTHING_METHODS,
    variant = "slides"
  )

  file_name_slides <- paste0("rental_", var_name, method_suffix, "_slides.pdf")
  save_plot_pdf(
    p_slides,
    file_name_slides,
    width = SLIDE_PLOT_WIDTH,
    height = SLIDE_PLOT_HEIGHT,
    dpi = SLIDE_PLOT_DPI
  )

  if (var_name %in% shared_legend_panel_vars) {
    p_slides_no_legend <- create_cs_plot(
      plot_data_rentals,
      spec$var,
      spec$label,
      "listing_price",
      "Rent Listing Price (£/month)",
      method = SMOOTHING_METHODS,
      variant = "slides",
      show_legend = FALSE
    )

    save_plot_pdf(
      p_slides_no_legend,
      no_legend_file_name(file_name_slides),
      width = SLIDE_PLOT_WIDTH,
      height = SLIDE_PLOT_HEIGHT,
      dpi = SLIDE_PLOT_DPI
    )
  }
  }

  save_radius_legend_pdf(
    "cross_section_radius_legend.pdf",
    variant = "paper",
    width = 9.0,
    height = 0.8
  )

  save_radius_legend_pdf(
    "cross_section_radius_legend_slides.pdf",
    variant = "slides",
    width = 6.0,
    height = 0.45
  )

  cat("\nAll plots generated\n")
  invisible(TRUE)
}

if (sys.nframe() == 0L) main()
