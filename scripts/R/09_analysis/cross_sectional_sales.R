# ==============================================================================
# Cross-Sectional Bivariate Plots
# ==============================================================================
# Purpose: Generate bivariate plots showing relationships between house prices
#          and sewage spill metrics (count, duration, distance) at different
#          distance thresholds (250m, 500m, 1000m)
#
# Author: Jacopo Olivieri
# Date: 2025-11-21
#
# Outputs: 4 PDF plots saved to output/figures/
#          - 4 variables (distance, spill_count, spill_duration, inverse_spill_count)
#          - Uses entire time period data only
# ==============================================================================

# Configuration ----------------------------------------------------------------
# Sample size options:
#   Numeric value (e.g., 300000) - sample specified number of houses
#   NULL                         - use entire dataset (no sampling)
SAMPLE_SIZE <- NULL
RADII_TO_INCLUDE <- c(250, 500, 1000)
PLOT_WIDTH <- 1.618 * 8
PLOT_HEIGHT <- 8
PLOT_DPI <- 300

# Smoothing method options:
#   c("lm", "loess") - both linear and local regression (default)
#   "lm"            - linear regression only
#   "loess"         - local regression only
SMOOTHING_METHODS <- c("lm")

# Package Management -----------------------------------------------------------
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "purrr",
  "here",
  "viridis",
  "data.table"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)

# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# ggplot Theme -----------------------------------------------------------------
theme_pref <- theme_minimal() +
  theme(
    text = element_text(size = 12),
    plot.title = element_text(
      face = "bold",
      size = 12,
      margin = ggplot2::margin(b = 10, unit = "pt")
    ),
    axis.title = element_text(face = "bold", size = 10),
    axis.text = element_text(angle = 45, hjust = 1, vjust = 1, size = 8),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    legend.position = "bottom",
    legend.title = element_text(face = "bold", size = 10),
    legend.text = element_text(size = 8),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )

# Data Loading -----------------------------------------------------------------

# House price data
house_price_data <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
)

# Trim to 5th-95th percentiles
house_price_data_trimmed <- house_price_data %>%
  filter(between(
    price,
    quantile(price, 0.05, na.rm = TRUE),
    quantile(price, 0.95, na.rm = TRUE)
  ))
trimmed_house_ids <- unique(house_price_data_trimmed$house_id)

# Cross-sectional aggregated data (all years)
dat_agg_all <- open_dataset(
  here::here("data", "processed", "cross_section", "sales", "all_years")
) %>%
  collect()

# Data Preparation -------------------------------------------------------------

# Filter to trimmed house IDs
dat_agg_all_trimmed <- dat_agg_all %>%
  filter(house_id %in% trimmed_house_ids)

# Sample houses (or use all if SAMPLE_SIZE is NULL)
if (is.null(SAMPLE_SIZE)) {
  # Use entire dataset
  sample_houses <- unique(dat_agg_all_trimmed$house_id)
} else {
  # Sample specified number of houses
  set.seed(123)
  sample_houses <- unique(dat_agg_all_trimmed$house_id) %>%
    sample(size = SAMPLE_SIZE)
}

# Filter and prepare data
plot_data <- dat_agg_all %>%
  filter(house_id %in% sample_houses) %>%
  filter(radius %in% RADII_TO_INCLUDE) %>%
  mutate(
    radius = factor(
      radius,
      levels = RADII_TO_INCLUDE,
      labels = paste0(RADII_TO_INCLUDE, "m")
    ),
    inverse_spill_count = spill_count / min_distance,
    inverse_spill_hrs = spill_hrs / min_distance,
    log_price = log(price)
  )

# Plotting Function ------------------------------------------------------------
create_cs_plot <- function(
  x_var,
  x_label,
  y_var,
  y_label,
  method = c("lm", "loess")
) {
  # Base plot
  p <- ggplot(
    plot_data,
    aes(y = .data[[y_var]], x = .data[[x_var]], color = radius)
  ) +
    scale_color_viridis_d(option = "plasma", end = 0.8) +
    labs(
      x = x_label,
      y = y_label,
      color = "Radius"
    ) +
    theme_pref +
    theme(legend.position = "bottom")

  # Add smoothing lines
  if (length(method) == 1) {
    p <- p + geom_smooth(method = method, se = TRUE, linewidth = 0.5)
  } else {
    p <- p +
      geom_smooth(method = method[1], se = TRUE, linewidth = 0.5) +
      geom_smooth(method = method[2], se = FALSE, linewidth = 0.5, linetype = 5)
  }

  return(p)
}

# Generate and Save Plots ------------------------------------------------------

# Plot specifications
plot_specs <- list(
  distance = list(
    var = "min_distance",
    label = "Minimum Distance to Spill Site"
  ),
  spill_count = list(
    var = "spill_count",
    label = "Spill Count"
  ),
  spill_duration = list(
    var = "spill_hrs",
    label = "Spill Duration"
  ),
  inverse_spill_count = list(
    var = "inverse_spill_count",
    label = "Inverse Spill Count - Distance"
  )
)

# Generate and save plots
for (var_name in names(plot_specs)) {
  spec <- plot_specs[[var_name]]

  # Create plot
  p <- create_cs_plot(
    spec$var,
    spec$label,
    "price",
    "House Price",
    method = SMOOTHING_METHODS
  )

  # Save plot
  file_name <- paste0(var_name, ".pdf")
  ggsave(
    filename = here::here(output_dir, file_name),
    plot = p,
    width = PLOT_WIDTH,
    height = PLOT_HEIGHT,
    dpi = PLOT_DPI,
    units = "cm"
  )
}
