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
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/cross_section/sales/all_years/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/all_years/ - Cross-sectional rentals
#
# Outputs:
#   - output/figures/sales_{variable}_{method}.pdf - Sales bivariate plots
#   - output/figures/rental_{variable}_{method}.pdf - Rental bivariate plots
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

# Smoothing method options:
#   c("lm", "loess") - both linear and local regression (default)
#   "lm"            - linear regression only
#   "loess"         - local regression only
SMOOTHING_METHODS <- c("lm")

# ==============================================================================
# 2. Package Management
# ==============================================================================
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
  "data.table",
  "scales",
  "showtext"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)

# ==============================================================================
# 3. Setup
# ==============================================================================

# 3.1 Font Setup ---------------------------------------------------------------
showtext::showtext_auto()
showtext::showtext_opts(dpi = 300)
sysfonts::font_add_google("Libertinus Serif", "libertinus", db_cache = FALSE)

# 3.2 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.3 ggplot Theme -------------------------------------------------------------
theme_pref <- theme_minimal() +
  theme(
    text = element_text(size = 10, family = "Libertinus Serif"),
    plot.title = element_text(
      face = "bold",
      size = 12,
      family = "Libertinus Serif",
      margin = ggplot2::margin(b = 9, unit = "pt")
    ),
    axis.title = element_text(face = "bold", size = 12, family = "Libertinus Serif"),
    axis.text = element_text(size = 10, family = "Libertinus Serif"),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "bottom",
    legend.title = element_text(face = "bold", size = 12, family = "Libertinus Serif"),
    legend.text = element_text(size = 10, family = "Libertinus Serif"),
    legend.background = element_rect(fill = "white", color = NA),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )

# 3.4 Plotting Function --------------------------------------------------------
create_cs_plot <- function(
  plot_data,
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
    scale_color_viridis_d(option = "magma", end = 0.8) +
    scale_y_continuous(labels = scales::comma) +
    labs(
      x = x_label,
      y = y_label,
      color = "Radius"
    ) +
    theme_pref +
    theme(legend.position = "bottom")

  # Add smoothing lines
  if (length(method) == 1) {
    p <- p + geom_smooth(method = method, se = TRUE, linewidth = 0.8)
  } else {
    p <- p +
      geom_smooth(method = method[1], se = TRUE, linewidth = 0.8) +
      geom_smooth(method = method[2], se = FALSE, linewidth = 0.8, linetype = 5)
  }

  return(p)
}

# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================

# 4.1 Process Sales Data -------------------------------------------------------
cat("Processing sales data...\n")

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
dat_agg_sales <- open_dataset(
  here::here("data", "processed", "cross_section", "sales", "all_years")
) %>%
  collect()

# Filter to trimmed house IDs
dat_agg_sales_trimmed <- dat_agg_sales %>%
  filter(house_id %in% trimmed_house_ids)

# Sample houses (or use all if SAMPLE_SIZE is NULL)
if (is.null(SAMPLE_SIZE)) {
  sample_houses <- unique(dat_agg_sales_trimmed$house_id)
} else {
  set.seed(123)
  sample_houses <- unique(dat_agg_sales_trimmed$house_id) %>%
    sample(size = SAMPLE_SIZE)
}

# Filter and prepare data
plot_data_sales <- dat_agg_sales %>%
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

# 4.2 Process Rentals Data -----------------------------------------------------
cat("Processing rentals data...\n")

# Zoopla rental data
zoopla_rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
)

# Trim to 5th-95th percentiles
zoopla_rentals_trimmed <- zoopla_rentals %>%
  filter(between(
    listing_price,
    quantile(listing_price, 0.05, na.rm = TRUE),
    quantile(listing_price, 0.95, na.rm = TRUE)
  ))
trimmed_rental_ids <- unique(zoopla_rentals_trimmed$rental_id)

# Cross-sectional aggregated data (all years)
dat_agg_rentals <- open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "all_years")
) %>%
  collect()

# Filter to trimmed rental IDs
dat_agg_rentals_trimmed <- dat_agg_rentals %>%
  filter(rental_id %in% trimmed_rental_ids)

# Sample rentals (or use all if SAMPLE_SIZE is NULL)
if (is.null(SAMPLE_SIZE)) {
  sample_rentals <- unique(dat_agg_rentals_trimmed$rental_id)
} else {
  set.seed(123)
  sample_rentals <- unique(dat_agg_rentals_trimmed$rental_id) %>%
    sample(size = SAMPLE_SIZE)
}

# Filter and prepare data
plot_data_rentals <- dat_agg_rentals %>%
  filter(rental_id %in% sample_rentals) %>%
  filter(radius %in% RADII_TO_INCLUDE) %>%
  mutate(
    radius = factor(
      radius,
      levels = RADII_TO_INCLUDE,
      labels = paste0(RADII_TO_INCLUDE, "m")
    ),
    listing_price = rent,
    inverse_spill_count = spill_count / min_distance,
    inverse_spill_hrs = spill_hrs / min_distance,
    log_price = log(listing_price)
  )

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

# Determine suffix based on smoothing methods
method_suffix <- if (length(SMOOTHING_METHODS) == 2) {
  "_lm_loess"
} else if (SMOOTHING_METHODS[1] == "lm") {
  "_lm"
} else {
  "_loess"
}

# Generate Sales Plots
cat("Generating sales plots...\n")
for (var_name in names(plot_specs)) {
  spec <- plot_specs[[var_name]]

  # Create plot
  p <- create_cs_plot(
    plot_data_sales,
    spec$var,
    spec$label,
    "price",
    "Sale Price (£)",
    method = SMOOTHING_METHODS
  )

  # Save plot
  file_name <- paste0("sales_", var_name, method_suffix, ".pdf")
  ggsave(
    filename = here::here(output_dir, file_name),
    plot = p,
    width = PLOT_WIDTH,
    height = PLOT_HEIGHT,
    dpi = PLOT_DPI,
    units = "cm"
  )
  cat("  Saved:", file_name, "\n")
}

# Generate Rental Plots
cat("Generating rental plots...\n")
for (var_name in names(plot_specs)) {
  spec <- plot_specs[[var_name]]

  # Create plot
  p <- create_cs_plot(
    plot_data_rentals,
    spec$var,
    spec$label,
    "listing_price",
    "Rent Listing Price (£/month)",
    method = SMOOTHING_METHODS
  )

  # Save plot
  file_name <- paste0("rental_", var_name, method_suffix, ".pdf")
  ggsave(
    filename = here::here(output_dir, file_name),
    plot = p,
    width = PLOT_WIDTH,
    height = PLOT_HEIGHT,
    dpi = PLOT_DPI,
    units = "cm"
  )
  cat("  Saved:", file_name, "\n")
}

cat("\nAll plots generated\n")
