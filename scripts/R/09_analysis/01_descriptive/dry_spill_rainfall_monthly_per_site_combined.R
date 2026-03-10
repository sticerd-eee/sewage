# ==============================================================================
# Average Monthly Dry Spill and Rainfall Distribution per Active Spill Site
# ==============================================================================
#
# Purpose: Plot the average monthly dry-spill count per unique active spill site
#          and average monthly rainfall across years in a dual-axis line chart.
#
# Author: Jacopo Olivieri
# Date: 2026-03-08
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_dry_mo.parquet
#   - data/processed/rainfall/rainfall_agg_mo.parquet
#
# Outputs:
#   - output/figures/dry_spill_rainfall_monthly_per_site_combined.pdf
#
# Notes:
#   - Dry-spill counts use the preferred r1_d01_weak definition.
#   - Monthly values are first computed within each year, then averaged across
#     years for each calendar month.
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 18 * 1.618
PLOT_HEIGHT <- 18
PLOT_DPI <- 300
BASE_YEAR <- 2021
COLOR_DRY <- "#B63679FF"
COLOR_RAIN <- "#21908CFF"
FONT_FAMILY <- "libertinus"


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "rio",
  "tidyverse",
  "here",
  "DescTools",
  "showtext",
  "scales"
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
    text = element_text(size = 10, family = FONT_FAMILY),
    axis.title = element_text(face = "bold", size = 12, family = FONT_FAMILY),
    axis.text = element_text(size = 10, family = FONT_FAMILY),
    axis.title.y.left = element_text(color = COLOR_DRY),
    axis.text.y.left = element_text(color = COLOR_DRY),
    axis.title.y.right = element_text(color = COLOR_RAIN),
    axis.text.y.right = element_text(color = COLOR_RAIN),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 10, family = FONT_FAMILY),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )


# ==============================================================================
# 4. Helper Functions
# ==============================================================================
prepare_dry_spill_monthly_distribution <- function(data) {
  data %>%
    dplyr::filter(dry_spill_hrs_mo_r1_d01_weak >= 0) %>%
    dplyr::mutate(
      month_num = (month_id - 1) %% 12 + 1,
      year = (month_id - 1) %/% 12 + BASE_YEAR
    ) %>%
    dplyr::group_by(year) %>%
    dplyr::mutate(
      dry_spill_count_mo_r1_d01_weak = DescTools::Winsorize(
        dry_spill_count_mo_r1_d01_weak,
        val = stats::quantile(
          dry_spill_count_mo_r1_d01_weak,
          probs = c(0.01, 0.99),
          na.rm = TRUE
        )
      )
    ) %>%
    dplyr::group_by(year, month_num) %>%
    dplyr::summarise(
      dry_spill_count_mo = sum(dry_spill_count_mo_r1_d01_weak, na.rm = TRUE),
      unique_spill_sites = dplyr::n_distinct(site_id),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      dry_spill_count_mo_site = dry_spill_count_mo / unique_spill_sites
    ) %>%
    dplyr::group_by(month_num) %>%
    dplyr::summarise(
      avg_dry_spill_count_mo_site = mean(dry_spill_count_mo_site, na.rm = TRUE),
      .groups = "drop"
    )
}

prepare_rainfall_monthly_distribution <- function(data) {
  data %>%
    dplyr::group_by(year, month) %>%
    dplyr::summarise(
      avg_rainfall_mo_r9 = mean(rainfall_r9_mo, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::group_by(month) %>%
    dplyr::summarise(
      avg_rainfall_mo_r9 = mean(avg_rainfall_mo_r9, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::rename(month_num = month)
}

build_combined_plot_data <- function(dry_data, rainfall_data) {
  combined_data <- dry_data %>%
    dplyr::left_join(rainfall_data, by = "month_num") %>%
    dplyr::mutate(
      month = factor(month.abb[month_num], levels = month.abb)
    ) %>%
    dplyr::arrange(month_num)

  max_dry <- max(combined_data$avg_dry_spill_count_mo_site, na.rm = TRUE)
  max_rain <- max(combined_data$avg_rainfall_mo_r9, na.rm = TRUE)
  scale_factor <- max_dry / max_rain

  combined_data %>%
    dplyr::mutate(
      avg_rainfall_mo_r9_scaled = avg_rainfall_mo_r9 * scale_factor
    ) %>%
    list(
      data = .,
      scale_factor = scale_factor
    )
}


# ==============================================================================
# 5. Data Loading and Preparation
# ==============================================================================
cat("Loading data...\n")

dry_spills_mo <- rio::import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_dry_mo.parquet"),
  trust = TRUE
)

rainfall_mo <- rio::import(
  here::here("data", "processed", "rainfall", "rainfall_agg_mo.parquet"),
  trust = TRUE
)

cat("Preparing monthly distributions...\n")

dry_spill_monthly <- prepare_dry_spill_monthly_distribution(dry_spills_mo)
rainfall_monthly <- prepare_rainfall_monthly_distribution(rainfall_mo)

combined_plot <- build_combined_plot_data(dry_spill_monthly, rainfall_monthly)
plot_data <- combined_plot$data
scale_factor <- combined_plot$scale_factor

cat(sprintf("  Dry-spill months: %d\n", nrow(dry_spill_monthly)))
cat(sprintf("  Rainfall months: %d\n", nrow(rainfall_monthly)))
cat(sprintf("  Scale factor: %.4f\n", scale_factor))


# ==============================================================================
# 6. Create and Save Plot
# ==============================================================================
cat("Creating plot...\n")

p <- ggplot(plot_data, aes(x = month)) +
  geom_line(
    aes(
      y = avg_dry_spill_count_mo_site,
      color = "Dry Spill Count per Active Spill Site",
      group = 1
    ),
    linewidth = 0.9
  ) +
  geom_point(
    aes(
      y = avg_dry_spill_count_mo_site,
      color = "Dry Spill Count per Active Spill Site"
    ),
    size = 2
  ) +
  geom_line(
    aes(
      y = avg_rainfall_mo_r9_scaled,
      color = "Average Rainfall (9 km²)",
      group = 1
    ),
    linewidth = 0.9
  ) +
  geom_point(
    aes(
      y = avg_rainfall_mo_r9_scaled,
      color = "Average Rainfall (9 km²)"
    ),
    size = 2
  ) +
  scale_color_manual(
    values = c(
      "Dry Spill Count per Active Spill Site" = COLOR_DRY,
      "Average Rainfall (9 km²)" = COLOR_RAIN
    )
  ) +
  scale_y_continuous(
    name = "Average Monthly Dry Spill Count per Unique Active Spill Site",
    limits = c(0, NA),
    breaks = scales::pretty_breaks(n = 5),
    sec.axis = sec_axis(
      ~ . / scale_factor,
      name = "Average Monthly Rainfall (mm), 9 km² grid",
      breaks = scales::pretty_breaks(n = 5)
    )
  ) +
  labs(x = NULL) +
  theme_pref

cat("Saving plot...\n")

file_name <- "dry_spill_rainfall_monthly_per_site_combined.pdf"
ggsave(
  filename = file.path(output_dir, file_name),
  plot = p,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

cat("  Saved:", file_name, "\n")
