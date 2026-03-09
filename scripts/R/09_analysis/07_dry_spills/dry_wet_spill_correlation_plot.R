# ==============================================================================
# Dry-Wet Spill Count Correlation Plot
# ==============================================================================
#
# Purpose: Generate a pooled site-year hexbin figure showing the correlation
#          between dry and wet spill counts across 2021-2023.
#
# Author: Jacopo Olivieri
# Date: 2026-03-08
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_dry_yr.parquet
#
# Outputs:
#   - output/figures/dry_wet_spill_count_correlation_2021_2023.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 9 * 1.618
PLOT_HEIGHT <- 9
PLOT_DPI <- 300
TARGET_YEARS <- 2021:2023
YEAR_RANGE_LABEL <- paste0(min(TARGET_YEARS), "-", max(TARGET_YEARS))
YEAR_FILENAME_LABEL <- paste(min(TARGET_YEARS), max(TARGET_YEARS), sep = "_")
DRY_SPILL_VAR <- "dry_spill_count_yr_r1_d01_weak"
HEX_BINS <- 35L
VIRIDIS_PALETTE <- "magma"
FONT_FAMILY <- "Libertinus Serif"


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "tidyverse",
  "here",
  "scales",
  "viridis",
  "hexbin"
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
# 3.1 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.2 ggplot Theme -------------------------------------------------------------
theme_pref <- theme_minimal(base_family = FONT_FAMILY) +
  theme(
    text = element_text(size = 10, family = FONT_FAMILY),
    plot.title = element_text(
      face = "bold",
      size = 12,
      family = FONT_FAMILY,
      margin = ggplot2::margin(b = 8, unit = "pt")
    ),
    plot.subtitle = element_text(
      size = 10,
      family = FONT_FAMILY,
      margin = ggplot2::margin(b = 10, unit = "pt")
    ),
    axis.title = element_text(face = "bold", size = 12, family = FONT_FAMILY),
    axis.text = element_text(size = 10, family = FONT_FAMILY),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "bottom",
    legend.direction = "horizontal",
    legend.title = element_text(face = "bold", size = 10, family = FONT_FAMILY),
    legend.text = element_text(size = 9, family = FONT_FAMILY),
    legend.key.width = grid::unit(2.4, "cm"),
    legend.key.height = grid::unit(0.4, "cm"),
    legend.margin = ggplot2::margin(t = 8, b = 2, unit = "pt"),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )

build_count_breaks <- function(max_value) {
  base_breaks <- c(0, 1, 2, 5, 10, 20, 50, 100, 200, 300, 400)
  base_breaks[base_breaks <= max_value]
}


# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================
cat("Loading pooled yearly dry-spill panel...\n")

plot_data <- arrow::read_parquet(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_dry_yr.parquet")
) %>%
  dplyr::filter(year %in% TARGET_YEARS) %>%
  dplyr::transmute(
    site_id,
    year,
    spill_count_yr,
    dry_spill_count_yr = .data[[DRY_SPILL_VAR]]
  ) %>%
  dplyr::filter(
    !is.na(spill_count_yr),
    !is.na(dry_spill_count_yr)
  ) %>%
  dplyr::mutate(
    wet_spill_count_yr = pmax(spill_count_yr - dry_spill_count_yr, 0)
  )

spearman_rho <- cor(
  plot_data$wet_spill_count_yr,
  plot_data$dry_spill_count_yr,
  method = "spearman"
)
n_obs <- nrow(plot_data)

cat(sprintf("  Retained %s site-years\n", scales::comma(n_obs)))
cat(sprintf("  Spearman rho: %.3f\n", spearman_rho))


# ==============================================================================
# 5. Create Plot
# ==============================================================================
cat("Creating dry-wet spill correlation figure...\n")

subtitle_text <- paste0(
  "Pooled site-years, ",
  YEAR_RANGE_LABEL,
  " | Spearman rho = ",
  formatC(spearman_rho, format = "f", digits = 2),
  " | N = ",
  scales::comma(n_obs)
)

x_breaks <- build_count_breaks(max(plot_data$wet_spill_count_yr, na.rm = TRUE))
y_breaks <- build_count_breaks(max(plot_data$dry_spill_count_yr, na.rm = TRUE))

p <- ggplot(
  plot_data,
  aes(x = wet_spill_count_yr, y = dry_spill_count_yr)
) +
  geom_hex(aes(fill = after_stat(count)), bins = HEX_BINS) +
  scale_fill_viridis_c(
    option = VIRIDIS_PALETTE,
    trans = "log10",
    begin = 0.1,
    end = 0.95,
    name = "Site-years"
  ) +
  scale_x_continuous(
    trans = scales::pseudo_log_trans(sigma = 1),
    breaks = x_breaks,
    labels = scales::label_comma(),
    expand = expansion(mult = c(0, 0.02))
  ) +
  scale_y_continuous(
    trans = scales::pseudo_log_trans(sigma = 1),
    breaks = y_breaks,
    labels = scales::label_comma(),
    expand = expansion(mult = c(0, 0.02))
  ) +
  guides(
    fill = guide_colorbar(
      title.position = "top",
      barwidth = grid::unit(5, "cm"),
      barheight = grid::unit(0.4, "cm")
    )
  ) +
  labs(
    subtitle = subtitle_text,
    x = "Annual wet spill count",
    y = "Annual dry spill count"
  ) +
  theme_pref


# ==============================================================================
# 6. Save Plot
# ==============================================================================
cat("Saving plot...\n")

file_name <- paste0(
  "dry_wet_spill_count_correlation_",
  YEAR_FILENAME_LABEL,
  ".pdf"
)

ggsave(
  filename = here::here(output_dir, file_name),
  plot = p,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  device = grDevices::cairo_pdf,
  units = "cm"
)

cat("  Saved:", file_name, "\n")
