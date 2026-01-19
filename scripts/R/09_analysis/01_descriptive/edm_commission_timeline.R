# ==============================================================================
# EDM Commission Timeline - Distribution of EDM Commissioning by Year
# ==============================================================================
#
# Purpose: Plot the percentage of EDM sites commissioned each year
#          showing the temporal rollout of EDM monitoring infrastructure.
#
# Author: Jacopo Olivieri
# Date: 2026-01-16
#
# Inputs:
#   - data/processed/unique_spill_sites.parquet
#
# Outputs:
#   - output/figures/edm_commission_timeline.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 9 * 1.618   # Width in cm
PLOT_HEIGHT <- 9          # Height in cm
PLOT_DPI <- 300           # Resolution
BAR_COLOR <- "#B63679FF"  # Viridis magma color


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
  "showtext",
  "lubridate"
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
    legend.position = "none",
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )


# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================
cat("Loading unique spill sites data...\n")

unique_sites <- arrow::read_parquet(
  here::here("data", "processed", "unique_spill_sites.parquet")
)

# 4.1 Process Data -------------------------------------------------------------
cat("Processing data...\n")

commission_by_year <- unique_sites %>%
  filter(!is.na(edm_commission_date)) %>%
  mutate(commission_year = lubridate::year(edm_commission_date)) %>%
  count(commission_year) %>%
  mutate(percentage = n / sum(n) * 100)

# Print summary statistics
cat("Summary of EDM commissioning:\n")
cat("  Total EDMs with commission date:", sum(commission_by_year$n), "\n")
cat("  Year range:", min(commission_by_year$commission_year), "-",
    max(commission_by_year$commission_year), "\n")
cat("  Percentages sum to:", round(sum(commission_by_year$percentage), 2), "%\n")


# ==============================================================================
# 5. Create and Save Plot
# ==============================================================================
cat("Creating plot...\n")

p <- ggplot(commission_by_year, aes(x = commission_year, y = percentage)) +
  geom_col(fill = BAR_COLOR) +
  scale_x_continuous(
    breaks = seq(
      min(commission_by_year$commission_year),
      max(commission_by_year$commission_year),
      by = 1
    )
  ) +
  scale_y_continuous(
    labels = scales::percent_format(scale = 1),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = "Year",
    y = "Percentage of EDMs Commissioned (%)"
  ) +
  theme_pref

# 5.1 Save Plot ----------------------------------------------------------------
cat("Saving plot...\n")

file_name <- "edm_commission_timeline.pdf"
ggsave(
  filename = here::here(output_dir, file_name),
  plot = p,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

cat("  Saved:", file_name, "\n")
