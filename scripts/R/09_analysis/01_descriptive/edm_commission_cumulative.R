# ==============================================================================
# EDM Commission Cumulative - Cumulative Distribution of EDM Commissioning
# ==============================================================================
#
# Purpose: Plot the cumulative percentage of EDM sites commissioned over time
#          showing the continuous rollout of EDM monitoring infrastructure.
#
# Author: Jacopo Olivieri
# Date: 2026-01-16
#
# Inputs:
#   - data/processed/unique_spill_sites.parquet
#
# Outputs:
#   - output/figures/edm_commission_cumulative.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 9 * 1.618   # Width in cm
PLOT_HEIGHT <- 9          # Height in cm
PLOT_DPI <- 300           # Resolution
LINE_COLOR <- "#B63679FF" # Viridis magma color


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

cumulative_data <- unique_sites %>%
  filter(!is.na(edm_commission_date)) %>%
  arrange(edm_commission_date) %>%
  mutate(
    cumulative_count = row_number(),
    cumulative_percentage = cumulative_count / n() * 100
  )

# Add final point at end of 2023 to extend line
end_point <- tibble(
  edm_commission_date = as.Date("2023-12-31"),
  cumulative_count = nrow(cumulative_data),
  cumulative_percentage = 100
)
cumulative_data <- bind_rows(cumulative_data, end_point)

# Print summary statistics
total_edms <- nrow(cumulative_data)
cat("Summary of EDM commissioning:\n")
cat("  Total EDMs with commission date:", total_edms, "\n")
cat("  Date range:", as.character(min(cumulative_data$edm_commission_date)), "to",
    as.character(max(cumulative_data$edm_commission_date)), "\n")


# ==============================================================================
# 5. Create and Save Plot
# ==============================================================================
cat("Creating plot...\n")

p <- ggplot(cumulative_data, aes(x = edm_commission_date, y = cumulative_percentage)) +
  geom_step(color = LINE_COLOR, linewidth = 0.9) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y"
  ) +
  scale_y_continuous(
    labels = scales::percent_format(scale = 1),
    breaks = seq(0, 100, 20),
    limits = c(0, 100),
    expand = expansion(mult = c(0, 0.02))
  ) +
  labs(
    x = "Date",
    y = "Cumulative Percentage of EDMs Commissioned (%)"
  ) +
  theme_pref

# 5.1 Save Plot ----------------------------------------------------------------
cat("Saving plot...\n")

file_name <- "edm_commission_cumulative.pdf"
ggsave(
  filename = here::here(output_dir, file_name),
  plot = p,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

cat("  Saved:", file_name, "\n")
