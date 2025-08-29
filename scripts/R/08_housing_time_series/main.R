library(arrow)
library(dplyr)
library(ggplot2)
library(here)

#--------------------------------
# Paths
#--------------------------------
# project root assumed = /Users/odran/Dropbox/sewage/
data_dir <- here("data", "final", "dat_panel_house")
out_dir  <- here("output", "figures", "Plots")

if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

#--------------------------------
# Helper: make plots for a file + threshold
#--------------------------------
make_plots <- function(parquet_file, threshold_m, label_prefix) {
  fp <- file.path(data_dir, parquet_file)

  dat <- arrow::read_parquet(fp)

  dat_clean <- dat %>%
    filter(!is.na(min_dist_m)) %>%
    mutate(
      group = ifelse(
        min_dist_m <= threshold_m,
        sprintf("Near (≤%dm)", threshold_m),
        sprintf("Far (>%dm)", threshold_m)
      )
    )

  dat_summary <- dat_clean %>%
    group_by(qtr_id, group) %>%
    summarise(
      mean_price = mean(price, na.rm = TRUE),
      mean_log_price = mean(log_price, na.rm = TRUE),
      n = n(),
      .groups = "drop"
    )

  # Plot: Raw prices
  p_raw <- ggplot(dat_summary, aes(x = qtr_id, y = mean_price, color = group)) +
    geom_line(size = 1.1) +
    labs(
      title = sprintf("Average House Price by Quarter: %s (≤/> %dm)", label_prefix, threshold_m),
      x = "Quarter",
      y = "Average Price (£)",
      color = "Distance to Spill"
    ) +
    theme_minimal()

  ggsave(
    filename = file.path(out_dir, sprintf("house_price_near_vs_far_%s_thresh%dm.png", label_prefix, threshold_m)),
    plot = p_raw, width = 8, height = 5, dpi = 300
  )

  # Plot: Log prices
  p_log <- ggplot(dat_summary, aes(x = qtr_id, y = mean_log_price, color = group)) +
    geom_line(size = 1.1) +
    labs(
      title = sprintf("Average Log House Price by Quarter: %s (≤/> %dm)", label_prefix, threshold_m),
      x = "Quarter",
      y = "Average log(price)",
      color = "Distance to Spill"
    ) +
    theme_minimal()

  ggsave(
    filename = file.path(out_dir, sprintf("house_log_price_near_vs_far_%s_thresh%dm.png", label_prefix, threshold_m)),
    plot = p_log, width = 8, height = 5, dpi = 300
  )
}

#--------------------------------
# Make all six graphs
#--------------------------------
# 1) 1000 m file, compare over/under 500 m
make_plots(parquet_file = "dat_panel_house_1000.parquet", threshold_m = 500, label_prefix = "1000m")

# 2) 500 m file, compare over/under 250 m
make_plots(parquet_file = "dat_panel_house_500.parquet", threshold_m = 250, label_prefix = "500m")

# 3) 250 m file, compare over/under 125 m
make_plots(parquet_file = "dat_panel_house_250.parquet", threshold_m = 125, label_prefix = "250m")
