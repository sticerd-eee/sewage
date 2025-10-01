# Correlate distance with price (1000 m file), ignore NAs
# Saves plots with relative paths via `here`

library(arrow)
library(dplyr)
library(ggplot2)
library(here)

#--------------------------------
# Paths
#--------------------------------
data_dir <- here("data", "final", "dat_panel_house")
fp       <- file.path(data_dir, "dat_panel_house_1000.parquet")
out_dir  <- here("output", "figures", "Plots")

if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

#--------------------------------
# Load only needed columns
#--------------------------------
dat <- arrow::read_parquet(
  fp,
  col_select = c("min_dist_m", "price", "log_price", "qtr_id")
)

#--------------------------------
# Clean: drop NAs for the variables used
#--------------------------------
dat_clean_price <- dat %>%
  filter(!is.na(min_dist_m), !is.na(price))

dat_clean_log <- dat %>%
  filter(!is.na(min_dist_m), !is.na(log_price))

#--------------------------------
# Correlations
#--------------------------------
# Pearson (default): linear association in levels/logs
cor_pearson_price <- cor(dat_clean_price$min_dist_m, dat_clean_price$price)
cor_pearson_log   <- cor(dat_clean_log$min_dist_m,   dat_clean_log$log_price)

# Optional: Spearman (rank) for robustness to outliers
cor_spearman_price <- cor(dat_clean_price$min_dist_m, dat_clean_price$price, method = "spearman")
cor_spearman_log   <- cor(dat_clean_log$min_dist_m,   dat_clean_log$log_price, method = "spearman")

cat("Pearson correlation (min_dist_m, price):      ", round(cor_pearson_price, 4), "\n")
cat("Pearson correlation (min_dist_m, log_price): ", round(cor_pearson_log, 4), "\n")
cat("Spearman correlation (min_dist_m, price):    ", round(cor_spearman_price, 4), "\n")
cat("Spearman correlation (min_dist_m, log_price):", round(cor_spearman_log, 4), "\n")

#--------------------------------
# Plots (downsample for speed/legibility)
#--------------------------------
# Adjust n_sample if plots are too dense/sparse
n_sample <- 100000L
set.seed(42)

plot_sample_price <- if (nrow(dat_clean_price) > n_sample) {
  dplyr::slice_sample(dat_clean_price, n = n_sample)
} else dat_clean_price

plot_sample_log <- if (nrow(dat_clean_log) > n_sample) {
  dplyr::slice_sample(dat_clean_log, n = n_sample)
} else dat_clean_log

# Scatter: raw price
p_price <- ggplot(plot_sample_price, aes(x = min_dist_m, y = price)) +
  geom_point(alpha = 0.1, size = 0.6) +
  geom_smooth(method = "loess", se = FALSE) +
  labs(
    title = "Distance vs House Price (1000 m file)",
    subtitle = paste0("Pearson r = ", round(cor_pearson_price, 3),
                      " | Spearman ρ = ", round(cor_spearman_price, 3)),
    x = "Min distance to spill site (m)",
    y = "Sale price (£)"
  ) +
  theme_minimal()

ggsave(
  filename = file.path(out_dir, "corr_distance_price_1000m.png"),
  plot = p_price, width = 8, height = 5, dpi = 300
)

# Scatter: log price
p_log <- ggplot(plot_sample_log, aes(x = min_dist_m, y = log_price)) +
  geom_point(alpha = 0.1, size = 0.6) +
  geom_smooth(method = "loess", se = FALSE) +
  labs(
    title = "Distance vs Log House Price (1000 m file)",
    subtitle = paste0("Pearson r = ", round(cor_pearson_log, 3),
                      " | Spearman ρ = ", round(cor_spearman_log, 3)),
    x = "Min distance to spill site (m)",
    y = "log(price)"
  ) +
  theme_minimal()

ggsave(
  filename = file.path(out_dir, "corr_distance_logprice_1000m.png"),
  plot = p_log, width = 8, height = 5, dpi = 300
)
