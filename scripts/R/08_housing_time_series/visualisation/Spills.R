#!/usr/bin/env Rscript
# ============================================================
# Time series & continuous distance plots:
#  1) Upstream vs Downstream: monthly price & log(price)
#  2) Continuous distance to spill vs price & log(price)
# ============================================================

suppressPackageStartupMessages({
  pkgs <- c("arrow","dplyr","lubridate","ggplot2","scales","readr","stringr")
  miss <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
  if (length(miss)) install.packages(miss)
  lapply(pkgs, library, character.only = TRUE)
})
options(arrow.use_threads = TRUE)

IN  <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/house_spill_enriched.parquet"
OUT <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/visuals"
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)
stopifnot(file.exists(IN))

# Label helper (replacement for defunct label_number_si)
num_short <- scales::label_number(scale_cut = scales::cut_short_scale())

# ---- Load & base fields ----
df <- arrow::read_parquet(IN) %>%
  mutate(
    price      = as.numeric(price),
    date       = as.Date(date_of_transfer),
    year_month = lubridate::floor_date(date, "month"),
    year       = lubridate::year(date),
    log_price  = ifelse(is.finite(price) & price > 0, log(price), NA_real_)
  )

# Prefer river-network distance when present; else Euclidean
has_river <- "river_dist_m_to_nearest_spill" %in% names(df)
df <- df %>%
  mutate(
    dist_m_pref  = dplyr::case_when(
      has_river & relation_to_nearest_spill == "downstream" ~ river_dist_m_to_nearest_spill,
      TRUE ~ dist_km_to_nearest_spill * 1000
    ),
    dist_km_pref = dist_m_pref / 1000
  )

# ============================================================
# 1) Upstream vs Downstream — MONTHLY time series
# ============================================================
df_ud <- df %>% filter(relation_to_nearest_spill %in% c("upstream","downstream"))

ts_ud <- df_ud %>%
  filter(!is.na(year_month)) %>%
  group_by(year_month, relation_to_nearest_spill) %>%
  summarise(
    n            = n(),
    mean_price   = mean(price, na.rm = TRUE),
    median_price = median(price, na.rm = TRUE),
    mean_log     = mean(log_price, na.rm = TRUE),
    median_log   = median(log_price, na.rm = TRUE),
    .groups = "drop"
  )

readr::write_csv(ts_ud, file.path(OUT, "ts_upstream_vs_downstream_monthly.csv"))

p_ud_mean_price <- ggplot(ts_ud, aes(year_month, mean_price, colour = relation_to_nearest_spill)) +
  geom_line(linewidth = 0.9, na.rm = TRUE) +
  scale_y_continuous(labels = num_short) +
  labs(title = "Mean Price over Time: Upstream vs Downstream",
       x = NULL, y = "Mean price", colour = NULL) +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "ts_ud_mean_price.png"), p_ud_mean_price, width = 10, height = 5.5, dpi = 150)

p_ud_median_price <- ggplot(ts_ud, aes(year_month, median_price, colour = relation_to_nearest_spill)) +
  geom_line(linewidth = 0.9, na.rm = TRUE) +
  scale_y_continuous(labels = num_short) +
  labs(title = "Median Price over Time: Upstream vs Downstream",
       x = NULL, y = "Median price", colour = NULL) +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "ts_ud_median_price.png"), p_ud_median_price, width = 10, height = 5.5, dpi = 150)

p_ud_mean_log <- ggplot(ts_ud, aes(year_month, mean_log, colour = relation_to_nearest_spill)) +
  geom_line(linewidth = 0.9, na.rm = TRUE) +
  labs(title = "Mean log(Price) over Time: Upstream vs Downstream",
       x = NULL, y = "Mean log(price)", colour = NULL) +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "ts_ud_mean_log.png"), p_ud_mean_log, width = 10, height = 5.5, dpi = 150)

p_ud_median_log <- ggplot(ts_ud, aes(year_month, median_log, colour = relation_to_nearest_spill)) +
  geom_line(linewidth = 0.9, na.rm = TRUE) +
  labs(title = "Median log(Price) over Time: Upstream vs Downstream",
       x = NULL, y = "Median log(price)", colour = NULL) +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "ts_ud_median_log.png"), p_ud_median_log, width = 10, height = 5.5, dpi = 150)

# ============================================================
# 2) Continuous distance to spill vs price/log(price)
#    (Focus on downstream rows where distance is meaningful)
# ============================================================

df_down <- df %>%
  filter(relation_to_nearest_spill == "downstream",
         is.finite(dist_km_pref),
         is.finite(price), price > 0,
         is.finite(log_price))

# --- Smooths (overall) ---
p_dist_price_smooth <- ggplot(df_down, aes(dist_km_pref, price)) +
  geom_point(alpha = 0.03, size = 0.4, show.legend = FALSE) +
  geom_smooth(method = "gam", formula = y ~ s(x, bs = "cs")) +
  scale_y_continuous(labels = num_short) +
  labs(title = "Price vs Distance from Spill (Downstream, continuous)",
       x = "Distance to nearest spill (km)", y = "Price") +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "dist_vs_price_smooth_downstream.png"),
       p_dist_price_smooth, width = 9, height = 5.5, dpi = 150)

p_dist_log_smooth <- ggplot(df_down, aes(dist_km_pref, log_price)) +
  geom_point(alpha = 0.03, size = 0.4, show.legend = FALSE) +
  geom_smooth(method = "gam", formula = y ~ s(x, bs = "cs")) +
  labs(title = "log(Price) vs Distance from Spill (Downstream, continuous)",
       x = "Distance to nearest spill (km)", y = "log(Price)") +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "dist_vs_logprice_smooth_downstream.png"),
       p_dist_log_smooth, width = 9, height = 5.5, dpi = 150)

# --- Optional: hexbin (handles very large N gracefully) ---
if (requireNamespace("ggforce", quietly = TRUE)) {
  suppressPackageStartupMessages(library(ggforce))

  p_hex_price <- ggplot(df_down, aes(dist_km_pref, price)) +
    ggforce::geom_hexbin(aes(z = price), bins = 60) +
    scale_fill_continuous(type = "viridis") +
    scale_y_continuous(labels = num_short) +
    labs(title = "Hexbin: Price vs Distance from Spill (Downstream)",
         x = "Distance (km)", y = "Price", fill = "Count") +
    theme_minimal(base_size = 12)
  ggsave(file.path(OUT, "dist_vs_price_hex_downstream.png"),
         p_hex_price, width = 9, height = 5.5, dpi = 150)

  p_hex_log <- ggplot(df_down, aes(dist_km_pref, log_price)) +
    ggforce::geom_hexbin(aes(z = log_price), bins = 60) +
    scale_fill_continuous(type = "viridis") +
    labs(title = "Hexbin: log(Price) vs Distance from Spill (Downstream)",
         x = "Distance (km)", y = "log(Price)", fill = "Count") +
    theme_minimal(base_size = 12)
  ggsave(file.path(OUT, "dist_vs_logprice_hex_downstream.png"),
         p_hex_log, width = 9, height = 5.5, dpi = 150)
}

# --- Optional: year facets (see change over time in the distance relationship) ---
p_dist_price_year <- ggplot(df_down, aes(dist_km_pref, price)) +
  geom_point(alpha = 0.02, size = 0.35) +
  geom_smooth(method = "gam", formula = y ~ s(x, bs = "cs"), colour = "black") +
  scale_y_continuous(labels = num_short) +
  facet_wrap(~ year, scales = "free_y") +
  labs(title = "Price vs Distance (Downstream) — Yearly smooths",
       x = "Distance (km)", y = "Price") +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "dist_vs_price_smooth_by_year.png"),
       p_dist_price_year, width = 12, height = 8, dpi = 150)

p_dist_log_year <- ggplot(df_down, aes(dist_km_pref, log_price)) +
  geom_point(alpha = 0.02, size = 0.35) +
  geom_smooth(method = "gam", formula = y ~ s(x, bs = "cs"), colour = "black") +
  facet_wrap(~ year, scales = "free_y") +
  labs(title = "log(Price) vs Distance (Downstream) — Yearly smooths",
       x = "Distance (km)", y = "log(Price)") +
  theme_minimal(base_size = 12)
ggsave(file.path(OUT, "dist_vs_logprice_smooth_by_year.png"),
       p_dist_log_year, width = 12, height = 8, dpi = 150)

cat("✅ Wrote outputs to:\n", OUT, "\n")
