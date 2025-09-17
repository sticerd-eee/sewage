# ==== Median house price at halfway bands (Near vs Far colours), saves PNG/CSV ====

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(scales)
  library(readr)
  library(stringr)
  library(tibble)
})

# ---------- Inputs ----------
paths <- c(
  d250  = "/Users/odran/Dropbox/sewage/data/final/dat_panel_house/dat_panel_house_250.parquet",
  d500  = "/Users/odran/Dropbox/sewage/data/final/dat_panel_house/dat_panel_house_500.parquet",
  d1000 = "/Users/odran/Dropbox/sewage/data/final/dat_panel_house/dat_panel_house_1000.parquet"
)

out_dir  <- "/Users/odran/Dropbox/sewage/output/figures/Plots"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_csv  <- file.path(out_dir, "median_price_half_bands.csv")
out_png  <- file.path(out_dir, "median_price_half_bands.png")

# ---------- Helper: read parquet safely as tibble (only needed cols) ----------
read_panel <- function(p) {
  if (!file.exists(p)) stop("File not found: ", p)
  cols <- c("price", "min_dist_m", "avg_dist_m", "within_radius", "radius")
  ds <- arrow::open_dataset(p, format = "parquet")
  have <- intersect(cols, names(ds))
  ds %>%
    select(all_of(have)) %>%
    collect() %>%
    mutate(
      price         = as.numeric(price),
      min_dist_m    = suppressWarnings(as.numeric(min_dist_m)),
      avg_dist_m    = suppressWarnings(as.numeric(avg_dist_m)),
      within_radius = as.logical(within_radius),
      radius        = suppressWarnings(as.numeric(radius))
    )
}

# ---------- Compute medians for two bands given cutpoints ----------
compute_half_medians <- function(df, inner_max, outer_max) {
  # Prefer min_dist_m; fallback to avg_dist_m
  dist_col <- if ("min_dist_m" %in% names(df) && any(!is.na(df$min_dist_m))) "min_dist_m" else
              if ("avg_dist_m" %in% names(df) && any(!is.na(df$avg_dist_m))) "avg_dist_m" else NA_character_
  if (is.na(dist_col)) {
    warning("No usable distance column (min_dist_m/avg_dist_m) found; returning NA medians.")
    return(tibble(
      band = c(paste0("<", inner_max, " m"), paste0(inner_max, "–", outer_max, " m")),
      n    = c(0L, 0L),
      median_price = c(NA_real_, NA_real_)
    ))
  }

  df_use <- df %>% filter(!is.na(.data[[dist_col]]), !is.na(price))
  if ("radius" %in% names(df_use) && !all(is.na(df_use$radius))) {
    df_use <- df_use %>% filter(.data[[dist_col]] <= outer_max + 1e-9)
  }

  inner_band <- df_use %>% filter(.data[[dist_col]] < inner_max)
  outer_band <- df_use %>% filter(.data[[dist_col]] >= inner_max, .data[[dist_col]] <= outer_max + 1e-9)

  tibble(
    band = c(paste0("<", inner_max, " m"), paste0(inner_max, "–", outer_max, " m")),
    n    = c(nrow(inner_band), nrow(outer_band)),
    median_price = c(median(inner_band$price, na.rm = TRUE),
                     median(outer_band$price, na.rm = TRUE))
  )
}

# ---------- Compute medians for each dataset ----------
res_list <- list()

# 250 m dataset: 0–125 vs 125–250
df250 <- read_panel(paths[["d250"]])
res_list[["≤250 m"]] <- compute_half_medians(df250, inner_max = 125, outer_max = 250) %>%
  mutate(dataset = "≤ 250 m (half @ 125 m)")

# 500 m dataset: 0–250 vs 250–500
df500 <- read_panel(paths[["d500"]])
res_list[["≤500 m"]] <- compute_half_medians(df500, inner_max = 250, outer_max = 500) %>%
  mutate(dataset = "≤ 500 m (half @ 250 m)")

# 1000 m dataset: 0–500 vs 500–1000
df1000 <- read_panel(paths[["d1000"]])
res_list[["≤1000 m"]] <- compute_half_medians(df1000, inner_max = 500, outer_max = 1000) %>%
  mutate(dataset = "≤ 1000 m (half @ 500 m)")

res <- bind_rows(res_list) %>%
  relocate(dataset) %>%
  mutate(
    # Nice ordering of datasets
    dataset = factor(dataset, levels = c("≤ 250 m (half @ 125 m)",
                                         "≤ 500 m (half @ 250 m)",
                                         "≤ 1000 m (half @ 500 m)")),
    # Two-colour legend (Near vs Far). Band label still shown by text on bars.
    half = ifelse(grepl("^<", band), "Near half", "Far half"),
    half = factor(half, levels = c("Near half", "Far half"))
  )

# Save table
readr::write_csv(res, out_csv)
message("Wrote: ", out_csv)

# ---------- Plot ----------
p <- ggplot(res, aes(x = dataset, y = median_price, fill = half)) +
  geom_col(position = position_dodge(width = 0.7), width = 0.65) +
  # Label bars with the actual band (e.g., "<125 m", "125–250 m") and the £ value
  geom_text(aes(label = paste0(band, "\n", scales::dollar(round(median_price), prefix = "£", big.mark=","))),
            position = position_dodge(width = 0.7), vjust = -0.25, size = 3) +
  scale_y_continuous(labels = label_dollar(prefix = "£", big.mark = ","), 
                     expand = expansion(mult = c(0.05, 0.22))) +
  scale_fill_manual(values = c("Near half" = "#9ecae1", "Far half" = "#3182bd"),
                    name = "Distance half") +
  labs(
    title = "Median house price by halfway distance bands",
    subtitle = "Per dataset radius: Near half vs Far half (uses min_dist_m; avg_dist_m as fallback)",
    x = NULL, y = "Median price"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top")

print(p)
ggsave(out_png, p, width = 10, height = 6, dpi = 300)
message("Saved plot: ", out_png)
