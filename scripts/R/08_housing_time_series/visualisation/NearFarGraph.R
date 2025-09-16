#!/usr/bin/env Rscript
# ============================================================
# England-only Near vs Far (MEDIAN only), Euclidean radius
# Rings:
#   0–125: Near ≤125  vs Far (125, 250]
#   0–250: Near ≤250  vs Far (250, 500]
#   0–500: Near ≤500  vs Far (500, 1000]  (extended so FAR exists)
# Saves to visuals/Radius; skips plots without both groups.
# ============================================================

suppressPackageStartupMessages({
  pkgs <- c("arrow","dplyr","lubridate","ggplot2","scales")
  miss <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
  if (length(miss)) install.packages(miss)
  lapply(pkgs, library, character.only = TRUE)
})
options(arrow.use_threads = TRUE, dplyr.summarise.inform = FALSE)

# ---------- Paths ----------
HOUSE_QTAG <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/house_spill_enriched_quartertag.parquet"
SPILLS_LOC <- "/Users/odran/Dropbox/sewage/data/processed/unique_spill_sites.parquet"
OUT_ROOT   <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/visuals"
OUT        <- file.path(OUT_ROOT, "Radius")
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)
stopifnot(file.exists(HOUSE_QTAG), file.exists(SPILLS_LOC))

# ---------- Rings ----------
RINGS <- list(
  list(name = "0-125", near_max = 125, far_min = 125, far_max = 250),
  list(name = "0-250", near_max = 250, far_min = 250, far_max = 500),
  list(name = "0-500", near_max = 500, far_min = 500, far_max = 1000) # change to 500 if you don't want extension
)

# ---------- Helpers ----------
num_short <- scales::label_number(scale_cut = scales::cut_short_scale())
dbg <- function(...) message(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = ""))

has_both_groups <- function(df_small, group_col = "prox_group") {
  any(df_small[[group_col]] == "near") && any(df_small[[group_col]] == "far")
}

make_ts <- function(df_ts, group_col = "prox_group") {
  df_ts %>%
    dplyr::filter(!is.na(year_month), .data[[group_col]] %in% c("near","far")) %>%
    dplyr::group_by(year_month, .data[[group_col]]) %>%
    dplyr::summarise(
      n            = dplyr::n(),
      median_price = stats::median(price, na.rm = TRUE),
      .groups = "drop"
    )
}

plot_median <- function(ts_df, group_col, title_txt, file_stub) {
  if (nrow(ts_df) == 0 || length(unique(ts_df[[group_col]])) < 2) return(invisible(NULL))
  lines_df <- ts_df %>% dplyr::group_by(.data[[group_col]]) %>% dplyr::filter(dplyr::n() >= 2) %>% dplyr::ungroup()
  N <- sum(ts_df$n, na.rm = TRUE)
  p <- ggplot(ts_df, aes(year_month, median_price, colour = .data[[group_col]])) +
    geom_point(alpha = 0.7, size = 1.2, na.rm = TRUE) +
    geom_line(data = lines_df, linewidth = 0.9, na.rm = TRUE) +
    scale_y_continuous(labels = num_short) +
    scale_colour_discrete(labels = c("far"="Far","near"="Near")) +
    labs(title = title_txt, subtitle = paste0("N = ", scales::comma(N)),
         x = NULL, y = "Median price", colour = NULL) +
    theme_minimal(base_size = 12)
  ggsave(file.path(OUT, paste0(file_stub, ".png")), p, width = 10, height = 5.5, dpi = 150)
}

stub <- function(ring) paste0("radius_ENGLAND_ring_", ring$name,
                              "__near0-", ring$near_max, "_vs_", ring$far_min, "-", ring$far_max)

# ---------- Load ----------
dbg("Loading households: ", HOUSE_QTAG)
df <- arrow::read_parquet(HOUSE_QTAG) %>%
  dplyr::mutate(
    price      = as.numeric(price),
    date       = as.Date(date_of_transfer),
    year_month = lubridate::floor_date(date, "month")
  )
if ("country" %in% names(df)) df <- df %>% dplyr::filter(.data$country == "England")

spills <- arrow::read_parquet(SPILLS_LOC) %>%
  dplyr::select(site_id, spill_easting = easting, spill_northing = northing)

need_cols <- c("spill_site_id","easting","northing")
if (!all(need_cols %in% names(df))) stop("Household file must contain: ", paste(need_cols, collapse=", "))

# Only houses at sites that spilled in the sale quarter; compute Euclidean dist to outlet
df_dist <- df %>%
  dplyr::filter(site_spilled %in% TRUE) %>%
  dplyr::inner_join(spills, by = c("spill_site_id" = "site_id"), na_matches = "never") %>%
  dplyr::mutate(
    dist_m = sqrt((as.numeric(easting) - as.numeric(spill_easting))^2 +
                  (as.numeric(northing) - as.numeric(spill_northing))^2)
  ) %>%
  dplyr::filter(is.finite(dist_m), dist_m >= 0)

# ---------- Build and save plots ----------
for (ring in RINGS) {
  dbg("Ring ", ring$name, " → Near ≤", ring$near_max, " m; Far ", ring$far_min, "–", ring$far_max, " m")

  df_ring <- df_dist %>%
    dplyr::mutate(
      prox_group = dplyr::case_when(
        dist_m <= ring$near_max ~ "near",
        dist_m >  ring$far_min & dist_m <= ring$far_max ~ "far",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::filter(!is.na(prox_group))

  if (!has_both_groups(df_ring)) next

  ts <- make_ts(df_ring)
  base <- stub(ring)

  plot_median(
    ts_df     = ts,
    group_col = "prox_group",
    title_txt = paste0("Near vs Far — England — Ring ", ring$name,
                       " (Near ≤", ring$near_max, " m vs Far ", ring$far_min, "–", ring$far_max, " m, Euclidean)"),
    file_stub = paste0(base, "__median_price")
  )
}

cat("✅ Wrote PNGs to:\n", OUT, "\n")
