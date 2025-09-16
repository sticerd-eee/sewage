#!/usr/bin/env Rscript
# ============================================================
# England-only Upstream vs Downstream (MEDIAN only)
# Network distance cuts: 0–125 m, 0–250 m, 0–500 m
# Saves to visuals/River; skips plots without both groups.
# ============================================================

suppressPackageStartupMessages({
  pkgs <- c("arrow","dplyr","lubridate","ggplot2","scales")
  miss <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
  if (length(miss)) install.packages(miss)
  lapply(pkgs, library, character.only = TRUE)
})
options(arrow.use_threads = TRUE, dplyr.summarise.inform = FALSE)

# ---- Paths ----
IN       <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/house_spill_enriched_quartertag.parquet"
OUT_ROOT <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/visuals"
OUT      <- file.path(OUT_ROOT, "River")
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)
stopifnot(file.exists(IN))

# ---- Config ----
CUTOFFS <- c(125, 250, 500)  # meters (inclusive)

# ---- Helpers ----
num_short <- scales::label_number(scale_cut = scales::cut_short_scale())
dbg <- function(...) message(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = ""))

has_both_groups <- function(df_small) {
  any(df_small$spill_relation == "upstream") && any(df_small$spill_relation == "downstream")
}

make_ts <- function(df_ts) {
  df_ts %>%
    dplyr::filter(!is.na(year_month), spill_relation %in% c("upstream","downstream")) %>%
    dplyr::group_by(year_month, spill_relation) %>%
    dplyr::summarise(
      n            = dplyr::n(),
      median_price = stats::median(price, na.rm = TRUE),
      .groups = "drop"
    )
}

plot_median <- function(ts_df, title_txt, file_stub) {
  if (nrow(ts_df) == 0 || length(unique(na.omit(ts_df$spill_relation))) < 2) return(invisible(NULL))
  lines_df <- ts_df %>% dplyr::group_by(spill_relation) %>% dplyr::filter(dplyr::n() >= 2) %>% dplyr::ungroup()
  N <- sum(ts_df$n, na.rm = TRUE)
  p <- ggplot(ts_df, aes(year_month, median_price, colour = spill_relation)) +
    geom_point(alpha = 0.7, size = 1.2, na.rm = TRUE) +
    geom_line(data = lines_df, linewidth = 0.9, na.rm = TRUE) +
    scale_y_continuous(labels = num_short) +
    labs(title = title_txt, subtitle = paste0("N = ", scales::comma(N)),
         x = NULL, y = "Median price", colour = NULL) +
    theme_minimal(base_size = 12)
  ggsave(file.path(OUT, paste0(file_stub, ".png")), p, width = 10, height = 5.5, dpi = 150)
}

# ---- Load & prepare ----
dbg("Loading: ", IN)
df <- arrow::read_parquet(IN) %>%
  dplyr::mutate(
    price      = as.numeric(price),
    date       = as.Date(date_of_transfer),
    year_month = lubridate::floor_date(date, "month")
  )

# England only if column exists
if ("country" %in% names(df)) df <- df %>% dplyr::filter(.data$country == "England")

# Houses at sites that spilled in the sale quarter; use network distance
df_base <- df %>%
  dplyr::filter(site_spilled %in% TRUE) %>%
  dplyr::mutate(dist_m = as.numeric(river_dist_m_to_spill)) %>%
  dplyr::filter(is.finite(dist_m), dist_m >= 0)

dbg("Rows after spill filter: ", nrow(df_base))

# ---- Loop over distance cutoffs ----
for (cut in CUTOFFS) {
  dbg("Cutoff ≤ ", cut, " m ...")
  df_cut <- df_base %>% dplyr::filter(dist_m <= cut)
  if (!has_both_groups(df_cut)) next
  ts_all <- make_ts(df_cut)
  plot_median(
    ts_df     = ts_all,
    title_txt = paste0("Upstream vs Downstream — England — Within 0–", cut, " m (network)"),
    file_stub = paste0("river_ENGLAND_within_", cut, "m_median_price")
  )
}

cat("✅ Wrote PNGs to:\n", OUT, "\n")
