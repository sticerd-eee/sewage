# ==============================
# Median Near vs Far plots (sales + rentals, quarterly)
# Self-contained with absolute paths
# ==============================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(rio)
})

# -------- Paths (ABSOLUTE) --------
ROOT <- "/Users/odran/Dropbox/sewage"

# Within-radius linkage (distance_m, ids, time)
PATH_WITHIN_SALES  <- file.path(ROOT, "data/processed/within_radius_panel/sales")
PATH_WITHIN_RENT   <- file.path(ROOT, "data/processed/within_radius_panel/rentals")

# Price tables
PATH_SALES_PRICE   <- file.path(ROOT, "data/processed/house_price.parquet")
PATH_RENT_PRICE    <- file.path(ROOT, "data/processed/zoopla/zoopla_rentals.parquet")

# Output
OUT_DIR <- file.path(ROOT, "output/figures/Plots")
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# -------- Helpers --------
file_ok <- function(p) { fi <- file.info(p); !is.na(fi$size) && fi$size > 0 }
coerce_price <- function(x) { if (is.numeric(x)) as.numeric(x) else as.numeric(gsub("[^0-9.]+", "", as.character(x))) }
threshold_for <- function(rad) as.integer(rad/2L)

nearest_per_txn <- function(dat, id_col) {
  dat %>%
    group_by(across(all_of(c(id_col, "qtr_id")))) %>%
    slice_min(order_by = distance_m, n = 1, with_ties = FALSE) %>%
    ungroup()
}

plot_median_lines <- function(df, threshold_m, label_prefix, y_lab) {
  p <- ggplot(df, aes(qtr_id, median_price, color = group)) +
    geom_line(linewidth = 1.1) +
    labs(
      title = sprintf("Median Price by Quarter: %s (Near/Far split=%dm)", label_prefix, threshold_m),
      x = "Quarter", y = y_lab, color = "Distance group"
    ) +
    theme_minimal()
  ggsave(file.path(OUT_DIR, sprintf("median_%s_thresh%dm.png", label_prefix, threshold_m)),
         p, width = 8, height = 5, dpi = 300)
}

# -------- Loaders (quarterly) --------
load_within_sales_q <- function(rad) {
  arrow::open_dataset(PATH_WITHIN_SALES) %>%
    filter(radius == rad, period_type == "quarterly") %>%
    collect()
}
load_within_rent_q <- function(rad) {
  arrow::open_dataset(PATH_WITHIN_RENT) %>%
    filter(radius == rad, period_type == "quarterly") %>%
    collect()
}

# -------- Clean price tables to one row per (id, qtr_id) --------
sales_price_table <- function() {
  if (!file_ok(PATH_SALES_PRICE)) stop("Sales price parquet missing/empty: ", PATH_SALES_PRICE)
  sp <- rio::import(PATH_SALES_PRICE, trust = TRUE)
  price_col <- c("price","price_gbp")[c("price","price_gbp") %in% names(sp)][1]
  if (is.na(price_col)) stop("Sales price file lacks 'price'/'price_gbp'. Columns: ", paste(names(sp), collapse=", "))
  sp %>%
    filter(!is.na(house_id), !is.na(qtr_id)) %>%
    mutate(price_num = coerce_price(.data[[price_col]])) %>%
    filter(!is.na(price_num)) %>%
    group_by(house_id, qtr_id) %>%
    summarise(price_num = median(price_num), .groups = "drop")
}

rent_price_table <- function() {
  if (!file_ok(PATH_RENT_PRICE)) stop("Rent price parquet missing/empty: ", PATH_RENT_PRICE)
  rp <- rio::import(PATH_RENT_PRICE, trust = TRUE)
  if (!all(c("rental_id","qtr_id") %in% names(rp))) {
    stop("Rent price file needs rental_id and qtr_id. Columns: ", paste(names(rp), collapse=", "))
  }
  if (!"listing_price" %in% names(rp)) stop("Rent price file lacks 'listing_price'.")
  rp %>%
    filter(!is.na(rental_id), !is.na(qtr_id)) %>%
    mutate(price_num = coerce_price(listing_price)) %>%
    filter(!is.na(price_num)) %>%
    group_by(rental_id, qtr_id) %>%
    summarise(price_num = median(price_num), .groups = "drop")
}

# -------- Pipeline: join, dedupe to nearest, split Near/Far, summarise median --------
median_near_far <- function(within_df, price_tbl, id_col, threshold_m) {
  stopifnot("distance_m" %in% names(within_df))
  jcols <- if (id_col == "house_id") c("house_id","qtr_id") else c("rental_id","qtr_id")

  dat <- within_df %>%
    left_join(price_tbl, by = jcols) %>%
    filter(!is.na(.data[[id_col]]), !is.na(qtr_id), !is.na(distance_m), !is.na(price_num))

  if (!nrow(dat)) return(NULL)
}

# -------- Driver --------
radii <- c(250L, 500L, 1000L, 2000L)  # add 1000L if available

# Pre-load price tables once
sp_tbl <- sales_price_table()
rp_tbl <- rent_price_table()

for (rad in radii) {
  thr <- threshold_for(rad)

  message(sprintf("Sales: rad=%d, threshold=%dm", rad, thr))
  ws <- load_within_sales_q(rad)
  if (nrow(ws)) {
    s_med <- median_near_far(ws, sp_tbl, id_col = "house_id", threshold_m = thr)
    if (!is.null(s_med) && nrow(s_med)) {
      plot_median_lines(s_med, thr, sprintf("sales_%dm", rad), "Median Sale Price")
    } else {
      message(sprintf("[sales %dm] No rows after join/filter; skipping plot.", rad))
    }
  } else message(sprintf("[sales %dm] within-panel empty; skipping.", rad))

  message(sprintf("Rentals: rad=%d, threshold=%dm", rad, thr))
  wr <- load_within_rent_q(rad)
  if (nrow(wr)) {
    r_med <- median_near_far(wr, rp_tbl, id_col = "rental_id", threshold_m = thr)
    if (!is.null(r_med) && nrow(r_med)) {
      plot_median_lines(r_med, thr, sprintf("rent_%dm", rad), "Median Rent (listing_price)")
    } else {
      message(sprintf("[rent %dm] No rows after join/filter; skipping plot.", rad))
    }
  } else message(sprintf("[rent %dm] within-panel empty; skipping.", rad))
}
