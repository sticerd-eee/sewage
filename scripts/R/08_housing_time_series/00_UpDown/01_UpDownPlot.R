#!/usr/bin/env Rscript
# ============================================================
# Upstream vs Downstream (NO winsorisation) from WITHIN PANEL
# - Sales + Rentals
# - Assign relation vs matched site_id using river network
# - Plot median price by quarter (no clipping/trimming)
# ============================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(sf)
  library(igraph)
  library(rio)
  library(rlang)
  library(here)
})

sf::sf_use_s2(FALSE)
options(arrow.use_threads = TRUE, dplyr.summarise.inform = FALSE)

# -----------------------------
# Project root + Paths (RELATIVE via {here})
# -----------------------------

here::i_am("scripts/R/08_housing_time_series/00_UpDown/01_UpDownPlot.R")

PATH_WITHIN_SALES <- here("data", "processed", "within_radius_panel", "sales")
PATH_WITHIN_RENT  <- here("data", "processed", "within_radius_panel", "rentals")

PATH_SALES_PRICE  <- here("data", "processed", "house_price.parquet")                 # house_id, qtr_id, price/price_gbp, easting, northing
PATH_RENT_PRICE   <- here("data", "processed", "zoopla", "zoopla_rentals.parquet")    # rental_id, qtr_id, listing_price, easting, northing

PATH_SPILL_SITES  <- here("data", "processed", "unique_spill_sites.parquet")          # site_id, easting, northing
PATH_FLOW         <- here("data", "processed", "Rivers", "River_Flow.parquet")        # easting, northing, flow_easting, flow_northing

OUT_DIR <- here("output", "figures", "Plots")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)


# -----------------------------
# Parameters
# -----------------------------
RADII       <- c(250L, 500L, 1000L, 2000L)
PERIOD_TYPE <- "quarterly"
CRS_BNG     <- 27700
SNAP_MAX_M  <- 250    # drop if house/rental snaps farther than this to river
RIVER_MAX_M <- 1000   # along-river path cap (set Inf to disable)

# -----------------------------
# Functions
# -----------------------------
# RIVER
build_graph <- function(flow_df) {
  flow_df <- flow_df %>%
    filter(is.finite(easting), is.finite(northing),
           is.finite(flow_easting), is.finite(flow_northing)) %>%
    filter(!(easting == flow_easting & northing == flow_northing)) %>%
    mutate(
      from = paste(easting, northing, sep = "_"),
      to   = paste(flow_easting, flow_northing, sep = "_"),
      weight = sqrt((easting - flow_easting)^2 + (northing - flow_northing)^2)
    )
  g <- igraph::graph_from_data_frame(flow_df[, c("from","to","weight")], directed = TRUE)
  g <- igraph::simplify(g, remove.loops = TRUE, edge.attr.comb = list(weight = "min"))
  if (is.null(igraph::edge_attr(g, "weight"))) igraph::E(g)$weight <- rep(1, igraph::ecount(g))
  g_rev <- igraph::reverse_edges(g)

  v_keys <- igraph::V(g)$name
  xy <- do.call(rbind, strsplit(v_keys, "_", fixed = TRUE))
  storage.mode(xy) <- "double"; colnames(xy) <- c("x","y")
  nodes_sf <- sf::st_as_sf(data.frame(key = v_keys, x = xy[,1], y = xy[,2]),
                           coords = c("x","y"), crs = CRS_BNG)

  comp_id <- igraph::components(igraph::as_undirected(g))$membership
  igraph::V(g)$comp <- comp_id

  list(g = g, g_rev = g_rev, nodes_sf = nodes_sf,
       vid_by_key = setNames(seq_along(v_keys), v_keys))
}

snap_points_to_nodes <- function(df_xy, id_col, nodes_sf, vid_by_key, max_m = Inf) {
  pts <- sf::st_as_sf(df_xy, coords = c("e","n"), crs = CRS_BNG)
  idx <- sf::st_nearest_feature(pts, nodes_sf)
  nn  <- nodes_sf[idx, , drop = FALSE]
  d   <- as.numeric(sf::st_distance(pts, nn, by_element = TRUE))
  vids <- as.integer(vid_by_key[nn$key])
  over <- d > max_m
  vids[over] <- NA_integer_; d[over] <- NA_real_
  out <- df_xy %>% dplyr::select(all_of(id_col)) %>% dplyr::mutate(vid = vids, snap_m = d)
  names(out) <- c(id_col, paste0(id_col, "_vid"), paste0(id_col, "_snap_m"))
  out
}

assign_relation_vs_site <- function(df2, g, g_rev, river_max_m = Inf) {
  # df2 needs: row_id, A_vid, site_id_vid, comp
  byc <- split(df2, df2$comp)
  out <- vector("list", length(byc)); names(out) <- names(byc)
  for (nm in names(byc)) {
    dsub <- byc[[nm]]
    uA <- unique(dsub$A_vid)
    uS <- unique(dsub$site_id_vid)

    D_ds <- igraph::distances(g,     v = uA, to = uS, mode = "out", weights = igraph::E(g)$weight)
    D_us <- igraph::distances(g_rev, v = uA, to = uS, mode = "out", weights = igraph::E(g_rev)$weight)
    D_ds <- as.matrix(D_ds); D_us <- as.matrix(D_us)

    a_ix <- match(dsub$A_vid, uA)
    s_ix <- match(dsub$site_id_vid, uS)

    ds <- D_ds[cbind(a_ix, s_ix)]
    us <- D_us[cbind(a_ix, s_ix)]

    ds[!is.finite(ds) | ds > river_max_m] <- Inf
    us[!is.finite(us) | us > river_max_m] <- Inf

    pick_down <- is.finite(ds) & (ds <= us | !is.finite(us))
    pick_up   <- is.finite(us) & (us <  ds | !is.finite(ds))
    rel <- rep(NA_character_, nrow(dsub)); rel[pick_down] <- "downstream"; rel[pick_up] <- "upstream"

    dist <- rep(NA_real_, nrow(dsub)); dist[pick_down] <- ds[pick_down]; dist[pick_up] <- us[pick_up]

    out[[nm]] <- tibble(row_id = dsub$row_id,
                        spill_relation = rel,
                        river_dist_m_to_site = dist)
  }
  bind_rows(out)
}

# -----------------------------
# Load data
# -----------------------------
stopifnot(dir.exists(PATH_WITHIN_SALES), dir.exists(PATH_WITHIN_RENT))
stopifnot(file_ok(PATH_SALES_PRICE), file_ok(PATH_RENT_PRICE))
stopifnot(file_ok(PATH_SPILL_SITES), file_ok(PATH_FLOW))

spill_sites <- arrow::read_parquet(PATH_SPILL_SITES) %>%
  select(site_id, easting, northing) %>%
  rename(site_e = easting, site_n = northing) %>%
  filter(is.finite(site_e), is.finite(site_n))

flow_df <- arrow::read_parquet(PATH_FLOW,
  col_select = c("easting","northing","flow_easting","flow_northing"))
G <- build_graph(flow_df)  # g, g_rev, nodes_sf, vid_by_key

# Price tables with coords (one row per id × qtr_id)
sales_raw <- rio::import(PATH_SALES_PRICE, trust = TRUE)
sales_price_col <- c("price","price_gbp")[c("price","price_gbp") %in% names(sales_raw)][1]
if (is.na(sales_price_col)) stop("Sales price file must have 'price' or 'price_gbp'.")

sales_prices <- sales_raw %>%
  filter(!is.na(house_id), !is.na(qtr_id)) %>%
  mutate(price_num = coerce_price(.data[[sales_price_col]])) %>%
  filter(is.finite(price_num)) %>%
  group_by(house_id, qtr_id) %>%
  summarise(price_num = median(price_num),
            e = dplyr::first(easting), n = dplyr::first(northing),
            .groups = "drop")

rent_raw <- rio::import(PATH_RENT_PRICE, trust = TRUE)
if (!all(c("rental_id","qtr_id","listing_price","easting","northing") %in% names(rent_raw)))
  stop("Zoopla rentals must have rental_id, qtr_id, listing_price, easting, northing.")

rent_prices <- rent_raw %>%
  filter(!is.na(rental_id), !is.na(qtr_id)) %>%
  mutate(price_num = coerce_price(listing_price)) %>%
  filter(is.finite(price_num)) %>%
  group_by(rental_id, qtr_id) %>%
  summarise(price_num = median(price_num),
            e = dplyr::first(easting), n = dplyr::first(northing),
            .groups = "drop")

# -----------------------------
# Core per-radius processor (generic: sales/rent)
# -----------------------------
process_updown <- function(rad, kind = c("sales","rent")) {
  kind <- match.arg(kind)
  message(sprintf("[%s] Radius %dm: loading within-panel (%s)", toupper(kind), rad, PERIOD_TYPE))

  ds_path <- if (kind == "sales") PATH_WITHIN_SALES else PATH_WITHIN_RENT
  id_col  <- if (kind == "sales") "house_id" else "rental_id"
  price_t <- if (kind == "sales") sales_prices else rent_prices
  ylab    <- if (kind == "sales") "Median Sale Price" else "Median Rent (listing_price)"

  within <- arrow::open_dataset(ds_path) %>%
    filter(radius == rad, period_type == PERIOD_TYPE) %>%
    collect()

  if (!nrow(within)) { message("  → empty panel; skipping."); return(invisible(NULL)) }

  # Keep real transactions; choose nearest site per (id, qtr)
  df <- within %>%
    filter(!is.na(.data[[id_col]]), !is.na(site_id), !is.na(qtr_id), is.finite(distance_m)) %>%
    group_by(.data[[id_col]], qtr_id) %>%
    slice_min(order_by = distance_m, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    left_join(price_t, by = join_by(!!sym(id_col), qtr_id)) %>%
    left_join(spill_sites, by = "site_id") %>%
    filter(is.finite(price_num), is.finite(e), is.finite(n),
           is.finite(site_e), is.finite(site_n)) %>%
    rename(A_e = e, A_n = n) %>%
    mutate(row_id = dplyr::row_number())

  if (!nrow(df)) { message("  → no rows after joins/filters; skipping."); return(invisible(NULL)) }

  # Snap A (house/rental) and site
  A_xy    <- df %>% distinct(!!sym(id_col), e = A_e, n = A_n)
  site_xy <- df %>% distinct(site_id,      e = site_e, n = site_n)
  A_snap    <- snap_points_to_nodes(A_xy, id_col, G$nodes_sf, G$vid_by_key, max_m = SNAP_MAX_M)
  site_snap <- snap_points_to_nodes(site_xy, "site_id", G$nodes_sf, G$vid_by_key, max_m = Inf)

  df2 <- df %>%
    left_join(A_snap,   by = id_col) %>%
    left_join(site_snap, by = "site_id") %>%
    mutate(
      # unify id-specific columns to generic names
      A_vid    = .data[[paste0(id_col, "_vid")]],
      A_snap_m = .data[[paste0(id_col, "_snap_m")]],
      comp      = igraph::V(G$g)$comp[A_vid],
      site_comp = igraph::V(G$g)$comp[site_id_vid]
    ) %>%
    filter(!is.na(A_vid), !is.na(site_id_vid), comp == site_comp)

  if (!nrow(df2)) { message("  → no rows with valid snaps/same component; skipping."); return(invisible(NULL)) }

  # Assign Up/Down relative to the matched site
  rel_df <- assign_relation_vs_site(
    df2 %>% select(row_id, A_vid, site_id_vid, comp),
    g = G$g, g_rev = G$g_rev, river_max_m = RIVER_MAX_M
  )

  df3 <- df2 %>%
    left_join(rel_df, by = "row_id") %>%
    filter(
      is.finite(price_num),
      is.finite(A_snap_m) & A_snap_m <= SNAP_MAX_M,
      is.finite(river_dist_m_to_site),
      spill_relation %in% c("upstream","downstream")
    )

  if (!nrow(df3)) { message("  → no rows with assigned relation; skipping."); return(invisible(NULL)) }

  # Summarise medians — NO winsorisation
  summ <- df3 %>%
    group_by(qtr_id, spill_relation) %>%
    summarise(median_price = median(price_num, na.rm = TRUE),
              n = n(), .groups = "drop")

  if (!nrow(summ)) { message("  → empty summary; skipping."); return(invisible(NULL)) }

  p <- ggplot(summ, aes(qtr_id, median_price, color = spill_relation)) +
    geom_line(linewidth = 1.1) +
    labs(
      title = sprintf("Median %s — Upstream vs Downstream (within-panel, r=%dm)",
                      if (kind == "sales") "Sale Price" else "Rent", rad),
      x = "Quarter", y = ylab, color = "Relation"
    ) +
    theme_minimal()

  outfile <- file.path(OUT_DIR, sprintf("median_updown_within_%s_r%dm.png", kind, rad))
  ggsave(outfile, p, width = 8, height = 5, dpi = 300)
  message("  → wrote: ", outfile)

  invisible(summ)
}

# -----------------------------
# Driver
# -----------------------------
for (rad in RADII) {
  process_updown(rad, "sales")
  process_updown(rad, "rent")
}

message("Done. Plots in: ", OUT_DIR)
# ============================================================
