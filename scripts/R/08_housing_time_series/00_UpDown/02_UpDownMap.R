#!/usr/bin/env Rscript
# ============================================================
# Visual diagnostic (pure absolute paths):
# Find multiple SITE × QUARTER combos with BOTH upstream_of_site and
# downstream_of_site assets, then plot each as a separate PNG.
# - River arrows: blue
# - upstream_of_site: green
# - downstream_of_site: red
# - Avoids slice_sample()/dplyr::n()
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(sf)
  library(igraph)
  library(arrow)
  library(rio)
  library(rlang)
})

# -----------------------------
# Absolute ROOT + paths (EDIT ROOT if needed)
# -----------------------------
ROOT <- "/Users/odran/Dropbox/sewage"

PATH_WITHIN_SALES <- file.path(ROOT, "data/processed/within_radius_panel/sales")
PATH_WITHIN_RENT  <- file.path(ROOT, "data/processed/within_radius_panel/rentals")

PATH_SALES_PRICE  <- file.path(ROOT, "data/processed/house_price.parquet")
PATH_RENT_PRICE   <- file.path(ROOT, "data/processed/zoopla/zoopla_rentals.parquet")

PATH_SPILL_SITES  <- file.path(ROOT, "data/processed/unique_spill_sites.parquet")
PATH_FLOW         <- file.path(ROOT, "data/processed/Rivers", "River_Flow.parquet")

OUT_DIR           <- file.path(ROOT, "output/figures/Diag")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# -----------------------------
# Global options
# -----------------------------
sf::sf_use_s2(FALSE)
options(arrow.use_threads = TRUE, dplyr.summarise.inform = FALSE)

# -----------------------------
# Parameters (tune here)
# -----------------------------
KIND              <- "sales"     # "sales" or "rent"
RADIUS            <- 1000L       # 250/500/1000/2000 etc.
PERIOD_TYPE       <- "quarterly"
SITE_ID           <- NULL        # e.g. 12345; NULL = auto-find several
QUARTER           <- NULL        # e.g. "2019Q1"; NULL = auto-find several
CRS_BNG           <- 27700
SNAP_MAX_M        <- 250
RIVER_MAX_M       <- 10000       # longer cap for viz
PLOT_BUFFER_M     <- 2000
SHOW_PATHS_FOR_N  <- 8           # 0 to disable; drawn in class colors
SEED_FOR_SAMPLING <- 42
MIN_EACH_BOTH     <- 3           # require at least this many upstream & downstream
TOP_K_CANDIDATES  <- 1000        # how many (site,quarter) to scan for matches
NUM_IMAGES        <- 10           # target number of images (set 3..5)

# Colors
COL_RIVER       <- "#1f77b4"  # blue
COL_UPSTREAM    <- "#2ca02c"  # green
COL_DOWNSTREAM  <- "#e41a1c"  # red
REL_COLORS      <- c("upstream_of_site" = COL_UPSTREAM,
                     "downstream_of_site" = COL_DOWNSTREAM)

# -----------------------------
# Helpers
# -----------------------------
coerce_price <- function(x) if (is.numeric(x)) as.numeric(x) else as.numeric(gsub("[^0-9.]+","",as.character(x)))

build_graph <- function(flow_df) {
  flow_df <- flow_df %>%
    filter(is.finite(easting), is.finite(northing),
           is.finite(flow_easting), is.finite(flow_northing)) %>%
    filter(!(easting == flow_easting & northing == flow_northing)) %>%
    mutate(
      from   = paste(easting, northing, sep = "_"),
      to     = paste(flow_easting, flow_northing, sep = "_"),
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

# Classification from the SITE's perspective:
# "downstream_of_site" == reachable along g (with flow) from site -> asset
# "upstream_of_site"   == reachable along g_rev (against flow) from site -> asset
assign_relation_site_to_asset <- function(df2, g, g_rev, river_max_m = Inf) {
  byc <- split(df2, df2$comp)
  out <- vector("list", length(byc)); names(out) <- names(byc)
  for (nm in names(byc)) {
    dsub <- byc[[nm]]
    uS <- unique(dsub$site_id_vid)
    uA <- unique(dsub$A_vid)

    D_ds <- igraph::distances(g,     v = uS, to = uA, mode = "out", weights = igraph::E(g)$weight)
    D_us <- igraph::distances(g_rev, v = uS, to = uA, mode = "out", weights = igraph::E(g_rev)$weight)
    D_ds <- as.matrix(D_ds); D_us <- as.matrix(D_us)

    s_ix <- match(dsub$site_id_vid, uS)
    a_ix <- match(dsub$A_vid,       uA)

    ds <- D_ds[cbind(s_ix, a_ix)]  # site -> asset with flow
    us <- D_us[cbind(s_ix, a_ix)]  # site -> asset against flow

    ds[!is.finite(ds) | ds > river_max_m] <- Inf
    us[!is.finite(us) | us > river_max_m] <- Inf

    pick_down <- is.finite(ds) & (ds <= us | !is.finite(us))
    pick_up   <- is.finite(us) & (us <  ds | !is.finite(ds))

    rel  <- rep(NA_character_, nrow(dsub))
    dist <- rep(NA_real_,       nrow(dsub))
    rel[pick_down]  <- "downstream_of_site"
    rel[pick_up]    <- "upstream_of_site"
    dist[pick_down] <- ds[pick_down]
    dist[pick_up]   <- us[pick_up]

    out[[nm]] <- dplyr::tibble(
      row_id = dsub$row_id,
      spill_relation = rel,
      river_dist_m_to_site = dist
    )
  }
  dplyr::bind_rows(out)
}

flow_to_sf_lines <- function(flow_df) {
  m <- as.matrix(flow_df[, c("easting","northing","flow_easting","flow_northing")])
  lines <- lapply(seq_len(nrow(m)), function(i) {
    xy <- matrix(c(m[i,1], m[i,2], m[i,3], m[i,4]), ncol = 2, byrow = TRUE)
    sf::st_linestring(xy)
  })
  sf::st_as_sf(flow_df, geometry = sf::st_sfc(lines, crs = CRS_BNG))
}

shortest_path_sf <- function(site_vid, asset_vid, relation, g, g_rev) {
  if (relation == "downstream_of_site") {
    path <- tryCatch(igraph::shortest_paths(g, from = site_vid, to = asset_vid, output = "vpath")$vpath[[1]], error = function(e) NULL)
  } else if (relation == "upstream_of_site") {
    path <- tryCatch(igraph::shortest_paths(g_rev, from = site_vid, to = asset_vid, output = "vpath")$vpath[[1]], error = function(e) NULL)
  } else {
    path <- NULL
  }
  if (is.null(path) || length(path) < 2) return(NULL)
  node_keys <- igraph::V(g)$name[as.integer(path)]
  coords <- do.call(rbind, strsplit(node_keys, "_", fixed = TRUE))
  storage.mode(coords) <- "double"
  geom <- sf::st_sfc(sf::st_linestring(coords), crs = CRS_BNG)
  sf::st_as_sf(data.frame(site_vid = site_vid, asset_vid = asset_vid, relation = relation), geometry = geom)
}

# -----------------------------
# Load data
# -----------------------------
stopifnot(dir.exists(PATH_WITHIN_SALES), dir.exists(PATH_WITHIN_RENT))
stopifnot(file.exists(PATH_SPILL_SITES), file.exists(PATH_FLOW))

spill_sites <- arrow::read_parquet(PATH_SPILL_SITES) %>%
  select(site_id, easting, northing) %>%
  rename(site_e = easting, site_n = northing) %>%
  filter(is.finite(site_e), is.finite(site_n))

flow_df <- arrow::read_parquet(PATH_FLOW,
  col_select = c("easting","northing","flow_easting","flow_northing"))

G <- build_graph(flow_df)
flow_sf <- flow_to_sf_lines(flow_df)

# Price tables (for asset coordinates)
sales_prices <- NULL; rent_prices <- NULL
if (KIND == "sales") {
  sales_raw <- rio::import(PATH_SALES_PRICE, trust = TRUE)
  price_col <- c("price","price_gbp")[c("price","price_gbp") %in% names(sales_raw)][1]
  if (is.na(price_col)) stop("Sales price file must have 'price' or 'price_gbp'.")
  sales_prices <- sales_raw %>%
    filter(!is.na(house_id), !is.na(qtr_id)) %>%
    mutate(price_num = coerce_price(.data[[price_col]])) %>%
    filter(is.finite(price_num)) %>%
    group_by(house_id, qtr_id) %>%
    summarise(price_num = median(price_num),
              e = dplyr::first(easting), n = dplyr::first(northing),
              .groups = "drop")
} else {
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
}

# Within-panel
ds_path <- if (KIND == "sales") PATH_WITHIN_SALES else PATH_WITHIN_RENT
within <- arrow::open_dataset(ds_path) %>%
  filter(radius == RADIUS, period_type == PERIOD_TYPE) %>%
  collect()
if (!nrow(within)) stop("No within-panel rows for this radius/period_type.")

id_col  <- if (KIND == "sales") "house_id" else "rental_id"
price_t <- if (KIND == "sales") sales_prices else rent_prices

# Prepare: pick nearest site per id×qtr + attach coords
df <- within %>%
  filter(!is.na(.data[[id_col]]), !is.na(site_id), !is.na(qtr_id), is.finite(distance_m)) %>%
  group_by(.data[[id_col]], qtr_id) %>%
  slice_min(order_by = distance_m, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  left_join(price_t, by = join_by(!!sym(id_col), qtr_id)) %>%
  left_join(spill_sites, by = "site_id") %>%
  filter(is.finite(e), is.finite(n), is.finite(site_e), is.finite(site_n)) %>%
  rename(A_e = e, A_n = n) %>%
  mutate(row_id = dplyr::row_number())
if (!nrow(df)) stop("No rows after joins/filters.")

# Snap assets and sites to river nodes; keep same component
A_xy    <- df %>% distinct(!!sym(id_col), e = A_e, n = A_n)
site_xy <- df %>% distinct(site_id,      e = site_e, n = site_n)
A_snap    <- snap_points_to_nodes(A_xy, id_col, G$nodes_sf, G$vid_by_key, max_m = SNAP_MAX_M)
site_snap <- snap_points_to_nodes(site_xy, "site_id", G$nodes_sf, G$vid_by_key, max_m = Inf)

df2 <- df %>%
  left_join(A_snap,   by = id_col) %>%
  left_join(site_snap, by = "site_id") %>%
  mutate(
    A_vid    = .data[[paste0(id_col, "_vid")]],
    A_snap_m = .data[[paste0(id_col, "_snap_m")]],
    comp      = igraph::V(G$g)$comp[A_vid],
    site_comp = igraph::V(G$g)$comp[site_id_vid]
  ) %>%
  filter(!is.na(A_vid), !is.na(site_id_vid), comp == site_comp,
         is.finite(A_snap_m) & A_snap_m <= SNAP_MAX_M)
if (!nrow(df2)) stop("No rows with valid snaps and same component.")

# -----------------------------
# Choose multiple (SITE × QUARTER) with BOTH classes
# -----------------------------
has_both_counts <- function(sub, g, g_rev, river_max_m) {
  rel <- assign_relation_site_to_asset(sub %>% select(row_id, A_vid, site_id_vid, comp), g, g_rev, river_max_m)
  wide <- sub %>% left_join(rel, by = "row_id") %>% filter(!is.na(spill_relation)) %>%
    count(spill_relation) %>% tidyr::pivot_wider(names_from = spill_relation, values_from = n, values_fill = 0)
  up   <- wide[["upstream_of_site"]]; down <- wide[["downstream_of_site"]]
  c(up = ifelse(is.null(up), 0L, up), down = ifelse(is.null(down), 0L, down))
}

find_combos_with_both <- function(df2, min_each, top_k, g, g_rev, river_max_m, want_k,
                                  one_per_site = TRUE) {
  out <- list()
  tried_keys <- character(0)
  used_sites <- integer(0)

  # if user specified a combo, try to include it first
  if (!is.null(SITE_ID) && !is.null(QUARTER)) {
    sub <- df2 %>% dplyr::filter(site_id == SITE_ID, qtr_id == QUARTER)
    if (nrow(sub)) {
      cnt <- has_both_counts(sub, g, g_rev, river_max_m)
      if (cnt["up"] >= min_each && cnt["down"] >= min_each) {
        out[[length(out) + 1]] <- list(site_id = SITE_ID, qtr_id = QUARTER)
        tried_keys <- c(tried_keys, paste(SITE_ID, QUARTER, sep = "_"))
        if (one_per_site) used_sites <- c(used_sites, SITE_ID)
      }
    }
  }

  cands <- df2 %>%
    dplyr::count(site_id, qtr_id, name = "n_pairs") %>%
    dplyr::arrange(dplyr::desc(n_pairs)) %>%
    dplyr::slice_head(n = top_k)

  for (i in seq_len(nrow(cands))) {
    sid <- cands$site_id[i]; qtr <- cands$qtr_id[i]
    key <- paste(sid, qtr, sep = "_")
    if (key %in% tried_keys) next
    if (one_per_site && sid %in% used_sites) next

    sub <- df2 %>% dplyr::filter(site_id == sid, qtr_id == qtr)
    cnt <- has_both_counts(sub, g, g_rev, river_max_m)
    if (cnt["up"] >= min_each && cnt["down"] >= min_each) {
      out[[length(out) + 1]] <- list(site_id = sid, qtr_id = qtr)
      tried_keys <- c(tried_keys, key)
      if (one_per_site) used_sites <- c(used_sites, sid)
      if (length(out) >= want_k) break
    }
  }

  if (!length(out)) {
    stop("Could not find any SITE × QUARTER with both classes. ",
         "Try increasing RADIUS or RIVER_MAX_M, or lowering MIN_EACH_BOTH.")
  }
  out
}

find_combos_with_both <- function(df2, min_each, top_k, g, g_rev, river_max_m, want_k,
                                  one_per_site = TRUE) {
  out <- list()
  tried_keys <- character(0)
  used_sites <- integer(0)

  # if user specified a combo, try to include it first
  if (!is.null(SITE_ID) && !is.null(QUARTER)) {
    sub <- df2 %>% dplyr::filter(site_id == SITE_ID, qtr_id == QUARTER)
    if (nrow(sub)) {
      cnt <- has_both_counts(sub, g, g_rev, river_max_m)
      if (cnt["up"] >= min_each && cnt["down"] >= min_each) {
        out[[length(out) + 1]] <- list(site_id = SITE_ID, qtr_id = QUARTER)
        tried_keys <- c(tried_keys, paste(SITE_ID, QUARTER, sep = "_"))
        if (one_per_site) used_sites <- c(used_sites, SITE_ID)
      }
    }
  }

  cands <- df2 %>%
    dplyr::count(site_id, qtr_id, name = "n_pairs") %>%
    dplyr::arrange(dplyr::desc(n_pairs)) %>%
    dplyr::slice_head(n = top_k)

  for (i in seq_len(nrow(cands))) {
    sid <- cands$site_id[i]; qtr <- cands$qtr_id[i]
    key <- paste(sid, qtr, sep = "_")
    if (key %in% tried_keys) next
    if (one_per_site && sid %in% used_sites) next

    sub <- df2 %>% dplyr::filter(site_id == sid, qtr_id == qtr)
    cnt <- has_both_counts(sub, g, g_rev, river_max_m)
    if (cnt["up"] >= min_each && cnt["down"] >= min_each) {
      out[[length(out) + 1]] <- list(site_id = sid, qtr_id = qtr)
      tried_keys <- c(tried_keys, key)
      if (one_per_site) used_sites <- c(used_sites, sid)
      if (length(out) >= want_k) break
    }
  }

  if (!length(out)) {
    stop("Could not find any SITE × QUARTER with both classes. ",
         "Try increasing RADIUS or RIVER_MAX_M, or lowering MIN_EACH_BOTH.")
  }
  out
}


combos <- find_combos_with_both(df2,
                                min_each = MIN_EACH_BOTH,
                                top_k = TOP_K_CANDIDATES,
                                g = G$g, g_rev = G$g_rev,
                                river_max_m = RIVER_MAX_M,
                                want_k = NUM_IMAGES,
                                one_per_site = TRUE)

message(sprintf("Generating %d map(s) with both classes...", length(combos)))

# -----------------------------
# Plot helper (one figure)
# -----------------------------
make_one_plot <- function(idx, sid, qtr) {
  # subset + classify
  df2_sub <- df2 %>% filter(site_id == sid, qtr_id == qtr)
  rel_df <- assign_relation_site_to_asset(
    df2_sub %>% select(row_id, A_vid, site_id_vid, comp),
    g = G$g, g_rev = G$g_rev, river_max_m = RIVER_MAX_M
  )
  df3 <- df2_sub %>%
    left_join(rel_df, by = "row_id") %>%
    filter(spill_relation %in% c("upstream_of_site","downstream_of_site"),
           is.finite(river_dist_m_to_site))

  if (!nrow(df3)) return(invisible(FALSE))

  # geometries
  site_row <- spill_sites %>% filter(site_id == sid) %>% slice(1)
  site_pt  <- sf::st_as_sf(site_row, coords = c("site_e","site_n"), crs = CRS_BNG)
  assets_sf <- sf::st_as_sf(df3, coords = c("A_e","A_n"), crs = CRS_BNG)
  win_poly <- sf::st_buffer(site_pt, dist = PLOT_BUFFER_M)

  # crop flow (midpoint heuristic)
  flow_seg_win <- flow_df %>%
    mutate(x = easting, y = northing, xend = flow_easting, yend = flow_northing,
           mx = (x + xend)/2, my = (y + yend)/2)
  mids <- sf::st_as_sf(flow_seg_win, coords = c("mx","my"), crs = CRS_BNG)
  keep <- sf::st_intersects(mids, win_poly, sparse = FALSE)[,1]
  flow_seg_win <- flow_seg_win[keep, c("x","y","xend","yend")]

  # optional shortest paths (sample with base R; color by relation)
  set.seed(SEED_FOR_SAMPLING + idx)  # vary per figure
  paths_sf <- NULL
  if (SHOW_PATHS_FOR_N > 0) {
    n_each <- max(1, SHOW_PATHS_FOR_N %/% 2)
    df_up   <- df3 %>% filter(spill_relation == "upstream_of_site")
    df_down <- df3 %>% filter(spill_relation == "downstream_of_site")
    pick <- function(nn, k) {
      if (nn <= 0 || k <= 0) return(integer(0))
      if (k >= nn) return(seq_len(nn))
      sample.int(nn, size = k, replace = FALSE)
    }
    idx_up   <- pick(nrow(df_up),   n_each)
    idx_down <- pick(nrow(df_down), n_each)
    df_paths <- bind_rows(
      if (length(idx_up))   df_up[idx_up, , drop = FALSE]   else NULL,
      if (length(idx_down)) df_down[idx_down, , drop = FALSE] else NULL
    )
    if (nrow(df_paths) > 0) {
      plist <- lapply(seq_len(nrow(df_paths)), function(i) {
        # returns 'relation' column with same values as spill_relation
        shortest_path_sf(
          site_vid  = df_paths$site_id_vid[i],
          asset_vid = df_paths$A_vid[i],
          relation  = df_paths$spill_relation[i],
          g = G$g, g_rev = G$g_rev
        )
      })
      plist <- Filter(Negate(is.null), plist)
      if (length(plist)) paths_sf <- do.call(rbind, plist)
    }
  }

  # plot
  p <- ggplot() +
    # River flow with BLUE arrows
    geom_segment(
      data = flow_seg_win,
      aes(x = x, y = y, xend = xend, yend = yend),
      linewidth = 0.25, alpha = 0.7, color = COL_RIVER,
      arrow = grid::arrow(length = grid::unit(2, "mm"), type = "closed")
    ) +
    # Shortest paths (color by relation)
    { if (!is.null(paths_sf)) geom_sf(data = paths_sf, aes(color = relation), linewidth = 1, alpha = 0.9) else NULL } +
    # Assets colored by relation
    geom_sf(data = assets_sf, aes(color = spill_relation), size = 2.4) +
    # Site marker
    geom_sf(data = site_pt, shape = 8, size = 4, stroke = 1.2, color = "black") +
    coord_sf(crs = sf::st_crs(CRS_BNG)) +
    scale_color_manual(values = REL_COLORS, breaks = c("downstream_of_site","upstream_of_site"),
                       labels = c("Downstream of site", "Upstream of site"), name = "Relation") +
    labs(
      title = sprintf("Site %s — %s, %s (r = %dm)", as.character(sid), toupper(KIND), as.character(qtr), RADIUS),
      subtitle = "Blue arrows show river flow. Colors show classification relative to the SITE.\nBold lines are sample shortest paths along the network.",
      x = "Easting (m, BNG)", y = "Northing (m, BNG)"
    ) +
    theme_minimal(base_size = 12) +
    theme(legend.position = "bottom")

  outfile <- file.path(OUT_DIR, sprintf("updown_map_MULTI_%s_site%s_q%s_r%d_%02d.png",
                                        KIND, as.character(sid), as.character(qtr), RADIUS, idx))
  ggsave(outfile, p, width = 9, height = 7, dpi = 300)
  message("Wrote: ", outfile)
  TRUE
}

# -----------------------------
# Render all requested images
# -----------------------------
for (i in seq_len(length(combos))) {
  if (i > NUM_IMAGES) break
  ok <- make_one_plot(i, combos[[i]]$site_id, combos[[i]]$qtr_id)
  if (!ok) message("Skipped index ", i, " due to empty subset.")
}

message("Done. Figures in: ", OUT_DIR)
