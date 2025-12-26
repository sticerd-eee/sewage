## 14_FinalFinal.R (clean, fixed, NO lwgeom dependency)
## Link sales houses to nearby spill sites along rivers, and build Up/Down tags.
##
## Key fixes vs your original:
##  - Uses geometric segment lengths (st_length) for network distance.
##  - Builds full downstream/upstream adjacency (all branches) from startNode/endNode.
##  - Labels Up/Down by directed reachability within ALONG_MAX_M (not nbrs[1]).
##  - Labels same-segment Up/Down by a robust sampling-based locate on THAT segment
##    (avoids merged-line orientation AND avoids lwgeom version issues).
##  - Keeps local merged LINESTRING for lateral distance + |along| filtering.
##  - Diagnostic plot for spill site_id == 10.
##  - Outputs HH_E/HH_N in final.

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(sf)
  library(ggplot2)
  library(data.table)
})

ROOT <- here::here()

path_spill       <- here("data", "processed", "unique_spill_sites.parquet")
path_sales       <- here("data", "processed", "house_price.parquet")
path_watercourse <- file.path(ROOT, "data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp")

crs_bng <- 27700  # OSGB36 / British National Grid
sf_use_s2(FALSE)  # projected coordinates; use GEOS

# -------------------------------------------------------------------
# 0. Read river layer and inspect attributes once
# -------------------------------------------------------------------
rivers_raw <- st_read(path_watercourse, quiet = TRUE)

cat("=== DEBUG: river attribute names as seen by R ===\n")
print(names(rivers_raw))

cat("\n=== DEBUG: first 5 rows of river attributes (no geometry) ===\n")
print(head(st_drop_geometry(rivers_raw), 5))
cat("==============================================\n\n")

# Rename to standard network names (segment id + node ids)
rivers <- rivers_raw %>%
  rename(
    id         = identifier,
    start_node = startNode,
    end_node   = endNode
  )

# -------------------------------------------------------------------
# Parameters
# -------------------------------------------------------------------
N_SPILLS_TEST  <- 50     # use first 50 spills as "universe" for testing
CAND_RADIUS_M  <- 1500   # Euclidean radius around spill to preselect houses
ALONG_MAX_M    <- 500    # ±500 m along network from spill (reachability window)
LATERAL_MAX_M  <- 500    # ≤500 m from merged local river line
N_SAMPLES_LINE <- 1001   # resolution for merged-line along-position approximation

# For SAME-SEGMENT ordering we can use fewer samples (cheaper; segments are short)
N_SAMPLES_SEG  <- 401

# radius to select local river network around each spill
RIVER_SEARCH_RADIUS_M <- max(CAND_RADIUS_M, ALONG_MAX_M) * 2  # e.g. 3000 m

# -------------------------------------------------------------------
# 1. Load data
# -------------------------------------------------------------------
cat("=== Step 1: Load data ===\n")
spill <- read_parquet(path_spill)
sales <- read_parquet(path_sales)

cat("Spill rows : ", nrow(spill),  "\n", sep = "")
cat("Sales rows : ", nrow(sales),  "\n", sep = "")
cat("Rivers rows: ", nrow(rivers), "\n\n", sep = "")

# -------------------------------------------------------------------
# 2. Convert to sf and prep rivers network
# -------------------------------------------------------------------
cat("=== Step 2: Convert to sf ===\n")

spill_sf_full <- spill %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(coords = c("easting", "northing"), crs = crs_bng, remove = FALSE)

n_spills_total <- nrow(spill_sf_full)
n_spills_use   <- min(N_SPILLS_TEST, n_spills_total)

spills_sf <- spill_sf_full %>% slice(1:n_spills_use)
cat("Using first ", n_spills_use, " spills (of ", n_spills_total, " total).\n", sep = "")

sales_sf <- sales %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(coords = c("easting", "northing"), crs = crs_bng, remove = FALSE)
cat("Sales with valid coords: ", nrow(sales_sf), "\n", sep = "")

# Rivers to BNG, drop Z/M
rivers_2d <- st_zm(rivers, drop = TRUE, what = "ZM")
if (st_crs(rivers_2d)$epsg != crs_bng) {
  rivers_2d <- st_transform(rivers_2d, crs_bng)
}
cat("Rivers CRS: ", st_crs(rivers_2d)$epsg, "\n", sep = "")

# Check required network columns
required_cols <- c("id", "start_node", "end_node")
missing_cols  <- setdiff(required_cols, names(rivers_2d))
if (length(missing_cols) > 0) {
  stop("River layer is missing required columns: ",
       paste(missing_cols, collapse = ", "))
}

# Extract network attributes
river_ids <- rivers_2d[["id"]]
start_ids <- rivers_2d[["start_node"]]
end_ids   <- rivers_2d[["end_node"]]

# Use geometric length for ALL segments (meters)
seg_len <- as.numeric(st_length(st_geometry(rivers_2d)))
n_segs  <- nrow(rivers_2d)

cat("Network attributes ready. Using geometric segment lengths.\n\n")

# -------------------------------------------------------------------
# 2b. Build adjacency (all branches) once from start/end nodes
# Edge A -> B iff end_node(A) == start_node(B)
# -------------------------------------------------------------------
cat("=== Step 2b: Build directed adjacency (all branches) ===\n")

seg_dt <- data.table(
  seg = 1:n_segs,
  start_node = start_ids,
  end_node   = end_ids
)[!is.na(start_node) & !is.na(end_node)]

setkey(seg_dt, start_node)

edges_dt <- seg_dt[seg_dt, on = .(start_node = end_node),
                   nomatch = 0L, allow.cartesian = TRUE]

# Correct direction: i.seg (A) -> seg (B)
from <- as.integer(edges_dt$i.seg)
to   <- as.integer(edges_dt$seg)

adj_down <- split(to, from)  # downstream neighbors per segment
adj_up   <- split(from, to)  # upstream neighbors per segment (reverse)

# Fill missing with empty vectors for safe indexing
adj_down_full <- vector("list", n_segs)
adj_up_full   <- vector("list", n_segs)
adj_down_full[as.integer(names(adj_down))] <- adj_down
adj_up_full[as.integer(names(adj_up))]     <- adj_up
adj_down <- lapply(adj_down_full, function(x) if (is.null(x)) integer(0) else as.integer(x))
adj_up   <- lapply(adj_up_full,   function(x) if (is.null(x)) integer(0) else as.integer(x))

rm(seg_dt, edges_dt, from, to, adj_down_full, adj_up_full)

cat("Adjacency ready. (Segments: ", n_segs, ")\n\n", sep = "")

# -------------------------------------------------------------------
# 3. Nearest river segment for each spill
# -------------------------------------------------------------------
cat("=== Step 3: Nearest river segment for each spill ===\n")
t_nn <- system.time({
  nn_spill <- st_nearest_feature(spills_sf, rivers_2d)
})
cat("Nearest rivers computed in ", round(t_nn["elapsed"], 2), " seconds.\n\n", sep = "")

# -------------------------------------------------------------------
# Helper: ensure a single LINESTRING geometry (pick closest if multiple)
# -------------------------------------------------------------------
as_single_linestring <- function(line_geom, ref_pt) {
  # line_geom: sfc_LINESTRING or sfc_MULTILINESTRING (length 1)
  if (inherits(line_geom, "sfc_LINESTRING") && length(line_geom) == 1L) return(line_geom)

  # cast to LINESTRINGs, pick nearest to ref point
  ls <- st_cast(line_geom, "LINESTRING")
  if (length(ls) == 0L) return(NULL)
  if (length(ls) == 1L) return(ls)

  d <- st_distance(ref_pt, ls)
  best <- which.min(as.numeric(d[1, ]))
  ls[best]
}

# -------------------------------------------------------------------
# Helper: approximate fractional position on a LINESTRING by sampling
# -------------------------------------------------------------------
approx_fraction_on_line <- function(line_geom, pts, n_samples) {
  # line_geom: sfc_LINESTRING length 1
  # pts: sfc_POINT length N (or 1)
  samples    <- st_line_sample(line_geom, sample = seq(0, 1, length.out = n_samples))
  samples_pt <- st_cast(samples, "POINT")

  dmat <- st_distance(samples_pt, pts)
  dmat <- as.matrix(dmat)

  if (ncol(dmat) == 1L) {
    idx_min <- which.min(as.numeric(dmat[, 1]))
  } else {
    idx_min <- apply(dmat, 2, function(col) which.min(as.numeric(col)))
  }

  (idx_min - 1) / (n_samples - 1)
}

# -------------------------------------------------------------------
# Helper: build a local merged river LINESTRING around a spill
# -------------------------------------------------------------------
build_local_river_line <- function(spill_point, rivers, river_idx0,
                                   search_radius_m = RIVER_SEARCH_RADIUS_M) {
  idx_list <- st_is_within_distance(spill_point, rivers, dist = search_radius_m)
  idx <- sort(unique(unlist(idx_list)))

  if (length(idx) == 0L && !is.na(river_idx0) && river_idx0 > 0L) idx <- river_idx0
  if (length(idx) == 0L) return(NULL)

  geom_union <- st_union(st_geometry(rivers[idx, ]))
  geom_multi <- st_cast(geom_union, "MULTILINESTRING")
  merged     <- st_line_merge(geom_multi)

  # may yield LINESTRING or MULTILINESTRING; cast below
  merged_ls  <- st_cast(merged, "LINESTRING")
  if (length(merged_ls) == 0L) return(NULL)
  if (length(merged_ls) == 1L) return(merged_ls)

  dists <- st_distance(spill_point, merged_ls)
  best_idx <- which.min(as.numeric(dists[1, ]))
  merged_ls[best_idx]
}

# -------------------------------------------------------------------
# Helper: multi-branch, distance-limited reachability on segment graph
# cost increment = length of NEXT segment (meters)
# -------------------------------------------------------------------
reachable_within <- function(start_seg, max_dist, adj, seg_len) {
  n <- length(seg_len)
  best <- rep(Inf, n)
  best[start_seg] <- 0

  open <- start_seg
  in_open <- rep(FALSE, n)
  in_open[start_seg] <- TRUE

  while (length(open) > 0) {
    current <- open[which.min(best[open])]
    open <- open[open != current]
    in_open[current] <- FALSE

    nbrs <- adj[[current]]
    if (length(nbrs) == 0L) next

    for (nbr in nbrs) {
      nd <- best[current] + seg_len[nbr]
      if (nd <= max_dist && nd < best[nbr]) {
        best[nbr] <- nd
        if (!in_open[nbr]) {
          open <- c(open, nbr)
          in_open[nbr] <- TRUE
        }
      }
    }
  }

  which(is.finite(best) & best > 0 & best <= max_dist)
}

# -------------------------------------------------------------------
# Helper: same-segment Up/Down using sampling on THAT segment geometry
# -------------------------------------------------------------------
label_same_segment <- function(center_row, spill_sf, hh_sf_same, rivers_2d,
                               n_samples_seg = N_SAMPLES_SEG) {
  seg_geom0 <- st_geometry(rivers_2d[center_row, ])
  seg_geom  <- as_single_linestring(seg_geom0, st_geometry(spill_sf))
  if (is.null(seg_geom)) return(rep(NA_character_, nrow(hh_sf_same)))

  t_spill <- approx_fraction_on_line(seg_geom, st_geometry(spill_sf), n_samples = n_samples_seg)

  # if only 1 house, approx_fraction_on_line returns length-1 numeric; otherwise vector
  t_hh <- approx_fraction_on_line(seg_geom, st_geometry(hh_sf_same), n_samples = n_samples_seg)

  ifelse(t_hh >= t_spill, "downstream", "upstream")
}

# -------------------------------------------------------------------
# 4. Loop over spills and construct house–spill pairs
# -------------------------------------------------------------------
cat("\n=== Step 4: Loop over spills and link houses ===\n")

pairs_list <- vector("list", nrow(spills_sf))

for (i in seq_len(nrow(spills_sf))) {
  sp <- spills_sf[i, ]
  sewage_id <- sp$site_id

  river_idx0 <- nn_spill[i]
  cat("Spill ", i, " / ", nrow(spills_sf),
      " - site_id: ", sewage_id,
      " - nearest river_idx: ", river_idx0, "\n", sep = "")

  if (is.na(river_idx0) || river_idx0 < 1L) {
    cat("  -> Invalid nearest river_idx; skipping.\n\n")
    next
  }
  center_row <- river_idx0

  # local merged line for distance + |along| filtering
  t_build <- system.time({
    line_geom <- build_local_river_line(sp, rivers_2d, river_idx0,
                                        search_radius_m = RIVER_SEARCH_RADIUS_M)
  })
  if (is.null(line_geom)) {
    cat("  -> Could not build local river line; skipping.\n\n")
    next
  }
  if (!(inherits(line_geom, "sfc_LINESTRING") && length(line_geom) == 1L)) {
    cat("  -> Local river geometry is not a single LINESTRING; skipping.\n\n")
    next
  }

  line_len <- as.numeric(st_length(line_geom))
  cat("  Local river length (m): ", round(line_len, 1),
      " (built in ", round(t_build["elapsed"], 2), " s)\n", sep = "")
  if (!is.finite(line_len) || line_len <= 0) {
    cat("  -> Zero/invalid river length; skipping.\n\n")
    next
  }

  # Spill position along merged local line (ONLY for |River_Len| filtering)
  t_spill   <- approx_fraction_on_line(line_geom, st_geometry(sp), n_samples = N_SAMPLES_LINE)
  pos_spill <- t_spill * line_len
  cat("  Spill t_on_line: ", round(t_spill, 4),
      " -> pos_spill (m): ", round(pos_spill, 1), "\n", sep = "")

  # Directed reachability sets (segment IDs) within ALONG_MAX_M
  down_rows <- reachable_within(center_row, ALONG_MAX_M, adj_down, seg_len)
  up_rows   <- reachable_within(center_row, ALONG_MAX_M, adj_up,   seg_len)
  down_ids  <- river_ids[down_rows]
  up_ids    <- river_ids[up_rows]

  # Candidate houses within Euclidean radius
  t_cand <- system.time({
    idx_list <- st_is_within_distance(sp, sales_sf, dist = CAND_RADIUS_M)
  })
  cand_idx <- unique(unlist(idx_list))
  cand_n   <- length(cand_idx)
  cat("  Candidate houses within ", CAND_RADIUS_M, " m: ",
      cand_n, " (t = ", round(t_cand["elapsed"], 2), " s)\n", sep = "")

  if (cand_n == 0L) {
    cat("  -> No candidate houses; skip spill.\n\n")
    next
  }

  hh_pts <- sales_sf[cand_idx, ]

  # Nearest segment per house (for UpDown tagging)
  hh_seg_idx <- st_nearest_feature(hh_pts, rivers_2d)
  hh_seg_id  <- river_ids[hh_seg_idx]

  # Lateral distance to merged local line
  t_dist <- system.time({
    dist_mat       <- st_distance(hh_pts, line_geom)
    Dist_River_vec <- as.numeric(dist_mat[, 1])
  })
  cat("  House->river distances computed (t = ",
      round(t_dist["elapsed"], 2), " s).\n", sep = "")

  # House position along merged local line (ONLY for |River_Len| filtering)
  t_pos <- system.time({
    t_hh   <- approx_fraction_on_line(line_geom, st_geometry(hh_pts), n_samples = N_SAMPLES_LINE)
    pos_hh <- t_hh * line_len
  })
  cat("  House positions along river computed (t = ",
      round(t_pos["elapsed"], 2), " s).\n", sep = "")

  River_Len_vec <- pos_hh - pos_spill  # sign NOT interpreted

  cat("  River_Len range (m): ",
      paste(round(range(River_Len_vec, na.rm = TRUE), 1), collapse = " to "),
      "\n", sep = "")
  cat("  Dist_River range (m): ",
      paste(round(range(Dist_River_vec, na.rm = TRUE), 1), collapse = " to "),
      "\n", sep = "")

  keep   <- (abs(River_Len_vec) <= ALONG_MAX_M) & (Dist_River_vec <= LATERAL_MAX_M)
  n_keep <- sum(keep, na.rm = TRUE)
  cat("  Houses passing filters (|River_Len| <= ", ALONG_MAX_M,
      ", Dist_River <= ", LATERAL_MAX_M, "): ", n_keep, "\n", sep = "")

  if (n_keep == 0L) {
    cat("  -> No houses kept for this spill.\n\n")
    next
  }

  hh_keep         <- hh_pts[keep, ]
  hh_seg_idx_keep <- hh_seg_idx[keep]
  hh_seg_id_keep  <- hh_seg_id[keep]
  River_Len_keep  <- River_Len_vec[keep]
  Dist_River_keep <- Dist_River_vec[keep]

  # Up/Down assignment
  UpDown_keep <- rep(NA_character_, length(hh_seg_id_keep))

  # Same segment: sampling-based locate on that segment geometry
  same_seg <- hh_seg_idx_keep == center_row
  if (any(same_seg)) {
    UpDown_keep[same_seg] <- label_same_segment(
      center_row = center_row,
      spill_sf   = sp,
      hh_sf_same = hh_keep[same_seg, ],
      rivers_2d  = rivers_2d,
      n_samples_seg = N_SAMPLES_SEG
    )
  }

  # Different segment: directed reachability within ALONG_MAX_M
  is_unknown <- is.na(UpDown_keep)
  if (any(is_unknown) && length(down_ids) > 0L) {
    in_down <- hh_seg_id_keep[is_unknown] %in% down_ids
    if (any(in_down)) UpDown_keep[which(is_unknown)[in_down]] <- "downstream"
  }

  is_unknown <- is.na(UpDown_keep)
  if (any(is_unknown) && length(up_ids) > 0L) {
    in_up <- hh_seg_id_keep[is_unknown] %in% up_ids
    if (any(in_up)) UpDown_keep[which(is_unknown)[in_up]] <- "upstream"
  }

  cat("  UpDown NA share among kept houses: ",
      round(mean(is.na(UpDown_keep)), 3), "\n", sep = "")

  pairs_list[[i]] <- tibble(
    HH_ID      = hh_keep$house_id,
    HH_E       = hh_keep$easting,
    HH_N       = hh_keep$northing,
    Sewage_ID  = sewage_id,
    River_Len  = River_Len_keep,
    Dist_River = Dist_River_keep,
    UpDown     = UpDown_keep
  )

  cat("  -> Added ", nrow(pairs_list[[i]]), " rows to pairs.\n\n", sep = "")
}

pairs_sales <- bind_rows(pairs_list)

if (nrow(pairs_sales) == 0L) {
  pairs_sales <- tibble(
    HH_ID      = integer(),
    HH_E       = double(),
    HH_N       = double(),
    Sewage_ID  = integer(),
    River_Len  = double(),
    Dist_River = double(),
    UpDown     = character()
  )
  cat("=== RESULT: pairs_sales is EMPTY (0 rows). ===\n")
} else {
  cat("=== RESULT: pairs_sales has ", nrow(pairs_sales), " rows. ===\n", sep = "")
}

cat("\nExample rows (up to 20):\n")
print(head(pairs_sales, 20))

# -------------------------------------------------------------------
# 5. Diagnostic visualisation for one spill (site_id == 10)
# -------------------------------------------------------------------
site_to_plot <- 10L
cat("\n=== Step 5: Diagnostic plot for spill site_id == ", site_to_plot, " ===\n", sep = "")

sp_plot <- spills_sf %>% filter(site_id == site_to_plot)

if (nrow(sp_plot) != 1L) {
  cat("No unique spill with site_id == ", site_to_plot, " found in spills_sf; skipping plot.\n", sep = "")
} else {
  i_plot     <- which(spills_sf$site_id == site_to_plot)[1]
  river_idx0 <- nn_spill[i_plot]

  if (is.na(river_idx0) || river_idx0 < 1L) {
    cat("  -> Invalid nearest river_idx for plot spill; skipping plot.\n")
  } else {
    line_geom <- build_local_river_line(sp_plot, rivers_2d, river_idx0,
                                        search_radius_m = RIVER_SEARCH_RADIUS_M)
    if (is.null(line_geom)) {
      cat("  -> Could not build local river line for plot; skipping plot.\n")
    } else {
      line_len <- as.numeric(st_length(line_geom))
      t_spill  <- approx_fraction_on_line(line_geom, st_geometry(sp_plot), n_samples = N_SAMPLES_LINE)
      pos_spill <- t_spill * line_len

      idx_list <- st_is_within_distance(sp_plot, sales_sf, dist = CAND_RADIUS_M)
      cand_idx <- unique(unlist(idx_list))

      if (length(cand_idx) == 0L) {
        cat("  -> No candidate houses near plot spill; skipping plot.\n")
      } else {
        hh_pts <- sales_sf[cand_idx, ]

        dist_mat       <- st_distance(hh_pts, line_geom)
        Dist_River_vec <- as.numeric(dist_mat[, 1])

        t_hh   <- approx_fraction_on_line(line_geom, st_geometry(hh_pts), n_samples = N_SAMPLES_LINE)
        pos_hh <- t_hh * line_len
        River_Len_vec <- pos_hh - pos_spill

        keep <- (abs(River_Len_vec) <= ALONG_MAX_M) & (Dist_River_vec <= LATERAL_MAX_M)
        if (!any(keep, na.rm = TRUE)) {
          cat("  -> No houses pass filters for plot spill; skipping plot.\n")
        } else {
          hh_keep <- hh_pts[keep, ]
          hh_keep$River_Len  <- round(River_Len_vec[keep], 1)
          hh_keep$Dist_River <- round(Dist_River_vec[keep], 1)

          # attach UpDown from pairs_sales using HH_ID only
          pairs_10 <- pairs_sales %>% filter(Sewage_ID == site_to_plot)
          if (nrow(pairs_10) > 0) {
            hh_keep <- hh_keep %>%
              left_join(pairs_10 %>% select(HH_ID, UpDown),
                        by = c("house_id" = "HH_ID"))
          }

          perp_segments <- st_nearest_points(hh_keep, line_geom)
          perp_sf <- st_sf(type = "house_link", geometry = perp_segments)

          spill_seg <- st_nearest_points(sp_plot, line_geom)
          spill_seg_sf <- st_sf(type = "spill_link", geometry = spill_seg)

          line_sf <- st_sf(type = "river", geometry = line_geom)

          p <- ggplot() +
            geom_sf(data = line_sf) +
            geom_sf(data = perp_sf, linetype = "dashed") +
            geom_sf(data = spill_seg_sf, linetype = "solid") +
            geom_sf(data = sp_plot, colour = "red", size = 3, shape = 4) +
            geom_sf(data = hh_keep, aes(colour = UpDown), size = 1) +
            ggtitle(paste0(
              "Spill site ", site_to_plot,
              " – houses within ±", ALONG_MAX_M,
              " m along (proxy) and ≤", LATERAL_MAX_M, " m from river"
            ))

          print(p)
          cat("  -> Diagnostic plot for site_id ", site_to_plot, " printed.\n", sep = "")
        }
      }
    }
  }
}
