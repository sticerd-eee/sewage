## 10_UpDown_basic_sales_500m.R
## Link sales houses to nearby spill sites along rivers (debug, first 50 spills)

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(sf)
})

ROOT <- here::here()

path_spill       <- here("data", "processed", "unique_spill_sites.parquet")
path_sales       <- here("data", "processed", "house_price.parquet")
path_watercourse <- file.path(ROOT, "data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp")

crs_bng <- 27700  # OSGB36 / British National Grid

sf_use_s2(FALSE)  # projected coordinates; use GEOS

# -------------------------------------------------------------------
# Parameters
# -------------------------------------------------------------------

N_SPILLS_TEST  <- 50     # use first 50 spills as "universe" for testing
CAND_RADIUS_M  <- 1500   # Euclidean radius around spill to preselect houses
ALONG_MAX_M    <- 500    # ±500 m along river from spill
LATERAL_MAX_M  <- 500    # ≤500 m from river
N_SAMPLES_LINE <- 1001    # resolution for along-river approximation

# -------------------------------------------------------------------
# 1. Load data
# -------------------------------------------------------------------

cat("=== Step 1: Load data ===\n")
spill  <- read_parquet(path_spill)
sales  <- read_parquet(path_sales)
rivers <- st_read(path_watercourse, quiet = TRUE)

cat("Spill rows : ", nrow(spill),  "\n", sep = "")
cat("Sales rows : ", nrow(sales),  "\n", sep = "")
cat("Rivers rows: ", nrow(rivers), "\n\n", sep = "")

# -------------------------------------------------------------------
# 2. Convert to sf, restrict spills to first N_SPILLS_TEST
# -------------------------------------------------------------------

cat("=== Step 2: Convert to sf ===\n")

spill_sf_full <- spill %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(
    coords = c("easting", "northing"),
    crs    = crs_bng,
    remove = FALSE
  )

n_spills_total <- nrow(spill_sf_full)
n_spills_use   <- min(N_SPILLS_TEST, n_spills_total)

# For testing: only first N_SPILLS_TEST spills
spills_sf <- spill_sf_full %>% slice(1:n_spills_use)
cat("Using first ", n_spills_use, " spills (of ", n_spills_total, " total).\n", sep = "")

sales_sf <- sales %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(
    coords = c("easting", "northing"),
    crs    = crs_bng,
    remove = FALSE
  )

cat("Sales with valid coords: ", nrow(sales_sf), "\n", sep = "")

rivers_2d <- st_zm(rivers, drop = TRUE, what = "ZM")
if (st_crs(rivers_2d)$epsg != crs_bng) {
  rivers_2d <- st_transform(rivers_2d, crs_bng)
}
cat("Rivers CRS: ", st_crs(rivers_2d)$epsg, "\n\n", sep = "")

# -------------------------------------------------------------------
# 3. Nearest river segment for each spill
# -------------------------------------------------------------------

cat("=== Step 3: Nearest river segment for each spill ===\n")
t_nn <- system.time({
  nn_spill <- st_nearest_feature(spills_sf, rivers_2d)
})
cat("Nearest rivers computed in ", round(t_nn["elapsed"], 2), " seconds.\n\n", sep = "")

# -------------------------------------------------------------------
# Helper: approximate along-river position via line sampling
# -------------------------------------------------------------------
# Given:
#   line_geom : sfc LINESTRING length 1
#   pts       : sf or sfc POINT (one or many)
# Returns numeric vector t in [0, 1] for each point

approx_t_on_line <- function(line_geom, pts, n_samples = N_SAMPLES_LINE) {
  samples    <- st_line_sample(line_geom, sample = seq(0, 1, length.out = n_samples))
  samples_pt <- st_cast(samples, "POINT")  # n_samples sample points
  
  dmat <- st_distance(samples_pt, pts)
  dmat <- as.matrix(dmat)
  
  if (ncol(dmat) == 1L) {
    idx_min <- which.min(as.numeric(dmat[, 1]))
  } else {
    idx_min <- apply(dmat, 2, function(col) which.min(as.numeric(col)))
  }
  
  t <- (idx_min - 1) / (n_samples - 1)
  as.numeric(t)
}

# -------------------------------------------------------------------
# 4. Loop over spills and construct house–spill pairs
# -------------------------------------------------------------------

cat("=== Step 4: Loop over spills and link houses ===\n")

pairs_list <- vector("list", nrow(spills_sf))

for (i in seq_len(nrow(spills_sf))) {
  sp <- spills_sf[i, ]
  sewage_id <- sp$site_id
  
  river_idx <- nn_spill[i]
  cat("Spill ", i, " / ", nrow(spills_sf),
      " - site_id: ", sewage_id,
      " - river_idx: ", river_idx, "\n", sep = "")
  
  if (is.na(river_idx) || river_idx < 1) {
    cat("  -> Invalid river_idx; skipping.\n\n")
    next
  }
  
  r_line    <- rivers_2d[river_idx, ]
  line_geom <- st_geometry(r_line)          # sfc LINESTRING length 1
  line_len  <- as.numeric(st_length(line_geom))
  cat("  River length (m): ", round(line_len, 1), "\n", sep = "")
  
  if (!is.finite(line_len) || line_len == 0) {
    cat("  -> Zero/invalid river length; skipping.\n\n")
    next
  }
  
  # Approximate spill position along river
  t_spill   <- approx_t_on_line(line_geom, st_geometry(sp))
  pos_spill <- t_spill * line_len
  cat("  Spill t_on_line: ", round(t_spill, 4),
      " -> pos_spill (m): ", round(pos_spill, 1), "\n", sep = "")
  
  # Candidate houses: within CAND_RADIUS_M Euclidean of this spill
  t_cand <- system.time({
    # IMPORTANT: spill as x, sales as y
    idx_list <- st_is_within_distance(sp, sales_sf, dist = CAND_RADIUS_M)
  })
  cand_idx <- unique(unlist(idx_list))  # indices of sales_sf
  cand_n   <- length(cand_idx)
  cat("  Candidate houses within ", CAND_RADIUS_M, " m: ",
      cand_n, " (t = ", round(t_cand["elapsed"], 2), " s)\n", sep = "")
  
  if (cand_n == 0L) {
    cat("  -> No candidate houses; skip spill.\n\n")
    next
  }
  
  hh_pts <- sales_sf[cand_idx, ]
  
  # Distance house -> this river segment (matrix, since r_line is length 1)
  t_dist <- system.time({
    dist_mat        <- st_distance(hh_pts, r_line)  # n_hh x 1
    Dist_River_vec  <- as.numeric(dist_mat[, 1])
  })
  cat("  House->river distances computed (t = ",
      round(t_dist["elapsed"], 2), " s).\n", sep = "")
  
  # Approximate house positions along same river
  t_pos <- system.time({
    t_hh   <- approx_t_on_line(line_geom, st_geometry(hh_pts))
    pos_hh <- t_hh * line_len
  })
  cat("  House positions along river computed (t = ",
      round(t_pos["elapsed"], 2), " s).\n", sep = "")
  
  River_Len_vec <- pos_hh - pos_spill  # signed along-river distance (m)
  
  cat("  River_Len range (m): ",
      paste(round(range(River_Len_vec, na.rm = TRUE), 1), collapse = " to "),
      "\n", sep = "")
  cat("  Dist_River range (m): ",
      paste(round(range(Dist_River_vec, na.rm = TRUE), 1), collapse = " to "),
      "\n", sep = "")
  
  # Filter houses: ±ALONG_MAX_M along river and ≤LATERAL_MAX_M from river
  keep   <- (abs(River_Len_vec) <= ALONG_MAX_M) & (Dist_River_vec <= LATERAL_MAX_M)
  n_keep <- sum(keep, na.rm = TRUE)
  cat("  Houses passing filters (|River_Len| <= ", ALONG_MAX_M,
      ", Dist_River <= ", LATERAL_MAX_M, "): ", n_keep, "\n", sep = "")
  
  if (n_keep == 0L) {
    cat("  -> No houses kept for this spill.\n\n")
    next
  }
  
  hh_keep <- hh_pts[keep, ]
  
pairs_list[[i]] <- tibble(
  HH_ID      = hh_keep$house_id,
  Sewage_ID  = sewage_id,
  River_Len  = round(River_Len_vec[keep], 1),   # or no rounding at all
  Dist_River = round(Dist_River_vec[keep], 1)
)

  
  cat("  -> Added ", nrow(pairs_list[[i]]), " rows to pairs.\n\n", sep = "")
}

pairs_sales <- bind_rows(pairs_list)

# Ensure correct structure even if empty
if (nrow(pairs_sales) == 0L) {
  pairs_sales <- tibble(
    HH_ID      = integer(),
    Sewage_ID  = integer(),
    River_Len  = double(),
    Dist_River = double()
  )
  cat("=== RESULT: pairs_sales is EMPTY (0 rows). ===\n")
} else {
  cat("=== RESULT: pairs_sales has ", nrow(pairs_sales), " rows. ===\n", sep = "")
}

cat("\nExample rows (up to 20):\n")
print(head(pairs_sales, 20))
