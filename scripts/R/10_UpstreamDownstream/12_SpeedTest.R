## 10_sample_runtime_estimates.R
## Goal:
##  1) Compute exact number of spills within 50 m of a river.
##  2) Using a random sample of N_SAMPLE_SPILLS spills:
##     - Run full river linking logic for SALES and RENT datasets.
##     - Estimate runtime for running all spills.
##     - Summarise average number of houses upstream/downstream per spill
##       (among spills that have at least one linked house) and the share
##       of spills with zero houses.

suppressPackageStartupMessages({
  library(here)
  library(arrow)
  library(dplyr)
  library(sf)
})

ROOT <- here::here()

path_spill       <- here("data", "processed", "unique_spill_sites.parquet")
path_sales       <- here("data", "processed", "house_price.parquet")
path_rent        <- here("data", "processed", "zoopla", "zoopla_rentals.parquet")
path_watercourse <- file.path(ROOT, "data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp")

crs_bng <- 27700  # OSGB36 / British National Grid

sf_use_s2(FALSE)  # projected coordinates; use GEOS

# -------------------------------------------------------------------
# Parameters
# -------------------------------------------------------------------

N_SAMPLE_SPILLS  <- 100L   # number of spills to sample at random
CAND_RADIUS_M    <- 1500   # Euclidean radius around spill to preselect houses
ALONG_MAX_M      <- 500    # ±500 m along river from spill
LATERAL_MAX_M    <- 500    # ≤500 m from river
N_SAMPLES_LINE   <- 1001   # resolution for along-river approximation
RIVER_SEARCH_RADIUS_M <- max(CAND_RADIUS_M, ALONG_MAX_M) * 2  # radius for local network

set.seed(123)  # reproducible spill sample

# -------------------------------------------------------------------
# 1. Load data
# -------------------------------------------------------------------

cat("=== Step 1: Load data ===\n")
spill  <- read_parquet(path_spill)
sales  <- read_parquet(path_sales)
rent   <- read_parquet(path_rent)

cat("Spill rows : ", nrow(spill), "\n", sep = "")
cat("Sales rows : ", nrow(sales), "\n", sep = "")
cat("Rent rows  : ", nrow(rent),  "\n", sep = "")

# Rivers
rivers_raw <- st_read(path_watercourse, quiet = TRUE)
cat("Rivers rows: ", nrow(rivers_raw), "\n\n", sep = "")

# Rename identifier → id, startNode/endNode → start_node/end_node
rivers <- rivers_raw %>%
  rename(
    id         = identifier,
    start_node = startNode,
    end_node   = endNode
  )

# -------------------------------------------------------------------
# 2. Convert to sf (spills, sales, rent) and rivers network prep
# -------------------------------------------------------------------

cat("=== Step 2: Convert to sf and prepare rivers ===\n")

spill_sf <- spill %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(coords = c("easting", "northing"),
           crs    = crs_bng,
           remove = FALSE)

sales_sf <- sales %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(coords = c("easting", "northing"),
           crs    = crs_bng,
           remove = FALSE)

rent_sf <- rent %>%
  filter(!is.na(easting), !is.na(northing)) %>%
  st_as_sf(coords = c("easting", "northing"),
           crs    = crs_bng,
           remove = FALSE)

cat("Spills with valid coords: ", nrow(spill_sf), "\n", sep = "")
cat("Sales with valid coords : ", nrow(sales_sf), "\n", sep = "")
cat("Rent with valid coords  : ", nrow(rent_sf),  "\n", sep = "")

# Rivers to BNG, drop Z/M
rivers_2d <- st_zm(rivers, drop = TRUE, what = "ZM")
if (st_crs(rivers_2d)$epsg != crs_bng) {
  rivers_2d <- st_transform(rivers_2d, crs_bng)
}
cat("Rivers CRS EPSG: ", st_crs(rivers_2d)$epsg, "\n", sep = "")

# Check required network columns
required_cols <- c("id", "start_node", "end_node", "length")
missing_cols  <- setdiff(required_cols, names(rivers_2d))
if (length(missing_cols) > 0) {
  stop("River layer is missing required columns: ",
       paste(missing_cols, collapse = ", "))
}

# Network attributes
river_ids <- rivers_2d[["id"]]
start_ids <- rivers_2d[["start_node"]]
end_ids   <- rivers_2d[["end_node"]]
seg_len   <- as.numeric(rivers_2d[["length"]])

# Fallback for bad lengths
bad_len <- !is.finite(seg_len) | seg_len <= 0
if (any(bad_len)) {
  seg_len[bad_len] <- as.numeric(st_length(st_geometry(rivers_2d[bad_len, ])))
}

cat("Network attributes ready.\n\n")

# -------------------------------------------------------------------
# 3. Distances from spills to nearest river – 50 m check
# -------------------------------------------------------------------

cat("=== Step 3: Distances from spills to nearest river ===\n")

t_nn <- system.time({
  nn_spill <- st_nearest_feature(spill_sf, rivers_2d)
})
cat("Nearest river indices computed in ",
    round(t_nn["elapsed"], 2), " s.\n", sep = "")

nearest_river_geom <- rivers_2d[nn_spill, ]
dist_spill_river   <- st_distance(spill_sf, nearest_river_geom, by_element = TRUE)
dist_spill_river_m <- as.numeric(dist_spill_river)

n_spills_total     <- nrow(spill_sf)
n_within_50m       <- sum(dist_spill_river_m <= 50, na.rm = TRUE)
prop_within_50m    <- n_within_50m / n_spills_total

cat("Spills within 50 m of a river: ",
    n_within_50m, " of ", n_spills_total,
    " (", round(100 * prop_within_50m, 1), "%).\n\n", sep = "")

# -------------------------------------------------------------------
# 4. Sample spills for runtime and HH-count estimation
# -------------------------------------------------------------------

cat("=== Step 4: Sample spills for estimation ===\n")

N_SAMPLE <- min(N_SAMPLE_SPILLS, n_spills_total)
sample_idx <- sample(seq_len(n_spills_total), size = N_SAMPLE, replace = FALSE)

spills_sample      <- spill_sf[sample_idx, ]
nn_spill_sample    <- nn_spill[sample_idx]
dist_sample_50m    <- dist_spill_river_m[sample_idx]

cat("Sampled ", N_SAMPLE, " spills (random sample of all spills).\n", sep = "")
cat("In this sample, spills within 50 m of river: ",
    sum(dist_sample_50m <= 50, na.rm = TRUE), " of ", N_SAMPLE, ".\n\n", sep = "")

# -------------------------------------------------------------------
# 5. Helper functions: along-river position and network walk
# -------------------------------------------------------------------

approx_t_on_line <- function(line_geom, pts, n_samples = N_SAMPLES_LINE) {
  samples    <- st_line_sample(line_geom, sample = seq(0, 1, length.out = n_samples))
  samples_pt <- st_cast(samples, "POINT")
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

build_local_river_line <- function(spill_point, rivers, river_idx0,
                                   search_radius_m = RIVER_SEARCH_RADIUS_M) {
  idx_list <- st_is_within_distance(spill_point, rivers, dist = search_radius_m)
  idx <- sort(unique(unlist(idx_list)))
  
  if (length(idx) == 0L && !is.na(river_idx0) && river_idx0 > 0L) {
    idx <- river_idx0
  }
  if (length(idx) == 0L) {
    return(NULL)
  }
  
  rivers_local <- rivers[idx, ]
  geom_local   <- st_geometry(rivers_local)
  geom_union   <- st_union(geom_local)
  geom_multi   <- st_cast(geom_union, "MULTILINESTRING")
  merged       <- st_line_merge(geom_multi)
  merged_ls    <- st_cast(merged, "LINESTRING")
  
  if (length(merged_ls) == 0L) {
    return(NULL)
  }
  if (length(merged_ls) == 1L) {
    return(merged_ls)
  }
  
  dists    <- st_distance(spill_point, merged_ls)
  best_idx <- which.min(as.numeric(dists[1, ]))
  
  merged_ls[best_idx]
}

get_downstream_rows <- function(center_row, max_dist,
                                start_ids, end_ids, seg_len) {
  res_rows <- integer(0)
  cum_dist <- 0
  current  <- center_row
  visited  <- logical(length(start_ids))
  
  repeat {
    if (is.na(current) || current < 1L || current > length(start_ids)) break
    
    node <- end_ids[current]
    if (is.na(node)) break
    
    nbrs <- which(start_ids == node & !visited)
    nbrs <- setdiff(nbrs, current)
    if (length(nbrs) == 0L) break
    
    next_row <- nbrs[1L]
    seg_len_next <- seg_len[next_row]
    if (!is.finite(seg_len_next) || seg_len_next <= 0) break
    
    new_dist <- cum_dist + seg_len_next
    if (new_dist > max_dist) break
    
    res_rows          <- c(res_rows, next_row)
    visited[next_row] <- TRUE
    cum_dist          <- new_dist
    current           <- next_row
  }
  
  res_rows
}

get_upstream_rows <- function(center_row, max_dist,
                              start_ids, end_ids, seg_len) {
  res_rows <- integer(0)
  cum_dist <- 0
  current  <- center_row
  visited  <- logical(length(start_ids))
  
  repeat {
    if (is.na(current) || current < 1L || current > length(start_ids)) break
    
    node <- start_ids[current]
    if (is.na(node)) break
    
    nbrs <- which(end_ids == node & !visited)
    nbrs <- setdiff(nbrs, current)
    if (length(nbrs) == 0L) break
    
    next_row <- nbrs[1L]
    seg_len_next <- seg_len[next_row]
    if (!is.finite(seg_len_next) || seg_len_next <= 0) break
    
    new_dist <- cum_dist + seg_len_next
    if (new_dist > max_dist) break
    
    res_rows          <- c(res_rows, next_row)
    visited[next_row] <- TRUE
    cum_dist          <- new_dist
    current           <- next_row
  }
  
  res_rows
}

# -------------------------------------------------------------------
# 6. Core: function to process sample spills for a given housing dataset
# -------------------------------------------------------------------

link_sample_for_dataset <- function(dataset_name,
                                    houses_sf,
                                    spills_sample,
                                    nn_spill_sample,
                                    rivers_2d,
                                    river_ids,
                                    start_ids,
                                    end_ids,
                                    seg_len) {
  cat("=== Processing dataset: ", dataset_name, " ===\n", sep = "")
  
  n_spills <- nrow(spills_sample)
  
  res <- tibble(
    site_id       = spills_sample$site_id,
    spill_index   = seq_len(n_spills),
    n_hh_total    = integer(n_spills),
    n_hh_upstream = integer(n_spills),
    n_hh_downstream = integer(n_spills),
    elapsed_sec   = numeric(n_spills)
  )
  
  for (k in seq_len(n_spills)) {
    sp        <- spills_sample[k, ]
    center_row <- nn_spill_sample[k]
    
    t_loop <- system.time({
      n_total <- 0L
      n_up    <- 0L
      n_down  <- 0L
      
      if (is.na(center_row) || center_row < 1L ||
          center_row > nrow(rivers_2d)) {
        # no river; nothing to do
      } else {
        line_geom <- build_local_river_line(sp, rivers_2d, center_row,
                                            search_radius_m = RIVER_SEARCH_RADIUS_M)
        if (!is.null(line_geom) &&
            inherits(line_geom, "sfc_LINESTRING") &&
            length(line_geom) == 1L) {
          
          line_len <- as.numeric(st_length(line_geom))
          if (is.finite(line_len) && line_len > 0) {
            t_spill   <- approx_t_on_line(line_geom, st_geometry(sp))
            pos_spill <- t_spill * line_len
            
            down_rows <- get_downstream_rows(center_row, ALONG_MAX_M,
                                             start_ids, end_ids, seg_len)
            up_rows   <- get_upstream_rows(center_row, ALONG_MAX_M,
                                           start_ids, end_ids, seg_len)
            down_ids  <- river_ids[down_rows]
            up_ids    <- river_ids[up_rows]
            
            # Candidate houses in Euclidean radius
            idx_list <- st_is_within_distance(sp, houses_sf, dist = CAND_RADIUS_M)
            cand_idx <- unique(unlist(idx_list))
            
            if (length(cand_idx) > 0L) {
              hh_pts <- houses_sf[cand_idx, ]
              
              hh_seg_idx <- st_nearest_feature(hh_pts, rivers_2d)
              hh_seg_id  <- river_ids[hh_seg_idx]
              
              dist_mat       <- st_distance(hh_pts, line_geom)
              Dist_River_vec <- as.numeric(dist_mat[, 1])
              
              t_hh   <- approx_t_on_line(line_geom, st_geometry(hh_pts))
              pos_hh <- t_hh * line_len
              
              River_Len_vec <- pos_hh - pos_spill
              
              keep <- (abs(River_Len_vec) <= ALONG_MAX_M) &
                      (Dist_River_vec <= LATERAL_MAX_M)
              
              if (any(keep, na.rm = TRUE)) {
                hh_seg_id_keep  <- hh_seg_id[keep]
                River_Len_keep  <- River_Len_vec[keep]
                Dist_River_keep <- Dist_River_vec[keep]
                
                UpDown_keep <- rep(NA_character_, length(hh_seg_id_keep))
                
                same_seg <- hh_seg_idx[keep] == center_row
                if (any(same_seg)) {
                  UpDown_keep[same_seg] <- ifelse(River_Len_keep[same_seg] >= 0,
                                                  "downstream", "upstream")
                }
                
                is_unknown <- is.na(UpDown_keep)
                if (any(is_unknown) && length(down_ids) > 0L) {
                  in_down <- hh_seg_id_keep[is_unknown] %in% down_ids
                  if (any(in_down)) {
                    UpDown_keep[which(is_unknown)[in_down]] <- "downstream"
                  }
                }
                is_unknown <- is.na(UpDown_keep)
                if (any(is_unknown) && length(up_ids) > 0L) {
                  in_up <- hh_seg_id_keep[is_unknown] %in% up_ids
                  if (any(in_up)) {
                    UpDown_keep[which(is_unknown)[in_up]] <- "upstream"
                  }
                }
                
                n_total <- length(UpDown_keep)
                n_up    <- sum(UpDown_keep == "upstream",   na.rm = TRUE)
                n_down  <- sum(UpDown_keep == "downstream", na.rm = TRUE)
              }
            }
          }
        }
      }
    }) # end system.time
    
    res$n_hh_total[k]     <- n_total
    res$n_hh_upstream[k]  <- n_up
    res$n_hh_downstream[k] <- n_down
    res$elapsed_sec[k]    <- as.numeric(t_loop["elapsed"])
    
    if (k %% 20 == 0 || k == n_spills) {
      cat("  Processed ", k, " / ", n_spills,
          " spills for ", dataset_name, ".\n", sep = "")
    }
  }
  
  cat("Finished dataset: ", dataset_name, ".\n\n", sep = "")
  res
}

# -------------------------------------------------------------------
# 7. Run for SALES and RENT on the sample
# -------------------------------------------------------------------

res_sales <- link_sample_for_dataset(
  dataset_name     = "sales",
  houses_sf        = sales_sf,
  spills_sample    = spills_sample,
  nn_spill_sample  = nn_spill_sample,
  rivers_2d        = rivers_2d,
  river_ids        = river_ids,
  start_ids        = start_ids,
  end_ids          = end_ids,
  seg_len          = seg_len
)

res_rent <- link_sample_for_dataset(
  dataset_name     = "rent",
  houses_sf        = rent_sf,
  spills_sample    = spills_sample,
  nn_spill_sample  = nn_spill_sample,
  rivers_2d        = rivers_2d,
  river_ids        = river_ids,
  start_ids        = start_ids,
  end_ids          = end_ids,
  seg_len          = seg_len
)

# -------------------------------------------------------------------
# 8. Summaries: runtime estimates and HH counts
# -------------------------------------------------------------------

summarise_results <- function(res, dataset_name, n_spills_total) {
  cat("=== Summary for dataset: ", dataset_name, " ===\n", sep = "")
  
  n_sample <- nrow(res)
  mean_time_per_spill <- mean(res$elapsed_sec, na.rm = TRUE)
  est_total_time_sec  <- mean_time_per_spill * n_spills_total
  
  n_with_any <- sum(res$n_hh_total > 0)
  n_with_zero <- sum(res$n_hh_total == 0)
  
  avg_hh_given_any <- if (n_with_any > 0) {
    mean(res$n_hh_total[res$n_hh_total > 0])
  } else NA_real_
  
  avg_up_given_any <- if (n_with_any > 0) {
    mean(res$n_hh_upstream[res$n_hh_total > 0])
  } else NA_real_
  
  avg_down_given_any <- if (n_with_any > 0) {
    mean(res$n_hh_downstream[res$n_hh_total > 0])
  } else NA_real_
  
  cat("Sample size (spills): ", n_sample, " (of ", n_spills_total, " total).\n", sep = "")
  cat("Spills with >=1 linked house (sample): ", n_with_any, "\n", sep = "")
  cat("Spills with 0 linked houses (sample):  ", n_with_zero, "\n", sep = "")
  cat("Avg houses per spill, conditional on >=1: ",
      round(avg_hh_given_any, 2), "\n", sep = "")
  cat("  - Avg upstream houses per such spill:   ",
      round(avg_up_given_any, 2), "\n", sep = "")
  cat("  - Avg downstream houses per such spill: ",
      round(avg_down_given_any, 2), "\n", sep = "")
  
  cat("\nRuntime estimate (from sample):\n")
  cat("  Mean time per spill in sample: ",
      round(mean_time_per_spill, 3), " seconds\n", sep = "")
  cat("  Estimated total time for all ", n_spills_total,
      " spills: ", round(est_total_time_sec / 60, 1), " minutes (",
      round(est_total_time_sec / 3600, 2), " hours)\n\n", sep = "")
}

cat("=== Step 8: Summaries ===\n")
summarise_results(res_sales, "sales", n_spills_total)
summarise_results(res_rent,  "rent",  n_spills_total)
