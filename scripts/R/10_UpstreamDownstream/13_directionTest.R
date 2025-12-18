## 13_directionTest_clean.R
## Purpose:
##   Validate whether startNode/endNode define a coherent directed river network
##   and whether "downstream" paths behave as expected between two known points.
##
## Outputs (printed + plots to device; nothing saved):
##   1) Degree sanity: Sources/Sinks/Confluences/Splits
##   2) Cycle (SCC) rate
##   3) Graph orientation check: out-neighbors vs node rule
##   4) Reachability + local map with directed path arrows (if a path exists)
##   5) Optional: local segment digitised-direction arrows (geometry start->end)

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(sf)
  library(data.table)
  library(igraph)
  library(ggplot2)
  library(grid)
})

sf_use_s2(FALSE)
crs_bng <- 27700

# ----------------------------
# Paths (edit if needed)
# ----------------------------
ROOT <- here::here()
path_watercourse <- file.path(ROOT, "data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp")

# ----------------------------
# Read + prepare rivers
# ----------------------------
rivers_raw <- st_read(path_watercourse, quiet = TRUE)

# Standardise names used in your other scripts
rivers <- rivers_raw %>%
  rename(
    id         = identifier,
    start_node = startNode,
    end_node   = endNode
  )

rivers_2d <- st_zm(rivers, drop = TRUE, what = "ZM")
if (st_crs(rivers_2d)$epsg != crs_bng) rivers_2d <- st_transform(rivers_2d, crs_bng)

# Required columns
required_cols <- c("start_node", "end_node")
missing_cols <- setdiff(required_cols, names(rivers_2d))
if (length(missing_cols) > 0) stop("Missing required columns: ", paste(missing_cols, collapse = ", "))

start_ids <- rivers_2d[["start_node"]]
end_ids   <- rivers_2d[["end_node"]]
n_segs    <- nrow(rivers_2d)

cat("Segments (rows): ", n_segs, "\n", sep = "")
cat("CRS EPSG: ", st_crs(rivers_2d)$epsg, "\n\n", sep = "")

# ============================================================
# 1) Degree sanity check at node level
# ============================================================
seg_df <- tibble(start_node = start_ids, end_node = end_ids) %>%
  filter(!is.na(start_node), !is.na(end_node))

out_deg <- seg_df %>% count(start_node, name = "out_deg") %>% rename(node = start_node)
in_deg  <- seg_df %>% count(end_node,   name = "in_deg")  %>% rename(node = end_node)

deg <- full_join(in_deg, out_deg, by = "node") %>%
  mutate(
    in_deg  = ifelse(is.na(in_deg),  0L, in_deg),
    out_deg = ifelse(is.na(out_deg), 0L, out_deg)
  )

sources <- sum(deg$in_deg == 0 & deg$out_deg >= 1)
sinks   <- sum(deg$out_deg == 0 & deg$in_deg >= 1)
conflu  <- sum(deg$in_deg > 1 & deg$out_deg == 1)
splits  <- sum(deg$out_deg > 1 & deg$in_deg == 1)

cat("Sources: ", sources, "\n", sep = "")
cat("Sinks  : ", sinks,   "\n", sep = "")
cat("Conflu : ", conflu,  "\n", sep = "")
cat("Splits : ", splits,  "\n", sep = "")
cat("Sources/Sinks ratio: ", round(sources / max(1, sinks), 2), "\n\n", sep = "")

# ============================================================
# 2) Build directed graph g where:
#    edge A -> B iff end_node(A) == start_node(B)
# ============================================================
build_flow_graph <- function(start_ids, end_ids) {
  n <- length(start_ids)

  seg_dt <- data.table(
    seg = 1:n,
    start_node = start_ids,
    end_node   = end_ids
  )[!is.na(start_node) & !is.na(end_node)]

  # Join: for each A (i), find B (x) where start_node(B) == end_node(A)
  setkey(seg_dt, start_node)
  edges_dt <- seg_dt[seg_dt, on = .(start_node = end_node),
                     nomatch = 0L, allow.cartesian = TRUE]

  # Correct direction: A -> B, i.e. i.seg -> seg
  edges <- data.frame(
    from = as.character(edges_dt$i.seg),
    to   = as.character(edges_dt$seg)
  )

  verts <- data.frame(name = as.character(1:n))
  graph_from_data_frame(edges, directed = TRUE, vertices = verts)
}

g <- build_flow_graph(start_ids, end_ids)

cat("Graph vertices: ", vcount(g), "\n", sep = "")
cat("Graph edges   : ", ecount(g), "\n\n", sep = "")

# ============================================================
# 3) Cycle check via SCC
# ============================================================
scc <- components(g, mode = "strong")
tab <- sort(table(scc$csize), decreasing = TRUE)

cat("Strongly connected components (size distribution top 10):\n")
print(head(tab, 10))

cyclic_vertices <- sum(scc$csize[scc$membership] > 1)
cat("Share of segments in directed cycles: ",
    round(100 * cyclic_vertices / vcount(g), 3), "%\n\n", sep = "")

# ============================================================
# 4) DEFINITIVE sanity: g out-neighbors match the rule
# ============================================================
check_graph_orientation <- function(g, start_ids, end_ids, n_test = 50, seed = 1) {
  set.seed(seed)
  test_segs <- sample.int(length(start_ids), n_test)

  n_ok <- 0L
  cat("=== Graph orientation check (out-neighbors vs rule) ===\n")

  for (s in test_segs) {
    nbr_rule  <- which(start_ids == end_ids[s])
    nbr_graph <- as.integer(names(neighbors(g, as.character(s), mode = "out")))
    ok <- setequal(nbr_rule, nbr_graph)
    if (ok) n_ok <- n_ok + 1L

    cat("seg ", s,
        " | ok? ", ok,
        " | rule_n=", length(nbr_rule),
        " | graph_n=", length(nbr_graph), "\n", sep = "")
  }

  cat("OK in ", n_ok, " / ", n_test, " cases.\n\n", sep = "")
  invisible(n_ok == n_test)
}

ok_graph <- check_graph_orientation(g, start_ids, end_ids, n_test = 20, seed = 1)
if (!ok_graph) {
  cat("STOP: Your graph g does not match the node rule.\n")
  cat("This means any 'upstream/downstream' result is invalid until fixed.\n\n")
}

# ============================================================
# 5) Reachability + local visualisation (two-point test)
#    IMPORTANT: use points on the river itself (bridges), not town centroids.
# ============================================================
pt_lonlat <- function(lon, lat) st_sfc(st_point(c(lon, lat)), crs = 4326) |> st_transform(crs_bng)

segs_within <- function(geom, dist_m) {
  idx <- st_is_within_distance(st_sf(geometry = geom), rivers_2d, dist = dist_m)[[1]]
  unique(as.integer(idx))
}

snap_seg_and_dist_m <- function(pt_bng) {
  pt_sf <- st_sf(geometry = pt_bng)
  seg   <- st_nearest_feature(pt_sf, rivers_2d)[1]
  d_m   <- as.numeric(st_distance(pt_sf, rivers_2d[seg, ], by_element = TRUE))
  list(seg = seg, dist_m = d_m)
}

midpoint_xy <- function(lines_sf) {
  geom <- st_cast(st_geometry(lines_sf), "LINESTRING")
  mids <- st_line_sample(geom, sample = 0.5)
  mids <- st_cast(mids, "POINT")
  m <- st_coordinates(mids)
  data.frame(x = m[, 1], y = m[, 2])
}

plot_pair_with_path_arrows <- function(name,
                                       lon_up, lat_up,
                                       lon_down, lat_down,
                                       map_buffer_m = 15000,
                                       target_buffer_m = 800) {
  pt_up   <- pt_lonlat(lon_up, lat_up)
  pt_down <- pt_lonlat(lon_down, lat_down)

  up_info   <- snap_seg_and_dist_m(pt_up)
  down_info <- snap_seg_and_dist_m(pt_down)

  targets_down <- segs_within(pt_down, target_buffer_m)

  d <- distances(g,
                 v  = as.character(up_info$seg),
                 to = as.character(targets_down),
                 mode = "out")
  ok <- which(is.finite(as.numeric(d)))[1]

  cat("\n", name, "\n", sep = "")
  cat("  up   snapped seg: ", up_info$seg,   " | snap dist (m): ", round(up_info$dist_m, 2), "\n", sep = "")
  cat("  down snapped seg: ", down_info$seg, " | snap dist (m): ", round(down_info$dist_m, 2), "\n", sep = "")
  cat("  # target segs near DOWN (", target_buffer_m, "m): ", length(targets_down), "\n", sep = "")

  if (is.na(ok)) {
    cat("  path found? FALSE\n", sep = "")
    cat("  Note: if snap distances are tiny but no path, direction/indexing is still wrong\n")
    cat("        or the downstream buffer is too tight around a braided/complex reach.\n")
    return(invisible(NULL))
  }

  target_seg <- targets_down[ok]
  vp <- shortest_paths(g,
                       from = as.character(up_info$seg),
                       to   = as.character(target_seg),
                       mode = "out")$vpath[[1]]
  path_segs <- as.integer(names(vp))

  cat("  chosen down target seg: ", target_seg, "\n", sep = "")
  cat("  path found? TRUE | path segments: ", length(path_segs), " | edges: ", max(0, length(path_segs) - 1L), "\n", sep = "")

  # Local map window
  local_idx <- sort(unique(c(segs_within(pt_up, map_buffer_m), segs_within(pt_down, map_buffer_m))))
  local <- rivers_2d[local_idx, , drop = FALSE]
  local$rowid <- local_idx
  local$style <- "local"
  local$style[local$rowid %in% path_segs] <- "path"
  local$style[local$rowid == up_info$seg] <- "up_snap"
  local$style[local$rowid == down_info$seg] <- "down_snap"
  local$style[local$rowid %in% targets_down] <- "down_targets"

  # Arrow overlay along the path: midpoint(i) -> midpoint(i+1)
  path_sf <- rivers_2d[path_segs, , drop = FALSE]
  mids <- midpoint_xy(path_sf)
  arrows <- data.frame(
    x = mids$x[-nrow(mids)],
    y = mids$y[-nrow(mids)],
    xend = mids$x[-1],
    yend = mids$y[-1]
  )

  pts <- st_sf(which = c("up_point", "down_point"), geometry = c(pt_up, pt_down))

  p <- ggplot() +
    geom_sf(data = local, aes(linetype = style), linewidth = 0.7) +
    geom_segment(
      data = arrows,
      aes(x = x, y = y, xend = xend, yend = yend),
      arrow = arrow(length = unit(0.12, "inches")),
      linewidth = 0.4
    ) +
    geom_sf(data = pts, aes(shape = which), size = 3) +
    ggtitle(paste0(name, " | directed path arrows")) +
    theme_minimal()

  print(p)
  invisible(p)
}

# OPTIONAL: show digitised geometry direction arrows in a local window
arrow_df_from_lines <- function(rivers_sf) {
  geom <- st_cast(st_geometry(rivers_sf), "LINESTRING")
  xy <- lapply(seq_along(geom), function(i) {
    m <- st_coordinates(geom[i])
    data.frame(
      x = m[1, "X"],  y = m[1, "Y"],
      xend = m[nrow(m), "X"], yend = m[nrow(m), "Y"]
    )
  })
  do.call(rbind, xy)
}

plot_local_geometry_arrows <- function(lon, lat, buffer_m = 8000) {
  pt <- pt_lonlat(lon, lat)
  idx <- segs_within(pt, buffer_m)
  local <- rivers_2d[idx, , drop = FALSE]
  df_ar <- arrow_df_from_lines(local)

  p <- ggplot() +
    geom_sf(data = local, linewidth = 0.4) +
    geom_segment(
      data = df_ar,
      aes(x = x, y = y, xend = xend, yend = yend),
      arrow = arrow(length = unit(0.10, "inches")),
      linewidth = 0.3
    ) +
    geom_sf(data = st_sf(geometry = pt), size = 3, shape = 4) +
    ggtitle(paste0("Geometry digitised direction arrows (", buffer_m, " m buffer)")) +
    theme_minimal()

  print(p)
  invisible(p)
}

# ----------------------------
# Test case (Severn): Iron Bridge -> Bewdley Bridge
# Use your own coords if you have more precise bridge points.
# ----------------------------
plot_pair_with_path_arrows(
  name = "Severn test: Iron Bridge -> Bewdley Bridge",
  lon_up = -2.48553, lat_up = 52.62724,
  lon_down = -2.31390, lat_down = 52.37650,
  map_buffer_m = 15000,
  target_buffer_m = 800
)

# Optional: geometry-direction arrows around the upstream point
plot_local_geometry_arrows(lon = -2.48553, lat = 52.62724, buffer_m = 8000)
