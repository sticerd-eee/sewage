#!/usr/bin/env Rscript

# ============================================================
# Build directed river network from OS WatercourseLink
# and save as a single RDS containing:
#   - g          : directed igraph (downstream)
#   - g_rev      : reversed igraph (upstream)
#   - nodes_sf   : sf POINTs for river nodes (in BNG)
#   - vid_by_key : named integer vector: node_id -> vertex id
# ============================================================

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(igraph)
})

sf::sf_use_s2(FALSE)
CRS_BNG <- 27700

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
PATH_WC  <- "/Users/odran/Dropbox/sewage/data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp"
OUT_DIR  <- "/Users/odran/Dropbox/sewage/data/raw/rivers/clean_OS"
OUT_RDS  <- file.path(OUT_DIR, "river_graph_OS.rds")

if (!file.exists(PATH_WC)) {
  stop("WatercourseLink.shp not found at: ", PATH_WC)
}

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

cat("Reading WatercourseLink from:\n  ", PATH_WC, "\n", sep = "")
wc <- sf::st_read(PATH_WC, quiet = TRUE)

# Ensure BNG
crs_wc <- sf::st_crs(wc)
cat("Input CRS:\n")
print(crs_wc)

if (is.na(crs_wc$epsg) || crs_wc$epsg != CRS_BNG) {
  cat("Transforming to EPSG:27700 (British National Grid)...\n")
  wc <- sf::st_transform(wc, CRS_BNG)
}

cat("Rows in WatercourseLink:", nrow(wc), "\n")

# ------------------------------------------------------------
# Extract start/end coordinates for each segment
# ------------------------------------------------------------

get_start_end_coords <- function(geom) {
  coords <- sf::st_coordinates(geom)
  c(
    start_x = coords[1, "X"],
    start_y = coords[1, "Y"],
    end_x   = coords[nrow(coords), "X"],
    end_y   = coords[nrow(coords), "Y"]
  )
}

coords_mat <- t(vapply(wc$geometry, get_start_end_coords, numeric(4L)))
coords_df  <- as.data.frame(coords_mat)
# Ensure proper column names (bug you hit)
colnames(coords_df) <- c("start_x", "start_y", "end_x", "end_y")

wc2 <- wc |>
  sf::st_drop_geometry() |>
  bind_cols(coords_df)

cat("Unique 'flow' values:\n")
print(unique(wc2$flow))

# ------------------------------------------------------------
# Node table: one row per unique OS node id (startNode / endNode)
# ------------------------------------------------------------

start_nodes <- wc2 |>
  transmute(key = startNode, x = start_x, y = start_y)

end_nodes <- wc2 |>
  transmute(key = endNode, x = end_x, y = end_y)

nodes_df <- bind_rows(start_nodes, end_nodes) |>
  filter(!is.na(key)) |>
  group_by(key) |>
  summarise(
    x = mean(x),
    y = mean(y),
    .groups = "drop"
  )

cat("Number of unique nodes:", nrow(nodes_df), "\n")

nodes_sf <- sf::st_as_sf(nodes_df, coords = c("x", "y"), crs = CRS_BNG)

# Mapping: node key -> vertex id (1..N)
vid_by_key <- setNames(seq_len(nrow(nodes_df)), nodes_df$key)

# ------------------------------------------------------------
# Edge list: directed, weighted by length (metres)
# Honour the 'flow' field:
#   - "in direction"          : startNode -> endNode
#   - "in opposite direction" : endNode   -> startNode
#   - "unknown"/"missing"     : fall back to startNode -> endNode
# ------------------------------------------------------------

edges_df <- wc2 |>
  filter(!is.na(startNode), !is.na(endNode)) |>
  mutate(
    from_key = dplyr::case_when(
      flow == "in direction"          ~ startNode,
      flow == "in opposite direction" ~ endNode,
      TRUE                            ~ startNode
    ),
    to_key = dplyr::case_when(
      flow == "in direction"          ~ endNode,
      flow == "in opposite direction" ~ startNode,
      TRUE                            ~ endNode
    )
  ) |>
  transmute(
    from_key,
    to_key,
    len_m    = length,    # OS length field in metres
    form,
    flow,
    fictitious
  ) |>
  mutate(
    from_vid = as.integer(vid_by_key[from_key]),
    to_vid   = as.integer(vid_by_key[to_key])
  ) |>
  filter(!is.na(from_vid), !is.na(to_vid)) |>
  filter(from_vid != to_vid)   # drop self-loops

cat("Number of directed edges:", nrow(edges_df), "\n")

# Build igraph
g <- igraph::graph_from_data_frame(
  edges_df |>
    transmute(from = from_vid, to = to_vid, weight = len_m),
  directed = TRUE,
  vertices = data.frame(
    vid = seq_len(nrow(nodes_df)),
    key = nodes_df$key
  )
)

# Connected components
comp_id <- igraph::components(igraph::as.undirected(g))$membership
igraph::V(g)$comp <- comp_id

cat(
  "Graph stats:\n",
  "  vertices   :", igraph::vcount(g), "\n",
  "  edges      :", igraph::ecount(g), "\n",
  "  components :", length(unique(comp_id)), "\n",
  sep = ""
)

# Reversed graph for upstream distances
g_rev <- igraph::reverse_edges(g)

# ------------------------------------------------------------
# Pack into a single object and save as RDS
# ------------------------------------------------------------
Gobj <- list(
  g          = g,
  g_rev      = g_rev,
  nodes_sf   = nodes_sf,
  vid_by_key = vid_by_key
)

saveRDS(Gobj, OUT_RDS)

cat("Saved river graph RDS to:\n  ", OUT_RDS, "\n", sep = "")
