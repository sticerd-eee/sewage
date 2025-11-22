#!/usr/bin/env Rscript

# -------------------------------------------------------------------
# Split OS WatercourseLink lines into ~25 m segments and keep:
#   identifier, startNode, endNode, length, flow, ...
# Example: 60 m -> 25 m, 25 m, 10 m
# -------------------------------------------------------------------

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(lwgeom)   # for st_linesubstring
})

sf::sf_use_s2(FALSE)
CRS_BNG <- 27700

# -----------------------------
# Paths
# -----------------------------
path_in  <- "/Users/odran/Dropbox/sewage/data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp"
path_out <- "/Users/odran/Dropbox/sewage/data/processed/shapefiles/WatercourseLink_25m.gpkg"
layer_out <- "WatercourseLink_25m"

segment_length <- 25  # metres

cat("Reading WatercourseLink from:\n  ", path_in, "\n", sep = "")
wc <- sf::st_read(path_in, quiet = TRUE)

# Ensure projected CRS in metres (should already be EPSG:27700)
if (is.na(sf::st_crs(wc))) {
  sf::st_crs(wc) <- CRS_BNG
} else if (sf::st_crs(wc)$epsg != CRS_BNG) {
  wc <- sf::st_transform(wc, CRS_BNG)
}

# Cast to LINESTRING (if MULTILINESTRING exists)
wc <- sf::st_cast(wc, "LINESTRING")

cat("Features in original WatercourseLink:", nrow(wc), "\n")

# sanity: check required fields
required_fields <- c("identifier", "startNode", "endNode", "length")
missing_fields <- setdiff(required_fields, names(wc))
if (length(missing_fields) > 0) {
  stop("These required fields are missing in WatercourseLink: ",
       paste(missing_fields, collapse = ", "))
}

# -----------------------------
# Function to split ONE line feature
# -----------------------------
split_line_into_segments <- function(row, seg_len) {
  geom <- sf::st_geometry(row)[[1]]
  L    <- as.numeric(sf::st_length(geom))

  attrs <- sf::st_drop_geometry(row)

  orig_id        <- as.character(attrs$identifier)
  orig_startNode <- as.character(attrs$startNode)
  orig_endNode   <- as.character(attrs$endNode)

  # If line length <= seg_len, keep as a single segment
  if (!is.finite(L) || L <= seg_len) {
    attrs$identifier <- orig_id         # unchanged
    attrs$startNode  <- orig_startNode  # unchanged
    attrs$endNode    <- orig_endNode    # unchanged
    attrs$length     <- L               # recompute to be safe

    return(
      sf::st_as_sf(attrs,
                   geometry = sf::st_sfc(geom, crs = sf::st_crs(row)))
    )
  }

  # Number of full 25m chunks
  n_full <- floor(L / seg_len)

  # Cut distances along line (metres)
  cuts_m <- c(0, seg_len * seq_len(n_full))
  if (tail(cuts_m, 1) < L) {
    cuts_m <- c(cuts_m, L)  # residual piece at end
  }

  # Convert to fractions [0,1]
  cuts_t <- cuts_m / L

  # Build geometry segments
  seg_geoms <- vector("list", length(cuts_t) - 1)
  seg_len_m <- numeric(length(cuts_t) - 1)

  for (k in seq_len(length(cuts_t) - 1)) {
    gk <- lwgeom::st_linesubstring(geom, cuts_t[k], cuts_t[k + 1])
    seg_geoms[[k]] <- gk
    seg_len_m[k]   <- as.numeric(sf::st_length(gk))
  }

  n_seg <- length(seg_geoms)

  # Build attributes for each segment:
  # new identifier, new startNode/endNode, new length
  # internal node IDs are synthetic: "<orig_id>_N1", "<orig_id>_N2", ...
  seg_df <- attrs[rep(1, n_seg), , drop = FALSE]

  # new identifiers, e.g. "ID_1", "ID_2", ...
  seg_df$identifier <- paste0(orig_id, "_", seq_len(n_seg))

  # node IDs:
  # seg 1: startNode = orig_startNode, endNode = orig_id_N1
  # seg k (1 < k < n_seg): startNode = orig_id_N{ k-1 }, endNode = orig_id_N{k}
  # seg n_seg: startNode = orig_id_N{ n_seg-1 }, endNode = orig_endNode
  if (n_seg == 1L) {
    # should not happen due to length check, but be safe
    seg_df$startNode <- orig_startNode
    seg_df$endNode   <- orig_endNode
  } else {
    new_internal_nodes <- paste0(orig_id, "_N", seq_len(n_seg - 1))

    seg_start_nodes <- character(n_seg)
    seg_end_nodes   <- character(n_seg)

    for (k in seq_len(n_seg)) {
      if (k == 1L) {
        seg_start_nodes[k] <- orig_startNode
        seg_end_nodes[k]   <- new_internal_nodes[1]
      } else if (k == n_seg) {
        seg_start_nodes[k] <- new_internal_nodes[n_seg - 1]
        seg_end_nodes[k]   <- orig_endNode
      } else {
        seg_start_nodes[k] <- new_internal_nodes[k - 1]
        seg_end_nodes[k]   <- new_internal_nodes[k]
      }
    }

    seg_df$startNode <- seg_start_nodes
    seg_df$endNode   <- seg_end_nodes
  }

  # set new length (metres) for each segment
  seg_df$length <- seg_len_m

  sf::st_as_sf(
    seg_df,
    geometry = sf::st_sfc(seg_geoms, crs = sf::st_crs(row))
  )
}

# -----------------------------
# Apply to all features with a progress bar
# -----------------------------
n <- nrow(wc)
cat("Splitting all lines into ~", segment_length, " m segments...\n", sep = "")

segmented_list <- vector("list", n)
pb <- utils::txtProgressBar(min = 0, max = n, style = 3)

for (i in seq_len(n)) {
  segmented_list[[i]] <- split_line_into_segments(wc[i, ], segment_length)
  utils::setTxtProgressBar(pb, i)
}
close(pb)

segmented <- do.call(rbind, segmented_list)

cat("\nFinished splitting.\n")
cat("Original features :", n, "\n")
cat("Segmented features:", nrow(segmented), "\n")

# -----------------------------
# Write to GeoPackage
# -----------------------------
if (file.exists(path_out)) {
  cat("Output file exists, overwriting layer:", path_out, "\n")
}

sf::st_write(segmented, path_out, layer = layer_out,
             delete_layer = TRUE, quiet = TRUE)

cat("Wrote segmented rivers to:\n  ", path_out,
    "\nLayer name:\n  ", layer_out, "\n", sep = "")
