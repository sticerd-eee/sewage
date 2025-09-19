# ==== Population within distance of spill sites (spilled_any / dry_only / no_spill), UK 2021 ====

suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(tibble); library(stringr)
  library(sf); library(terra); library(arrow); library(scales)
})
if (!requireNamespace("rnrfa", quietly = TRUE)) install.packages("rnrfa")
suppressPackageStartupMessages(library(rnrfa))

`%||%` <- function(x, y) if (is.null(x)) y else x

# -----------------
# User inputs
# -----------------
# Spill data (monthly aggregates)
agg_base <- "/Users/odran/Dropbox/sewage/data/processed/agg_spill_stats"
p_all_mo <- file.path(agg_base, "agg_spill_mo.parquet")
p_dry_mo <- file.path(agg_base, "agg_spill_dry_mo.parquet")

# Population raster (2021)
pop_tif  <- "/Users/odran/Dropbox/sewage/data/raw/population_grid/data/uk_residential_population_2021.tif"

# Output (table)
out_dir  <- "/Users/odran/Dropbox/sewage/output/tables"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_csv  <- file.path(out_dir, "population_within_buffers_by_category_new.csv")

# Analysis window & dry definition
year_min <- 2021L; year_max <- 2023L
dry_metric <- "dry_spill_count_mo_r1_d0123_strict"

# Buffer distances in meters
distances_m <- c(50, 100, 250, 500, 1000)

# -----------------
# Load & classify sites (2021–2023)
# -----------------
stopifnot(file.exists(p_all_mo), file.exists(p_dry_mo))
base_year <- 2021L
all_mo <- read_parquet(p_all_mo) |> as_tibble() |> mutate(
  site_id = as.integer(site_id),
  year = base_year + floor((month_id - 1L) / 12)
)
dry_mo <- read_parquet(p_dry_mo) |> as_tibble() |> mutate(
  site_id = as.integer(site_id),
  year = base_year + floor((month_id - 1L) / 12)
)
stopifnot(dry_metric %in% names(dry_mo))

all_win <- all_mo |> filter(year >= year_min, year <= year_max)
dry_win <- dry_mo |> filter(year >= year_min, year <= year_max)

site_any <- all_win |>
  group_by(site_id, water_company) |>
  summarise(total_spills = sum(spill_count_mo, na.rm = TRUE), .groups = "drop")

site_dry <- dry_win |>
  group_by(site_id, water_company) |>
  summarise(total_dry = sum(.data[[dry_metric]], na.rm = TRUE), .groups = "drop")

# New categories:
# - spilled_any: any spill (wet or dry) > 0
# - dry_only:    at least one dry spill (regardless of wet)  << interpreted as “had any dry”
# - no_spill:    zero spills in window
site_status <- site_any |>
  left_join(site_dry, by = c("site_id","water_company")) |>
  mutate(
    total_dry = replace_na(total_dry, 0),
    spilled_any = total_spills > 0,
    dry_only    = total_dry > 0,      # “dry only” interpreted as “has any dry”
    no_spill    = total_spills == 0
  ) |>
  transmute(
    site_id, water_company,
    category = case_when(
      no_spill              ~ "no_spill",
      dry_only              ~ "dry_only",
      spilled_any           ~ "spilled_any",
      TRUE                  ~ "no_spill"
    )
  )

# -----------------
# One coordinate per site (NGR -> WGS84). Your osg_parse returns (lon, lat).
# -----------------
ngr_df <- dry_mo |>
  transmute(site_id = as.integer(site_id), water_company,
            ngr = dplyr::coalesce(dplyr::na_if(ngr, ""), dplyr::na_if(ngr_og, ""))) |>
  distinct(site_id, water_company, .keep_all = TRUE) |>
  mutate(ngr = toupper(gsub("\\s+", "", ngr))) |>
  filter(!is.na(ngr),
         grepl("^[A-Z]{2}[0-9]{2,10}$", ngr),
         nchar(sub("^[A-Z]{2}", "", ngr)) %% 2 == 0)

safe_parse <- function(s) {
  out <- tryCatch(rnrfa::osg_parse(s, coord_system = "WGS84"), error = function(e) c(NA_real_, NA_real_))
  v <- suppressWarnings(as.numeric(out))
  if (length(v) >= 2) c(v[1], v[2]) else c(NA_real_, NA_real_)
}
ll <- t(vapply(ngr_df$ngr, safe_parse, numeric(2L)))
colnames(ll) <- c("lon","lat")
loc_df <- bind_cols(ngr_df, as.data.frame(ll)) |> filter(!is.na(lat), !is.na(lon))

sites_sf <- st_as_sf(loc_df, coords = c("lon","lat"), crs = 4326) |>
  left_join(site_status, by = c("site_id","water_company")) |>
  filter(!is.na(category))

# -----------------
# Load population raster & pick working CRS for buffering
# -----------------
stopifnot(file.exists(pop_tif))
pop_r <- terra::rast(pop_tif)

if (terra::is.lonlat(pop_r)) {
  # Use a metric CRS for buffering; project raster once to British National Grid (EPSG:27700)
  message("Population raster is lon/lat; projecting to EPSG:27700 for metric buffers...")
  pop_r_m <- terra::project(pop_r, "EPSG:27700", method = "near")
  sites_proj <- st_transform(sites_sf, 27700)
} else {
  pop_r_m <- pop_r
  sites_proj <- st_transform(sites_sf, terra::crs(pop_r_m))
}

# Convert sites to terra vect
sites_v <- terra::vect(sites_proj)

# -----------------
# Helper: population within unioned buffers (no double-counting)
# -----------------
pop_within_union_buffers <- function(pop_raster, pts_vect, dist_m) {
  if (length(pts_vect) == 0) return(0)
  buf <- terra::buffer(pts_vect, width = dist_m)
  buf_u <- terra::aggregate(buf, dissolve = TRUE)
  pop_crop <- terra::crop(pop_raster, buf_u)
  vals <- terra::extract(pop_crop, buf_u, fun = sum, na.rm = TRUE)
  # 'vals' has ID column then the sum for the raster layer(s); take numeric sum of non-ID columns
  sums <- vals[, -1, drop = FALSE]
  sum(as.numeric(unlist(sums)), na.rm = TRUE)
}

# -----------------
# Compute totals per category & distance
# -----------------
cats <- c("spilled_any", "dry_only", "no_spill")
res_list <- list()

for (cat in cats) {
  pts_cat <- sites_v[which(sites_v$category == cat), ]
  totals <- vapply(distances_m, function(d) pop_within_union_buffers(pop_r_m, pts_cat, d), numeric(1))
  res_list[[cat]] <- totals
}

res_mat <- do.call(rbind, res_list)
colnames(res_mat) <- paste0("within_", distances_m, "m")
pop_table <- as_tibble(res_mat, rownames = "category") |>
  mutate(across(-category, ~ round(.x)))

# Add site counts per category for context
site_counts <- sites_sf |> st_drop_geometry() |> count(category, name = "n_sites")
pop_table <- pop_table |>
  left_join(site_counts, by = "category") |>
  relocate(n_sites, .after = category)

# -----------------
# Output
# -----------------
print(pop_table)
readr::write_csv(pop_table, out_csv)
message("Wrote table: ", out_csv)

# -----------------
# (Optional) Double-counting variant (per-site buffers summed without dissolve)
# -----------------
# pop_within_per_site <- function(pop_raster, pts_vect, dist_m) {
#   if (length(pts_vect) == 0) return(0)
#   vals <- terra::extract(pop_raster, pts_vect, fun = sum, buffer = dist_m, na.rm = TRUE)
#   sums <- vals[, -1, drop = FALSE]
#   sum(as.numeric(unlist(sums)), na.rm = TRUE)
# }
# res_dc <- list()
# for (cat in cats) {
#   pts_cat <- sites_v[which(sites_v$category == cat), ]
#   totals <- vapply(distances_m, function(d) pop_within_per_site(pop_r_m, pts_cat, d), numeric(1))
#   res_dc[[cat]] <- totals
# }
# res_dc_mat <- do.call(rbind, res_dc)
# colnames(res_dc_mat) <- paste0("within_", distances_m, "m_dc")
# pop_table_dc <- as_tibble(res_dc_mat, rownames = "category") |>
#   mutate(across(-category, ~ round(.x))) |>
#   left_join(site_counts, by = "category") |>
#   relocate(n_sites, .after = category)
# print(pop_table_dc)
# readr::write_csv(pop_table_dc, file.path(out_dir, "population_within_buffers_by_category_new_doublecount.csv"))
