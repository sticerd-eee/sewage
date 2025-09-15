# ---- Packages ----
suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(sf)
  library(here)
  library(fs)
  library(scales)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(RColorBrewer)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

# =========================
# ==== USER PARAMETERS ====
# =========================

# Shapefiles (paths you provided)
lsoa_shp <- "/Users/odran/Dropbox/sewage/data/raw/shapefiles/LSOA/LSOA_2021_EW_BFE_V10.shp"
msoa_shp <- "/Users/odran/Dropbox/sewage/data/raw/shapefiles/MSOA/MSOA_2021_EW_BFC_V7.shp"

# Spill data
dp_path       <- here::here("data", "processed")
never_dir     <- fs::path(dp_path, "never_spilled_sites")
data_rdata    <- fs::path(dp_path, "merged_edm_1224_dry_spill_data.RData")
data_rds      <- fs::path(dp_path, "merged_edm_1224_dry_spill_data.rds")

# Output directory (absolute)
fig_dir <- "/Users/odran/Dropbox/sewage/output/figures/Maps"
fs::dir_create(fig_dir)

# Optional filters (set to NULL to include all)
year_range     <- c(2021L, 2024L)  # or NULL for all
company_filter <- NULL             # e.g., c("Thames Water","Severn Trent") or NULL

# =========================
# ======= HELPERS =========
# =========================

latest_csv <- function(dir, pattern) {
  if (!fs::dir_exists(dir)) return(NULL)
  f <- fs::dir_ls(dir, regexp = pattern, type = "file", fail = FALSE)
  if (length(f) == 0) return(NULL)
  f[order(fs::file_info(f)$modification_time, decreasing = TRUE)][1]
}

read_never_csv <- function(path) {
  if (is.null(path) || !fs::file_exists(path)) return(NULL)
  df <- suppressMessages(readr::read_csv(path, show_col_types = FALSE))
  nm <- tolower(names(df))
  if (!all(c("easting","northing") %in% nm)) return(NULL)
  df |>
    rename_with(~tolower(.x)) |>
    filter(!is.na(easting), !is.na(northing)) |>
    mutate(spill_count = 0L) |>
    distinct(easting, northing, .keep_all = TRUE)
}

df27700_to_sf4326 <- function(df27700) {
  st_as_sf(df27700, coords = c("easting","northing"), crs = 27700) |>
    st_transform(4326)
}

pick_name_col <- function(g, priority) {
  candidates <- c(priority, toupper(priority), tolower(priority),
                  "NAME","Name","name")
  hits <- candidates[candidates %in% names(g)]
  if (length(hits)) hits[1] else {
    chrs <- names(g)[vapply(g, function(x) is.character(x) || is.factor(x), logical(1))]
    if (length(chrs)) chrs[1] else names(g)[1]
  }
}

# Save a ggplot choropleth (NO borders)
write_map <- function(sf_poly, fill_col, title, subtitle, legend_title, out_path) {
  vals <- sf_poly[[fill_col]]
  if (length(vals) == 0 || all(is.na(vals))) {
    message("No data to plot for: ", title)
    png(out_path, width = 1800, height = 1400, res = 200)
    plot.new(); text(0.5, 0.5, paste0(title, "\n(no data)"))
    dev.off()
    return(invisible(NULL))
  }
  p <- ggplot(sf_poly) +
    geom_sf(aes(fill = .data[[fill_col]]), color = NA, linewidth = 0) +
    scale_fill_gradientn(colors = RColorBrewer::brewer.pal(9, "YlOrRd"),
                         name = legend_title, na.value = "grey90",
                         labels = comma) +
    labs(title = title, subtitle = subtitle, caption = "Source: EDM spill dataset / Never-spilled exports / ONS boundaries (2021)") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "right")
  ggsave(out_path, p, width = 9, height = 7.2, dpi = 300)
  message("Wrote: ", out_path)
}

# Generic aggregator for a polygon layer with name field 'nm_col'
aggregate_for_layer <- function(points_sf, polys_sf, nm_col) {
  # Join points to polygons
  joined <- st_join(points_sf, polys_sf, join = st_within, left = FALSE)

  # Name column as a one-col data frame (avoid vector drop)
  nm_df <- dplyr::select(st_drop_geometry(polys_sf), dplyr::all_of(nm_col))

  # 1) events = number of rows (each row = an event)
  agg_events <- joined |>
    st_drop_geometry() |>
    group_by(across(all_of(nm_col))) |>
    summarise(events = n(), .groups = "drop") |>
    right_join(nm_df, by = setNames(nm_col, nm_col)) |>
    mutate(events = events %||% 0L)

  polys_events <- polys_sf |>
    left_join(select(agg_events, all_of(c(nm_col, "events"))),
              by = setNames(nm_col, nm_col))

  # 2) sites = unique sites with >=1 event
  agg_sites <- joined |>
    st_drop_geometry() |>
    group_by(across(all_of(nm_col))) |>
    summarise(sites = n_distinct(site_id %||% row_number()), .groups = "drop") |>
    right_join(nm_df, by = setNames(nm_col, nm_col)) |>
    mutate(sites = sites %||% 0L)

  polys_sites <- polys_sf |>
    left_join(select(agg_sites, all_of(c(nm_col, "sites"))),
              by = setNames(nm_col, nm_col))

  list(events = polys_events, sites = polys_sites)
}

# Never-spilled aggregation helper
agg_never <- function(pts_sf, polys_sf, nm_col, out_col) {
  nm_vals <- st_drop_geometry(polys_sf)[[nm_col]]
  nm_df   <- tibble(!!nm_col := nm_vals)

  if (is.null(pts_sf) || nrow(pts_sf) == 0) {
    out <- nm_df |> mutate(tmp = 0L)
    names(out)[names(out) == "tmp"] <- out_col
    return(polys_sf |> left_join(out, by = setNames(nm_col, nm_col)))
  }

  j <- st_join(pts_sf, polys_sf, join = st_within, left = FALSE)

  counts <- j |>
    st_drop_geometry() |>
    group_by(across(all_of(nm_col))) |>
    summarise(n = n(), .groups = "drop") |>
    right_join(nm_df, by = nm_col) |>
    mutate(n = n %||% 0L)

  names(counts)[names(counts) == "n"] <- out_col
  polys_sf |> left_join(counts, by = setNames(nm_col, nm_col))
}

# One-shot map writer for a geography layer (NO border variants)
make_maps_for_geo <- function(geo_name, nm_col, polys_sf,
                              spill_pts_sf, never_loose_sf, never_strict_sf,
                              year_range, company_filter, fig_dir, ts_tag) {

  # Aggregate (events, sites)
  ag <- aggregate_for_layer(spill_pts_sf, polys_sf, nm_col)
  polys_events <- ag$events
  polys_sites  <- ag$sites

  # Never-spilled (if available)
  polys_never_loose  <- agg_never(never_loose_sf,  polys_sf, nm_col, "never_loose")
  polys_never_strict <- agg_never(never_strict_sf, polys_sf, nm_col, "never_strict")

  subtitle_tag <- paste0(
    if (!is.null(year_range)) sprintf("Years: %s–%s; ", year_range[1], year_range[2]) else "",
    if (!is.null(company_filter)) sprintf("Companies: %s; ", paste(company_filter, collapse = ", ")) else "",
    sprintf("Projection: WGS84; Join: point-in-polygon to %s (2021)", toupper(geo_name))
  )

  metrics <- list(
    list(obj = polys_events,       col = "events",
         title = sprintf("Actual Spills — Total Events by %s", toupper(geo_name)),
         legend = "Spill events", slug = "events"),
    list(obj = polys_sites,        col = "sites",
         title = sprintf("All Spill Sites — Unique Sites by %s", toupper(geo_name)),
         legend = "Unique sites", slug = "sites"),
    list(obj = polys_never_loose,  col = "never_loose",
         title = sprintf("Never-Spilled Sites (Loose) — Count by %s", toupper(geo_name)),
         legend = "Never-spilled sites", slug = "never_loose"),
    list(obj = polys_never_strict, col = "never_strict",
         title = sprintf("Never-Spilled Sites (Strict) — Count by %s", toupper(geo_name)),
         legend = "Never-spilled sites", slug = "never_strict")
  )

  for (m in metrics) {
    out_path <- file.path(fig_dir, sprintf("%s_heatmap_%s_%s.png",
                                           tolower(geo_name), m$slug, ts_tag))
    write_map(
      sf_poly = m$obj,
      fill_col = m$col,
      title = m$title,
      subtitle = subtitle_tag,
      legend_title = m$legend,
      out_path = out_path
    )
  }
}

# =========================
# ===== LOAD SHAPEFILES ===
# =========================

# LSOA
stopifnot(fs::file_exists(lsoa_shp))
lsoa <- suppressWarnings(try(sf::st_read(lsoa_shp, quiet = TRUE), silent = TRUE))
if (!inherits(lsoa, "sf")) stop("Could not read LSOA shapefile: ", lsoa_shp)
lsoa <- lsoa |> st_make_valid() |> st_transform(4326)
lsoa_name_col <- pick_name_col(lsoa, "LSOA21NM")
lsoa$geo_name <- as.character(lsoa[[lsoa_name_col]])
lsoa <- dplyr::select(lsoa, geo_name, geometry)

# MSOA
stopifnot(fs::file_exists(msoa_shp))
msoa <- suppressWarnings(try(sf::st_read(msoa_shp, quiet = TRUE), silent = TRUE))
if (!inherits(msoa, "sf")) stop("Could not read MSOA shapefile: ", msoa_shp)
msoa <- msoa |> st_make_valid() |> st_transform(4326)
msoa_name_col <- pick_name_col(msoa, "MSOA21NM")
msoa$geo_name <- as.character(msoa[[msoa_name_col]])
msoa <- dplyr::select(msoa, geo_name, geometry)

# =========================
# ===== LOAD SPILL DATA ===
# =========================

loaded <- FALSE
dry_spills_defined <- NULL

if (fs::file_exists(data_rdata) && fs::file_info(data_rdata)$size > 0) {
  load(data_rdata) # should define dry_spills_defined
  loaded <- exists("dry_spills_defined") && nrow(dry_spills_defined) > 0
} else if (fs::file_exists(data_rds) && fs::file_info(data_rds)$size > 0) {
  dry_spills_defined <- readRDS(data_rds)
  loaded <- is.data.frame(dry_spills_defined) && nrow(dry_spills_defined) > 0
}

if (!loaded) {
  stop(
    "Could not load spill data. Expected either:\n• ",
    data_rdata, "\n• ", data_rds,
    "\nCheck that here::here('data','processed') resolves correctly."
  )
}

# ---- Flags & time fields ----
to_yes <- function(x) tolower(as.character(x)) == "yes"

spill_data <- dry_spills_defined |>
  mutate(
    year      = suppressWarnings(lubridate::year(block_start)),
    month     = suppressWarnings(lubridate::month(block_start)),
    dry_d1    = to_yes(`dry_day_1` %||% NA),
    dry_d2    = to_yes(`dry_day_2` %||% NA),
    dry_ea    = to_yes(`ea_dry_spill` %||% NA),
    dry_bbc   = to_yes(`bbc_dry_spill` %||% NA),
    any_dry   = (dry_d1 %||% FALSE) | (dry_d2 %||% FALSE) | (dry_ea %||% FALSE) | (dry_bbc %||% FALSE),
    wet_spill = !any_dry
  )

# Optional filters
if (!is.null(year_range) && length(year_range) == 2) {
  spill_data <- spill_data |>
    filter(between(year, year_range[1], year_range[2]))
}
if (!is.null(company_filter) && length(company_filter) > 0) {
  spill_data <- spill_data |>
    filter(water_company %in% company_filter)
}

# Keep valid coords and convert to sf points (BNG -> WGS84)
spill_pts_df <- spill_data |> filter(!is.na(easting), !is.na(northing))
spill_pts_sf <- st_as_sf(spill_pts_df, coords = c("easting","northing"), crs = 27700) |>
  st_transform(4326)

# =========================
# == NEVER-SPILLED (opt) ==
# =========================

strict_csv <- latest_csv(never_dir, "STRICT_\\d{4}_\\d{4}\\.csv$")
loose_csv  <- latest_csv(never_dir, "LOOSE_\\d{4}_\\d{4}\\.csv$")

never_strict_df <- read_never_csv(strict_csv)
never_loose_df  <- read_never_csv(loose_csv)

never_strict_sf <- if (!is.null(never_strict_df)) df27700_to_sf4326(never_strict_df) else NULL
never_loose_sf  <- if (!is.null(never_loose_df))  df27700_to_sf4326(never_loose_df)  else NULL

# =========================
# ======= PLOTTING ========
# =========================

ts_tag <- format(Sys.time(), "%Y%m%d_%H%M%S")

# LSOA maps (no borders)
make_maps_for_geo(
  geo_name = "lsoa", nm_col = "geo_name", polys_sf = lsoa,
  spill_pts_sf = spill_pts_sf,
  never_loose_sf = never_loose_sf, never_strict_sf = never_strict_sf,
  year_range = year_range, company_filter = company_filter,
  fig_dir = fig_dir, ts_tag = ts_tag
)

# MSOA maps (no borders)
make_maps_for_geo(
  geo_name = "msoa", nm_col = "geo_name", polys_sf = msoa,
  spill_pts_sf = spill_pts_sf,
  never_loose_sf = never_loose_sf, never_strict_sf = never_strict_sf,
  year_range = year_range, company_filter = company_filter,
  fig_dir = fig_dir, ts_tag = ts_tag
)

message("\nDone. Figures saved to: ", fig_dir,
        "\nFiles are named like:\n",
        "  lsoa_heatmap_events_YYYYMMDD_HHMMSS.png\n",
        "  lsoa_heatmap_never_loose_YYYYMMDD_HHMMSS.png\n",
        "  msoa_heatmap_sites_YYYYMMDD_HHMMSS.png\n")
