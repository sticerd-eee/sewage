# ==== MSOA choropleths (counts + log-scaled), red palette, 2021–2023 ====

suppressPackageStartupMessages({
  library(dplyr); library(sf); library(ggplot2); library(arrow)
  library(tibble); library(tidyr); library(scales)
})
if (!requireNamespace("rnrfa", quietly = TRUE)) install.packages("rnrfa")
suppressPackageStartupMessages(library(rnrfa))

`%||%` <- function(x, y) if (is.null(x)) y else x

# --- Inputs ---
msoa_shp <- "/Users/odran/Dropbox/sewage/data/raw/shapefiles/MSOA/MSOA_2021_EW_BFC_V7.shp"
agg_base <- "/Users/odran/Dropbox/sewage/data/processed/spill_aggregated"
p_all_mo <- file.path(agg_base, "agg_spill_mo.parquet")
p_dry_mo <- file.path(agg_base, "agg_spill_dry_mo.parquet")
out_dir  <- "/Users/odran/Dropbox/sewage/output/figures/Maps"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Aggregation window (all months across these years are combined/summed)
year_min <- 2021L; year_max <- 2023L

# Dry definition to use from agg_spill_dry_mo.parquet
dry_metric <- "dry_spill_count_mo_r1_d0123_strict"

# --- Read MSOA shapefile ---
stopifnot(file.exists(msoa_shp))
msoa <- st_read(msoa_shp, quiet = TRUE) |>
  st_make_valid() |>
  st_transform(4326)

name_candidates <- c("MSOA21NM","MSOA11NM","MSOA_NAME","name","NAME")
nm_hit <- name_candidates[name_candidates %in% names(msoa)]
MSOA_NAME <- if (length(nm_hit)) nm_hit[1] else {
  chr_cols <- names(msoa)[vapply(msoa, function(x) is.character(x) || is.factor(x), logical(1))]
  if (length(chr_cols)) chr_cols[1] else names(msoa)[1]
}
msoa <- msoa |>
  mutate(MSOA_NAME = as.character(.data[[MSOA_NAME]])) |>
  select(MSOA_NAME, geometry)

# --- Read monthly aggregated data ---
stopifnot(file.exists(p_all_mo), file.exists(p_dry_mo))
all_mo <- read_parquet(p_all_mo) |> as_tibble() |> mutate(site_id = as.integer(site_id))
dry_mo <- read_parquet(p_dry_mo) |> as_tibble() |> mutate(site_id = as.integer(site_id))
stopifnot(dry_metric %in% names(dry_mo))

# --- Window filter (combine months across years) ---
all_win <- all_mo |> filter(year >= year_min, year <= year_max)
dry_win <- dry_mo |> filter(year >= year_min, year <= year_max)

# --- Site totals & classification over the window ---
site_any <- all_win |>
  group_by(site_id, water_company) |>
  summarise(total_spills = sum(spill_count_mo, na.rm = TRUE), .groups = "drop")

site_dry <- dry_win |>
  group_by(site_id, water_company) |>
  summarise(total_dry = sum(.data[[dry_metric]], na.rm = TRUE), .groups = "drop")

site_status <- site_any |>
  left_join(site_dry, by = c("site_id","water_company")) |>
  mutate(
    total_dry = replace_na(total_dry, 0),
    status = case_when(
      total_spills == 0 ~ "no_spill",
      total_spills > 0 & total_dry == 0 ~ "wet_only",
      total_dry > 0 ~ "has_dry",
      TRUE ~ "no_spill"
    )
  ) |>
  select(site_id, water_company, status, total_spills, total_dry)

# --- One NGR per site -> WGS84 points
# NOTE: osg_parse returns (lon, lat) on your setup.
ngr_df <- dry_mo |>
  transmute(site_id = as.integer(site_id), water_company,
            ngr = dplyr::coalesce(na_if(ngr, ""), na_if(ngr_og, ""))) |>
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
latlon <- t(vapply(ngr_df$ngr, safe_parse, numeric(2L)))
colnames(latlon) <- c("lon","lat")
loc_df <- bind_cols(ngr_df, as.data.frame(latlon)) |>
  filter(!is.na(lat), !is.na(lon))

pts <- st_as_sf(loc_df, coords = c("lon","lat"), crs = 4326) |>
  left_join(site_status, by = c("site_id","water_company")) |>
  filter(!is.na(status))

# --- Join to MSOAs and aggregate **site counts** per class ---
j <- if (nrow(pts) > 0) st_join(pts, msoa, join = st_within, left = FALSE) else pts[0,]
j <- st_drop_geometry(j)

msoa_counts <- j |>
  group_by(MSOA_NAME) |>
  summarise(
    sites_wet_only = sum(status == "wet_only"),
    sites_has_dry  = sum(status == "has_dry"),
    sites_no_spill = sum(status == "no_spill"),
    .groups = "drop"
  )

msoa_agg <- msoa |>
  left_join(msoa_counts, by = "MSOA_NAME") |>
  mutate(across(starts_with("sites_"), ~ replace_na(.x, 0L)))

# --- Plot helpers (no borders) with professional legend titles ---
plot_red <- function(sf_obj, col, title, legend_title, subtitle_extra = "") {
  ggplot(sf_obj) +
    geom_sf(aes(fill = .data[[col]]), color = NA) +
    scale_fill_gradient(
      low = "#fff5f0", high = "#cb181d",
      name = legend_title,
      labels = scales::comma
    ) +
    labs(
      title = title,
      subtitle = paste0("MSOA • ", year_min, "–", year_max,
                        if (nzchar(subtitle_extra)) paste0(" • ", subtitle_extra) else ""),
      caption = "Aggregated EDM spills (monthly); MSOA 2021 boundaries"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "right", legend.title = element_text(face = "bold"))
}

# Log map (Option A): map & legend use same log1p transform; labels show raw counts
pretty_count_breaks <- function(max_val) {
  cand <- c(0,1,2,5,10,20,50,100,200,500,1000,2000,5000,10000,20000,50000,100000)
  cand[cand <= max_val & !duplicated(cand)]
}

plot_red_log <- function(sf_obj, col, title, legend_title) {
  vmax <- max(sf_obj[[col]], na.rm = TRUE)
  brks <- pretty_count_breaks(vmax)
  if (length(brks) < 2) brks <- c(0, 1)  # ensure at least two ticks
  ggplot(sf_obj) +
    geom_sf(aes(fill = .data[[col]]), color = NA) +
    scale_fill_gradient(
      low = "#fff5f0", high = "#cb181d",
      name = legend_title,
      trans = scales::log1p_trans(),   # <- same transform used for map & legend
      breaks = brks,
      labels = scales::comma
    ) +
    labs(
      title = title,
      subtitle = paste0("MSOA • ", year_min, "–", year_max, " • log scale (log1p)"),
      caption = "Aggregated EDM spills (monthly); MSOA 2021 boundaries"
    ) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "right", legend.title = element_text(face = "bold"))
}

# --- Build six plots: raw counts + log-scale (same data, different scale) ---
p_wet      <- plot_red(msoa_agg, "sites_wet_only", "Sites with Wet-Only Spills", "Sites (count)")
p_wet_log  <- plot_red_log(msoa_agg, "sites_wet_only", "Sites with Wet-Only Spills", "Sites (count)")

p_dry      <- plot_red(msoa_agg, "sites_has_dry",  "Sites with ≥1 Dry Spill", "Sites (count)")
p_dry_log  <- plot_red_log(msoa_agg, "sites_has_dry",  "Sites with ≥1 Dry Spill", "Sites (count)")

p_nosp     <- plot_red(msoa_agg, "sites_no_spill", "Sites with No Spills", "Sites (count)")
p_nosp_log <- plot_red_log(msoa_agg, "sites_no_spill", "Sites with No Spills", "Sites (count)")

# --- Show ---
print(p_wet); print(p_wet_log)
print(p_dry); print(p_dry_log)
print(p_nosp); print(p_nosp_log)

# --- Saves ---
ggsave(file.path(out_dir, "msoa_wet_only.png"),       p_wet,     width = 9, height = 7, dpi = 300)
ggsave(file.path(out_dir, "msoa_wet_only_log.png"),   p_wet_log, width = 9, height = 7, dpi = 300)
ggsave(file.path(out_dir, "msoa_has_dry.png"),        p_dry,     width = 9, height = 7, dpi = 300)
ggsave(file.path(out_dir, "msoa_has_dry_log.png"),    p_dry_log, width = 9, height = 7, dpi = 300)
ggsave(file.path(out_dir, "msoa_no_spill.png"),       p_nosp,    width = 9, height = 7, dpi = 300)
ggsave(file.path(out_dir, "msoa_no_spill_log.png"),   p_nosp_log,width = 9, height = 7, dpi = 300)
