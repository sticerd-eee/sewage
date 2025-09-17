# ---- Plot all site points only (diagnostic) ----
suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(arrow); library(ggplot2); library(sf)
})
if (!requireNamespace("rnrfa", quietly = TRUE)) install.packages("rnrfa")
suppressPackageStartupMessages(library(rnrfa))

agg_base <- "/Users/odran/Dropbox/sewage/data/processed/spill_aggregated"
p_dry_mo <- file.path(agg_base, "agg_spill_dry_mo.parquet")

# 1) Read monthly dry table (for site IDs + NGR fields)
dry_mo <- read_parquet(p_dry_mo) |> as_tibble()

# 2) One NGR per site (prefer ngr then ngr_og); clean & keep plausible OSGB refs
ngr_df <- dry_mo |>
  transmute(site_id = as.integer(site_id), water_company,
            ngr = dplyr::coalesce(dplyr::na_if(ngr, ""), dplyr::na_if(ngr_og, ""))) |>
  distinct(site_id, water_company, .keep_all = TRUE) |>
  mutate(ngr = toupper(gsub("\\s+", "", ngr))) |>
  filter(!is.na(ngr),
         grepl("^[A-Z]{2}[0-9]{2,10}$", ngr),
         nchar(sub("^[A-Z]{2}", "", ngr)) %% 2 == 0)

message("Sites with plausible NGRs: ", nrow(ngr_df))

# 3) Safe parse; keep raw two columns as A/B (order unknown)
safe_parse <- function(s) {
  out <- tryCatch(rnrfa::osg_parse(s, coord_system = "WGS84"), error = function(e) c(NA_real_, NA_real_))
  v <- suppressWarnings(as.numeric(out))
  if (length(v) >= 2) c(v[1], v[2]) else c(NA_real_, NA_real_)
}
raw_mat <- t(vapply(ngr_df$ngr, safe_parse, numeric(2L)))
colnames(raw_mat) <- c("A","B")
loc_raw <- bind_cols(ngr_df, as.data.frame(raw_mat))

# 4) Two candidate interpretations
pts_as_is <- loc_raw |> transmute(site_id, water_company, lat = A, lon = B) |> filter(!is.na(lat), !is.na(lon))
pts_swapped <- loc_raw |> transmute(site_id, water_company, lat = B, lon = A) |> filter(!is.na(lat), !is.na(lon))

rng_as_is <- pts_as_is  |> summarise(lat_min=min(lat), lat_max=max(lat), lon_min=min(lon), lon_max=max(lon))
rng_swap  <- pts_swapped |> summarise(lat_min=min(lat), lat_max=max(lat), lon_min=min(lon), lon_max=max(lon))

message("As-is ranges:   lat [", round(rng_as_is$lat_min,3), ", ", round(rng_as_is$lat_max,3),
        "]  lon [", round(rng_as_is$lon_min,3), ", ", round(rng_as_is$lon_max,3), "]")
message("Swapped ranges:  lat [", round(rng_swap$lat_min,3), ", ", round(rng_swap$lat_max,3),
        "]  lon [", round(rng_swap$lon_min,3), ", ", round(rng_swap$lon_max,3), "]")

# 5) Plot points only (no basemap, no save)
p_as_is <- ggplot(pts_as_is, aes(x = lon, y = lat)) +
  geom_point(alpha = 0.3, size = 0.3) +
  coord_equal() +
  labs(title = "All site points — assuming osg_parse = (lat, lon)",
       subtitle = "No basemap; diagnostic scatter",
       x = "lon", y = "lat") +
  theme_minimal(base_size = 11)

p_swapped <- ggplot(pts_swapped, aes(x = lon, y = lat)) +
  geom_point(alpha = 0.3, size = 0.3) +
  coord_equal() +
  labs(title = "All site points — assuming osg_parse = (lon, lat) [swapped]",
       subtitle = "No basemap; diagnostic scatter",
       x = "lon", y = "lat") +
  theme_minimal(base_size = 11)

print(p_as_is)
print(p_swapped)
