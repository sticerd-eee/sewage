# ==============================================================================
# Spill Maps with London Inset - MSOA-Level Sewage Spill Spatial Distribution
# ==============================================================================
#
# Purpose: Generate static PDF maps showing spill counts by MSOA for 2021-2023
#          with a London inset panel.
#          Produces two maps: total spills and dry spills (sum across years).
#
# Author: Jacopo Olivieri
# Date: 2026-03-20
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill data
#   - data/processed/agg_spill_stats/agg_spill_dry_yr.parquet - Dry spill data
#   - data/processed/unique_spill_sites.parquet - Site coordinates
#   - data/raw/shapefiles/msoa_bcg_2021/ - MSOA boundary shapefiles
#   - data/raw/shapefiles/msoa_population_2021/sapemsoasyoatablefinal.xlsx
#   - data/raw/shapefiles/local_authorities_uk_buc/ - London LAD boundaries
#
# Outputs:
#   - output/figures/maps/spill_total_count_2021_2023_london_inset.pdf
#   - output/figures/maps/dry_spill_total_count_2021_2023_london_inset.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 7
PLOT_HEIGHT <- 11
PLOT_DPI <- 300
TARGET_YEARS <- 2021:2023
YEAR_RANGE_LABEL <- paste0(min(TARGET_YEARS), "-", max(TARGET_YEARS))
YEAR_FILENAME_LABEL <- paste(min(TARGET_YEARS), max(TARGET_YEARS), sep = "_")
NO_SPILL_COLOR <- "grey90"
VIRIDIS_PALETTE <- "magma"
LONDON_LAD_PREFIX <- "^E09"
INSET_PAD_FRACTION <- -0.05
INSET_SCALE <- 2.7
INSET_TARGET_X_FRACTION <- 1.04
INSET_TARGET_Y_FRACTION <- 0.05
INSET_CLAMP_RELAX_FRACTION <- 0.25
INSET_UNITS <- "km"


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "rio",
  "tidyverse",
  "here",
  "sf",
  "DescTools",
  "readxl",
  "showtext",
  "sysfonts",
  "ggmapinset"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)


# ==============================================================================
# 3. Setup
# ==============================================================================

# 3.1 Font Setup ---------------------------------------------------------------
showtext::showtext_auto()
showtext::showtext_opts(dpi = 300)
sysfonts::font_add_google("Libertinus Serif", "libertinus", db_cache = FALSE)
FONT_FAMILY <- "libertinus"

# 3.2 Publication-Quality Map Theme --------------------------------------------
theme_map_publication <- function() {
  theme_void(base_family = FONT_FAMILY) +
    theme(
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.title = element_text(size = 10, face = "bold", margin = margin(b = 5)),
      legend.text = element_text(size = 9),
      legend.key.width = unit(2.5, "cm"),
      legend.key.height = unit(0.4, "cm"),
      legend.margin = margin(t = 15, b = 5),
      plot.margin = margin(t = 8, r = 25, b = 25, l = 8)
    )
}

# 3.3 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures", "maps")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}


# ==============================================================================
# 4. Data Loading
# ==============================================================================
cat("Loading data...\n")

# Regular spill data
dat_yr <- rio::import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_yr.parquet")
)

# Dry spill data
dat_dry_yr <- rio::import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_dry_yr.parquet")
)

# Spill sites (coordinates)
spill_sites <- rio::import(
  here::here("data", "processed", "unique_spill_sites.parquet"),
  trust = TRUE
)

# MSOA boundaries (England only)
msoa_boundaries <- sf::st_read(
  here::here("data", "raw", "shapefiles", "msoa_bcg_2021"),
  quiet = TRUE
) %>%
  filter(str_detect(MSOA21CD, "^E")) %>%
  st_simplify(dTolerance = 200) %>%
  select(MSOA21CD, MSOA21NM, geometry) %>%
  rename(msoa_code = MSOA21CD, msoa_name = MSOA21NM) %>%
  mutate(msoa_area_km2 = as.numeric(st_area(.)) / 1000000)

# MSOA population data
msoa_population <- readxl::read_excel(
  here::here(
    "data", "raw", "shapefiles", "msoa_population_2021",
    "sapemsoasyoatablefinal.xlsx"
  ),
  sheet = "Mid-2022 MSOA 2021",
  range = "A4:E7268"
) %>%
  select(msoa_code = 3, msoa_population = 5) %>%
  filter(!is.na(msoa_code), !is.na(msoa_population))

# London LAD boundaries
london_lads <- sf::st_read(
  here::here(
    "data", "raw", "shapefiles", "local_authorities_uk_buc",
    "Local_Authority_Districts_(May_2025)_Boundaries_UK_BUC.shp"
  ),
  quiet = TRUE
) %>%
  filter(str_detect(LAD25CD, LONDON_LAD_PREFIX)) %>%
  select(LAD25CD, LAD25NM, geometry) %>%
  st_transform(st_crs(msoa_boundaries))

if (nrow(london_lads) == 0) {
  stop("No London LAD boundaries found using prefix ", LONDON_LAD_PREFIX, ".")
}

cat("  Data loaded successfully\n")


# ==============================================================================
# 5. Helper Functions
# ==============================================================================

# Aggregate regular spills to MSOA level
aggregate_spills_to_msoa <- function(data, spill_sites, years) {
  # Join spill data with site coordinates
  dat_clean <- left_join(
    select(data, -ngr),
    spill_sites,
    by = join_by(water_company, site_id)
  )

  # Convert to sf and filter for year range
  spill_sites_sf <- dat_clean %>%
    filter(year %in% !!years) %>%
    mutate(
      across(
        c(spill_count_yr, spill_hrs_yr),
        ~ DescTools::Winsorize(
          .x,
          val = stats::quantile(.x, probs = c(0.00, 0.99), na.rm = TRUE)
        )
      )
    ) %>%
    st_as_sf(coords = c("easting", "northing"), crs = 27700)

  # Spatial join with MSOA boundaries
  sites_with_msoa <- st_join(spill_sites_sf, msoa_boundaries, join = st_within)

  # Aggregate to MSOA
  msoa_aggregated <- sites_with_msoa %>%
    st_drop_geometry() %>%
    filter(!is.na(msoa_code)) %>%
    group_by(msoa_code, msoa_name) %>%
    summarise(
      spill_count_yr = sum(spill_count_yr, na.rm = TRUE),
      spill_hrs_yr = sum(spill_hrs_yr, na.rm = TRUE),
      site_count = n(),
      .groups = "drop"
    )

  # Join to boundaries and compute derived metrics
  msoa_boundaries %>%
    left_join(msoa_population, by = "msoa_code") %>%
    left_join(msoa_aggregated, by = c("msoa_code", "msoa_name")) %>%
    mutate(
      spill_count_yr = replace_na(spill_count_yr, 0),
      spill_hrs_yr = replace_na(spill_hrs_yr, 0),
      site_count = replace_na(site_count, 0)
    ) %>%
    mutate(
      across(
        c(spill_count_yr, spill_hrs_yr),
        ~ log1p(.x),
        .names = "log_{.col}"
      )
    )
}

# Aggregate dry spills to MSOA level
aggregate_dry_spills_to_msoa <- function(data, spill_sites, years) {
  # Join spill data with site coordinates
  dat_clean <- left_join(
    select(data, -ngr),
    spill_sites,
    by = join_by(water_company, site_id)
  )

  # Convert to sf and filter for year range
  dry_spill_sites_sf <- dat_clean %>%
    filter(year %in% !!years) %>%
    select(
      water_company, site_id, easting, northing,
      dry_spill_count_yr = dry_spill_count_yr_r1_d01_weak,
      dry_spill_hrs_yr = dry_spill_hrs_yr_r1_d01_weak
    ) %>%
    filter(!is.na(dry_spill_count_yr), !is.na(dry_spill_hrs_yr)) %>%
    mutate(
      across(
        c(dry_spill_count_yr, dry_spill_hrs_yr),
        ~ DescTools::Winsorize(
          .x,
          val = stats::quantile(.x, probs = c(0.00, 0.99), na.rm = TRUE)
        )
      )
    ) %>%
    st_as_sf(coords = c("easting", "northing"), crs = 27700)

  # Spatial join with MSOA boundaries
  sites_with_msoa <- st_join(
    dry_spill_sites_sf,
    msoa_boundaries,
    join = st_within
  )

  # Aggregate to MSOA
  msoa_aggregated <- sites_with_msoa %>%
    st_drop_geometry() %>%
    filter(!is.na(msoa_code)) %>%
    group_by(msoa_code, msoa_name) %>%
    summarise(
      dry_spill_count_yr = sum(dry_spill_count_yr, na.rm = TRUE),
      dry_spill_hrs_yr = sum(dry_spill_hrs_yr, na.rm = TRUE),
      site_count = n(),
      .groups = "drop"
    )

  # Join to boundaries and compute derived metrics
  msoa_boundaries %>%
    left_join(msoa_population, by = "msoa_code") %>%
    left_join(msoa_aggregated, by = c("msoa_code", "msoa_name")) %>%
    mutate(
      dry_spill_count_yr = replace_na(dry_spill_count_yr, 0),
      dry_spill_hrs_yr = replace_na(dry_spill_hrs_yr, 0),
      site_count = replace_na(site_count, 0)
    ) %>%
    mutate(
      across(
        c(dry_spill_count_yr, dry_spill_hrs_yr),
        ~ log1p(.x),
        .names = "log_{.col}"
      )
    )
}

create_london_inset_config <- function(
  map_sf,
  london_sf,
  pad_fraction = INSET_PAD_FRACTION,
  scale = INSET_SCALE,
  target_x_fraction = INSET_TARGET_X_FRACTION,
  target_y_fraction = INSET_TARGET_Y_FRACTION,
  clamp_relax_fraction = INSET_CLAMP_RELAX_FRACTION,
  units = INSET_UNITS
) {
  clamp <- function(x, low, high) {
    pmax(low, pmin(x, high))
  }

  london_union <- st_union(london_sf)
  london_bbox <- st_bbox(london_union)

  london_width <- as.numeric(london_bbox["xmax"] - london_bbox["xmin"])
  london_height <- as.numeric(london_bbox["ymax"] - london_bbox["ymin"])
  pad_x <- london_width * pad_fraction
  pad_y <- london_height * pad_fraction

  london_bbox_padded <- london_bbox
  london_bbox_padded["xmin"] <- london_bbox["xmin"] - pad_x
  london_bbox_padded["xmax"] <- london_bbox["xmax"] + pad_x
  london_bbox_padded["ymin"] <- london_bbox["ymin"] - pad_y
  london_bbox_padded["ymax"] <- london_bbox["ymax"] + pad_y

  london_bbox_sfc <- st_as_sfc(london_bbox_padded)
  london_bbox_sf <- st_as_sf(
    tibble::tibble(inset_region = "london"),
    geometry = london_bbox_sfc
  )

  london_center <- st_coordinates(st_centroid(london_bbox_sfc))[1, c("X", "Y")]

  map_bbox <- st_bbox(map_sf)
  map_width <- as.numeric(map_bbox["xmax"] - map_bbox["xmin"])
  map_height <- as.numeric(map_bbox["ymax"] - map_bbox["ymin"])
  source_width_padded <- as.numeric(london_bbox_padded["xmax"] - london_bbox_padded["xmin"])
  source_height_padded <- as.numeric(london_bbox_padded["ymax"] - london_bbox_padded["ymin"])

  # Bottom-right placement by relative target fractions.
  target_center_x_raw <- as.numeric(map_bbox["xmin"] + target_x_fraction * map_width)
  target_center_y_raw <- as.numeric(map_bbox["ymin"] + target_y_fraction * map_height)

  # Relaxed clamping keeps the inset near/outside corners.
  clamp_relax_fraction <- clamp(clamp_relax_fraction, 0, 0.90)
  half_target_width <- 0.5 * source_width_padded * scale
  half_target_height <- 0.5 * source_height_padded * scale
  relaxed_half_width <- half_target_width * (1 - clamp_relax_fraction)
  relaxed_half_height <- half_target_height * (1 - clamp_relax_fraction)

  target_center <- c(
    clamp(
      target_center_x_raw,
      as.numeric(map_bbox["xmin"] + relaxed_half_width),
      as.numeric(map_bbox["xmax"] - relaxed_half_width)
    ),
    clamp(
      target_center_y_raw,
      as.numeric(map_bbox["ymin"] + relaxed_half_height),
      as.numeric(map_bbox["ymax"] - relaxed_half_height)
    )
  )

  translation_km <- (target_center - london_center) / 1000

  ggmapinset::configure_inset(
    shape = ggmapinset::shape_sf(london_bbox_sf),
    scale = scale,
    translation = as.numeric(translation_km),
    units = units
  )
}

plot_static_map_with_inset <- function(data, value_col, legend_title, inset_config) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  ggplot() +
    ggmapinset::geom_sf_inset(
      data = data_for_map,
      aes(fill = plot_value),
      color = NA,
      linewidth = 0.0001
    ) +
    ggmapinset::geom_inset_frame(
      inset = inset_config,
      source.aes = list(fill = NA, colour = "grey40", linewidth = 0.25),
      target.aes = list(fill = NA, colour = "grey30", linewidth = 0.35),
      lines.aes = list(colour = "grey35", linewidth = 0.25)
    ) +
    scale_fill_viridis_c(
      option = VIRIDIS_PALETTE,
      direction = -1,
      limits = c(0, 10),
      breaks = c(0, 2.5, 5, 7.5, 10),
      na.value = NO_SPILL_COLOR,
      name = legend_title,
      guide = guide_colorbar(
        title.position = "top",
        title.hjust = 0.5,
        barwidth = unit(5, "cm"),
        barheight = unit(0.2, "cm"),
        frame.colour = NA,
        ticks.colour = "grey40"
      )
    ) +
    ggmapinset::coord_sf_inset(
      inset = inset_config,
      expand = FALSE,
      clip = "off",
      datum = NA
    ) +
    theme_map_publication() +
    theme(
      legend.title = element_text(size = 8),
      legend.text = element_text(size = 7)
    )
}


# ==============================================================================
# 6. Data Processing
# ==============================================================================
cat("Processing spill data for", YEAR_RANGE_LABEL, "...\n")

# Aggregate regular spills
msoa_spills <- aggregate_spills_to_msoa(dat_yr, spill_sites, TARGET_YEARS)
cat("  Regular spills aggregated\n")

# Aggregate dry spills
msoa_dry_spills <- aggregate_dry_spills_to_msoa(
  dat_dry_yr,
  spill_sites,
  TARGET_YEARS
)
cat("  Dry spills aggregated\n")

# Build shared inset configuration
london_inset_config <- create_london_inset_config(
  map_sf = msoa_boundaries,
  london_sf = london_lads
)
cat("  London inset configuration prepared\n")


# ==============================================================================
# 7. Generate and Save Maps
# ==============================================================================
cat("Generating maps with London inset...\n")

# Regular spill map
map_spills <- plot_static_map_with_inset(
  msoa_spills,
  "log_spill_count_yr",
  "Spill count (log)",
  london_inset_config
)

file_name_spills <- paste0(
  "spill_total_count_",
  YEAR_FILENAME_LABEL,
  "_london_inset.pdf"
)
ggsave(
  filename = file.path(output_dir, file_name_spills),
  plot = map_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  units = "cm",
  dpi = PLOT_DPI,
  device = cairo_pdf
)
cat("  Saved:", file_name_spills, "\n")

# Dry spill map
map_dry_spills <- plot_static_map_with_inset(
  msoa_dry_spills,
  "log_dry_spill_count_yr",
  "Dry spill count (log)",
  london_inset_config
)

file_name_dry_spills <- paste0(
  "dry_spill_total_count_",
  YEAR_FILENAME_LABEL,
  "_london_inset.pdf"
)
ggsave(
  filename = file.path(output_dir, file_name_dry_spills),
  plot = map_dry_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  units = "cm",
  dpi = PLOT_DPI,
  device = cairo_pdf
)
cat("  Saved:", file_name_dry_spills, "\n")

cat("\nAll inset maps generated successfully!\n")
