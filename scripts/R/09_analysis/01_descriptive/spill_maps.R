# ==============================================================================
# Spill Maps - MSOA-Level Sewage Spill Spatial Distribution
# ==============================================================================
#
# Purpose: Generate static PDF maps showing spill counts by MSOA for 2021-2023.
#          Produces two maps: total spills and dry spills (sum across years).
#
# Author: Jacopo Olivieri
# Date: 2025-12-17
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill data
#   - data/processed/agg_spill_stats/agg_spill_dry_yr.parquet - Dry spill data
#   - data/processed/unique_spill_sites.parquet - Site coordinates
#   - data/raw/shapefiles/msoa_bcg_2021/ - MSOA boundary shapefiles
#   - data/raw/shapefiles/msoa_population_2021/sapemsoasyoatablefinal.xlsx - Population
#
# Outputs:
#   - output/figures/maps/spill_total_count_2021_2023.pdf
#   - output/figures/maps/dry_spill_total_count_2021_2023.pdf
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
  "sysfonts"
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
      plot.margin = margin(t = 5, r = 5, b = 5, l = 5)
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

# MSOA boundaries (England only) - load first to get England bounding box
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
  msoa_with_spills <- msoa_boundaries %>%
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
    ) %>%
    st_transform(crs = 4326)

  msoa_with_spills
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
  msoa_with_dry_spills <- msoa_boundaries %>%
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
    ) %>%
    st_transform(crs = 4326)

  msoa_with_dry_spills
}

# Create static map for regular spills (ggplot2 version)
plot_static_spill_map <- function(data, value_col) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  # Create ggplot2 map
  ggplot() +
    # MSOA polygons with spill data
    geom_sf(
      data = data_for_map,
      aes(fill = plot_value),
      color = NA,
      linewidth = 0.0001
    ) +
    # Color scale
    scale_fill_viridis_c(
      option = "magma",
      direction = -1,
      limits = c(0, 10),
      breaks = c(0, 2.5, 5, 7.5, 10),
      na.value = NO_SPILL_COLOR,
      name = "Spill count (log)",
      guide = guide_colorbar(
        title.position = "top",
        title.hjust = 0.5,
        barwidth = unit(5, "cm"),
        barheight = unit(0.2, "cm"),
        frame.colour = NA,
        ticks.colour = "grey40"
      )
    ) +
    coord_sf(expand = FALSE, datum = NA) +
    theme_map_publication() +
    theme(
      legend.title = element_text(size = 8),
      legend.text = element_text(size = 7)
    )
}

# Create static map for dry spills (ggplot2 version)
plot_static_dry_spill_map <- function(data, value_col) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  # Create ggplot2 map
  ggplot() +
    # MSOA polygons with dry spill data
    geom_sf(
      data = data_for_map,
      aes(fill = plot_value),
      color = NA,
      linewidth = 0.0001
    ) +
    # Color scale
    scale_fill_viridis_c(
      option = "magma",
      direction = -1,
      limits = c(0, 10),
      breaks = c(0, 2.5, 5, 7.5, 10),
      na.value = NO_SPILL_COLOR,
      name = "Dry spill count (log)",
      guide = guide_colorbar(
        title.position = "top",
        title.hjust = 0.5,
        barwidth = unit(5, "cm"),
        barheight = unit(0.2, "cm"),
        frame.colour = NA,
        ticks.colour = "grey40"
      )
    ) +
    coord_sf(expand = FALSE, datum = NA) +
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

# ==============================================================================
# 7. Generate and Save Maps
# ==============================================================================
cat("Generating maps...\n")

# Regular spill map
map_spills <- plot_static_spill_map(
  msoa_spills,
  "log_spill_count_yr"
)

file_name_spills <- paste0("spill_total_count_", YEAR_FILENAME_LABEL, ".pdf")
ggsave(
  filename = here::here(output_dir, file_name_spills),
  plot = map_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  units = "cm",
  dpi = PLOT_DPI,
  device = cairo_pdf
)
cat("  Saved:", file_name_spills, "\n")

# Dry spill map
map_dry_spills <- plot_static_dry_spill_map(
  msoa_dry_spills,
  "log_dry_spill_count_yr"
)

file_name_dry_spills <- paste0(
  "dry_spill_total_count_",
  YEAR_FILENAME_LABEL,
  ".pdf"
)
ggsave(
  filename = here::here(output_dir, file_name_dry_spills),
  plot = map_dry_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  units = "cm",
  dpi = PLOT_DPI,
  device = cairo_pdf
)
cat("  Saved:", file_name_dry_spills, "\n")

cat("\nAll maps generated successfully!\n")
