# ==============================================================================
# Spill Maps with London Inset - MSOA-Level Sewage Spill Spatial Distribution
# ==============================================================================
# Purpose: Generate static PDF maps showing spill counts by MSOA for 2021-2023
#          with a zoomed inset for Greater London
#          Produces two maps: total spills and dry spills (sum across 2021-2023)
#
# Author: Jacopo Olivieri
# Date: 2025-12-17
#
# Outputs: PDF maps saved to output/figures/maps/
#          - spill_total_count_2021_2023_inset.pdf
#          - dry_spill_total_count_2021_2023_inset.pdf
# ==============================================================================

# Configuration ----------------------------------------------------------------
PLOT_WIDTH <- 7
PLOT_HEIGHT <- 11
PLOT_DPI <- 300
TARGET_YEARS <- 2021:2023
YEAR_RANGE_LABEL <- paste0(min(TARGET_YEARS), "-", max(TARGET_YEARS))
YEAR_FILENAME_LABEL <- paste(min(TARGET_YEARS), max(TARGET_YEARS), sep = "_")
NO_SPILL_COLOR <- "grey90"
VIRIDIS_PALETTE <- "magma"

# London bounding box (approximate Greater London extent in WGS84)
LONDON_BBOX <- list(
  xmin = -0.51,
  xmax = 0.33,
  ymin = 51.28,
  ymax = 51.70
)

# Inset positioning (relative coordinates) - bottom right, above legend
INSET_X <- 0.55
INSET_Y <- 0.15
INSET_SIZE <- 0.42  # Diameter for circular inset

# London center for circle marker
LONDON_CENTER <- list(
  x = -0.1,
  y = 51.5
)

# Package Management -----------------------------------------------------------
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
  "cowplot",
  "grid"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)

# Font Setup -------------------------------------------------------------------
showtext::showtext_auto()
showtext::showtext_opts(dpi = 300)
sysfonts::font_add_google("Libertinus Serif", "libertinus", db_cache = FALSE)
FONT_FAMILY <- "libertinus"

# Publication-Quality Map Theme ------------------------------------------------
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

# Theme for inset map (no legend, no border - circular border added separately)
theme_inset <- function() {
  theme_void(base_family = FONT_FAMILY) +
    theme(
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      legend.position = "none",
      plot.margin = margin(t = 0, r = 0, b = 0, l = 0)
    )
}

# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "figures", "maps")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Load Data --------------------------------------------------------------------
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
  here::here("data", "raw", "shapefiles", "MSOA_BCG"),
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
    "data", "raw", "shapefiles", "MSOA_population",
    "sapemsoasyoatablefinal.xlsx"
  ),
  sheet = "Mid-2022 MSOA 2021",
  range = "A4:E7268"
) %>%
  select(msoa_code = 3, msoa_population = 5) %>%
  filter(!is.na(msoa_code), !is.na(msoa_population))

cat("  Data loaded successfully\n")

# Helper Functions -------------------------------------------------------------

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

# Create London center point for circle marker on main map
create_london_marker <- function() {
  data.frame(
    x = LONDON_CENTER$x,
    y = LONDON_CENTER$y
  )
}

# Create static map for regular spills with London circle marker
plot_static_spill_map <- function(data, value_col) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  london_marker <- create_london_marker()

  # Create ggplot2 map
  ggplot() +
    # MSOA polygons with spill data
    geom_sf(
      data = data_for_map,
      aes(fill = plot_value),
      color = "white",
      linewidth = 0.0001
    ) +
    # London circle marker
    geom_point(
      data = london_marker,
      aes(x = x, y = y),
      shape = 1,
      size = 8,
      stroke = 0.8,
      color = "black"
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

# Create static map for dry spills with London circle marker
plot_static_dry_spill_map <- function(data, value_col) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  london_marker <- create_london_marker()

  # Create ggplot2 map
  ggplot() +
    # MSOA polygons with dry spill data
    geom_sf(
      data = data_for_map,
      aes(fill = plot_value),
      color = "white",
      linewidth = 0.0001
    ) +
    # London circle marker
    geom_point(
      data = london_marker,
      aes(x = x, y = y),
      shape = 1,
      size = 8,
      stroke = 0.8,
      color = "black"
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

# Create London inset map for regular spills
plot_london_inset_spill <- function(data, value_col) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  # Create ggplot2 map zoomed to London
  ggplot() +
    geom_sf(
      data = data_for_map,
      aes(fill = plot_value),
      color = "white",
      linewidth = 0.0001
    ) +
    scale_fill_viridis_c(
      option = "magma",
      direction = -1,
      limits = c(0, 10),
      breaks = c(0, 2.5, 5, 7.5, 10),
      na.value = NO_SPILL_COLOR,
      guide = "none"
    ) +
    coord_sf(
      xlim = c(LONDON_BBOX$xmin, LONDON_BBOX$xmax),
      ylim = c(LONDON_BBOX$ymin, LONDON_BBOX$ymax),
      expand = FALSE,
      datum = NA
    ) +
    theme_inset()
}

# Create London inset map for dry spills
plot_london_inset_dry_spill <- function(data, value_col) {
  # Prepare data: set zero values to NA for grey rendering
  data_for_map <- data %>%
    mutate(
      plot_value = if_else(.data[[value_col]] == 0, NA_real_, .data[[value_col]])
    )

  # Create ggplot2 map zoomed to London
  ggplot() +
    geom_sf(
      data = data_for_map,
      aes(fill = plot_value),
      color = "white",
      linewidth = 0.0001
    ) +
    scale_fill_viridis_c(
      option = "magma",
      direction = -1,
      limits = c(0, 10),
      breaks = c(0, 2.5, 5, 7.5, 10),
      na.value = NO_SPILL_COLOR,
      guide = "none"
    ) +
    coord_sf(
      xlim = c(LONDON_BBOX$xmin, LONDON_BBOX$xmax),
      ylim = c(LONDON_BBOX$ymin, LONDON_BBOX$ymax),
      expand = FALSE,
      datum = NA
    ) +
    theme_inset()
}

# Create circular mask grob (white corners to hide square edges)
create_circular_mask <- function() {
  # Create a path that forms a square with a circular hole
  # This masks the corners of the inset plot
  theta <- seq(0, 2 * pi, length.out = 100)
  circle_x <- 0.5 + 0.5 * cos(theta)
  circle_y <- 0.5 + 0.5 * sin(theta)

  grid::pathGrob(
    x = c(0, 1, 1, 0, 0, circle_x),
    y = c(0, 0, 1, 1, 0, circle_y),
    id = c(rep(1, 5), rep(2, 100)),
    rule = "evenodd",
    gp = grid::gpar(fill = "white", col = NA)
  )
}

# Compose main map with circular inset
compose_map_with_inset <- function(main_map, inset_map) {
  # Convert inset to grob
  inset_grob <- ggplotGrob(inset_map)

  # Create circular mask and border
  mask_grob <- create_circular_mask()
  border_grob <- grid::circleGrob(
    x = 0.5, y = 0.5, r = 0.495,
    gp = grid::gpar(fill = NA, col = "black", lwd = 2)
  )

  # Combine inset with mask and border
  inset_with_mask <- grid::grobTree(
    inset_grob,
    mask_grob,
    border_grob
  )

  # Create the composite
  cowplot::ggdraw() +
    cowplot::draw_plot(main_map) +
    cowplot::draw_grob(
      inset_with_mask,
      x = INSET_X,
      y = INSET_Y,
      width = INSET_SIZE,
      height = INSET_SIZE
    )
}

# Process Data -----------------------------------------------------------------
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

# Generate and Save Maps -------------------------------------------------------
cat("Generating maps with London inset...\n")

# Regular spill map with inset
map_spills_main <- plot_static_spill_map(
  msoa_spills,
  "log_spill_count_yr"
)
map_spills_inset <- plot_london_inset_spill(
  msoa_spills,
  "log_spill_count_yr"
)
map_spills_composite <- compose_map_with_inset(map_spills_main, map_spills_inset)

file_name_spills <- paste0("spill_total_count_", YEAR_FILENAME_LABEL, "_inset.pdf")
ggsave(
  filename = here::here(output_dir, file_name_spills),
  plot = map_spills_composite,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  units = "cm",
  dpi = PLOT_DPI,
  device = cairo_pdf
)
cat("  Saved:", file_name_spills, "\n")

# Dry spill map with inset
map_dry_spills_main <- plot_static_dry_spill_map(
  msoa_dry_spills,
  "log_dry_spill_count_yr"
)
map_dry_spills_inset <- plot_london_inset_dry_spill(
  msoa_dry_spills,
  "log_dry_spill_count_yr"
)
map_dry_spills_composite <- compose_map_with_inset(
  map_dry_spills_main,
  map_dry_spills_inset
)

file_name_dry_spills <- paste0(
  "dry_spill_total_count_",
  YEAR_FILENAME_LABEL,
  "_inset.pdf"
)
ggsave(
  filename = here::here(output_dir, file_name_dry_spills),
  plot = map_dry_spills_composite,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  units = "cm",
  dpi = PLOT_DPI,
  device = cairo_pdf
)
cat("  Saved:", file_name_dry_spills, "\n")

cat("\nAll maps with London inset generated successfully!\n")
