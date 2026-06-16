# ==============================================================================
# Spill Maps - MSOA-Level Sewage Spill Spatial Distribution
# ==============================================================================
#
# Purpose: Generate static PDF maps showing average annual spill counts by MSOA
#          for 2021-2023. Produces two maps: all spills and dry spills.
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
#   - output/figures/maps/spill_avg_annual_count_2021_2023.pdf
#   - output/figures/maps/dry_spill_avg_annual_count_2021_2023.pdf
#   - output/figures/maps/spill_avg_annual_count_2021_2023_slides.pdf
#   - output/figures/maps/dry_spill_avg_annual_count_2021_2023_slides.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 7
PLOT_HEIGHT <- 11
PLOT_DPI <- 300
SLIDE_PLOT_WIDTH <- 16
SLIDE_PLOT_HEIGHT <- 12
SLIDE_PLOT_DPI <- 300
TARGET_YEARS <- 2021:2023
YEAR_RANGE_LABEL <- paste0(min(TARGET_YEARS), "-", max(TARGET_YEARS))
YEAR_FILENAME_LABEL <- paste(min(TARGET_YEARS), max(TARGET_YEARS), sep = "_")
NO_SPILL_COLOR <- "grey90"
RAW_COUNT_LEGEND_BREAKS <- c(1, 10, 100, 2000)
LOG_COUNT_LEGEND_BREAKS <- log(RAW_COUNT_LEGEND_BREAKS)
RAW_COUNT_LEGEND_LABELS <- c("1", "10", "100", "2,000")
VIRIDIS_PALETTE <- "magma"

# ==============================================================================
# 2. Package Management
# ==============================================================================

required_packages <- c(
  "rio",
  "nanoparquet",
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
add_libertinus_font <- function() {
  local_font_files <- c(
    regular = path.expand("~/Library/Fonts/LibertinusSerif-Regular.ttf"),
    bold = path.expand("~/Library/Fonts/LibertinusSerif-Bold.ttf"),
    italic = path.expand("~/Library/Fonts/LibertinusSerif-Italic.ttf"),
    bolditalic = path.expand("~/Library/Fonts/LibertinusSerif-BoldItalic.ttf")
  )

  if (all(file.exists(local_font_files))) {
    do.call(
      sysfonts::font_add,
      c(list(family = "libertinus"), as.list(local_font_files))
    )
    return(invisible(TRUE))
  }

  sysfonts::font_add_google("Libertinus Serif", "libertinus", db_cache = TRUE)
  invisible(TRUE)
}

showtext::showtext_auto()
showtext::showtext_opts(dpi = 300)
add_libertinus_font()
FONT_FAMILY <- "libertinus"

# 3.2 Publication-Quality Map Theme --------------------------------------------
map_variant_style <- function(variant = c("paper", "slides")) {
  variant <- match.arg(variant)

  if (variant == "slides") {
    return(list(
      legend_title_size = 13,
      legend_text_size = 11,
      legend_key_width = 4.0,
      legend_key_height = 0.55,
      legend_bar_width = 9.0,
      legend_bar_height = 0.35,
      legend_margin = margin(t = 8, b = 2),
      plot_margin = margin(t = 4, r = 4, b = 4, l = 4)
    ))
  }

  list(
    legend_title_size = 8,
    legend_text_size = 7,
    legend_key_width = 2.5,
    legend_key_height = 0.4,
    legend_bar_width = 5.0,
    legend_bar_height = 0.2,
    legend_margin = margin(t = 15, b = 5),
    plot_margin = margin(t = 5, r = 5, b = 5, l = 5)
  )
}

theme_map_publication <- function(variant = c("paper", "slides")) {
  style <- map_variant_style(variant)

  theme_void(base_family = FONT_FAMILY) +
    theme(
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.title = element_text(
        size = style$legend_title_size,
        face = "bold",
        margin = margin(b = 5)
      ),
      legend.text = element_text(size = style$legend_text_size),
      legend.key.width = unit(style$legend_key_width, "cm"),
      legend.key.height = unit(style$legend_key_height, "cm"),
      legend.margin = style$legend_margin,
      plot.margin = style$plot_margin
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
  n_years <- length(years)

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
      avg_annual_spill_count = spill_count_yr / n_years,
      avg_annual_spill_hrs = spill_hrs_yr / n_years
    ) %>%
    mutate(
      across(
        c(avg_annual_spill_count, avg_annual_spill_hrs),
        ~ if_else(.x > 0, log(.x), NA_real_),
        .names = "log_{.col}"
      )
    ) %>%
    st_transform(crs = 4326)

  msoa_with_spills
}

# Aggregate dry spills to MSOA level
aggregate_dry_spills_to_msoa <- function(data, spill_sites, years) {
  n_years <- length(years)

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
      avg_annual_dry_spill_count = dry_spill_count_yr / n_years,
      avg_annual_dry_spill_hrs = dry_spill_hrs_yr / n_years
    ) %>%
    mutate(
      across(
        c(avg_annual_dry_spill_count, avg_annual_dry_spill_hrs),
        ~ if_else(.x > 0, log(.x), NA_real_),
        .names = "log_{.col}"
      )
    ) %>%
    st_transform(crs = 4326)

  msoa_with_dry_spills
}

# Create static map for regular spills (ggplot2 version)
plot_static_spill_map <- function(data, value_col, variant = c("paper", "slides")) {
  variant <- match.arg(variant)
  style <- map_variant_style(variant)

  # Zeros are already represented as NA in the positive-only log columns.
  data_for_map <- data %>%
    mutate(
      plot_value = .data[[value_col]]
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
      option = VIRIDIS_PALETTE,
      direction = -1,
      limits = range(LOG_COUNT_LEGEND_BREAKS),
      breaks = LOG_COUNT_LEGEND_BREAKS,
      labels = RAW_COUNT_LEGEND_LABELS,
      oob = scales::squish,
      na.value = NO_SPILL_COLOR,
      name = "Average annual spill count (log scale)",
      guide = guide_colorbar(
        title.position = "top",
        title.hjust = 0.5,
        barwidth = unit(style$legend_bar_width, "cm"),
        barheight = unit(style$legend_bar_height, "cm"),
        frame.colour = NA,
        ticks.colour = "grey40"
      )
    ) +
    coord_sf(expand = FALSE, datum = NA) +
    theme_map_publication(variant)
}

# Create static map for dry spills (ggplot2 version)
plot_static_dry_spill_map <- function(data, value_col, variant = c("paper", "slides")) {
  variant <- match.arg(variant)
  style <- map_variant_style(variant)

  # Zeros are already represented as NA in the positive-only log columns.
  data_for_map <- data %>%
    mutate(
      plot_value = .data[[value_col]]
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
      option = VIRIDIS_PALETTE,
      direction = -1,
      limits = range(LOG_COUNT_LEGEND_BREAKS),
      breaks = LOG_COUNT_LEGEND_BREAKS,
      labels = RAW_COUNT_LEGEND_LABELS,
      oob = scales::squish,
      na.value = NO_SPILL_COLOR,
      name = "Average annual dry spill count (log scale)",
      guide = guide_colorbar(
        title.position = "top",
        title.hjust = 0.5,
        barwidth = unit(style$legend_bar_width, "cm"),
        barheight = unit(style$legend_bar_height, "cm"),
        frame.colour = NA,
        ticks.colour = "grey40"
      )
    ) +
    coord_sf(expand = FALSE, datum = NA) +
    theme_map_publication(variant)
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

save_map_pdf <- function(plot, file_name, width, height, dpi = PLOT_DPI) {
  ggsave(
    filename = file.path(output_dir, file_name),
    plot = plot,
    width = width,
    height = height,
    units = "cm",
    dpi = dpi,
    device = cairo_pdf
  )
  cat("  Saved:", file_name, "\n")
}

# Regular spill map
map_spills <- plot_static_spill_map(
  msoa_spills,
  "log_avg_annual_spill_count",
  variant = "paper"
)

file_name_spills <- paste0("spill_avg_annual_count_", YEAR_FILENAME_LABEL, ".pdf")
save_map_pdf(
  map_spills,
  file_name_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT
)

map_spills_slides <- plot_static_spill_map(
  msoa_spills,
  "log_avg_annual_spill_count",
  variant = "slides"
)

file_name_spills_slides <- paste0(
  "spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  "_slides.pdf"
)
save_map_pdf(
  map_spills_slides,
  file_name_spills_slides,
  width = SLIDE_PLOT_WIDTH,
  height = SLIDE_PLOT_HEIGHT,
  dpi = SLIDE_PLOT_DPI
)

# Dry spill map
map_dry_spills <- plot_static_dry_spill_map(
  msoa_dry_spills,
  "log_avg_annual_dry_spill_count",
  variant = "paper"
)

file_name_dry_spills <- paste0(
  "dry_spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  ".pdf"
)
save_map_pdf(
  map_dry_spills,
  file_name_dry_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT
)

map_dry_spills_slides <- plot_static_dry_spill_map(
  msoa_dry_spills,
  "log_avg_annual_dry_spill_count",
  variant = "slides"
)

file_name_dry_spills_slides <- paste0(
  "dry_spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  "_slides.pdf"
)
save_map_pdf(
  map_dry_spills_slides,
  file_name_dry_spills_slides,
  width = SLIDE_PLOT_WIDTH,
  height = SLIDE_PLOT_HEIGHT,
  dpi = SLIDE_PLOT_DPI
)

cat("\nAll maps generated successfully!\n")
