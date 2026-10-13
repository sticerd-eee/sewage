# ==============================================================================
# Spill Maps with London Inset - MSOA-Level Sewage Spill Spatial Distribution
# ==============================================================================
#
# Purpose: Generate static PDF maps showing average annual spill counts by MSOA
#          for 2021-2024 with a London inset panel.
#          Produces two maps: all spills and dry spills.
#
# Author: Jacopo Olivieri
# Date: 2026-03-20
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill data
#   - data/processed/agg_spill_stats/agg_spill_dry_yr.parquet - Dry spill data
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#   - data/raw/shapefiles/msoa_bcg_2021/ - MSOA boundary shapefiles
#   - data/raw/shapefiles/msoa_population_2021/sapemsoasyoatablefinal.xlsx
#   - data/raw/shapefiles/local_authorities_uk_buc/ - London LAD boundaries
#
# Outputs:
#   - output/figures/maps/spill_avg_annual_count_2021_2024_london_inset.pdf
#   - output/figures/maps/dry_spill_avg_annual_count_2021_2024_london_inset.pdf
#   - output/figures/maps/spill_avg_annual_count_2021_2024_london_inset_slides.pdf
#   - output/figures/maps/dry_spill_avg_annual_count_2021_2024_london_inset_slides.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 7
PLOT_HEIGHT <- 9.7
PLOT_DPI <- 300
# Slide maps are used as side-by-side half-width Beamer panels.
# Export near that final display size so LaTeX does not shrink labels away.
SLIDE_PLOT_WIDTH <- 7.0
SLIDE_PLOT_HEIGHT <- 5.6
SLIDE_PLOT_DPI <- 300
TARGET_YEARS <- 2021:2024
YEAR_RANGE_LABEL <- paste0(min(TARGET_YEARS), "-", max(TARGET_YEARS))
YEAR_FILENAME_LABEL <- paste(min(TARGET_YEARS), max(TARGET_YEARS), sep = "_")
NO_SPILL_COLOR <- "grey90"
NO_DATA_COLOR <- "grey60"
VIRIDIS_PALETTE <- "magma"
RAW_COUNT_LEGEND_BREAKS <- c(1, 10, 100, 2000)
LOG_COUNT_LEGEND_BREAKS <- log(RAW_COUNT_LEGEND_BREAKS)
RAW_COUNT_LEGEND_LABELS <- c("1", "10", "100", "2,000")
LONDON_LAD_PREFIX <- "^E09"
INSET_PAD_FRACTION <- -0.05
INSET_SCALE <- 2.9
INSET_TARGET_X_FRACTION <- 1.05
INSET_TARGET_Y_FRACTION <- 0.02
INSET_CLAMP_RELAX_FRACTION <- 0.3
INSET_UNITS <- "km"


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
source(here::here("scripts", "R", "utils", "site_group_utils.R"))
source(here::here("scripts", "R", "utils", "spill_missing_utils.R"))


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
      legend_title_size = 7.8,
      legend_text_size = 8.3,
      legend_key_width = 2.0,
      legend_key_height = 0.30,
      legend_bar_width = 4.3,
      legend_bar_height = 0.20,
      legend_margin = margin(t = 0, b = 0),
      plot_margin = margin(t = 1, r = 2, b = 1, l = 2),
      no_data_key_size = 0.30,
      no_data_text_size = 7.8
    ))
  }

  list(
    legend_title_size = 7.6,
    legend_text_size = 7,
    legend_key_width = 2.5,
    legend_key_height = 0.4,
    legend_bar_width = 4.6,
    legend_bar_height = 0.2,
    legend_margin = margin(t = 6, b = 0),
    plot_margin = margin(t = 2, r = 8, b = 4, l = 3),
    no_data_key_size = 0.35,
    no_data_text_size = 7
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
      legend.box.spacing = unit(0.2, "cm"),
      legend.spacing.x = unit(0.15, "cm"),
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
spill_sites <- read_site_group_projection(
  here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  years = 2021:2024
)

# MSOA boundaries (England only)
msoa_boundaries_raw <- sf::st_read(
  here::here("data", "raw", "shapefiles", "msoa_bcg_2021"),
  quiet = TRUE
) %>%
  filter(str_detect(MSOA21CD, "^E")) %>%
  select(MSOA21CD, MSOA21NM, geometry) %>%
  rename(msoa_code = MSOA21CD, msoa_name = MSOA21NM) %>%
  mutate(msoa_area_km2 = as.numeric(st_area(.)) / 1000000)

msoa_boundaries <- msoa_boundaries_raw %>%
  st_simplify(dTolerance = 200)

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

# Average each overflow over its non-missing years (rule in
# spill_missing_utils.R) and sum within each MSOA. Yearly values are
# winsorised before averaging.
aggregate_overflow_averages <- function(
  data,
  years,
  count_col,
  hrs_col,
  avg_count_name,
  avg_hrs_name
) {
  value_cols <- c(count_col, hrs_col)

  site_year_values <- data %>%
    filter(
      year %in% !!years,
      is.finite(easting),
      is.finite(northing)
    ) %>%
    mutate(
      across(
        all_of(value_cols),
        ~ if_else(is_missing_group_year, NA_real_, as.numeric(.x))
      )
    ) %>%
    mutate(
      across(
        all_of(value_cols),
        ~ DescTools::Winsorize(
          .x,
          val = stats::quantile(.x, probs = c(0.00, 0.99), na.rm = TRUE)
        )
      )
    )

  # One row per overflow
  site_averages <- site_year_values %>%
    summarise_overflow_annual_averages(value_cols) %>%
    left_join(
      site_year_values %>% distinct(site_id, easting, northing),
      by = "site_id"
    ) %>%
    st_as_sf(coords = c("easting", "northing"), crs = 27700)

  # Spatial join with MSOA boundaries
  sites_with_msoa <- st_join(
    site_averages,
    msoa_boundaries_raw %>% select(msoa_code, msoa_name),
    join = st_within
  )
  if (anyDuplicated(sites_with_msoa$site_id)) {
    stop("Some overflows were assigned to more than one MSOA.")
  }

  # Aggregate to MSOA (reclassified below once empty MSOAs are filled in)
  msoa_aggregated <- sites_with_msoa %>%
    st_drop_geometry() %>%
    aggregate_overflow_averages_to_msoa(value_col = count_col) %>%
    select(-spill_class) %>%
    left_join(
      sites_with_msoa %>%
        st_drop_geometry() %>%
        filter(!is.na(msoa_code)) %>%
        group_by(msoa_code) %>%
        summarise(
          summed_average_hrs = sum(
            .data[[paste0("avg_annual_", hrs_col)]],
            na.rm = TRUE
          ),
          .groups = "drop"
        ),
      by = "msoa_code"
    )

  # Join to boundaries and compute derived metrics
  msoa_boundaries %>%
    left_join(msoa_population, by = "msoa_code") %>%
    left_join(msoa_aggregated, by = c("msoa_code", "msoa_name")) %>%
    mutate(
      site_count = replace_na(site_count, 0L),
      sites_with_data = replace_na(sites_with_data, 0L),
      spill_class = classify_msoa_spill_status(
        site_count, sites_with_data, summed_average
      ),
      "{avg_count_name}" := replace_na(summed_average, 0),
      "{avg_hrs_name}" := replace_na(summed_average_hrs, 0)
    ) %>%
    select(-summed_average, -summed_average_hrs) %>%
    mutate(
      across(
        all_of(c(avg_count_name, avg_hrs_name)),
        ~ if_else(.x > 0, log(.x), NA_real_),
        .names = "log_{.col}"
      )
    )
}

# Aggregate regular spills to MSOA level
aggregate_spills_to_msoa <- function(data, spill_sites, years) {
  missing_flags <- flag_missing_group_years(data)

  # Join spill data with site coordinates
  dat_clean <- left_join_site_group_projection(
    select(data, -any_of("ngr")),
    select(spill_sites, site_id, easting, northing),
    context = "Inset regular map Site Group coordinate join"
  ) %>%
    attach_missing_group_year_flag(missing_flags)

  aggregate_overflow_averages(
    dat_clean,
    years = years,
    count_col = "spill_count_yr",
    hrs_col = "spill_hrs_yr",
    avg_count_name = "avg_annual_spill_count",
    avg_hrs_name = "avg_annual_spill_hrs"
  )
}

# Aggregate dry spills to MSOA level (missing flag taken from the main
# yearly aggregates, since the dry columns are zero-filled upstream)
aggregate_dry_spills_to_msoa <- function(data, main_data, spill_sites, years) {
  missing_flags <- flag_missing_group_years(main_data)

  # Join spill data with site coordinates
  dat_clean <- left_join_site_group_projection(
    select(data, -any_of("ngr")),
    select(spill_sites, site_id, easting, northing),
    context = "Inset dry map Site Group coordinate join"
  ) %>%
    attach_missing_group_year_flag(missing_flags) %>%
    select(
      water_company, site_id, year, easting, northing,
      is_missing_group_year,
      dry_spill_count_yr = dry_spill_count_yr_r1_d01_weak,
      dry_spill_hrs_yr = dry_spill_hrs_yr_r1_d01_weak
    )

  aggregate_overflow_averages(
    dat_clean,
    years = years,
    count_col = "dry_spill_count_yr",
    hrs_col = "dry_spill_hrs_yr",
    avg_count_name = "avg_annual_dry_spill_count",
    avg_hrs_name = "avg_annual_dry_spill_hrs"
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

plot_static_map_with_inset <- function(
  data,
  value_col,
  legend_title,
  inset_config,
  variant = c("paper", "slides")
) {
  variant <- match.arg(variant)
  style <- map_variant_style(variant)

  # Zeros are already represented as NA in the positive-only log columns.
  # No-data MSOAs are drawn as a separate layer with its own legend key.
  data_for_map <- data %>%
    mutate(
      plot_value = .data[[value_col]]
    )
  data_no_data <- data_for_map %>%
    filter(spill_class == "no_data")

  ggplot() +
    ggmapinset::geom_sf_inset(
      data = data_for_map,
      aes(fill = plot_value),
      color = NA,
      linewidth = 0.0001
    ) +
    ggmapinset::geom_sf_inset(
      data = data_no_data,
      aes(alpha = "No data"),
      fill = NO_DATA_COLOR,
      color = NA,
      linewidth = 0.0001
    ) +
    scale_alpha_manual(
      name = NULL,
      values = c("No data" = 1),
      guide = guide_legend(
        order = 2,
        override.aes = list(fill = NO_DATA_COLOR, colour = NA),
        theme = theme(
          legend.text = element_text(size = style$no_data_text_size),
          legend.key.width = unit(style$no_data_key_size, "cm"),
          legend.key.height = unit(style$no_data_key_size, "cm"),
          legend.key.spacing.x = unit(0.15, "cm")
        )
      )
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
      limits = range(LOG_COUNT_LEGEND_BREAKS),
      breaks = LOG_COUNT_LEGEND_BREAKS,
      labels = RAW_COUNT_LEGEND_LABELS,
      oob = scales::squish,
      na.value = NO_SPILL_COLOR,
      name = legend_title,
      guide = guide_colorbar(
        order = 1,
        title.position = "top",
        title.hjust = 0.5,
        barwidth = unit(style$legend_bar_width, "cm"),
        barheight = unit(style$legend_bar_height, "cm"),
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
  dat_yr,
  spill_sites,
  TARGET_YEARS
)
cat("  Dry spills aggregated\n")

print_msoa_class_counts <- function(map_data, label) {
  counts <- map_data %>%
    st_drop_geometry() %>%
    count(spill_class, .drop = FALSE)
  cat("  MSOA classes (", label, "): ", sep = "")
  cat(paste0(counts$spill_class, " = ", counts$n, collapse = "; "), "\n")
}
print_msoa_class_counts(msoa_spills, "all spills")
print_msoa_class_counts(msoa_dry_spills, "dry spills")

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

# cairo_pdf can fail to open silently, so probe it before writing any map.
cairo_pdf_available <- function() {
  tmp <- tempfile(fileext = ".pdf")
  on.exit(unlink(tmp), add = TRUE)
  n_before <- length(grDevices::dev.list())
  ok <- tryCatch(
    {
      suppressWarnings(grDevices::cairo_pdf(tmp))
      opened <- length(grDevices::dev.list()) > n_before
      if (opened) grDevices::dev.off()
      opened && file.exists(tmp)
    },
    error = function(e) FALSE
  )
  isTRUE(ok)
}

if (!cairo_pdf_available()) {
  stop("cairo_pdf is unavailable (on macOS, install XQuartz).")
}

save_map_pdf <- function(plot, file_name, width, height, dpi = PLOT_DPI) {
  ggsave(
    filename = file.path(output_dir, file_name),
    plot = plot,
    width = width,
    height = height,
    units = "cm",
    dpi = dpi,
    device = grDevices::cairo_pdf
  )
  cat("  Saved:", file_name, "\n")
}

# Regular spill map
map_spills <- plot_static_map_with_inset(
  msoa_spills,
  "log_avg_annual_spill_count",
  "Average annual spill count (log-scale)",
  london_inset_config,
  variant = "paper"
)

file_name_spills <- paste0(
  "spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  "_london_inset.pdf"
)
save_map_pdf(
  map_spills,
  file_name_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT
)

map_spills_slides <- plot_static_map_with_inset(
  msoa_spills,
  "log_avg_annual_spill_count",
  "Average annual spill count (log-scale)",
  london_inset_config,
  variant = "slides"
)

file_name_spills_slides <- paste0(
  "spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  "_london_inset_slides.pdf"
)
save_map_pdf(
  map_spills_slides,
  file_name_spills_slides,
  width = SLIDE_PLOT_WIDTH,
  height = SLIDE_PLOT_HEIGHT,
  dpi = SLIDE_PLOT_DPI
)

# Dry spill map
map_dry_spills <- plot_static_map_with_inset(
  msoa_dry_spills,
  "log_avg_annual_dry_spill_count",
  "Average annual dry spill count (log-scale)",
  london_inset_config,
  variant = "paper"
)

file_name_dry_spills <- paste0(
  "dry_spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  "_london_inset.pdf"
)
save_map_pdf(
  map_dry_spills,
  file_name_dry_spills,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT
)

map_dry_spills_slides <- plot_static_map_with_inset(
  msoa_dry_spills,
  "log_avg_annual_dry_spill_count",
  "Average annual dry spill count (log-scale)",
  london_inset_config,
  variant = "slides"
)

file_name_dry_spills_slides <- paste0(
  "dry_spill_avg_annual_count_",
  YEAR_FILENAME_LABEL,
  "_london_inset_slides.pdf"
)
save_map_pdf(
  map_dry_spills_slides,
  file_name_dry_spills_slides,
  width = SLIDE_PLOT_WIDTH,
  height = SLIDE_PLOT_HEIGHT,
  dpi = SLIDE_PLOT_DPI
)

cat("\nAll inset maps generated successfully!\n")
