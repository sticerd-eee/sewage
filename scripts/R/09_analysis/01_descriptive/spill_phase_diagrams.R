# ==============================================================================
# Sewage Spill Persistence Phase Diagrams
# ==============================================================================
#
# Purpose: Generate phase diagrams showing year-to-year transition probabilities
#          for sewage spill sites across quartile bins (0, Q1, Q2, Q3, Q4).
#
# Author: Jacopo Olivieri
# Date: 2025-11-22
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill data
#
# Outputs:
#   - output/figures/spill_count_persistence.pdf
#   - output/figures/spill_count_persistence_slides.pdf
#   - output/figures/spill_hours_persistence.pdf
#   - output/figures/spill_hours_persistence_slides.pdf
#
# Note: Years are hardcoded to 2021-2023 period
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_DPI <- 300
VIRIDIS_PALETTE <- "magma"
FONT_FAMILY <- "libertinus"
GG_TEXT_SIZE_PT <- 2.845276

PLOT_VARIANTS <- list(
  paper = list(
    width_cm = 14.5,
    height_cm = 11.5,
    base_size = 9.5,
    axis_title_size = 9.5,
    axis_text_size = 8.5,
    legend_title_size = 9.5,
    legend_text_size = 8.5,
    cell_text_size_pt = 8.8,
    tile_linewidth = 0.5,
    axis_x_title = "State in Year t",
    axis_y_title = "State in Year t+1",
    legend_title = "Transition Probability",
    legend_title_position = "top",
    legend_key_width = 3.6,
    legend_key_height = 0.30,
    plot_margin = ggplot2::margin(t = 3, r = 3, b = 2, l = 3, unit = "pt")
  ),
  slides = list(
    width_cm = 7.2,
    height_cm = 6.5,
    base_size = 9.5,
    axis_title_size = 9.5,
    axis_text_size = 8.5,
    legend_title_size = 8.5,
    legend_text_size = 7.5,
    cell_text_size_pt = 8,
    tile_linewidth = 0.35,
    axis_x_title = "State at t",
    axis_y_title = "State at t + 1",
    legend_title = "Transition Probability",
    legend_title_position = "top",
    legend_key_width = 3.6,
    legend_key_height = 0.28,
    plot_margin = ggplot2::margin(t = 2, r = 2, b = 1, l = 2, unit = "pt")
  )
)

# ==============================================================================
# 2. Package Management
# ==============================================================================

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "purrr",
  "here",
  "viridis",
  "scales",
  "showtext"
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
      c(list(family = FONT_FAMILY), as.list(local_font_files))
    )
    return(FONT_FAMILY)
  }

  tryCatch(
    {
      sysfonts::font_add_google(
        "Libertinus Serif",
        FONT_FAMILY,
        db_cache = TRUE
      )
      FONT_FAMILY
    },
    error = function(e) {
      warning(
        "Libertinus Serif font unavailable; falling back to the default serif font."
      )
      "serif"
    }
  )
}

FONT_FAMILY <- add_libertinus_font()

# 3.2 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.3 Helper Functions ---------------------------------------------------------

pt_to_gg_text_size <- function(size_pt) {
  size_pt / GG_TEXT_SIZE_PT
}

theme_phase_diagram <- function(settings) {
  theme_minimal(base_family = FONT_FAMILY, base_size = settings$base_size) +
    theme(
      text = element_text(family = FONT_FAMILY),
      axis.title = element_text(
        face = "bold",
        size = settings$axis_title_size,
        family = FONT_FAMILY
      ),
      axis.text = element_text(
        face = "bold",
        size = settings$axis_text_size,
        family = FONT_FAMILY
      ),
      panel.grid = element_blank(),
      panel.background = element_rect(fill = "white", color = NA),
      plot.background = element_rect(fill = "white", color = NA),
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.title = element_text(
        face = "bold",
        size = settings$legend_title_size,
        family = FONT_FAMILY
      ),
      legend.text = element_text(
        size = settings$legend_text_size,
        family = FONT_FAMILY
      ),
      legend.background = element_rect(fill = "white", color = NA),
      legend.box.spacing = grid::unit(0, "pt"),
      legend.margin = ggplot2::margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"),
      plot.margin = settings$plot_margin
    )
}

# Bin spill metrics by year (0 or quartiles for positive values)
bin_spills_by_year <- function(data, metric_col) {
  data %>%
    group_by(year) %>%
    mutate(
      metric_value = .data[[metric_col]],
      # For non-zero values, compute quartiles within each year
      metric_for_quartile = if_else(metric_value == 0, NA_real_, metric_value),
      quartile = ntile(metric_for_quartile, 4),
      state = case_when(
        is.na(metric_value) ~ NA_character_,
        metric_value == 0 ~ "0",
        TRUE ~ paste0("Q", quartile)
      )
    ) %>%
    ungroup() %>%
    select(site_id, year, state)
}

# Create year-to-year transitions (t -> t+1)
create_transitions <- function(state_data, state_col) {
  state_data %>%
    arrange(site_id, year) %>%
    group_by(site_id) %>%
    mutate(
      state_next = lead(.data[[state_col]]),
      year_next = lead(year)
    ) %>%
    ungroup() %>%
    filter(
      !is.na(.data[[state_col]]),
      !is.na(state_next),
      year_next == year + 1 # Ensure consecutive years
    ) %>%
    rename(
      state_current = !!state_col,
      transition_year = year
    ) %>%
    select(site_id, transition_year, state_current, state_next)
}

# Calculate transition probability matrix
calc_transition_matrix <- function(transitions_data) {
  # Count transitions
  transition_counts <- transitions_data %>%
    count(state_current, state_next, name = "n_transitions")

  # Calculate totals for each starting state
  state_totals <- transition_counts %>%
    group_by(state_current) %>%
    summarise(total = sum(n_transitions), .groups = "drop")

  # Calculate probabilities
  transition_probs <- transition_counts %>%
    left_join(state_totals, by = "state_current") %>%
    mutate(
      probability = n_transitions / total,
      pct_label = sprintf("%.1f%%", probability * 100)
    ) %>%
    select(state_current, state_next, probability, pct_label)

  return(transition_probs)
}

# Create phase diagram heatmap
plot_phase_diagram <- function(trans_prob_data, settings) {
  # Define state order
  state_levels <- c("0", "Q1", "Q2", "Q3", "Q4")

  # Ensure all state combinations exist (fill missing with 0)
  complete_grid <- expand.grid(
    state_current = state_levels,
    state_next = state_levels,
    stringsAsFactors = FALSE
  )

  plot_data <- complete_grid %>%
    left_join(trans_prob_data, by = c("state_current", "state_next")) %>%
    mutate(
      probability = replace_na(probability, 0),
      pct_label = if_else(
        probability == 0,
        "",
        sprintf("%.1f%%", probability * 100)
      ),
      state_current = factor(state_current, levels = state_levels),
      state_next = factor(state_next, levels = rev(state_levels))
    )

  ggplot(
    plot_data,
    aes(x = state_current, y = state_next, fill = probability)
  ) +
    geom_tile(color = "white", linewidth = settings$tile_linewidth) +
    geom_text(
      aes(label = pct_label),
      color = "black",
      size = pt_to_gg_text_size(settings$cell_text_size_pt),
      family = FONT_FAMILY
    ) +
    scale_fill_viridis_c(
      option = VIRIDIS_PALETTE,
      direction = -1,
      limits = c(0, 1),
      begin = 0.15,
      end = 1,
      labels = scales::percent_format(accuracy = 1),
      name = settings$legend_title,
      guide = guide_colorbar(
        title.position = settings$legend_title_position,
        title.hjust = 0.5,
        label.position = "bottom",
        barwidth = grid::unit(settings$legend_key_width, "cm"),
        barheight = grid::unit(settings$legend_key_height, "cm"),
        ticks = TRUE
      )
    ) +
    labs(
      x = settings$axis_x_title,
      y = settings$axis_y_title
    ) +
    theme_phase_diagram(settings) +
    coord_fixed()
}

save_phase_diagram <- function(trans_prob_data, file_name, settings) {
  p <- plot_phase_diagram(trans_prob_data, settings)

  ggsave(
    filename = file.path(output_dir, file_name),
    plot = p,
    width = settings$width_cm,
    height = settings$height_cm,
    dpi = PLOT_DPI,
    units = "cm",
    device = cairo_pdf
  )

  cat("  Saved:", file_name, "\n")
}

# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================
cat("Loading sewage spill data...\n")

path_spill_yr <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_yr.parquet"
)

spills <- import(path_spill_yr, trust = TRUE)

# 4.1 Prepare Transition Data --------------------------------------------------
cat("Preparing transition data...\n")

# Spill count states
spill_count_states <- bin_spills_by_year(spills, "spill_count_yr") %>%
  rename(state_count = state)

# Spill hours states
spill_hrs_states <- bin_spills_by_year(spills, "spill_hrs_yr") %>%
  rename(state_hrs = state)

# Create transitions
transitions_count <- create_transitions(spill_count_states, "state_count")
transitions_hrs <- create_transitions(spill_hrs_states, "state_hrs")

# 4.2 Calculate Transition Probabilities --------------------------------------
cat("Calculating transition probabilities...\n")

trans_prob_count <- calc_transition_matrix(transitions_count)
trans_prob_hrs <- calc_transition_matrix(transitions_hrs)

# ==============================================================================
# 5. Generate and Save Plots
# ==============================================================================
cat("Generating phase diagrams...\n")

plot_specs <- list(
  list(
    data = trans_prob_count,
    file_name = "spill_count_persistence.pdf",
    settings = PLOT_VARIANTS$paper
  ),
  list(
    data = trans_prob_count,
    file_name = "spill_count_persistence_slides.pdf",
    settings = PLOT_VARIANTS$slides
  ),
  list(
    data = trans_prob_hrs,
    file_name = "spill_hours_persistence.pdf",
    settings = PLOT_VARIANTS$paper
  ),
  list(
    data = trans_prob_hrs,
    file_name = "spill_hours_persistence_slides.pdf",
    settings = PLOT_VARIANTS$slides
  )
)

purrr::walk(
  plot_specs,
  \(spec) save_phase_diagram(spec$data, spec$file_name, spec$settings)
)

cat("\nAll phase diagrams generated successfully!\n")
