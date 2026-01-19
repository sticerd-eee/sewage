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
#   - output/figures/spill_hours_persistence.pdf
#
# Note: Years are hardcoded to 2021-2023 period
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 18 * 1.618
PLOT_HEIGHT <- 18
PLOT_DPI <- 300
VIRIDIS_PALETTE <- "magma"

# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

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
sysfonts::font_add_google("Libertinus Serif", "libertinus", db_cache = FALSE)

# 3.2 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.3 ggplot Theme -------------------------------------------------------------
theme_pref <- theme_minimal() +
  theme(
    text = element_text(size = 10, family = "libertinus"),
    axis.title = element_text(face = "bold", size = 12, family = "libertinus"),
    axis.text = element_text(face = "bold", size = 10, family = "libertinus"),
    panel.grid = element_blank(),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "bottom",
    legend.title = element_text(face = "bold", size = 12, family = "libertinus"),
    legend.text = element_text(size = 10, family = "libertinus"),
    legend.background = element_rect(fill = "white", color = NA),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )

# 3.4 Helper Functions ---------------------------------------------------------

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
plot_phase_diagram <- function(trans_prob_data) {
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
    geom_tile(color = "white", linewidth = 0.5) +
    geom_text(
      aes(label = pct_label),
      color = "black",
      size = 3.5,
      #fontface = "bold"
    ) +
    scale_fill_viridis_c(
      option = VIRIDIS_PALETTE,
      direction = -1,
      limits = c(0, 1),
      begin = 0.15,
      end = 1,
      labels = scales::percent_format(accuracy = 1),
      name = "Transition\nProbability"
    ) +
    labs(
      x = "State in Year t",
      y = "State in Year t+1"
    ) +
    theme_pref +
    coord_fixed()
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

# Spill Count Phase Diagram
p_count <- plot_phase_diagram(trans_prob_count)

file_name_count <- "spill_count_persistence.pdf"
ggsave(
  filename = here::here(output_dir, file_name_count),
  plot = p_count,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  unit = "cm"
)
cat("  Saved:", file_name_count, "\n")

# Spill Hours Phase Diagram
p_hrs <- plot_phase_diagram(trans_prob_hrs)

file_name_hrs <- "spill_hours_persistence.pdf"
ggsave(
  filename = here::here(output_dir, file_name_hrs),
  plot = p_hrs,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  unit = "cm"
)
cat("  Saved:", file_name_hrs, "\n")

cat("\nAll phase diagrams generated successfully!\n")
