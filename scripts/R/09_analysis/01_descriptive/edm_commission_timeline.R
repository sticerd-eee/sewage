# ==============================================================================
# Canonical EDM Commission Timeline
# ==============================================================================
#
# Grain: Canonical Spill Site (`site_id_canonical`).
# Percentages are conditional on histories with resolved commission dates.
# Pre-2016 and unresolved histories are disclosed from the full universe.
#
# Environment overrides:
#   EDM_COMMISSION_INPUT_PATH  canonical unique_spill_sites parquet
#   EDM_COMMISSION_FIGURE_DIR  destination directory
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this script.", call. = FALSE)
}

source(
  here::here("scripts", "R", "utils", "edm_commission_figure_utils.R"),
  local = TRUE
)

build_edm_commission_timeline_plot <- function(figure_data) {
  annual_timing <- figure_data$annual_timing
  note <- paste(
    strwrap(format_edm_commission_figure_note(figure_data), width = 92L),
    collapse = "\n"
  )

  ggplot2::ggplot(
    annual_timing,
    ggplot2::aes(
      x = .data$commission_year,
      y = .data$conditional_percentage
    )
  ) +
    ggplot2::geom_col(fill = "#B63679FF") +
    ggplot2::scale_x_continuous(
      breaks = seq(
        min(annual_timing$commission_year),
        max(annual_timing$commission_year),
        by = 1L
      )
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(scale = 1),
      expand = ggplot2::expansion(mult = c(0, 0.05))
    ) +
    ggplot2::labs(
      title = "Annual EDM commissioning of Canonical Spill Sites",
      subtitle = "Canonical Spill Sites with commissioned EDM coverage",
      x = "Commission year",
      y = "Conditional share (%)",
      caption = note
    ) +
    ggplot2::theme_minimal(base_family = "serif", base_size = 10) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold", size = 12),
      plot.subtitle = ggplot2::element_text(size = 9),
      axis.title = ggplot2::element_text(face = "bold"),
      panel.grid.minor = ggplot2::element_blank(),
      panel.grid.major.x = ggplot2::element_line(color = "gray95"),
      panel.grid.major.y = ggplot2::element_line(color = "gray95"),
      plot.caption = ggplot2::element_text(hjust = 0, size = 6.5),
      plot.margin = ggplot2::margin(10, 10, 10, 10, unit = "pt")
    )
}

run_edm_commission_timeline <- function(
    input_path = Sys.getenv(
      "EDM_COMMISSION_INPUT_PATH",
      unset = here::here("data", "processed", "unique_spill_sites.parquet")
    ),
    output_dir = Sys.getenv(
      "EDM_COMMISSION_FIGURE_DIR",
      unset = here::here("output", "figures")
    )) {
  run_edm_commission_figure(
    input_path = input_path,
    output_dir = output_dir,
    output_filename = "edm_commission_timeline.pdf",
    plot_builder = build_edm_commission_timeline_plot
  )
}

if (sys.nframe() == 0L) {
  run_edm_commission_timeline()
}
