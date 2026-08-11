# ==============================================================================
# Canonical EDM Commission Cumulative Distribution
# ==============================================================================
#
# Grain: Canonical Spill Site (`site_id_canonical`).
# The curve advances by observed commission year, never by artificial exact date.
# Percentages are conditional on histories with resolved commission dates.
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

build_edm_commission_cumulative_plot <- function(figure_data) {
  annual_cumulative <- figure_data$annual_cumulative
  note <- paste(
    strwrap(format_edm_commission_figure_note(figure_data), width = 92L),
    collapse = "\n"
  )

  ggplot2::ggplot(
    annual_cumulative,
    ggplot2::aes(
      x = .data$commission_year,
      y = .data$cumulative_percentage
    )
  ) +
    ggplot2::geom_step(color = "#B63679FF", linewidth = 0.9) +
    ggplot2::geom_point(color = "#B63679FF", size = 1.3) +
    ggplot2::scale_x_continuous(
      breaks = seq(
        min(annual_cumulative$commission_year),
        max(annual_cumulative$commission_year),
        by = 1L
      )
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(scale = 1),
      breaks = seq(0, 100, by = 20),
      limits = c(0, 100),
      expand = ggplot2::expansion(mult = c(0, 0.02))
    ) +
    ggplot2::labs(
      title = "Cumulative EDM commissioning of Canonical Spill Sites",
      subtitle = "Canonical Spill Sites with commissioned EDM coverage",
      x = "Commission year",
      y = "Cumulative conditional share (%)",
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

run_edm_commission_cumulative <- function(
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
    output_filename = "edm_commission_cumulative.pdf",
    plot_builder = build_edm_commission_cumulative_plot
  )
}

if (sys.nframe() == 0L) {
  run_edm_commission_cumulative()
}
