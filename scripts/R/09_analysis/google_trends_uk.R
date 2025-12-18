# ==============================================================================
# Google Trends Analysis - "Sewage Spill" Search Interest Over Time
# ==============================================================================
# Purpose: Plot Google Trends data for "Sewage spill" web searches in the UK
#          showing temporal patterns from 2018-2024
#
# Author: Jacopo Olivieri
# Date: 2025-11-24
#
# Outputs: PDF plot saved to output/figures/
#          - google_trends_sewage_spill_uk.pdf
# ==============================================================================

# Configuration ----------------------------------------------------------------
PLOT_WIDTH <- 20 * 1.618# Width in cm
PLOT_HEIGHT <- 20       # Height in cm
PLOT_DPI <- 300         # Resolution
START_YEAR <- 2018      # First year to display
END_YEAR <- 2024        # Last year to display
LINE_COLOR <- "#B63679FF"  # Viridis magma color

# Event annotations
EVENT1_DATE <- "2021-09-01"  # Parliament debate date
EVENT1_LABEL <- "Parliament debates\nsewage-overflow amendments\nin Environment Bill"
EVENT2_DATE <- "2022-08-01"  # BBC article date
EVENT2_LABEL <- "BBC article:\nbeaches sewage\npollution warning"
EVENT3_DATE <- "2023-04-01"  # BBC article date
EVENT3_LABEL <- "BBC article:\nsewage spill article"
EVENT4_DATE <- "2023-09-01"  # BBC article date
EVENT4_LABEL <- "BBC article:\nsewage spill\ninvestigation"
EVENT_LINE_COLOR <- "grey70"  # Light grey for reference lines
EVENT_TEXT_COLOR <- "black"  # Black text for annotations
EVENT_TEXT_SIZE <- 4.5  # Text size for annotations

# Package Management -----------------------------------------------------------
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "readxl",
  "tidyverse",
  "here",
  "scales",
  "showtext",
  "lubridate"
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

# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# ggplot Theme -----------------------------------------------------------------
theme_pref <- theme_minimal() +
  theme(
    text = element_text(size = 9, family = "Libertinus Serif"),
    plot.title = element_text(
      face = "bold",
      size = 11,
      family = "Libertinus Serif",
      margin = ggplot2::margin(b = 9, unit = "pt")
    ),
    axis.title = element_text(face = "bold", size = 11, family = "Libertinus Serif"),
    axis.text = element_text(size = 9, family = "Libertinus Serif"),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "none",
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )

# Load Data --------------------------------------------------------------------
cat("Loading Google Trends data...\n")

google_trends <- readxl::read_excel(
  here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
  sheet = "united_kingdom"
)

# Process Data -----------------------------------------------------------------
cat("Processing data...\n")

# Convert date string to proper Date object and filter to 2018-2024
google_trends_clean <- google_trends %>%
  mutate(
    # Convert "YYYY-MM" string to Date (first day of month)
    date = lubridate::ymd(paste0(Date, "-01")),
    # Rename search column for easier reference
    search_interest = `'Sewage Spill' Google Searches`
  ) %>%
  filter(
    Year >= START_YEAR,
    Year <= END_YEAR,
    !is.na(date),
    !is.na(search_interest)
  ) %>%
  select(date, search_interest)

# Create Plot ------------------------------------------------------------------
cat("Creating plot...\n")

p <- ggplot(google_trends_clean, aes(x = date, y = search_interest)) +
  geom_line(color = LINE_COLOR, linewidth = 0.9) +
  # Event 1: September 2021 Parliament debate
  geom_segment(
    aes(x = as.Date(EVENT1_DATE), xend = as.Date(EVENT1_DATE),
        y = 0, yend = 60),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT1_DATE),
    y = 60,
    label = EVENT1_LABEL,
    hjust = 1.05,
    vjust = 1,
    size = EVENT_TEXT_SIZE,
    family = "Libertinus Serif",
    color = EVENT_TEXT_COLOR,
    lineheight = 0.9
  ) +
  # Event 2: August 2022 BBC article
  geom_segment(
    aes(x = as.Date(EVENT2_DATE), xend = as.Date(EVENT2_DATE),
        y = 0, yend = 100),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT2_DATE),
    y = 100,
    label = EVENT2_LABEL,
    hjust = -0.075,
    vjust = 1,
    size = EVENT_TEXT_SIZE,
    family = "Libertinus Serif",
    color = EVENT_TEXT_COLOR,
    lineheight = 0.9
  ) +
  # Event 3: April 2023 BBC article
  geom_segment(
    aes(x = as.Date(EVENT3_DATE), xend = as.Date(EVENT3_DATE),
        y = 0, yend = 80),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT3_DATE),
    y = 80,
    label = EVENT3_LABEL,
    hjust = -0.075,
    vjust = 1,
    size = EVENT_TEXT_SIZE,
    family = "Libertinus Serif",
    color = EVENT_TEXT_COLOR,
    lineheight = 0.9
  ) +
  # Event 4: September 2023 BBC article
  geom_segment(
    aes(x = as.Date(EVENT4_DATE), xend = as.Date(EVENT4_DATE),
        y = 0, yend = 60),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT4_DATE),
    y = 60,
    label = EVENT4_LABEL,
    hjust = -0.075,
    vjust = 1,
    size = EVENT_TEXT_SIZE,
    family = "Libertinus Serif",
    color = EVENT_TEXT_COLOR,
    lineheight = 0.9
  ) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    limits = as.Date(c(paste0(START_YEAR, "-01-01"),
                       paste0(END_YEAR, "-12-31")))
  ) +
  scale_y_continuous(
    breaks = seq(0, 100, 20),
    limits = c(0, NA)
  ) +
  labs(
    x = "Year",
    y = "'Sewage Spill' Google Web Searches"
  ) +
  theme_pref

# Save Plot --------------------------------------------------------------------
cat("Saving plot...\n")

file_name <- "google_trends_sewage_spill_uk.pdf"
ggsave(
  filename = here::here(output_dir, file_name),
  plot = p,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

cat("  Saved:", file_name, "\n")
