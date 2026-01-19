# ==============================================================================
# Combined Google Trends & Article Counts - Dual Y-Axis Time Series Plot
# ==============================================================================
#
# Purpose: Plot dual y-axis figure showing Google Trends "Sewage spill" searches
#          and UK newspaper article counts over time (2018-2024), with event
#          annotations. Scales aligned so maxima match for easy comparison.
#
# Author: Jacopo Olivieri
# Date: 2026-01-12
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx - Google Trends data
#   - data/processed/lexis_nexis/search1_monthly.parquet - Monthly article counts
#
# Outputs:
#   - output/figures/google_trends_article_counts_combined.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
PLOT_WIDTH <- 18 * 1.618  # Width in cm
PLOT_HEIGHT <- 18         # Height in cm
PLOT_DPI <- 300           # Resolution
START_YEAR <- 2018        # First year to display
END_YEAR <- 2024          # Last year to display

# Line colors
COLOR_GOOGLE <- "#B63679FF"  # Viridis magma pink for Google Trends
COLOR_ARTICLES <- "#21908CFF" # Viridis teal for article counts

# Event annotations
EVENT1_DATE <- "2021-09-01"  # Parliament debate date
EVENT1_LABEL <- "Parliament debates\nsewage-overflow amendments\nin Environment Bill"
EVENT1_Y <- 60  # Y position for label
EVENT2_DATE <- "2022-08-01"  # BBC article date
EVENT2_LABEL <- "BBC article:\nbeaches sewage\npollution warning"
EVENT2_Y <- 100  # Y position for label
EVENT3_DATE <- "2023-04-01"  # BBC article date
EVENT3_LABEL <- "BBC article:\nsewage spill article"
EVENT3_Y <- 80  # Y position for label
EVENT4_DATE <- "2023-09-01"  # BBC article date
EVENT4_LABEL <- "BBC article:\nsewage spill\ninvestigation"
EVENT4_Y <- 60  # Y position for label
EVENT_LINE_COLOR <- "grey70"  # Light grey for reference lines
EVENT_TEXT_COLOR <- "black"   # Black text for annotations
EVENT_TEXT_SIZE <- 4.5        # Text size for annotations


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "readxl",
  "tidyverse",
  "here",
  "scales",
  "showtext",
  "lubridate",
  "arrow"
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
    text = element_text(size = 10, family = "Libertinus Serif"),
    plot.title = element_text(
      face = "bold",
      size = 12,
      family = "Libertinus Serif",
      margin = ggplot2::margin(b = 9, unit = "pt")
    ),
    axis.title = element_text(face = "bold", size = 12, family = "Libertinus Serif"),
    axis.text = element_text(size = 10, family = "Libertinus Serif"),
    axis.title.y.right = element_text(
      face = "bold",
      size = 12,
      family = "Libertinus Serif",
      color = COLOR_ARTICLES
    ),
    axis.text.y.right = element_text(color = COLOR_ARTICLES),
    axis.title.y.left = element_text(color = COLOR_GOOGLE),
    axis.text.y.left = element_text(color = COLOR_GOOGLE),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 10, family = "Libertinus Serif"),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )


# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================
cat("Loading data...\n")

# 4.1 Load Google Trends data --------------------------------------------------
google_trends <- readxl::read_excel(
  here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
  sheet = "united_kingdom"
)

google_trends_clean <- google_trends %>%
  mutate(
    date = lubridate::ymd(paste0(Date, "-01")),
    google_value = `'Sewage Spill' Google Searches`
  ) %>%
  filter(
    Year >= START_YEAR,
    Year <= END_YEAR,
    !is.na(date),
    !is.na(google_value)
  ) %>%
  select(date, google_value)

cat(sprintf("  Google Trends: %d months (max = %d)\n",
            nrow(google_trends_clean),
            max(google_trends_clean$google_value, na.rm = TRUE)))

# 4.2 Load LexisNexis article counts -------------------------------------------
articles <- arrow::read_parquet(
  here::here("data", "processed", "lexis_nexis", "search1_monthly.parquet")
)

articles_clean <- articles %>%
  mutate(
    date = lubridate::ymd(paste(year, month, "01", sep = "-")),
    article_value = article_count
  ) %>%
  filter(
    year >= START_YEAR,
    year <= END_YEAR,
    !is.na(date),
    !is.na(article_value)
  ) %>%
  select(date, article_value)

cat(sprintf("  Article counts: %d months (max = %d)\n",
            nrow(articles_clean),
            max(articles_clean$article_value, na.rm = TRUE)))

# 4.3 Calculate scale factor to align maxima -----------------------------------
max_google <- max(google_trends_clean$google_value, na.rm = TRUE)
max_articles <- max(articles_clean$article_value, na.rm = TRUE)
scale_factor <- max_google / max_articles

cat(sprintf("  Scale factor: %.3f (max_google=%d, max_articles=%d)\n",
            scale_factor, max_google, max_articles))

# 4.4 Merge datasets -----------------------------------------------------------
combined_data <- google_trends_clean %>%
  full_join(articles_clean, by = "date") %>%
  mutate(
    # Replace NA article counts with 0 (missing = no articles)
    article_value = replace_na(article_value, 0),
    # Scale article values to match Google Trends scale
    article_scaled = article_value * scale_factor
  )

cat(sprintf("  Combined dataset: %d rows\n", nrow(combined_data)))


# ==============================================================================
# 5. Create and Save Plot
# ==============================================================================
cat("Creating plot...\n")

p <- ggplot(combined_data, aes(x = date)) +
  # Event 1: September 2021 Parliament debate
  geom_segment(
    aes(x = as.Date(EVENT1_DATE), xend = as.Date(EVENT1_DATE),
        y = 0, yend = EVENT1_Y),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT1_DATE),
    y = EVENT1_Y,
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
        y = 0, yend = EVENT2_Y),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT2_DATE),
    y = EVENT2_Y,
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
        y = 0, yend = EVENT3_Y),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT3_DATE),
    y = EVENT3_Y,
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
        y = 0, yend = EVENT4_Y),
    linetype = "dashed",
    color = EVENT_LINE_COLOR,
    linewidth = 0.6,
    inherit.aes = FALSE
  ) +
  annotate(
    "text",
    x = as.Date(EVENT4_DATE),
    y = EVENT4_Y,
    label = EVENT4_LABEL,
    hjust = -0.075,
    vjust = 1,
    size = EVENT_TEXT_SIZE,
    family = "Libertinus Serif",
    color = EVENT_TEXT_COLOR,
    lineheight = 0.9
  ) +
  # Google Trends line
  geom_line(
    aes(y = google_value, color = "Google Trends"),
    linewidth = 0.9,
    na.rm = TRUE
  ) +
  # Article counts line (scaled)
  geom_line(
    aes(y = article_scaled, color = "UK Media Articles"),
    linewidth = 0.9,
    na.rm = TRUE
  ) +
  # Color scale
  scale_color_manual(
    values = c(
      "Google Trends" = COLOR_GOOGLE,
      "UK Media Articles" = COLOR_ARTICLES
    )
  ) +
  # X-axis settings
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    limits = as.Date(c(paste0(START_YEAR, "-01-01"),
                       paste0(END_YEAR, "-12-31")))
  ) +
  # Dual Y-axis: left for Google Trends, right for article counts
  scale_y_continuous(
    breaks = seq(0, 100, 20),
    limits = c(0, NA),
    sec.axis = sec_axis(
      ~ . / scale_factor,
      name = "UK Media Article Count",
      breaks = scales::pretty_breaks(n = 5)
    )
  ) +
  labs(
    x = "Year",
    y = "'Sewage Spill' Google Web Searches"
  ) +
  theme_pref

# 5.1 Save Plot ----------------------------------------------------------------
cat("Saving plot...\n")

file_name <- "google_trends_article_counts_combined.pdf"
ggsave(
  filename = here::here(output_dir, file_name),
  plot = p,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

cat("  Saved:", file_name, "\n")
