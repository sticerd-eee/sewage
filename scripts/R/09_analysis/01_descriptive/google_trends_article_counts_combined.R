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
PLOT_DPI <- 300           # Resolution
START_YEAR <- 2018        # First year to display
END_YEAR <- 2024.5          # Last year to display
FONT_FAMILY <- "libertinus"
GG_TEXT_SIZE_PT <- 2.845276

PLOT_VARIANTS <- list(
  paper = list(
    file_name = "google_trends_article_counts_combined.pdf",
    width_cm = 14.5,
    height_cm = 9.0,
    base_size = 9.5,
    left_axis_title = "'Sewage Spill' Google Web Searches",
    right_axis_title = "UK Media Article Count",
    axis_title_size = 9.5,
    axis_text_size = 8.5,
    legend_text_size = 8.5,
    event_text_size_pt = 8.5,
    line_width = 0.55,
    event_line_width = 0.35,
    plot_margin = ggplot2::margin(t = 5, r = 5, b = 4, l = 5, unit = "pt"),
    event_positions = list(
      event1 = list(label_date = "2021-08-01", label_y = 60, line_y = 60),
      event2 = list(label_date = "2022-06-15", label_y = 100, line_y = 100),
      event3 = list(label_date = "2023-04-10", label_y = 100, line_y = 100),
      event4 = list(label_date = "2023-09-10", label_y = 75, line_y = 75)
    )
  ),
  slides = list(
    file_name = "google_trends_article_counts_combined_slides.pdf",
    width_cm = 13.2,
    height_cm = 6.6,
    base_size = 9,
    left_axis_title = "Google Web Searches",
    right_axis_title = "UK Media Articles",
    axis_title_size = 10,
    axis_text_size = 8.5,
    legend_text_size = 9,
    event_text_size_pt = 7.8,
    line_width = 0.65,
    event_line_width = 0.35,
    plot_margin = ggplot2::margin(t = 5, r = 5, b = 3, l = 5, unit = "pt"),
    event_positions = list(
      event1 = list(label_date = "2021-08-01", label_y = 65, line_y = 65),
      event2 = list(label_date = "2022-06-15", label_y = 100, line_y = 100),
      event3 = list(label_date = "2023-04-10", label_y = 100, line_y = 100),
      event4 = list(label_date = "2023-09-10", label_y = 80, line_y = 80)
    )
  )
)

# Line colors
COLOR_GOOGLE <- "#B63679FF"  # Viridis magma pink for Google Trends
COLOR_ARTICLES <- "#21908CFF" # Viridis teal for article counts

# Event annotations
EVENT1_DATE <- "2021-09-01"  # Parliament debate date
EVENT1_LABEL <- "Parliament debates\nsewage-overflow amendments\nin Environment Bill"
EVENT2_DATE <- "2022-08-01"  # BBC article date
EVENT2_LABEL <- "BBC:\nbeaches sewage\npollution warning"
EVENT3_DATE <- "2023-04-01"  # BBC article date
EVENT3_LABEL <- "BBC:\nsewage spill article"
EVENT4_DATE <- "2023-09-01"  # BBC article date
EVENT4_LABEL <- "BBC:\nsewage spill\ninvestigation"
EVENT_LINE_COLOR <- "grey70"  # Light grey for reference lines
EVENT_TEXT_COLOR <- "black"   # Black text for annotations


# ==============================================================================
# 2. Package Management
# ==============================================================================

required_packages <- c(
  "readxl",
  "tidyverse",
  "here",
  "scales",
  "showtext",
  "sysfonts",
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
add_libertinus_font <- function() {
  local_font_files <- c(
    regular = "~/Library/Fonts/LibertinusSerif-Regular.ttf",
    bold = "~/Library/Fonts/LibertinusSerif-Bold.ttf",
    italic = "~/Library/Fonts/LibertinusSerif-Italic.ttf",
    bolditalic = "~/Library/Fonts/LibertinusSerif-BoldItalic.ttf"
  )

  local_font_files <- path.expand(local_font_files)
  if (all(file.exists(local_font_files))) {
    do.call(
      sysfonts::font_add,
      c(list(family = FONT_FAMILY), as.list(local_font_files))
    )
    return(invisible(FONT_FAMILY))
  }

  sysfonts::font_add_google("Libertinus Serif", FONT_FAMILY, db_cache = TRUE)
  invisible(FONT_FAMILY)
}

showtext::showtext_auto()
showtext::showtext_opts(dpi = 300)
add_libertinus_font()

# 3.2 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.3 Plot Helpers -------------------------------------------------------------
pt_to_gg_text_size <- function(size_pt) {
  size_pt / GG_TEXT_SIZE_PT
}

theme_attention <- function(settings) {
  theme_minimal(base_family = FONT_FAMILY, base_size = settings$base_size) +
    theme(
      text = element_text(family = FONT_FAMILY),
      plot.title = element_text(
        face = "bold",
        size = settings$axis_title_size,
        family = FONT_FAMILY,
        margin = ggplot2::margin(b = 9, unit = "pt")
      ),
      axis.title = element_text(
        face = "bold",
        size = settings$axis_title_size,
        family = FONT_FAMILY
      ),
      axis.text = element_text(
        size = settings$axis_text_size,
        family = FONT_FAMILY
      ),
      axis.title.y.right = element_text(
        face = "bold",
        size = settings$axis_title_size,
        family = FONT_FAMILY,
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
      legend.box.spacing = grid::unit(0, "pt"),
      legend.margin = ggplot2::margin(t = -1, r = 0, b = 0, l = 0, unit = "pt"),
      legend.text = element_text(
        size = settings$legend_text_size,
        family = FONT_FAMILY
      ),
      plot.margin = settings$plot_margin
    )
}


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
build_attention_plot <- function(data, scale_factor, settings) {
  event_text_size <- pt_to_gg_text_size(settings$event_text_size_pt)
  event_positions <- settings$event_positions

  ggplot(data, aes(x = date)) +
    # Google Trends line
    geom_line(
      aes(y = google_value, color = "Google Trends"),
      linewidth = settings$line_width,
      na.rm = TRUE
    ) +
    # Article counts line (scaled)
    geom_line(
      aes(y = article_scaled, color = "UK Media Articles"),
      linewidth = settings$line_width,
      na.rm = TRUE
    ) +
    # Event 1: September 2021 Parliament debate
    annotate(
      "segment",
      x = as.Date(EVENT1_DATE),
      xend = as.Date(EVENT1_DATE),
      y = 0,
      yend = event_positions$event1$line_y,
      linetype = "dashed",
      color = EVENT_LINE_COLOR,
      linewidth = settings$event_line_width
    ) +
    annotate(
      "text",
      x = as.Date(event_positions$event1$label_date),
      y = event_positions$event1$label_y,
      label = EVENT1_LABEL,
      hjust = 1,
      vjust = 1,
      size = event_text_size,
      family = FONT_FAMILY,
      color = EVENT_TEXT_COLOR,
      lineheight = 0.9
    ) +
    # Event 2: August 2022 BBC article
    annotate(
      "segment",
      x = as.Date(EVENT2_DATE),
      xend = as.Date(EVENT2_DATE),
      y = 0,
      yend = event_positions$event2$line_y,
      linetype = "dashed",
      color = EVENT_LINE_COLOR,
      linewidth = settings$event_line_width
    ) +
    annotate(
      "text",
      x = as.Date(event_positions$event2$label_date),
      y = event_positions$event2$label_y,
      label = EVENT2_LABEL,
      hjust = 1,
      vjust = 1,
      size = event_text_size,
      family = FONT_FAMILY,
      color = EVENT_TEXT_COLOR,
      lineheight = 0.9
    ) +
    # Event 3: April 2023 BBC article
    annotate(
      "segment",
      x = as.Date(EVENT3_DATE),
      xend = as.Date(EVENT3_DATE),
      y = 0,
      yend = event_positions$event3$line_y,
      linetype = "dashed",
      color = EVENT_LINE_COLOR,
      linewidth = settings$event_line_width
    ) +
    annotate(
      "text",
      x = as.Date(event_positions$event3$label_date),
      y = event_positions$event3$label_y,
      label = EVENT3_LABEL,
      hjust = 0,
      vjust = 1,
      size = event_text_size,
      family = FONT_FAMILY,
      color = EVENT_TEXT_COLOR,
      lineheight = 0.9
    ) +
    # Event 4: September 2023 BBC article
    annotate(
      "segment",
      x = as.Date(EVENT4_DATE),
      xend = as.Date(EVENT4_DATE),
      y = 0,
      yend = event_positions$event4$line_y,
      linetype = "dashed",
      color = EVENT_LINE_COLOR,
      linewidth = settings$event_line_width
    ) +
    annotate(
      "text",
      x = as.Date(event_positions$event4$label_date),
      y = event_positions$event4$label_y,
      label = EVENT4_LABEL,
      hjust = 0,
      vjust = 1,
      size = event_text_size,
      family = FONT_FAMILY,
      color = EVENT_TEXT_COLOR,
      lineheight = 0.9
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
        name = settings$right_axis_title,
        breaks = scales::pretty_breaks(n = 5)
      )
    ) +
    labs(
      x = "Year",
      y = settings$left_axis_title
    ) +
    theme_attention(settings)
}

# 5.1 Save Plot ----------------------------------------------------------------
cat("Saving plot...\n")

for (variant_name in names(PLOT_VARIANTS)) {
  settings <- PLOT_VARIANTS[[variant_name]]
  p <- build_attention_plot(combined_data, scale_factor, settings)

  ggsave(
    filename = here::here(output_dir, settings$file_name),
    plot = p,
    width = settings$width_cm,
    height = settings$height_cm,
    dpi = PLOT_DPI,
    units = "cm"
  )

  cat("  Saved:", settings$file_name, "\n")
}
