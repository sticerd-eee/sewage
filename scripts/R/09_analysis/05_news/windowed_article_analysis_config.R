# ==============================================================================
# Windowed Article Analysis Specification Catalog
# ==============================================================================

windowed_article_windows <- c(3L, 6L, 12L)

windowed_article_salience_cols <- c(
  Cumulative = "log_cumulative_articles",
  `3m` = "log_articles_3m",
  `6m` = "log_articles_6m",
  `12m` = "log_articles_12m"
)

windowed_article_measure_slugs <- c(
  Cumulative = "cumulative",
  `3m` = "3m",
  `6m` = "6m",
  `12m` = "12m"
)

windowed_article_intensive_radii <- c(
  `250m` = 250L,
  `500m` = 500L,
  `1000m` = 1000L
)

windowed_article_extensive_comparisons <- list(
  `250 vs 250-1000` = list(
    comparison_id = "250_vs_250_1000",
    comparison_label = "0-250m vs 250-1000m",
    near_min = 0L,
    near_max = 250L,
    far_min = 250L,
    far_max = 1000L
  ),
  `250 vs 500-1000` = list(
    comparison_id = "250_vs_500_1000",
    comparison_label = "0-250m vs 500-1000m",
    near_min = 0L,
    near_max = 250L,
    far_min = 500L,
    far_max = 1000L
  ),
  `500 vs 500-2000` = list(
    comparison_id = "500_vs_500_2000",
    comparison_label = "0-500m vs 500-2000m",
    near_min = 0L,
    near_max = 500L,
    far_min = 500L,
    far_max = 2000L
  ),
  `500 vs 1000-2000` = list(
    comparison_id = "500_vs_1000_2000",
    comparison_label = "0-500m vs 1000-2000m",
    near_min = 0L,
    near_max = 500L,
    far_min = 1000L,
    far_max = 2000L
  )
)
