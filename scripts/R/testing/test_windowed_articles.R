# ==============================================================================
# Validate windowed LexisNexis article-count construction
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this validation. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "here",
  "tibble"
)

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "09_analysis", "05_news", "extensive_margin_news_utils.R"),
  local = TRUE
)

ARTICLE_PATH <- here::here(
  "data", "processed", "lexis_nexis", "search1_monthly.parquet"
)
WINDOWS <- c(3L, 6L, 12L)
START_MONTH_ID <- 1L
END_MONTH_ID <- 36L

raw_articles <- arrow::read_parquet(ARTICLE_PATH)

complete_grid <- build_windowed_article_grid(
  articles = raw_articles,
  windows = WINDOWS,
  start_month_id = START_MONTH_ID,
  end_month_id = END_MONTH_ID
)

expected_grid <- seq.int(START_MONTH_ID - max(WINDOWS), END_MONTH_ID)
stopifnot(
  identical(as.integer(complete_grid$month_id), expected_grid),
  !anyDuplicated(complete_grid$month_id)
)

dec_2020 <- complete_grid |>
  dplyr::filter(.data$month_id == 0L)
stopifnot(nrow(dec_2020) == 1L)
stopifnot(identical(as.integer(dec_2020$article_count), 0L))

windowed_articles <- load_windowed_articles_data(
  path = ARTICLE_PATH,
  windows = WINDOWS,
  start_month_id = START_MONTH_ID,
  end_month_id = END_MONTH_ID
)

expected_window_count <- function(month_id, window) {
  window_months <- seq.int(month_id - window + 1L, month_id)
  sum(complete_grid$article_count[complete_grid$month_id %in% window_months])
}

for (window in normalise_article_windows(WINDOWS)) {
  count_col <- paste0("articles_", window, "m")
  expected_counts <- vapply(
    windowed_articles$month_id,
    expected_window_count,
    numeric(1),
    window = window
  )

  stopifnot(isTRUE(all.equal(
    as.numeric(windowed_articles[[count_col]]),
    expected_counts,
    tolerance = 0
  )))
}

month_1 <- windowed_articles |>
  dplyr::filter(.data$month_id == 1L)
month_3 <- windowed_articles |>
  dplyr::filter(.data$month_id == 3L)

stopifnot(
  nrow(windowed_articles) == 36L,
  nrow(month_1) == 1L,
  nrow(month_3) == 1L,
  identical(as.integer(month_1$articles_3m), 6L),
  identical(as.integer(month_3$articles_3m), 7L),
  all(windowed_articles$articles_12m >= windowed_articles$articles_6m),
  all(windowed_articles$articles_6m >= windowed_articles$articles_3m),
  all(is.finite(windowed_articles$log_articles_3m)),
  all(is.finite(windowed_articles$log_articles_6m)),
  all(is.finite(windowed_articles$log_articles_12m))
)

window_count_columns <- paste0("articles_", normalise_article_windows(WINDOWS), "m")
window_log_columns <- paste0("log_", window_count_columns)

expected_columns <- c(
  "month_id",
  "article_count",
  "cumulative_articles",
  "log_cumulative_articles",
  as.vector(rbind(window_count_columns, window_log_columns))
)
stopifnot(all(expected_columns %in% names(windowed_articles)))

legacy_articles <- load_articles_data(
  path = ARTICLE_PATH,
  start_month_id = START_MONTH_ID,
  end_month_id = END_MONTH_ID
) |>
  dplyr::select(
    "month_id",
    "cumulative_articles",
    "log_cumulative_articles"
  )

windowed_legacy_columns <- windowed_articles |>
  dplyr::select(
    "month_id",
    "cumulative_articles",
    "log_cumulative_articles"
  )

stopifnot(identical(legacy_articles, windowed_legacy_columns))

cat("Windowed article-count validation passed.\n")
cat(sprintf("  Rows returned: %d\n", nrow(windowed_articles)))
cat(
  "  Columns: ",
  paste(expected_columns, collapse = ", "),
  "\n",
  sep = ""
)
