# ==============================================================================
# Shared News-Lag Utilities
# ==============================================================================
#
# Purpose: Provide margin-agnostic helpers for lagged public-attention
#          analyses of sewage spills and property prices.
#
# Author: Jacopo Olivieri
# Date: 2026-08-06
#
# ==============================================================================

#' Construct a lag-shifted post-peak indicator
#'
#' @param month_id Numeric vector of transaction month identifiers.
#' @param peak_month_id Integer month identifier of the attention peak.
#' @param lag Non-negative integer lag in months.
#' @return Integer vector equal to one from `peak_month_id + lag` onward.
shifted_post_indicator <- function(month_id, peak_month_id, lag) {
  if (!is.numeric(month_id)) {
    stop("`month_id` must be numeric.", call. = FALSE)
  }
  if (
    length(peak_month_id) != 1L ||
      !is.numeric(peak_month_id) ||
      is.na(peak_month_id) ||
      !is.finite(peak_month_id) ||
      peak_month_id != as.integer(peak_month_id)
  ) {
    stop("`peak_month_id` must be a finite integer scalar.", call. = FALSE)
  }
  if (
    length(lag) != 1L ||
      !is.numeric(lag) ||
      is.na(lag) ||
      !is.finite(lag) ||
      lag < 0 ||
      lag != as.integer(lag)
  ) {
    stop("`lag` must be a non-negative integer scalar.", call. = FALSE)
  }

  as.integer(month_id >= as.integer(peak_month_id) + as.integer(lag))
}

#' Join cumulative article measures at a lagged month
#'
#' @param sample Data frame containing transaction `month_id`.
#' @param articles Data frame containing unique monthly `month_id`,
#'   `cumulative_articles`, and `log_cumulative_articles` values.
#' @param lag Non-negative integer lag in months.
#' @param start_month_id Integer first month with article-measure support.
#' @return Input sample restricted to supported lagged months and joined to the
#'   lagged cumulative article measures, with `lagged_month_id` retained.
join_lagged_cumulative_articles <- function(
  sample,
  articles,
  lag,
  start_month_id
) {
  if (!is.data.frame(sample) || !"month_id" %in% names(sample)) {
    stop("`sample` must be a data frame containing `month_id`.", call. = FALSE)
  }

  required_article_columns <- c(
    "month_id",
    "cumulative_articles",
    "log_cumulative_articles"
  )
  if (
    !is.data.frame(articles) ||
      !all(required_article_columns %in% names(articles))
  ) {
    stop(
      "`articles` must be a data frame containing `month_id`, ",
      "`cumulative_articles`, and `log_cumulative_articles`.",
      call. = FALSE
    )
  }
  if (anyDuplicated(articles$month_id)) {
    stop("`articles$month_id` must uniquely identify article rows.", call. = FALSE)
  }
  if (
    length(lag) != 1L ||
      !is.numeric(lag) ||
      is.na(lag) ||
      !is.finite(lag) ||
      lag < 0 ||
      lag != as.integer(lag)
  ) {
    stop("`lag` must be a non-negative integer scalar.", call. = FALSE)
  }
  if (
    length(start_month_id) != 1L ||
      !is.numeric(start_month_id) ||
      is.na(start_month_id) ||
      !is.finite(start_month_id) ||
      start_month_id != as.integer(start_month_id)
  ) {
    stop("`start_month_id` must be a finite integer scalar.", call. = FALSE)
  }

  sample |>
    dplyr::mutate(lagged_month_id = .data$month_id - as.integer(lag)) |>
    dplyr::filter(.data$lagged_month_id >= as.integer(start_month_id)) |>
    dplyr::inner_join(
      dplyr::select(
        articles,
        "month_id",
        "cumulative_articles",
        "log_cumulative_articles"
      ),
      by = c("lagged_month_id" = "month_id")
    )
}

#' Restrict observations to the common cumulative-article sample
#'
#' @param sample Data frame containing transaction `month_id`.
#' @param start_month_id Integer first month with article-measure support.
#' @param max_lag Non-negative integer maximum lag in the comparison.
#' @return Input sample restricted to months supported at every compared lag.
restrict_to_common_sample <- function(sample, start_month_id, max_lag) {
  if (!is.data.frame(sample) || !"month_id" %in% names(sample)) {
    stop("`sample` must be a data frame containing `month_id`.", call. = FALSE)
  }
  if (
    length(start_month_id) != 1L ||
      !is.numeric(start_month_id) ||
      is.na(start_month_id) ||
      !is.finite(start_month_id) ||
      start_month_id != as.integer(start_month_id)
  ) {
    stop("`start_month_id` must be a finite integer scalar.", call. = FALSE)
  }
  if (
    length(max_lag) != 1L ||
      !is.numeric(max_lag) ||
      is.na(max_lag) ||
      !is.finite(max_lag) ||
      max_lag < 0 ||
      max_lag != as.integer(max_lag)
  ) {
    stop("`max_lag` must be a non-negative integer scalar.", call. = FALSE)
  }

  sample |>
    dplyr::filter(
      .data$month_id >= as.integer(start_month_id) + as.integer(max_lag)
    )
}

#' Run lightweight sanity checks for the shared lag mechanics
#'
#' @return Invisibly returns `TRUE`; aborts if a lag contract is violated.
run_news_lag_sanity_checks <- function() {
  stopifnot(
    shifted_post_indicator(20, 20, 0) == 1L,
    shifted_post_indicator(31, 20, 12) == 0L,
    shifted_post_indicator(32, 20, 12) == 1L
  )

  sample <- data.frame(
    row_id = seq_len(15L),
    month_id = seq_len(15L)
  )
  articles <- data.frame(
    month_id = seq_len(15L),
    cumulative_articles = cumsum(seq_len(15L)),
    log_cumulative_articles = log1p(cumsum(seq_len(15L)))
  )

  contemporaneous <- join_lagged_cumulative_articles(
    sample, articles, lag = 0L, start_month_id = 1L
  )
  direct <- dplyr::inner_join(sample, articles, by = "month_id")
  stopifnot(
    identical(contemporaneous$row_id, direct$row_id),
    identical(contemporaneous$month_id, direct$month_id),
    identical(
      contemporaneous$cumulative_articles,
      direct$cumulative_articles
    ),
    identical(
      contemporaneous$log_cumulative_articles,
      direct$log_cumulative_articles
    )
  )

  lagged <- join_lagged_cumulative_articles(
    sample, articles, lag = 3L, start_month_id = 1L
  )
  stopifnot(
    nrow(lagged) == nrow(sample) - 3L,
    min(lagged$month_id) == 4L,
    all(lagged$lagged_month_id == lagged$month_id - 3L)
  )

  common <- restrict_to_common_sample(
    sample, start_month_id = 1L, max_lag = 12L
  )
  stopifnot(
    min(common$month_id) == 13L,
    all(common$row_id %in% sample$row_id),
    nrow(common) < nrow(sample)
  )

  invisible(TRUE)
}
