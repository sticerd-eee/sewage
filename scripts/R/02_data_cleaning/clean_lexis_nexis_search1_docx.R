# ==============================================================================
# LexisNexis Search_1 DOCX Article Cleaner
# ==============================================================================
#
# Purpose: Convert LexisNexis search_1 DOCX exports (one article per file) into
#          an article-level dataset, apply within-body de-duplication, and write
#          the interim `articles_clean.parquet`.
#
# Author: Ana Werneck
# Date: 2026-07-16
#
# Inputs:
#   - data/raw/lexis_nexis/search_1_docx/*.docx
#   - scripts/R/utils/nexis_docx_conversion.R
#
# Outputs:
#   - data/processed/lexis_nexis/articles_clean.parquet   # never overwrites search1_articles.parquet
#   - output/log/clean_lexis_nexis_search1_docx.log
#   - data/processed/lexis_nexis/articles_clean_monthly.parquet
#   - output/figures/search1_docx_monthly.png
#
# Source:
#   - LexisNexis Academic database
#   - search_1: hlead(sewage) AND incident AND hlead(leak OR overflow); UK sources
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow", 
  "dplyr", 
  "ggplot2", 
  "glue",
  "logger", 
  "lubridate", 
  "officer", 
  "stringr"
)

LOG_FILE <- here::here("output", "log", "clean_lexis_nexis_search1_docx.log")

check_required_packages(REQUIRED_PACKAGES)

# Configuration
############################################################

CONFIG <- list(
  base_year = 2021,
  input_dir = here::here("data", "raw", "lexis_nexis", "search_1_docx"),
  output_dir = here::here("data", "processed", "lexis_nexis"),
  figure_dir = here::here("output", "figures"),
  docx_converter_path = here::here("scripts", "R", "utils", "nexis_docx_conversion.R")
)
 
# Setup Functions
############################################################
 
#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}
 
#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
} 

# Data Loading Functions
############################################################
 
#' Source the DOCX conversion function from the co-located conversion helper
#' @return NULL
load_docx_converter <- function() {
  if (!file.exists(CONFIG$docx_converter_path)) {
    err_msg <- glue::glue("DOCX converter script not found: {CONFIG$docx_converter_path}")
    logger::log_error(err_msg)
    stop(err_msg)
  }
 
  source(CONFIG$docx_converter_path)
  logger::log_info("DOCX converter function loaded from {CONFIG$docx_converter_path}")
}
 
#' Load and convert all search_1 DOCX files to article-level data
#' @return Data frame of articles with columns: file, source, date, heading, body
load_docx_data <- function() {
  if (!dir.exists(CONFIG$input_dir)) {
    err_msg <- glue::glue("Input folder not found: {CONFIG$input_dir}")
    logger::log_error(err_msg)
    stop(err_msg)
  }
 
  n_files <- length(list.files(CONFIG$input_dir, pattern = "\\.docx$", ignore.case = TRUE))
  logger::log_info("Converting {n_files} DOCX files from {CONFIG$input_dir}")
 
  # Use the nexis_docx_to_table function (loaded via source())
  articles <- nexis_docx_to_table(CONFIG$input_dir)
 
  logger::log_info("Extracted {nrow(articles)} articles from DOCX files")
  return(articles)
}
 
# Data Processing Functions
############################################################
 
#' Remove duplicated text within each article body.
#' Splits each body into sentences, drops exact repeats, rejoins. Website
#' furniture is NOT touched here.
#' @param df Article-level data frame with a `body` column
#' @return Data frame with de-duplicated bodies
dedup_article_bodies <- function(df) {
  dedup_one <- function(body) {
    sentences <- stringr::str_split(body, "(?<=\\.)\\s+")[[1]]
    sentences <- sentences[!duplicated(sentences)]
    stringr::str_squish(paste(sentences, collapse = " "))
  }
  df %>%
    mutate(body = vapply(body, dedup_one, character(1)))
}
 
#' Add time identifiers to article data
#' @param df Data frame with date column
#' @return Data frame with year, month, month_id, qtr_id columns added
add_time_identifiers <- function(df) {
  base_year <- CONFIG$base_year
 
  df %>%
    mutate(
      year = lubridate::year(date),
      month = lubridate::month(date),
      month_id = (year - base_year) * 12 + month,
      qtr_id = (year - base_year) * 4 + lubridate::quarter(date)
    )
}
 
#' Aggregate articles to monthly counts (national series - deprecated target,
#' kept for comparison against the PDF-based series)
#' @param df Data frame with time identifiers
#' @return Data frame with monthly article counts
aggregate_monthly <- function(df) {
  df %>%
    group_by(year, month, month_id, qtr_id) %>%
    summarise(
      article_count = n(),
      .groups = "drop"
    ) %>%
    arrange(year, month)
}
 
# Plotting Functions
############################################################
 
#' Plot the monthly coverage series and save it to output/figures
#' @param monthly_df Monthly aggregated data frame
#' @return NULL
plot_monthly_counts <- function(monthly_df) {
  dir.create(CONFIG$figure_dir, recursive = TRUE, showWarnings = FALSE)
 
  # Build a date column for the x-axis (first of each month)
  monthly_df <- monthly_df %>%
    mutate(date = as.Date(paste(year, month, "01", sep = "-")))
 
  p <- ggplot(monthly_df, aes(x = date, y = article_count)) +
    geom_line() +
    geom_point(size = 0.8) +
    labs(
      x = "Date",
      y = "Article Count",
      title = "UK Media Coverage of Sewage Over Time",
      subtitle = "LexisNexis search_1_docx monthly article counts"
    ) +
    theme_minimal() +
    theme(axis.title.y = element_text(angle = 0, vjust = 0.5))
 
  fig_path <- file.path(CONFIG$figure_dir, "search1_docx_monthly.png")
  ggsave(fig_path, plot = p, width = 9, height = 5, dpi = 150)
  logger::log_info("Monthly time-series plot saved to {fig_path}")
 
  # Return the plot object so it also displays in interactive use
  invisible(p)
}
 
# Export Functions
############################################################
 
#' Export processed data to Parquet files.
#' Writes articles_clean.parquet and articles_clean_monthly.parquet only -
#' never overwrites the PDF pipeline's search1_articles.parquet or
#' search1_monthly.parquet.
#' @param articles_df Cleaned article-level data frame
#' @param monthly_df Monthly aggregated data frame
#' @return NULL
export_data <- function(articles_df, monthly_df) {
  # Create output directory if needed
  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)
 
  protected <- c("search1_articles.parquet", "search1_monthly.parquet")
 
  articles_path <- file.path(CONFIG$output_dir, "articles_clean.parquet")
  monthly_path  <- file.path(CONFIG$output_dir, "articles_clean_monthly.parquet")
 
  # Guard: never write to a protected filename
  if (basename(articles_path) %in% protected || basename(monthly_path) %in% protected) {
    stop("Refusing to overwrite a protected PDF-pipeline output file")
  }
 
  # Export cleaned article-level data
  tryCatch(
    {
      arrow::write_parquet(articles_df, articles_path)
      logger::log_info("Cleaned article-level data exported to {articles_path}")
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export article data: {e$message}")
      logger::log_error(err_msg)
      stop(err_msg)
    }
  )
 
  # Export monthly aggregates
  tryCatch(
    {
      arrow::write_parquet(monthly_df, monthly_path)
      logger::log_info("Monthly aggregates exported to {monthly_path}")
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export monthly data: {e$message}")
      logger::log_error(err_msg)
      stop(err_msg)
    }
  )
}
 
# Main Execution
############################################################
 
#' Main execution function
#' @return NULL
main <- function() {
  # Setup
  initialise_environment()
  initialise_logging()
 
  # Load DOCX converter function
  load_docx_converter()
 
  # Load and convert DOCX files
  articles_raw <- load_docx_data()
 
  # Remove within-body duplicated text
  articles <- dedup_article_bodies(articles_raw)
 
  # Add time identifiers
  articles <- add_time_identifiers(articles)
 
  # Log date range
  date_range <- range(articles$date, na.rm = TRUE)
  logger::log_info("Article date range: {date_range[1]} to {date_range[2]}")
 
  # Aggregate to monthly
  monthly <- aggregate_monthly(articles)

  # Export results
  export_data(articles, monthly)
 
  # Plot the monthly series
  plot_monthly_counts(monthly)
 
  logger::log_info("Script finished at {Sys.time()}")
 
  # Return articles invisibly for interactive use
  invisible(articles)
}
 
# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
