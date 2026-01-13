############################################################
# Clean LexisNexis Search_1 PDF Data
# Project: Sewage
# Date: 09/01/2026
# Author: Jacopo Olivieri
############################################################

#' This script converts LexisNexis search_1 PDF files to article-level data
#' and aggregates to monthly counts for merging with main analysis datasets.
#' The search_1 PDFs are pre-filtered to UK sources only.
#' @source LexisNexis Academic database

# Configuration
############################################################

CONFIG <- list(
  base_year = 2021,
  input_dir = here::here("data", "raw", "lexis_nexis", "search_1"),
  output_dir = here::here("data", "processed", "lexis_nexis"),
  pdf_converter_path = here::here("news_lexisnexis", "scripts", "nexis_pdf_conversion.R"),
  pdf_files = c(
    "search_1_1.PDF",
    "search_1_2.PDF",
    "search_1_3.PDF"
  )
)

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  required_packages <- c(
    "dplyr",      # Data manipulation
    "tidyr",      # Data reshaping
    "stringr",    # String operations
    "lubridate",  # Date handling
    "here",       # Project-rooted paths
    "logger",     # Structured logging
    "glue",       # String interpolation
    "arrow",      # Parquet I/O
    "pdftools"    # PDF text extraction (required by nexis_pdf_conversion.R)
  )

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "clean_lexis_nexis_search1.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Data Loading Functions
############################################################

#' Source the PDF conversion function from news_lexisnexis scripts
#' @return NULL
load_pdf_converter <- function() {
  if (!file.exists(CONFIG$pdf_converter_path)) {
    err_msg <- glue::glue("PDF converter script not found: {CONFIG$pdf_converter_path}")
    logger::log_error(err_msg)
    stop(err_msg)
  }

  source(CONFIG$pdf_converter_path)
  logger::log_info("PDF converter function loaded from {CONFIG$pdf_converter_path}")
}

#' Load and convert all search_1 PDF files to article-level data
#' @return Data frame of articles with columns: search_term, date, source, heading, article, text
load_pdf_data <- function() {
  pdf_paths <- file.path(CONFIG$input_dir, CONFIG$pdf_files)

  # Check all files exist
  missing_files <- pdf_paths[!file.exists(pdf_paths)]
  if (length(missing_files) > 0) {
    err_msg <- glue::glue("PDF files not found: {paste(missing_files, collapse = ', ')}")
    logger::log_error(err_msg)
    stop(err_msg)
  }

  logger::log_info("Converting {length(pdf_paths)} PDF files to article data")

  # Use the nexis_pdf_to_table function (loaded via source())
  articles <- nexis_pdf_to_table(pdf_paths)

  logger::log_info("Extracted {nrow(articles)} articles from PDFs")
  return(articles)
}

# Data Processing Functions
############################################################

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

#' Aggregate articles to monthly counts
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

# Export Functions
############################################################

#' Export processed data to Parquet files
#' @param articles_df Article-level data frame
#' @param monthly_df Monthly aggregated data frame
#' @return NULL
export_data <- function(articles_df, monthly_df) {
  # Create output directory if needed
  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  # Export monthly aggregates
  monthly_path <- file.path(CONFIG$output_dir, "search1_monthly.parquet")
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

  # Export article-level data
  articles_path <- file.path(CONFIG$output_dir, "search1_articles.parquet")
  tryCatch(
    {
      arrow::write_parquet(articles_df, articles_path)
      logger::log_info("Article-level data exported to {articles_path}")
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export article data: {e$message}")
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
  setup_logging()

  # Load PDF converter function
  load_pdf_converter()

  # Load and convert PDFs
  articles_raw <- load_pdf_data()

  # Add time identifiers
  articles <- add_time_identifiers(articles_raw)

  # Log date range
  date_range <- range(articles$date, na.rm = TRUE)
  logger::log_info("Article date range: {date_range[1]} to {date_range[2]}")

  # Aggregate to monthly
  monthly <- aggregate_monthly(articles)
  logger::log_info("Created monthly aggregates: {nrow(monthly)} months")

  # Export results
  export_data(articles, monthly)

  logger::log_info("Script finished at {Sys.time()}")

  # Return monthly data invisibly for interactive use
  invisible(monthly)
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}

library(tidyverse)
library(ggplot2)
library(here)

# Load the monthly data
news <- import(here("data", "processed", "lexis_nexis", "search1_monthly.parquet"))

# Create date column for plotting
news$date <- as.Date(paste(news$year, news$month, "01", sep = "-"))

# Plot time series
ggplot(news, aes(x = date, y = article_count)) +
  geom_line() +
  geom_point(size = 0.8) +
  labs(
    x = "Date",
    y = "Article Count",
    title = "UK Media Coverage of Sewage Over Time",
    subtitle = "LexisNexis search_1 monthly article counts"
  ) +
  theme_minimal() +
  theme(axis.title.y = element_text(angle = 0, vjust = 0.5))