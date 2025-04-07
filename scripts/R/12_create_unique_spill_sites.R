############################################################
# Create Unique Spill Sites
# Project: Sewage
# Date: 03/02/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates unique spill sites from the merged EDM data
#' by cleaning location data and extracting distinct sites.
#' It's part of the data preparation pipeline for analysing sewage discharge
#' events.

# Set Up
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
    if (!requireNamespace("renv", quietly = TRUE)) {
        install.packages("renv")
        renv::init()
    }

    required_packages <- c(
        "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs", "rnrfa"
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
    log_path <- here::here("output", "log", "13_create_unique_spill_sites.log")
    dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

    logger::log_appender(logger::appender_file(log_path))
    logger::log_layout(logger::layout_glue_colors)
    logger::log_threshold(logger::DEBUG)
    logger::log_info("Script started at {Sys.time()}")
}

############################################################

# Configuration
############################################################

CONFIG <- list(
    processed_dir = here::here("data", "processed")
)
############################################################

# Functions
############################################################

#' Load individual sewage spill data
#' @param file_name Name of the data file to load
#' @return Individual spill data with location
load_data <- function(file_name = "merged_edm_data_with_summary.RData") {
    file_path <- fs::path(CONFIG$processed_dir, file_name)
    logger::log_info("Loading data: {file_path}")

    if (!file.exists(file_path)) {
        stop(glue::glue("File not found: {file_path}"))
    }

    tryCatch(
        {
            df <- rio::import(file_path, trust = TRUE)
            if (is.list(df)) df <- df[[1]]
            logger::log_info("Loaded data")
            return(df)
        },
        error = function(e) {
            error_msg <- glue::glue("Failed to load data: {e$message}")
            logger::log_error(error_msg)
            stop(error_msg)
        }
    )
}


#' Clean location data by filtering invalid entries and standardizing NGR format
#' @param data Data frame from load_data output
#' @return Data frame with cleaned location data and converted eastings/northings
clean_location <- function(data) {
    logger::log_info("Cleaning location data and converting NGR coordinates")

    cleaned_data <- data %>%
        distinct(water_company, year, site_id, outlet_discharge_ngr) %>%
        filter(!is.na(outlet_discharge_ngr)) %>%
        mutate(available = TRUE) %>%
        pivot_wider(
            id_cols = c(water_company, site_id, outlet_discharge_ngr),
            names_from = year,
            values_from = available,
            names_prefix = "available_year_",
            values_fill = FALSE
        )

    # Convert NGR to easting/northing
    coordinates <- osg_parse(
        cleaned_data$outlet_discharge_ngr,
        coord_system = "BNG"
    )

    cleaned_data %>%
        mutate(
            northing = coordinates$northing / 10,
            easting = coordinates$easting
        )
}

# Main execution
############################################################

main <- function() {
    tryCatch(
        {
            # Setup
            initialise_environment()
            setup_logging()

            # Load and process data
            logger::log_info("Loading processed merged data")
            spill_data <- load_data()

            logger::log_info("Creating unique spill sites")
            unique_sites <- spill_data %>%
                clean_location() %>%
                unique() %>%
                mutate(site_id = row_number()) %>%
                relocate(site_id, everything())

            # Export results
            logger::log_info("Exporting unique spill sites")
            saveRDS(unique_sites,
                file = file.path(CONFIG$processed_dir, "unique_spill_sites.rds")
            )

            logger::log_info("Processing completed successfully")
        },
        error = function(e) {
            logger::log_error("Fatal error: {e$message}")
            stop(e)
        }
    )
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
    main()
}
