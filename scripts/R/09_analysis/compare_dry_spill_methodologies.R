# Compare Dry Spill Classification Methodologies
# 
# This script compares the original block-first approach with the new
# filter-first approach to identify differences in classifications
# and assess which methodology is more appropriate for the research.
#
# Author: Academic sewage spills research project  
# Date: 2024

# Load required libraries ----
library(dplyr)
library(ggplot2)
library(here)
library(purrr)
library(tidyr)
library(stringr)

# Configuration ----
CONFIG <- list(
  # File paths
  original_results_path = here("data/processed/merged_edm_1224_dry_spill_data.RData"),
  filter_first_results_path = here("data/processed/filter_first_dry_spill_results.RData"),
  comparison_output_dir = here("output/methodology_comparison"),
  
  # Analysis parameters
  classification_methods = c("dry_day_1", "dry_day_2", "ea_dry_spill", "bbc_dry_spill")
)

# Comparison Functions ----

#' Load and prepare results from both methodologies
#' 
#' @return List with original and filter_first results
load_methodology_results <- function() {
  cat("Loading methodology results...\n")
  
  # Load original results (block-first)
  if (!file.exists(CONFIG$original_results_path)) {
    stop("Original results file not found: ", CONFIG$original_results_path)
  }
  
  load(CONFIG$original_results_path)  # Should load 'dry_spills_defined'
  original_results <- dry_spills_defined
  
  # Load filter-first results
  if (!file.exists(CONFIG$filter_first_results_path)) {
    stop("Filter-first results file not found: ", CONFIG$filter_first_results_path)
  }
  
  load(CONFIG$filter_first_results_path)  # Should load 'filter_first_results' list
  
  cat("Loaded original results:", nrow(original_results), "observations\n")
  cat("Loaded filter-first results:", nrow(filter_first_results$final_results), "observations\n")
  
  return(list(
    original = original_results,
    filter_first = filter_first_results
  ))
}

#' Compare sample sizes and composition between methodologies
#' 
#' @param results List with original and filter_first results
#' @return data.frame with sample size comparisons
compare_sample_sizes <- function(results) {
  cat("Comparing sample sizes...\n")
  
  original <- results$original
  filter_first <- results$filter_first$final_results
  
  # Overall sample sizes
  sample_comparison <- data.frame(
    methodology = c("Original (Block-First)", "Filter-First"),
    total_observations = c(nrow(original), nrow(filter_first)),
    unique_sites = c(
      length(unique(original$outlet_discharge_ngr)),
      length(unique(filter_first$outlet_discharge_ngr))
    ),
    stringsAsFactors = FALSE
  )
  
  # Classification counts for each method
  classification_counts <- map_dfr(CONFIG$classification_methods, function(method) {
    
    # Original results
    orig_col <- method
    orig_dry <- sum(original[[orig_col]] == "yes", na.rm = TRUE)
    orig_total <- sum(!is.na(original[[orig_col]]))
    
    # Filter-first results (block-level classifications)
    ff_col <- paste0(method, "_block")
    ff_dry <- if (ff_col %in% names(filter_first)) {
      sum(filter_first[[ff_col]] == "yes", na.rm = TRUE)
    } else {
      NA_integer_
    }
    ff_total <- if (ff_col %in% names(filter_first)) {
      sum(!is.na(filter_first[[ff_col]]))
    } else {
      NA_integer_
    }
    
    data.frame(
      classification_method = method,
      original_dry_count = orig_dry,
      original_total = orig_total,
      original_dry_rate = orig_dry / orig_total,
      filter_first_dry_count = ff_dry,
      filter_first_total = ff_total,
      filter_first_dry_rate = ff_dry / ff_total,
      stringsAsFactors = FALSE
    )
  })
  
  return(list(
    sample_sizes = sample_comparison,
    classification_counts = classification_counts
  ))
}

#' Compare temporal and spatial patterns between methodologies
#' 
#' @param results List with original and filter_first results
#' @return List with temporal and spatial comparison summaries
compare_patterns <- function(results) {
  cat("Comparing temporal and spatial patterns...\n")
  
  original <- results$original
  filter_first <- results$filter_first$final_results
  
  # Temporal patterns - by year
  temporal_comparison <- bind_rows(
    original %>%
      group_by(year) %>%
      summarise(
        methodology = "Original",
        total_blocks = n(),
        dry_spills_bbc = sum(bbc_dry_spill == "yes", na.rm = TRUE),
        dry_rate_bbc = dry_spills_bbc / total_blocks,
        .groups = "drop"
      ),
    
    filter_first %>%
      group_by(year) %>%
      summarise(
        methodology = "Filter-First",
        total_blocks = n(),
        dry_spills_bbc = sum(bbc_dry_spill_block == "yes", na.rm = TRUE),
        dry_rate_bbc = dry_spills_bbc / total_blocks,
        .groups = "drop"
      )
  )
  
  # Spatial patterns - by water company
  spatial_comparison <- bind_rows(
    original %>%
      group_by(water_company) %>%
      summarise(
        methodology = "Original",
        total_blocks = n(),
        dry_spills_bbc = sum(bbc_dry_spill == "yes", na.rm = TRUE),
        dry_rate_bbc = dry_spills_bbc / total_blocks,
        .groups = "drop"
      ),
    
    filter_first %>%
      group_by(water_company) %>%
      summarise(
        methodology = "Filter-First", 
        total_blocks = n(),
        dry_spills_bbc = sum(bbc_dry_spill_block == "yes", na.rm = TRUE),
        dry_rate_bbc = dry_spills_bbc / total_blocks,
        .groups = "drop"
      )
  )
  
  return(list(
    temporal = temporal_comparison,
    spatial = spatial_comparison
  ))
}

#' Analyze filtering impact on sample composition
#' 
#' @param results List with original and filter_first results  
#' @return data.frame with filtering impact analysis
analyze_filtering_impact <- function(results) {
  cat("Analyzing filtering impact...\n")
  
  # Get the individual classification results to see filtering impact
  individual_classified <- results$filter_first$classified_individuals
  research_sample <- results$filter_first$research_sample
  
  # Filtering stages analysis
  filtering_impact <- data.frame(
    stage = c(
      "1. Raw Spills", 
      "2. Individual Classification",
      "3. Research Sample (Filtered)",
      "4. Final 12/24 Blocks"
    ),
    observations = c(
      nrow(individual_classified),  # Assuming this represents raw spills
      nrow(individual_classified),
      nrow(research_sample),
      nrow(results$filter_first$final_results)
    ),
    stringsAsFactors = FALSE
  )
  
  # Calculate retention rates
  filtering_impact <- filtering_impact %>%
    mutate(
      retention_rate = observations / first(observations),
      stage_retention = observations / lag(observations, default = first(observations))
    )
  
  # Quality flags analysis
  quality_analysis <- individual_classified %>%
    summarise(
      total_spills = n(),
      high_confidence = sum(high_confidence, na.rm = TRUE),
      clearly_dry = sum(classification_clarity == "clearly_dry", na.rm = TRUE),
      clearly_wet = sum(classification_clarity == "clearly_wet", na.rm = TRUE),
      borderline = sum(classification_clarity == "borderline", na.rm = TRUE),
      missing_data = sum(data_completeness < 0.9, na.rm = TRUE),
      .groups = "drop"
    )
  
  return(list(
    filtering_stages = filtering_impact,
    quality_flags = quality_analysis
  ))
}

#' Create visualizations comparing methodologies
#' 
#' @param comparison_results List with all comparison analyses
#' @return List of ggplot objects
create_comparison_plots <- function(comparison_results) {
  cat("Creating comparison visualizations...\n")
  
  plots <- list()
  
  # Sample size comparison
  plots$sample_sizes <- comparison_results$sample_sizes$sample_sizes %>%
    pivot_longer(cols = c(total_observations, unique_sites), 
                names_to = "metric", values_to = "count") %>%
    ggplot(aes(x = methodology, y = count, fill = methodology)) +
    geom_col() +
    facet_wrap(~metric, scales = "free_y") +
    labs(
      title = "Sample Size Comparison Between Methodologies",
      x = "Methodology", y = "Count"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  # Classification rates comparison
  plots$classification_rates <- comparison_results$sample_sizes$classification_counts %>%
    select(classification_method, original_dry_rate, filter_first_dry_rate) %>%
    pivot_longer(cols = ends_with("_dry_rate"), names_to = "methodology", values_to = "dry_rate") %>%
    mutate(methodology = str_remove(methodology, "_dry_rate"),
           methodology = str_to_title(str_replace(methodology, "_", " "))) %>%
    ggplot(aes(x = classification_method, y = dry_rate, fill = methodology)) +
    geom_col(position = "dodge") +
    labs(
      title = "Dry Spill Rates by Classification Method and Methodology",
      x = "Classification Method", y = "Dry Spill Rate",
      fill = "Methodology"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  # Temporal patterns
  plots$temporal_patterns <- comparison_results$patterns$temporal %>%
    ggplot(aes(x = factor(year), y = dry_rate_bbc, color = methodology, group = methodology)) +
    geom_line(size = 1) +
    geom_point(size = 3) +
    labs(
      title = "BBC Dry Spill Rates by Year and Methodology",
      x = "Year", y = "BBC Dry Spill Rate",
      color = "Methodology"
    ) +
    theme_minimal()
  
  # Filtering impact
  if ("filtering_impact" %in% names(comparison_results)) {
    plots$filtering_impact <- comparison_results$filtering_impact$filtering_stages %>%
      ggplot(aes(x = factor(stage, levels = stage), y = observations)) +
      geom_col(fill = "steelblue") +
      geom_text(aes(label = paste0(round(retention_rate * 100, 1), "%")), 
                vjust = -0.5, size = 3) +
      labs(
        title = "Sample Retention Through Filter-First Pipeline",
        subtitle = "Percentages show retention rate from original sample",
        x = "Processing Stage", y = "Number of Observations"
      ) +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))
  }
  
  return(plots)
}

#' Generate comprehensive methodology comparison report
#' 
#' @param comparison_results List with all comparison analyses
#' @param plots List of ggplot objects
#' @return Character vector with report text
generate_comparison_report <- function(comparison_results, plots) {
  cat("Generating comparison report...\n")
  
  sample_sizes <- comparison_results$sample_sizes$sample_sizes
  class_counts <- comparison_results$sample_sizes$classification_counts
  
  report <- c(
    "# Dry Spill Classification Methodology Comparison Report",
    "",
    paste("Generated on:", Sys.Date()),
    "",
    "## Executive Summary",
    "",
    paste("This report compares two approaches to dry spill classification:"),
    paste("- **Original (Block-First)**: Apply 12/24 counting then classify blocks"),
    paste("- **Filter-First**: Classify individual spills then apply 12/24 to filtered sample"),
    "",
    "## Sample Size Impact",
    "",
    paste("- Original methodology:", format(sample_sizes$total_observations[1], big.mark = ","), "observations"),
    paste("- Filter-first methodology:", format(sample_sizes$total_observations[2], big.mark = ","), "observations"),
    paste("- Sample retention:", round(100 * sample_sizes$total_observations[2] / sample_sizes$total_observations[1], 1), "%"),
    "",
    "## Classification Rate Differences (BBC Method)",
    "",
    paste("- Original dry spill rate:", round(100 * class_counts$original_dry_rate[class_counts$classification_method == "bbc_dry_spill"], 1), "%"),
    paste("- Filter-first dry spill rate:", round(100 * class_counts$filter_first_dry_rate[class_counts$classification_method == "bbc_dry_spill"], 1), "%"),
    "",
    "## Methodology Assessment",
    "",
    "### Original (Block-First) Approach",
    "**Advantages:**",
    "- Larger sample size",
    "- Consistent with established 12/24 counting methodology",
    "- No sample selection bias from filtering",
    "",
    "**Disadvantages:**",
    "- Classifications based on artificial block timing, not actual spill timing",
    "- May misclassify spills due to temporal misalignment",
    "- Includes low-quality/ambiguous classifications",
    "",
    "### Filter-First Approach", 
    "**Advantages:**",
    "- Classifications based on actual spill environmental conditions",
    "- Higher quality sample with clear dry/wet distinctions", 
    "- Transparent sample selection criteria",
    "- Better causal identification for policy analysis",
    "",
    "**Disadvantages:**",
    "- Smaller sample size due to filtering",
    "- Potential selection bias if filtered spills are systematically different",
    "- More complex methodology requiring multiple steps",
    "",
    "## Recommendations",
    "",
    "1. **For academic research**: Use filter-first approach for stronger causal identification",
    "2. **For descriptive analysis**: Consider both approaches for robustness checks", 
    "3. **For policy work**: Filter-first approach provides cleaner dry spill identification",
    "4. **For validation**: Compare results across both methodologies as sensitivity analysis"
  )
  
  return(report)
}

# Main Analysis Function ----

#' Run complete methodology comparison analysis
#' 
#' @return List with all comparison results and outputs
run_methodology_comparison <- function() {
  cat("Starting methodology comparison analysis...\n")
  
  # Create output directory
  if (!dir.exists(CONFIG$comparison_output_dir)) {
    dir.create(CONFIG$comparison_output_dir, recursive = TRUE)
  }
  
  # Load results
  results <- load_methodology_results()
  
  # Run comparisons
  sample_comparison <- compare_sample_sizes(results)
  pattern_comparison <- compare_patterns(results)
  filtering_analysis <- analyze_filtering_impact(results)
  
  # Combine all results
  comparison_results <- list(
    sample_sizes = sample_comparison,
    patterns = pattern_comparison,
    filtering_impact = filtering_analysis
  )
  
  # Create visualizations
  plots <- create_comparison_plots(comparison_results)
  
  # Save plots
  plot_dir <- file.path(CONFIG$comparison_output_dir, "plots")
  if (!dir.exists(plot_dir)) {
    dir.create(plot_dir)
  }
  
  iwalk(plots, function(plot, name) {
    ggsave(
      filename = file.path(plot_dir, paste0(name, ".png")),
      plot = plot,
      width = 10, height = 6, dpi = 300
    )
  })
  
  # Generate report
  report_text <- generate_comparison_report(comparison_results, plots)
  
  # Save report
  report_file <- file.path(CONFIG$comparison_output_dir, "methodology_comparison_report.md")
  writeLines(report_text, report_file)
  
  # Save detailed results
  results_file <- file.path(CONFIG$comparison_output_dir, "comparison_results.RData")
  comparison_output <- list(
    comparison_results = comparison_results,
    plots = plots,
    report = report_text
  )
  save(comparison_output, file = results_file)
  
  cat("Methodology comparison completed successfully\n")
  cat("Results saved to:", CONFIG$comparison_output_dir, "\n")
  
  return(comparison_output)
}

# Script Execution ----

if (!interactive()) {
  # Run the comparison analysis
  comparison_results <- run_methodology_comparison()
  cat("Methodology comparison analysis completed\n")
}