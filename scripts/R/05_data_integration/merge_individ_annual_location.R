# ==============================================================================
# Individual and Annual EDM Location Merge
# ==============================================================================
#
# Purpose: Link event-level EDM spill records to annual-return works identities,
#          locations, and annual EA totals through a works-register crosswalk.
#
# Author: Jacopo Olivieri
# Date: 2025-05-18
# Date Modified: 2026-07-05
#
# Inputs:
#   - data/processed/combined_edm_data.parquet
#   - data/processed/annual_return_edm.parquet
#   - data/processed/annual_return_lookup.parquet
#
# Outputs:
#   - data/processed/matched_events_annual_data/site_works_crosswalk.parquet
#   - data/processed/matched_events_annual_data/matched_events_annual_data.parquet
#   - data/processed/matched_events_annual_data/events_unmatched.parquet
#   - data/processed/matched_events_annual_data/annual_unmatched.parquet
#   - data/processed/matched_events_annual_data/near_miss_report.parquet
#   - output/log/05_merge_individ_annual_location.log
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
  "conflicted",
  "data.table",
  "dplyr",
  "glue",
  "here",
  "logger",
  "lubridate",
  "purrr",
  "rnrfa",
  "stringr",
  "tibble",
  "tidyr",
  "vctrs"
)

LOG_FILE <- here::here("output", "log", "05_merge_individ_annual_location.log")

check_required_packages(REQUIRED_PACKAGES)

# Setup Functions
# ==============================================================================

#' Attach packages used unqualified by this orchestrator and its sourced utils
#' @return NULL
initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
    library(pkg, character.only = TRUE)
  }))

  conflicted::conflict_prefer("filter", "dplyr")
  conflicted::conflict_prefer("first", "dplyr")
  conflicted::conflict_prefer("select", "dplyr")
}

#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "INFO")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
# ==============================================================================

CONFIG <- list(
  years = 2021:2024,
  data_path_individual = here::here(
    "data", "processed", "combined_edm_data.parquet"
  ),
  data_path_annual = here::here(
    "data", "processed", "annual_return_edm.parquet"
  ),
  data_path_lookup = here::here(
    "data", "processed", "annual_return_lookup.parquet"
  ),
  manual_overrides_path = here::here(
    "data", "processed", "matched_events_annual_data_manual_overrides.csv"
  ),
  output_dir = here::here("data", "processed", "matched_events_annual_data"),
  works_merge_ngr_m = 250,
  works_near_miss_m = 1000,
  agreement_rel_error = 0.25,
  agreement_runner_up_ratio = 2,
  agreement_abs_floor_hrs = 1,
  near_miss_name_max_edit_distance = 2,
  near_miss_name_min_chars = 6
)

INPUT_CONTRACT <- list(
  events = c(
    "water_company", "year", "site_name_ea", "site_name_wa_sc",
    "start_time", "end_time", "permit_reference_ea",
    "permit_reference_wa_sc", "activity_reference", "unique_id"
  ),
  annual = c(
    "water_company", "year", "site_name_ea", "site_name_wa_sc",
    "permit_reference_ea", "permit_reference_wa_sc", "activity_reference",
    "outlet_discharge_ngr", "spill_hrs_ea", "spill_count_ea"
  ),
  lookup = c("site_id", paste0("site_id_", CONFIG$years))
)

# Sourced Utilities
# ==============================================================================

source(here::here("scripts", "R", "utils", "ngr_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "merge_works_register_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "merge_matching_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "merge_outputs_utils.R"), local = TRUE)

# Preflight and Loading
# ==============================================================================

parquet_names <- function(path) {
  names(arrow::read_parquet(path, as_data_frame = FALSE))
}

assert_parquet_contract <- function(path, required_columns, label) {
  if (!file.exists(path)) {
    stop(glue::glue("{label} input not found: {path}"), call. = FALSE)
  }

  missing_columns <- setdiff(required_columns, parquet_names(path))
  if (length(missing_columns) > 0) {
    stop(
      glue::glue(
        "{label} input is missing required columns: ",
        "{paste(missing_columns, collapse = ', ')}"
      ),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

preflight_inputs <- function(config = CONFIG) {
  assert_parquet_contract(
    config$data_path_individual,
    INPUT_CONTRACT$events,
    "Event"
  )
  assert_parquet_contract(
    config$data_path_annual,
    INPUT_CONTRACT$annual,
    "Annual-return"
  )
  assert_parquet_contract(
    config$data_path_lookup,
    INPUT_CONTRACT$lookup,
    "Annual-return lookup"
  )
  invisible(TRUE)
}

read_manual_overrides <- function(path = CONFIG$manual_overrides_path) {
  empty <- tibble::tibble(
    water_company = character(),
    year = integer(),
    site_name_ea = character(),
    site_name_wa_sc = character(),
    permit_reference_ea = character(),
    permit_reference_wa_sc = character(),
    activity_reference = character(),
    unique_id = character(),
    site_id = integer()
  )

  if (!file.exists(path)) {
    return(empty)
  }

  overrides <- utils::read.csv(path, stringsAsFactors = FALSE)
  missing_columns <- setdiff(names(empty), names(overrides))
  if (length(missing_columns) > 0) {
    stop(
      "Manual-overrides file missing required columns: ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  overrides %>%
    dplyr::mutate(year = as.integer(.data$year), site_id = as.integer(.data$site_id)) %>%
    dplyr::select(dplyr::all_of(names(empty)))
}

# Year filter guard: rows with NA year are a hard stop (silent loss of rows
# whose year we cannot even classify), while rows with real out-of-range years
# are logged and dropped (config-scoped runs are legitimate).
filter_years_with_guard <- function(data, years, label) {
  n_na_year <- sum(is.na(data$year))
  if (n_na_year > 0) {
    stop(
      glue::glue(
        "{label} input has {n_na_year} row(s) with NA year; ",
        "refusing to drop them silently."
      ),
      call. = FALSE
    )
  }

  filtered <- data %>% dplyr::filter(.data$year %in% years)
  n_dropped <- nrow(data) - nrow(filtered)
  if (n_dropped > 0) {
    dropped_years <- sort(unique(data$year[!data$year %in% years]))
    logger::log_warn(
      "{label} input: dropped {n_dropped} row(s) with out-of-range year(s) {paste(dropped_years, collapse = ', ')} (config years: {paste(years, collapse = ', ')})"
    )
  }

  filtered
}

load_merge_inputs <- function(config = CONFIG) {
  logger::log_info("Reading event input: {config$data_path_individual}")
  events <- arrow::read_parquet(config$data_path_individual) %>%
    filter_years_with_guard(config$years, "Event")

  # Anglian 2024 rows carry their unique_id only in the typo-named
  # `new_unqiue_id` column (empty everywhere else in the feed); without it the
  # unique_id rung is silently skipped and 167k rows degrade to name-only
  # matching or no_usable_key.
  if ("new_unqiue_id" %in% names(events)) {
    recovered <- sum(is.na(events$unique_id) & !is.na(events$new_unqiue_id))
    events <- events %>%
      dplyr::mutate(
        unique_id = dplyr::coalesce(.data$unique_id, .data$new_unqiue_id)
      )
    logger::log_info(
      "Recovered {recovered} unique_id values from new_unqiue_id column"
    )
  }

  logger::log_info("Reading annual-return input: {config$data_path_annual}")
  annual_returns <- arrow::read_parquet(config$data_path_annual) %>%
    filter_years_with_guard(config$years, "Annual-return")

  logger::log_info("Reading lookup input: {config$data_path_lookup}")
  lookup <- arrow::read_parquet(config$data_path_lookup)

  logger::log_info(
    "Loaded inputs: events={nrow(events)}, annual_rows={nrow(annual_returns)}, lookup_rows={nrow(lookup)}"
  )

  list(events = events, annual_returns = annual_returns, lookup = lookup)
}

# Pipeline
# ==============================================================================

log_decision_counts <- function(decisions) {
  matched <- decisions %>% dplyr::filter(!is.na(.data$site_id))
  unmatched <- decisions %>% dplyr::filter(is.na(.data$site_id))

  logger::log_info("Tuple decisions: total={nrow(decisions)}, matched={nrow(matched)}, unmatched={nrow(unmatched)}")
  if (nrow(matched) > 0) {
    method_counts <- matched %>% dplyr::count(.data$match_method, name = "n")
    logger::log_info("Matched tuple counts by method: {paste(method_counts$match_method, method_counts$n, sep='=', collapse=', ')}")
  }
  if (nrow(unmatched) > 0) {
    reason_counts <- unmatched %>% dplyr::count(.data$reason, name = "n")
    logger::log_info("Unmatched tuple counts by reason: {paste(reason_counts$reason, reason_counts$n, sep='=', collapse=', ')}")
  }
}

log_output_counts <- function(outputs) {
  logger::log_info(
    "Output rows: crosswalk={nrow(outputs$site_works_crosswalk)}, matched_events={nrow(outputs$matched_events)}, events_unmatched={nrow(outputs$events_unmatched)}, annual_unmatched={nrow(outputs$annual_unmatched)}, near_miss_report={nrow(outputs$near_miss_report)}"
  )
}

build_merge_outputs_from_data <- function(events, annual_returns, lookup,
                                          manual_overrides = tibble::tibble(),
                                          config = CONFIG) {
  logger::log_info("Building works register")
  register <- build_works_register(
    annual_returns,
    lookup,
    config = list(
      works_merge_ngr_m = config$works_merge_ngr_m,
      works_near_miss_m = config$works_near_miss_m
    ),
    years = config$years
  )
  logger::log_info(
    "Works register: members={nrow(register$membership)}, edges={nrow(register$edges)}, near_misses={nrow(register$near_misses)}"
  )
  if (nrow(register$edge_summary) > 0) {
    logger::log_info("Register edges by justification: {paste(register$edge_summary$justification, register$edge_summary$n_edges, sep='=', collapse=', ')}")
  }

  logger::log_info("Resolving event tuples")
  decisions <- resolve_merge_matches(
    events,
    register$resolver,
    manual_overrides = manual_overrides,
    config = list(
      agreement_rel_error = config$agreement_rel_error,
      agreement_runner_up_ratio = config$agreement_runner_up_ratio,
      agreement_abs_floor_hrs = config$agreement_abs_floor_hrs
    )
  )
  # Grab the attribute immediately: any dplyr verb applied to `decisions`
  # downstream returns a copy without it.
  agreement_near_misses <- attr(decisions, "agreement_near_misses")
  if (is.null(agreement_near_misses)) {
    agreement_near_misses <- tibble::tibble()
  }
  log_decision_counts(decisions)
  logger::log_info(
    "Agreement near-miss evidence rows: {nrow(agreement_near_misses)}"
  )

  logger::log_info("Assembling merge outputs")
  outputs <- assemble_merge_outputs(
    events = events,
    membership = register$membership,
    resolver = register$resolver,
    decisions = decisions,
    register_near_misses = register$near_misses,
    agreement_near_misses = agreement_near_misses,
    years = config$years,
    name_near_miss_max_edit_distance = config$near_miss_name_max_edit_distance,
    name_near_miss_min_chars = config$near_miss_name_min_chars
  )
  validate_publishable_merge_outputs(outputs)
  log_output_counts(outputs)

  list(register = register, decisions = decisions, outputs = outputs)
}

run_merge_pipeline <- function(config = CONFIG, publish = TRUE) {
  preflight_inputs(config)
  inputs <- load_merge_inputs(config)
  manual_overrides <- read_manual_overrides(config$manual_overrides_path)
  if (nrow(manual_overrides) > 0) {
    logger::log_info("Loaded manual overrides: {nrow(manual_overrides)}")
  } else {
    logger::log_info("No manual overrides file found or file is empty")
  }

  result <- build_merge_outputs_from_data(
    events = inputs$events,
    annual_returns = inputs$annual_returns,
    lookup = inputs$lookup,
    manual_overrides = manual_overrides,
    config = config
  )

  if (isTRUE(publish)) {
    logger::log_info("Publishing outputs to {config$output_dir}")
    publish_merge_outputs(result$outputs, config$output_dir)
    logger::log_info("Publish completed successfully")
  }

  invisible(result)
}

# Main Execution
# ==============================================================================

#' Main execution function
#' @param config Configuration list
#' @return Invisibly returns the pipeline result
main <- function(config = CONFIG) {
  tryCatch({
    initialise_environment()
    initialise_logging()
    logger::log_info("===== merge_individ_annual_location rebuild started =====")
    result <- run_merge_pipeline(config = config, publish = TRUE)
    logger::log_info("===== merge_individ_annual_location rebuild completed successfully =====")
    invisible(result)
  }, error = function(e) {
    logger::log_error("Fatal error: {conditionMessage(e)}")
    stop(conditionMessage(e), call. = FALSE)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

if (sys.nframe() == 0) {
  main()
}
