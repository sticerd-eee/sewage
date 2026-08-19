# ==============================================================================
# Study-Period Exposure Source Comparison
# ==============================================================================
#
# Purpose: Quantify how much the study-period cross-sections move when exposure
#          is measured from matched individual EDM events instead of the EA
#          Annual Returns, and assert the structural invariants that must hold
#          between the two.
#
#          Structural invariants fail loudly. The two sources share a source
#          ledger, so they must still agree exactly on the public key set. They
#          no longer share a missingness rule: Stage 2 masks the event reading
#          on unverifiable positives, an evidence gap the Annual Returns cannot
#          see. So the EA reading's NA rows must be a subset of the event
#          reading's, and the difference must be exactly the rows whose window
#          contains a `reported_positive` year with zero matched events. That
#          divergent set is recomputed here from the crosswalk and the lookup
#          rather than read back off `has_missing_site`, so the divergence is
#          asserted against its cause instead of against itself.
#
#          Distributional differences are the point of the comparison and are
#          only reported.
#
#          Which source is canonical for the paper is an open research question;
#          this script exists so the choice can be argued from numbers.
#
# Inputs:
#   - data/processed/cross_section/sales/study_period/       (events)
#   - data/processed/cross_section/sales/study_period_ea/    (Annual Returns)
#   - data/processed/cross_section/rentals/study_period/      (events)
#   - data/processed/cross_section/rentals/study_period_ea/   (Annual Returns)
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#
# Outputs:
#   - output/log/verify_study_period_exposure_sources.log
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required. Install project dependencies with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow", "data.table", "dplyr", "here", "logger", "tibble", "tidyr"
)
LOG_FILE <- here::here(
  "output", "log", "verify_study_period_exposure_sources.log"
)

check_required_packages(REQUIRED_PACKAGES)

# The divergent set is recomputed through the builders' own seams, so the
# verifier and the engine cannot drift apart in how they read the crosswalk.
source(
  here::here("scripts", "R", "utils", "spill_aggregation_utils.R"),
  local = TRUE
)
source(here::here("scripts", "R", "utils", "site_group_utils.R"), local = TRUE)
source(
  here::here("scripts", "R", "utils", "cross_section_study_period_utils.R"),
  local = TRUE
)

EXPOSURE_COLUMNS <- c("spill_count", "spill_hrs")
SUPPORTED_RADII <- c(250L, 500L, 1000L)

# Every path and both window bounds come from the two builders being compared,
# read out of their own CONFIG. Hard-coding them here would let a window or a
# lookup move in a builder and leave the verifier silently recomputing the
# divergent set from the wrong inputs — the drift this comparison exists to
# catch.
study_period_builder_config <- function(script) {
  path <- here::here("scripts", "R", "06_analysis_datasets", script)
  if (!file.exists(path)) {
    stop("Study-period builder does not exist: ", path, call. = FALSE)
  }
  adapter <- new.env(parent = globalenv())
  sys.source(path, envir = adapter)
  adapter$CONFIG
}

study_period_source_spec <- function(events_script, annual_script, id) {
  events_config <- study_period_builder_config(events_script)
  annual_config <- study_period_builder_config(annual_script)
  for (field in c("lookup_path", "crosswalk_path", "start_date", "end_date")) {
    if (!identical(events_config[[field]], annual_config[[field]])) {
      stop(
        "The ", events_config$market, " builders disagree on ", field,
        "; the two readings are no longer of one study period.",
        call. = FALSE
      )
    }
  }
  list(
    market = events_config$market,
    id = id,
    events_path = events_config$output_path,
    annual_path = annual_config$output_path,
    crosswalk_path = events_config$crosswalk_path,
    lookup_path = events_config$lookup_path,
    window = study_period_window(
      events_config$start_date, events_config$end_date
    )
  )
}

study_period_source_specs <- function() {
  list(
    study_period_source_spec(
      "cross_section_sales.R", "cross_section_sales_ea.R", "house_id"
    ),
    study_period_source_spec(
      "cross_section_rental.R", "cross_section_rental_ea.R", "rental_id"
    )
  )
}

read_study_period_exposure <- function(path, id) {
  if (!dir.exists(path)) {
    stop("Study-period dataset does not exist: ", path, call. = FALSE)
  }
  columns <- c(id, "radius", "spatially_eligible", "has_missing_site",
               EXPOSURE_COLUMNS)
  dataset <- arrow::open_dataset(path)
  missing_columns <- setdiff(columns, dataset$schema$names)
  if (length(missing_columns) > 0L) {
    stop(
      "Study-period dataset ", path, " is missing column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  # as.data.frame() materializes arrow's chunked columns before data.table sees
  # them; data.table joins on collected-but-unmaterialized character keys drop
  # rows nondeterministically. See
  # docs/solutions/logic-errors/arrow-altrep-data-table-join-nondeterminism.md
  exposure <- dataset |>
    dplyr::select(dplyr::all_of(columns)) |>
    dplyr::collect() |>
    as.data.frame() |>
    data.table::as.data.table()
  data.table::setnames(exposure, id, "transaction_id")
  data.table::setkey(exposure, transaction_id, radius)
  exposure
}

# Invariant: both sources are built from one source ledger over one radius set,
# so their public keys must coincide exactly and each appear once.
assert_study_period_key_parity <- function(events, annual, market) {
  for (side in list(list("events", events), list("annual returns", annual))) {
    if (anyDuplicated(side[[2L]][, .(transaction_id, radius)])) {
      stop(
        "The ", market, " ", side[[1L]],
        " dataset contains a duplicate transaction-radius key.",
        call. = FALSE
      )
    }
  }
  if (nrow(events) != nrow(annual)) {
    stop(
      "The ", market, " sources disagree on row count: events=",
      nrow(events), ", annual returns=", nrow(annual), ".",
      call. = FALSE
    )
  }
  if (!identical(events$transaction_id, annual$transaction_id) ||
      !identical(events$radius, annual$radius)) {
    stop(
      "The ", market,
      " sources do not share an identical transaction-radius key set.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

# The Site Groups whose window contains a `reported_positive` year with zero
# matched events. This is the one evidence gap the event reading masks on and
# the Annual-Returns reading does not, so it is the sole licensed cause of a
# divergence between them. It is recomputed through the engine's own grid so
# the verifier reads the crosswalk exactly as the builders do.
unverifiable_positive_site_ids <- function(crosswalk_path, window) {
  if (!file.exists(crosswalk_path)) {
    stop("Annual-return crosswalk does not exist: ", crosswalk_path, call. = FALSE)
  }
  annual <- study_period_read_parquet_columns(
    crosswalk_path,
    c(
      "site_id", "year", "annual_status", "spill_count_ea", "spill_hrs_ea",
      "matched_event_count"
    ),
    "Annual-return crosswalk"
  )
  evidence <- study_period_annual_evidence_grid(annual, window)$site_evidence
  flagged <- evidence$reported_positive_without_matched_events
  if (anyNA(flagged)) {
    stop(
      "The unverifiable-positive flag is missing for at least one Site Group; ",
      "the crosswalk did not carry matched_event_count.",
      call. = FALSE
    )
  }
  sort(as.integer(evidence$site_id[flagged]))
}

# Lift those Site Groups to the public grain: a transaction-radius row is
# divergence-eligible exactly when one of them lies within the radius. The
# lookup is filtered to the flagged Site Groups before collection, so the whole
# pair table never has to be materialised.
unverifiable_positive_keys <- function(lookup_path, id, flagged, radii) {
  empty <- data.table::data.table(
    transaction_id = character(), radius = integer()
  )
  data.table::setkey(empty, transaction_id, radius)
  if (length(flagged) == 0L) return(empty)
  if (!file.exists(lookup_path)) {
    stop("Site Group lookup does not exist: ", lookup_path, call. = FALSE)
  }
  dataset <- arrow::open_dataset(lookup_path)
  columns <- c(id, "site_id", "distance_m")
  missing_columns <- setdiff(columns, dataset$schema$names)
  if (length(missing_columns) > 0L) {
    stop(
      "Site Group lookup ", lookup_path, " is missing column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  pairs <- dataset |>
    dplyr::filter(!is.na(site_id), site_id %in% flagged) |>
    dplyr::select(dplyr::all_of(columns)) |>
    dplyr::collect() |>
    as.data.frame() |>
    data.table::as.data.table()
  data.table::setnames(pairs, id, "transaction_id")
  if (nrow(pairs) == 0L) return(empty)
  if (anyNA(pairs$distance_m)) {
    stop(
      "The Site Group lookup carries a missing distance for a flagged pair.",
      call. = FALSE
    )
  }
  keys <- data.table::rbindlist(lapply(radii, function(radius) {
    within <- unique(pairs[distance_m <= radius, transaction_id])
    data.table::data.table(
      transaction_id = within, radius = rep(as.integer(radius), length(within))
    )
  }))
  data.table::setkey(keys, transaction_id, radius)
  keys
}

# Mark the divergence-eligible rows of one published dataset, in its row order.
study_period_divergence_mask <- function(exposure, unverifiable_keys) {
  mask <- rep(FALSE, nrow(exposure))
  if (nrow(unverifiable_keys) == 0L) return(mask)
  matched <- exposure[unverifiable_keys, on = .(transaction_id, radius),
                      which = TRUE, nomatch = 0L]
  mask[matched] <- TRUE
  mask
}

# Invariant: the two sources still run one spatial eligibility rule, which no
# stage of the plan touches.
assert_study_period_eligibility_parity <- function(events, annual, market) {
  if (!identical(events$spatially_eligible, annual$spatially_eligible)) {
    stop(
      "The ", market, " sources disagree on spatially_eligible; ",
      "the shared spatial rule was not applied identically.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

# Invariant: the Annual Returns remain the evidence oracle both readings share,
# and Stage 2 adds exactly one gap on top of it that only the event reading can
# see. So the EA reading's unknown rows are a subset of the event reading's,
# and the difference is exactly the divergence-eligible rows that the EA
# reading did not already call unknown. Anything else is an unexplained
# divergence, not the harmonized rule.
assert_study_period_na_harmonization <- function(events, annual, divergent,
                                                 market) {
  # The flag is published two-state on both sides, and the subset relation
  # below would read an NA as FALSE, so an NA has to fail here instead.
  for (side in list(list("events", events), list("annual returns", annual))) {
    if (anyNA(side[[2L]]$has_missing_site)) {
      stop(
        "The ", market, " ", side[[1L]],
        " dataset carries a missing has_missing_site flag.",
        call. = FALSE
      )
    }
  }
  readings <- c(
    list(has_missing_site = list(
      events = events$has_missing_site,
      annual = annual$has_missing_site
    )),
    stats::setNames(lapply(EXPOSURE_COLUMNS, function(column) list(
      events = is.na(events[[column]]), annual = is.na(annual[[column]])
    )), EXPOSURE_COLUMNS)
  )
  for (column in names(readings)) {
    events_unknown <- readings[[column]]$events
    annual_unknown <- readings[[column]]$annual
    ea_only <- sum(annual_unknown & !events_unknown)
    if (ea_only > 0L) {
      stop(
        "The ", market, " annual-returns ", column,
        " unknown rows are not a subset of the event-based ones: ",
        ea_only, " row(s) are unknown under the Annual Returns alone.",
        call. = FALSE
      )
    }
    unexplained <- sum(
      (events_unknown & !annual_unknown) != (divergent & !annual_unknown)
    )
    if (unexplained > 0L) {
      stop(
        "The ", market, " sources' ", column,
        " difference set is not the unverifiable-positive rows: ",
        unexplained, " row(s) unaccounted for.",
        call. = FALSE
      )
    }
  }
  invisible(TRUE)
}

# Correlations are undefined on an all-NA or zero-variance subset. Report NA
# rather than letting stats::cor raise and abort the whole comparison.
safe_correlation <- function(x, y, method) {
  complete <- !is.na(x) & !is.na(y)
  if (sum(complete) < 2L) return(NA_real_)
  x <- x[complete]
  y <- y[complete]
  if (length(unique(x)) < 2L || length(unique(y)) < 2L) return(NA_real_)
  suppressWarnings(stats::cor(x, y, method = method))
}

compare_study_period_exposure_column <- function(events, annual, column) {
  event_values <- events[[column]]
  annual_values <- annual[[column]]
  comparable <- !is.na(event_values) & !is.na(annual_values)
  difference <- event_values[comparable] - annual_values[comparable]
  data.table::data.table(
    measure = column,
    n_comparable = length(difference),
    events_zero_share = if (sum(!is.na(event_values)) == 0L) NA_real_ else
      base::mean(event_values[!is.na(event_values)] == 0),
    annual_zero_share = if (sum(!is.na(annual_values)) == 0L) NA_real_ else
      base::mean(annual_values[!is.na(annual_values)] == 0),
    events_mean = if (length(difference) == 0L) NA_real_ else
      base::mean(event_values[comparable]),
    annual_mean = if (length(difference) == 0L) NA_real_ else
      base::mean(annual_values[comparable]),
    pearson = safe_correlation(event_values, annual_values, "pearson"),
    spearman = safe_correlation(event_values, annual_values, "spearman"),
    diff_mean = if (length(difference) == 0L) NA_real_ else base::mean(difference),
    diff_median = if (length(difference) == 0L) NA_real_ else
      stats::median(difference),
    diff_sd = if (length(difference) < 2L) NA_real_ else stats::sd(difference),
    diff_min = if (length(difference) == 0L) NA_real_ else base::min(difference),
    diff_max = if (length(difference) == 0L) NA_real_ else base::max(difference)
  )
}

compare_study_period_sources <- function(events, annual, unverifiable_keys,
                                         market) {
  assert_study_period_key_parity(events, annual, market)
  assert_study_period_eligibility_parity(events, annual, market)
  divergent <- study_period_divergence_mask(events, unverifiable_keys)
  assert_study_period_na_harmonization(events, annual, divergent, market)

  radii <- sort(unique(events$radius))
  if (!all(radii %in% SUPPORTED_RADII)) {
    stop(
      "The ", market, " sources contain an unsupported radius.",
      call. = FALSE
    )
  }
  report <- data.table::rbindlist(lapply(radii, function(radius) {
    rows <- events$radius == radius
    radius_events <- events[rows]
    radius_annual <- annual[rows]
    radius_divergent <- divergent[rows]
    per_measure <- data.table::rbindlist(lapply(
      EXPOSURE_COLUMNS,
      function(column) compare_study_period_exposure_column(
        radius_events, radius_annual, column
      )
    ))
    per_measure[, `:=`(
      market = market,
      radius = as.integer(radius),
      n_rows = nrow(radius_events),
      n_missing_exposure = base::sum(is.na(radius_events$spill_hrs)),
      n_missing_exposure_ea = base::sum(is.na(radius_annual$spill_hrs)),
      n_divergent = base::sum(radius_divergent),
      n_ineligible = base::sum(!radius_events$spatially_eligible)
    )]
    per_measure
  }))
  data.table::setcolorder(report, c(
    "market", "radius", "measure", "n_rows", "n_ineligible",
    "n_missing_exposure", "n_missing_exposure_ea", "n_divergent", "n_comparable"
  ))
  report[]
}

log_study_period_comparison <- function(report) {
  for (index in seq_len(nrow(report))) {
    row <- report[index]
    logger::log_info(paste0(
      "{row$market} r={row$radius} {row$measure}: ",
      "rows={row$n_rows}, ineligible={row$n_ineligible}, ",
      "na_exposure events={row$n_missing_exposure} / ",
      "ea={row$n_missing_exposure_ea}, divergent={row$n_divergent}, ",
      "comparable={row$n_comparable}, ",
      "zero_share events={round(row$events_zero_share, 4)} / ",
      "ea={round(row$annual_zero_share, 4)}, ",
      "mean events={round(row$events_mean, 4)} / ",
      "ea={round(row$annual_mean, 4)}, ",
      "pearson={round(row$pearson, 4)}, spearman={round(row$spearman, 4)}, ",
      "diff mean={round(row$diff_mean, 4)}, median={round(row$diff_median, 4)}, ",
      "sd={round(row$diff_sd, 4)}, min={round(row$diff_min, 4)}, ",
      "max={round(row$diff_max, 4)}"
    ))
  }
  invisible(report)
}

verify_study_period_exposure_sources <- function(
    specs = study_period_source_specs()) {
  data.table::rbindlist(lapply(specs, function(spec) {
    logger::log_info(paste0(
      "Comparing {spec$market} study-period exposure sources: ",
      "events={spec$events_path}, annual returns={spec$annual_path}."
    ))
    events <- read_study_period_exposure(spec$events_path, spec$id)
    annual <- read_study_period_exposure(spec$annual_path, spec$id)
    flagged <- unverifiable_positive_site_ids(
      spec$crosswalk_path, spec$window
    )
    logger::log_info(paste0(
      "{spec$market} window {min(spec$window$years)}-",
      "{max(spec$window$years)} carries {length(flagged)} Site Group(s) with ",
      "an unverifiable positive."
    ))
    keys <- unverifiable_positive_keys(
      spec$lookup_path, spec$id, flagged, SUPPORTED_RADII
    )
    report <- compare_study_period_sources(events, annual, keys, spec$market)
    logger::log_info(paste0(
      "{spec$market} structural invariants hold: key parity, and the ",
      "annual-returns unknown rows are a subset of the event-based ones whose ",
      "difference is exactly the {nrow(keys)} unverifiable-positive row(s), ",
      "across {nrow(events)} transaction-radius rows."
    ))
    log_study_period_comparison(report)
    report
  }))
}

main <- function() {
  setup_logging(LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info(
    "Study-period exposure source comparison started at {Sys.time()}."
  )
  tryCatch(
    {
      report <- verify_study_period_exposure_sources()
      logger::log_info(paste0(
        "Study-period exposure source comparison completed: ",
        "{nrow(report)} market-radius-measure rows written to {LOG_FILE}."
      ))
      print(report)
      invisible(report)
    },
    error = function(error) {
      logger::log_error(
        "Study-period exposure source comparison failed: {conditionMessage(error)}"
      )
      stop(conditionMessage(error), call. = FALSE)
    }
  )
}

if (sys.nframe() == 0L) main()
