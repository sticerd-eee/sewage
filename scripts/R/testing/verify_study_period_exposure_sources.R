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
#          ledger and a missingness rule, so they must agree exactly on the
#          public key set and on which rows carry NA exposure. Distributional
#          differences are the point of the comparison and are only reported.
#
#          Which source is canonical for the paper is an open research question;
#          this script exists so the choice can be argued from numbers.
#
# Inputs:
#   - data/processed/cross_section/sales/study_period/       (events)
#   - data/processed/cross_section/sales/study_period_ea/    (Annual Returns)
#   - data/processed/cross_section/rentals/study_period/      (events)
#   - data/processed/cross_section/rentals/study_period_ea/   (Annual Returns)
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

REQUIRED_PACKAGES <- c("arrow", "data.table", "dplyr", "here", "logger")
LOG_FILE <- here::here(
  "output", "log", "verify_study_period_exposure_sources.log"
)

check_required_packages(REQUIRED_PACKAGES)

EXPOSURE_COLUMNS <- c("spill_count", "spill_hrs")
SUPPORTED_RADII <- c(250L, 500L, 1000L)

study_period_source_specs <- function() {
  cross_section <- here::here("data", "processed", "cross_section")
  list(
    list(
      market = "sale",
      id = "house_id",
      events_path = file.path(cross_section, "sales", "study_period"),
      annual_path = file.path(cross_section, "sales", "study_period_ea")
    ),
    list(
      market = "rental",
      id = "rental_id",
      events_path = file.path(cross_section, "rentals", "study_period"),
      annual_path = file.path(cross_section, "rentals", "study_period_ea")
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
  exposure <- dataset |>
    dplyr::select(dplyr::all_of(columns)) |>
    dplyr::collect() |>
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

# Invariant: the Annual Returns are the evidence oracle for both sources, so a
# row is NA under one exactly when it is NA under the other.
assert_study_period_na_parity <- function(events, annual, market) {
  for (column in c("spatially_eligible", "has_missing_site")) {
    if (!identical(events[[column]], annual[[column]])) {
      stop(
        "The ", market, " sources disagree on ", column,
        "; the shared missingness rule was not applied identically.",
        call. = FALSE
      )
    }
  }
  for (column in EXPOSURE_COLUMNS) {
    discrepancies <- sum(is.na(events[[column]]) != is.na(annual[[column]]))
    if (discrepancies > 0L) {
      stop(
        "The ", market, " sources disagree on the ", column,
        " missingness pattern in ", discrepancies, " row(s).",
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

compare_study_period_sources <- function(events, annual, market) {
  assert_study_period_key_parity(events, annual, market)
  assert_study_period_na_parity(events, annual, market)

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
      n_ineligible = base::sum(!radius_events$spatially_eligible)
    )]
    per_measure
  }))
  data.table::setcolorder(report, c(
    "market", "radius", "measure", "n_rows", "n_ineligible",
    "n_missing_exposure", "n_comparable"
  ))
  report[]
}

log_study_period_comparison <- function(report) {
  for (index in seq_len(nrow(report))) {
    row <- report[index]
    logger::log_info(paste0(
      "{row$market} r={row$radius} {row$measure}: ",
      "rows={row$n_rows}, ineligible={row$n_ineligible}, ",
      "na_exposure={row$n_missing_exposure}, comparable={row$n_comparable}, ",
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
    report <- compare_study_period_sources(events, annual, spec$market)
    logger::log_info(paste0(
      "{spec$market} structural invariants hold: key parity and NA-pattern ",
      "equality across {nrow(events)} transaction-radius rows."
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
