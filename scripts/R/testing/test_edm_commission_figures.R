# ==============================================================================
# Canonical EDM Commission Figure Contract Tests
# ==============================================================================

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(
      paste0(
        message,
        "\nExpected: ", paste(capture.output(str(expected)), collapse = " "),
        "\nActual: ", paste(capture.output(str(actual)), collapse = " ")
      ),
      call. = FALSE
    )
  }
}

assert_error <- function(expression, pattern, message) {
  error <- tryCatch(
    {
      force(expression)
      NULL
    },
    error = identity
  )
  if (is.null(error) || !grepl(pattern, conditionMessage(error), perl = TRUE)) {
    stop(
      paste0(
        message,
        if (is.null(error)) "\nNo error was raised." else
          paste0("\nActual error: ", conditionMessage(error))
      ),
      call. = FALSE
    )
  }
}

suppressPackageStartupMessages({
  library(dplyr)
  library(here)
  library(tibble)
})

source(here::here("scripts", "R", "utils", "edm_commission_figure_utils.R"))

statuses <- c(
  rep("resolved", 3L),
  "missing",
  "future_only",
  "not_commissioned",
  "not_feasible",
  "actual_state_conflict",
  "conflicting_actual_dates",
  "before_2016",
  "invalid_placeholder",
  "unparseable"
)

fixture <- tibble(
  site_id_canonical = seq_along(statuses),
  # Deliberately repeated: Site Group identity must not define figure counts.
  site_id = c(10L, 10L, 11L, seq.int(20L, 28L)),
  edm_commission_date = as.Date(c(
    "2019-07-14", "2020-05-01", "2020-01-01", rep(NA_character_, 9L)
  )),
  edm_commission_date_precision = c(
    "day", "month", "year",
    "unknown", "unknown", "unknown", "unknown",
    "conflict", "conflict", "vague", "unknown", "unknown"
  ),
  edm_commission_resolution_status = statuses
)

result <- prepare_edm_commission_figure_data(fixture)

assert_identical(
  result$diagnostics$n_canonical_sites,
  12L,
  "The full-universe denominator must count unique Canonical Spill Sites."
)
assert_identical(
  result$diagnostics$n_resolved,
  3L,
  "The timing denominator must include only resolved canonical histories."
)
assert_identical(
  result$annual_timing$commission_year,
  c(2019L, 2020L),
  "Day, month, and year precision must all map to annual timing."
)
assert_identical(
  result$annual_timing$n_canonical_sites,
  c(1L, 2L),
  "Annual timing must count canonical identities, not Site Groups."
)
assert_true(
  abs(sum(result$annual_timing$conditional_percentage) - 100) < 1e-10,
  "Resolved annual timing percentages must sum to 100%."
)
assert_true(
  all(diff(result$annual_cumulative$cumulative_percentage) >= 0),
  "The annual cumulative series must be monotone."
)
assert_true(
  abs(tail(result$annual_cumulative$cumulative_percentage, 1L) - 100) < 1e-10,
  "The annual cumulative series must end at 100% of resolved histories."
)
assert_identical(
  nrow(result$annual_cumulative),
  2L,
  "The cumulative series must contain observed commission years only."
)
assert_true(
  !any(result$annual_cumulative$commission_year == 2023L),
  "The old synthetic terminal 2023 row must not be retained."
)

assert_identical(
  result$completeness$edm_commission_resolution_status,
  EDM_COMMISSION_RESOLUTION_STATUSES,
  "Completeness must represent every resolution status separately."
)
assert_identical(
  result$completeness$n_canonical_sites,
  c(3L, rep(1L, 9L)),
  "Completeness status counts must reconcile to the full universe."
)
assert_true(
  abs(sum(result$completeness$share_of_canonical_universe) - 100) < 1e-10,
  "Completeness shares must use and exhaust the full canonical universe."
)

pre_2016 <- result$timing_categories |>
  filter(.data$timing_category == "Pre-2016 (imprecise)")
assert_identical(
  pre_2016$n_canonical_sites,
  1L,
  "Pre-2016 evidence must appear as a distinct timing category."
)
assert_true(
  is.na(pre_2016$commission_year),
  "Pre-2016 evidence must not acquire a synthetic plotted year or date."
)

note <- format_edm_commission_figure_note(result)
assert_true(
  grepl("Canonical Spill Site", note, fixed = TRUE) &&
    grepl("conditional on 3 resolved", note, fixed = TRUE),
  "Figure notes must name the canonical unit and conditional denominator."
)
for (label in result$completeness$status_label[-1L]) {
  assert_true(
    grepl(label, note, fixed = TRUE),
    paste("Figure notes must disclose the status category:", label)
  )
}

timeline_env <- new.env(parent = globalenv())
source(
  here::here(
    "scripts", "R", "09_analysis", "01_descriptive",
    "edm_commission_timeline.R"
  ),
  local = timeline_env
)
cumulative_env <- new.env(parent = globalenv())
source(
  here::here(
    "scripts", "R", "09_analysis", "01_descriptive",
    "edm_commission_cumulative.R"
  ),
  local = cumulative_env
)
timeline_plot <- timeline_env$build_edm_commission_timeline_plot(result)
cumulative_plot <- cumulative_env$build_edm_commission_cumulative_plot(result)
assert_true(
  grepl(
    "Canonical Spill Sites with commissioned EDM coverage",
    timeline_plot$labels$subtitle,
    fixed = TRUE
  ),
  "The timeline label must state the canonical estimand."
)
assert_true(
  grepl(
    "Canonical Spill Sites with commissioned EDM coverage",
    cumulative_plot$labels$subtitle,
    fixed = TRUE
  ),
  "The cumulative label must state the canonical estimand."
)
assert_identical(
  cumulative_plot$data$commission_year,
  c(2019L, 2020L),
  "The cumulative plot must use annual timing rather than exact dates."
)

duplicate_canonical <- bind_rows(fixture, fixture[1L, ])
assert_error(
  prepare_edm_commission_figure_data(duplicate_canonical),
  "site_id_canonical must be non-missing and unique",
  "Duplicate canonical rows must fail closed."
)

invalid_status <- fixture
invalid_status$edm_commission_resolution_status[1L] <- "invented"
assert_error(
  prepare_edm_commission_figure_data(invalid_status),
  "Unknown commission resolution status",
  "Unknown resolution statuses must fail closed."
)

invalid_combination <- fixture
invalid_combination$edm_commission_date_precision[1L] <- "unknown"
assert_error(
  prepare_edm_commission_figure_data(invalid_combination),
  "Invalid commission resolution combination",
  "Invalid status, precision, and date combinations must fail closed."
)

no_resolved <- fixture |>
  filter(.data$edm_commission_resolution_status != "resolved")
assert_error(
  prepare_edm_commission_figure_data(no_resolved),
  "no resolved commission histories",
  "A conditional timing figure without a denominator must fail closed."
)

cat("All canonical EDM commission figure contract tests passed.\n")
