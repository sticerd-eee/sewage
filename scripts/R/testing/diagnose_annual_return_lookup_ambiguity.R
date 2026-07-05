# ==============================================================================
# Annual Return Lookup Ambiguity Diagnostic
# ==============================================================================
#
# Re-derives the ambiguity figures used by the merge_individ_annual_location
# rebuild plan. Run with plain Rscript, for example:
#
#   Rscript scripts/R/testing/diagnose_annual_return_lookup_ambiguity.R \
#     --lookup data/processed/annual_return_lookup.parquet \
#     --label regenerated \
#     --check-origin
#
# The lookup is used as a coverage gate: every annual-return row must resolve
# through the supplied lookup before the name-grain diagnostic is computed.
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this diagnostic. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "glue",
  "here",
  "purrr",
  "stringr",
  "tibble",
  "tidyr"
)

check_required_packages(REQUIRED_PACKAGES)

suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

CONFIG <- list(
  years = 2021:2024,
  annual_path = here::here("data", "processed", "annual_return_edm.parquet"),
  events_path = here::here("data", "processed", "combined_edm_data.parquet"),
  lookup_path = here::here("data", "processed", "annual_return_lookup.parquet"),
  output_dir = here::here(
    "scripts", "R", "testing", "output",
    "annual_return_lookup_ambiguity_diagnostic"
  )
)

parse_args <- function(args = commandArgs(trailingOnly = TRUE)) {
  out <- list(
    annual_path = CONFIG$annual_path,
    events_path = CONFIG$events_path,
    lookup_path = CONFIG$lookup_path,
    output_dir = CONFIG$output_dir,
    label = format(Sys.Date(), "%Y%m%d"),
    check_origin = FALSE
  )

  if ("--help" %in% args || "-h" %in% args) {
    cat(
      paste(
        "Usage: Rscript scripts/R/testing/diagnose_annual_return_lookup_ambiguity.R",
        "  [--lookup PATH] [--annual PATH] [--events PATH]",
        "  [--output-dir DIR] [--label LABEL] [--check-origin]",
        sep = "\n"
      ),
      "\n"
    )
    quit(status = 0)
  }

  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "--check-origin")) {
      out$check_origin <- TRUE
      i <- i + 1L
      next
    }

    if (i == length(args)) {
      stop("Missing value for argument ", key, call. = FALSE)
    }

    value <- args[[i + 1L]]
    if (identical(key, "--lookup")) {
      out$lookup_path <- value
    } else if (identical(key, "--annual")) {
      out$annual_path <- value
    } else if (identical(key, "--events")) {
      out$events_path <- value
    } else if (identical(key, "--output-dir")) {
      out$output_dir <- value
    } else if (identical(key, "--label")) {
      out$label <- value
    } else {
      stop("Unknown argument: ", key, call. = FALSE)
    }
    i <- i + 2L
  }

  out
}

stop_if_missing <- function(paths) {
  missing_paths <- paths[!file.exists(paths)]
  if (length(missing_paths) > 0) {
    stop(
      "Required input file(s) not found: ",
      paste(missing_paths, collapse = ", "),
      call. = FALSE
    )
  }
}

add_annual_site_id <- function(annual) {
  annual %>%
    mutate(
      annual_site_id = case_when(
        year == 2021L ~ as.numeric(.data$site_id_2021),
        year == 2022L ~ as.numeric(.data$site_id_2022),
        year == 2023L ~ as.numeric(.data$site_id_2023),
        year == 2024L ~ as.numeric(.data$site_id_2024),
        TRUE ~ NA_real_
      )
    )
}

lookup_to_long <- function(lookup) {
  lookup %>%
    pivot_longer(
      cols = starts_with("site_id_"),
      names_to = "year_col",
      values_to = "annual_site_id"
    ) %>%
    mutate(
      year = as.integer(str_remove(.data$year_col, "^site_id_")),
      annual_site_id = as.numeric(.data$annual_site_id),
      canonical_site_id = as.numeric(.data$site_id)
    ) %>%
    filter(!is.na(.data$annual_site_id)) %>%
    select("canonical_site_id", "year", "annual_site_id")
}

attach_lookup_or_stop <- function(annual, lookup_long) {
  duplicate_links <- lookup_long %>%
    count(.data$year, .data$annual_site_id, name = "n") %>%
    filter(.data$n > 1L)

  if (nrow(duplicate_links) > 0) {
    stop(
      "Lookup has duplicate annual site links by year/site_id: ",
      nrow(duplicate_links),
      call. = FALSE
    )
  }

  linked <- annual %>%
    left_join(lookup_long, by = c("year", "annual_site_id"))

  missing_links <- linked %>%
    filter(is.na(.data$canonical_site_id))

  if (nrow(missing_links) > 0) {
    by_year <- missing_links %>%
      count(.data$year, name = "missing_rows") %>%
      arrange(.data$year)
    stop(
      "Lookup does not cover all annual-return rows: ",
      paste(
        paste0(by_year$year, "=", by_year$missing_rows),
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  linked
}

top_two_ratio <- function(x) {
  vals <- sort(as.numeric(x), decreasing = TRUE, na.last = NA)
  if (length(vals) < 2L || is.na(vals[[2L]]) || vals[[2L]] <= 0) {
    return(NA_real_)
  }
  vals[[1L]] / vals[[2L]]
}

build_ambiguity_groups <- function(annual_linked) {
  annual_linked %>%
    filter(!is.na(.data$site_name_ea), .data$site_name_ea != "") %>%
    group_by(.data$water_company, .data$year, .data$site_name_ea) %>%
    summarise(
      n_annual_rows = n(),
      n_canonical_site_ids = n_distinct(.data$canonical_site_id),
      n_unique_ngr = n_distinct(.data$outlet_discharge_ngr, na.rm = TRUE),
      n_unique_permit = n_distinct(.data$permit_reference_ea, na.rm = FALSE),
      n_unique_activity = n_distinct(.data$activity_reference, na.rm = TRUE),
      top_two_spill_hrs_ratio = top_two_ratio(.data$spill_hrs_ea),
      max_spill_hrs_ea = suppressWarnings(max(.data$spill_hrs_ea, na.rm = TRUE)),
      second_spill_hrs_ea = {
        vals <- sort(as.numeric(.data$spill_hrs_ea), decreasing = TRUE, na.last = NA)
        if (length(vals) < 2L) NA_real_ else vals[[2L]]
      },
      .groups = "drop"
    ) %>%
    filter(.data$n_annual_rows > 1L) %>%
    mutate(
      identical_ngr = .data$n_unique_ngr == 1L,
      identical_permit_reference_ea = .data$n_unique_permit == 1L,
      activity_reference_differs = .data$n_unique_activity > 1L,
      top_two_ratio_at_least_3x = .data$top_two_spill_hrs_ratio >= 3
    ) %>%
    arrange(.data$year, .data$water_company, .data$site_name_ea)
}

count_event_rows_in_ambiguous_names <- function(events_path, groups) {
  events <- arrow::read_parquet(
    events_path,
    col_select = c("water_company", "year", "site_name_ea")
  )

  total_event_rows <- nrow(events)
  named_event_rows <- events %>%
    filter(!is.na(.data$site_name_ea), .data$site_name_ea != "") %>%
    nrow()
  ambiguous_event_rows <- events %>%
    semi_join(
      groups %>% select("water_company", "year", "site_name_ea"),
      by = c("water_company", "year", "site_name_ea")
    ) %>%
    nrow()

  tibble(
    total_event_rows = total_event_rows,
    named_event_rows = named_event_rows,
    ambiguous_event_rows = ambiguous_event_rows,
    ambiguous_share_of_all_events = ambiguous_event_rows / total_event_rows,
    ambiguous_share_of_named_events = ambiguous_event_rows / named_event_rows
  )
}

summarise_groups <- function(groups, event_summary, lookup_long, annual_linked, label) {
  ratio_known <- !is.na(groups$top_two_spill_hrs_ratio)

  tibble(
    label = label,
    lookup_links = nrow(lookup_long),
    annual_rows = nrow(annual_linked),
    lookup_canonical_site_ids = n_distinct(annual_linked$canonical_site_id),
    ambiguous_groups = nrow(groups),
    ambiguous_annual_rows = sum(groups$n_annual_rows),
    identical_ngr_groups = sum(groups$identical_ngr),
    identical_ngr_share = mean(groups$identical_ngr),
    identical_permit_reference_ea_groups =
      sum(groups$identical_permit_reference_ea),
    identical_permit_reference_ea_share =
      mean(groups$identical_permit_reference_ea),
    activity_reference_differs_groups =
      sum(groups$activity_reference_differs),
    activity_reference_differs_share =
      mean(groups$activity_reference_differs),
    top_two_ratio_denominator_groups = sum(ratio_known),
    top_two_ratio_at_least_3x_groups =
      sum(groups$top_two_ratio_at_least_3x, na.rm = TRUE),
    top_two_ratio_at_least_3x_share =
      mean(groups$top_two_ratio_at_least_3x, na.rm = TRUE),
    top_two_ratio_median =
      median(groups$top_two_spill_hrs_ratio, na.rm = TRUE)
  ) %>%
    bind_cols(event_summary)
}

check_origin_calibration <- function(summary) {
  checks <- c(
    "ambiguous groups" =
      summary$ambiguous_groups == 3116L,
    "ambiguous annual rows" =
      summary$ambiguous_annual_rows == 7872L,
    "identical NGR share" =
      round(summary$identical_ngr_share * 100, 1) == 64.6,
    "identical permit share" =
      round(summary$identical_permit_reference_ea_share * 100, 1) == 71.7,
    "top-two ratio >=3x share" =
      round(summary$top_two_ratio_at_least_3x_share * 100, 1) == 61.6
  )

  if (!all(checks)) {
    failed <- names(checks)[!checks]
    stop(
      "Origin calibration failed for: ",
      paste(failed, collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

write_outputs <- function(summary, groups, output_dir, label) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  safe_label <- str_replace_all(label, "[^A-Za-z0-9_.-]+", "_")
  summary_path <- file.path(output_dir, paste0(safe_label, "_summary.csv"))
  groups_path <- file.path(output_dir, paste0(safe_label, "_groups.csv"))
  markdown_path <- file.path(output_dir, paste0(safe_label, "_summary.md"))

  utils::write.csv(summary, summary_path, row.names = FALSE, na = "")
  utils::write.csv(groups, groups_path, row.names = FALSE, na = "")

  lines <- c(
    paste0("# Annual Return Lookup Ambiguity Diagnostic: ", label),
    "",
    paste0("- Lookup links: ", summary$lookup_links),
    paste0("- Annual rows: ", summary$annual_rows),
    paste0("- Canonical site IDs: ", summary$lookup_canonical_site_ids),
    paste0("- Ambiguous groups: ", summary$ambiguous_groups),
    paste0("- Ambiguous annual rows: ", summary$ambiguous_annual_rows),
    paste0(
      "- Identical NGR groups: ",
      round(summary$identical_ngr_share * 100, 1),
      "%"
    ),
    paste0(
      "- Identical permit_reference_ea groups: ",
      round(summary$identical_permit_reference_ea_share * 100, 1),
      "%"
    ),
    paste0(
      "- Activity_reference differs groups: ",
      round(summary$activity_reference_differs_share * 100, 1),
      "%"
    ),
    paste0(
      "- Top-two spill_hrs_ea ratio >=3x: ",
      round(summary$top_two_ratio_at_least_3x_share * 100, 1),
      "% of groups with a positive runner-up"
    ),
    paste0(
      "- Median top-two spill_hrs_ea ratio: ",
      round(summary$top_two_ratio_median, 2)
    ),
    paste0(
      "- Ambiguous event rows: ",
      summary$ambiguous_event_rows,
      " (",
      round(summary$ambiguous_share_of_all_events * 100, 1),
      "% of all events; ",
      round(summary$ambiguous_share_of_named_events * 100, 1),
      "% of named events)"
    ),
    "",
    paste0("- Summary CSV: ", summary_path),
    paste0("- Group CSV: ", groups_path)
  )
  writeLines(lines, markdown_path)

  list(
    summary_path = summary_path,
    groups_path = groups_path,
    markdown_path = markdown_path
  )
}

main <- function() {
  args <- parse_args()
  stop_if_missing(c(args$annual_path, args$events_path, args$lookup_path))

  annual <- arrow::read_parquet(args$annual_path) %>%
    filter(.data$year %in% CONFIG$years) %>%
    add_annual_site_id()
  lookup_long <- arrow::read_parquet(args$lookup_path) %>%
    lookup_to_long()
  annual_linked <- attach_lookup_or_stop(annual, lookup_long)

  groups <- build_ambiguity_groups(annual_linked)
  event_summary <- count_event_rows_in_ambiguous_names(args$events_path, groups)
  summary <- summarise_groups(
    groups = groups,
    event_summary = event_summary,
    lookup_long = lookup_long,
    annual_linked = annual_linked,
    label = args$label
  )

  if (isTRUE(args$check_origin)) {
    check_origin_calibration(summary)
  }

  paths <- write_outputs(summary, groups, args$output_dir, args$label)

  print(summary)
  cat("Wrote diagnostic outputs:\n")
  cat(" - ", paths$summary_path, "\n", sep = "")
  cat(" - ", paths$groups_path, "\n", sep = "")
  cat(" - ", paths$markdown_path, "\n", sep = "")

  invisible(summary)
}

if (sys.nframe() == 0) {
  main()
}
