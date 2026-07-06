############################################################
# CH10 site-keyed consumer old-vs-new diff report
# Project: Sewage
# Date: 06/07/2026
############################################################

required_packages <- c(
  "arrow", "dplyr", "fs", "glue", "here", "readr", "rlang",
  "stringr", "tibble", "tidyr"
)

invisible(lapply(required_packages, function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}))

output_root <- here::here("output", "merge_rebuild_downstream_migration_2026-07-06")
baseline_dir <- file.path(output_root, "baseline_old_pipeline", "ch10_site_keyed")
new_dir <- here::here("data", "processed")
evidence_dir <- file.path(output_root, "ch10")
fs::dir_create(evidence_dir)

artifact_specs <- tibble::tribble(
  ~artifact, ~old_rel, ~new_rel, ~spatial_site_output,
  "spill_site_grid_lookup", "rainfall/spill_site_grid_lookup.parquet", "rainfall/spill_site_grid_lookup.parquet", TRUE,
  "dry_spills", "rainfall/dry_spills.parquet", "rainfall/dry_spills.parquet", FALSE,
  "agg_spill_daily", "agg_spill_stats/agg_spill_daily.parquet", "agg_spill_stats/agg_spill_daily.parquet", FALSE,
  "agg_spill_dry_yr", "agg_spill_stats/agg_spill_dry_yr.parquet", "agg_spill_stats/agg_spill_dry_yr.parquet", FALSE,
  "agg_spill_dry_mo", "agg_spill_stats/agg_spill_dry_mo.parquet", "agg_spill_stats/agg_spill_dry_mo.parquet", FALSE,
  "agg_spill_dry_qtr", "agg_spill_stats/agg_spill_dry_qtr.parquet", "agg_spill_stats/agg_spill_dry_qtr.parquet", FALSE,
  "agg_spill_stats_mo", "agg_spill_stats/agg_spill_stats_mo.parquet", "agg_spill_stats/agg_spill_stats_mo.parquet", FALSE,
  "agg_spill_stats_qtr", "agg_spill_stats/agg_spill_stats_qtr.parquet", "agg_spill_stats/agg_spill_stats_qtr.parquet", FALSE,
  "spill_blocks_rainfall_yr", "rainfall/spill_blocks_rainfall_yr.parquet", "rainfall/spill_blocks_rainfall_yr.parquet", TRUE,
  "spill_blocks_rainfall_mo", "rainfall/spill_blocks_rainfall_mo.parquet", "rainfall/spill_blocks_rainfall_mo.parquet", TRUE,
  "spill_blocks_rainfall_qt", "rainfall/spill_blocks_rainfall_qt.parquet", "rainfall/spill_blocks_rainfall_qt.parquet", TRUE,
  "spill_house_lookup", "spill_house_lookup.parquet", "spill_house_lookup.parquet", TRUE,
  "spill_rental_lookup", "zoopla/spill_rental_lookup.parquet", "zoopla/spill_rental_lookup.parquet", TRUE,
  "general_panel_sales", "general_panel/sales", "general_panel/sales", TRUE,
  "general_panel_rentals", "general_panel/rentals", "general_panel/rentals", TRUE,
  "prior_to_sale", "cross_section/sales/prior_to_sale", "cross_section/sales/prior_to_sale", FALSE,
  "prior_to_sale_house_site", "cross_section/sales/prior_to_sale_house_site", "cross_section/sales/prior_to_sale_house_site", TRUE,
  "prior_to_rental", "cross_section/rentals/prior_to_rental", "cross_section/rentals/prior_to_rental", FALSE,
  "prior_to_rental_rental_site", "cross_section/rentals/prior_to_rental_rental_site", "cross_section/rentals/prior_to_rental_rental_site", TRUE
)

consumer_logs <- tibble::tribble(
  ~script, ~log_rel,
  "clean_rainfall_data.R", "clean_rainfall_data.log",
  "identify_dry_spills.R", "identify_dry_spills.log",
  "aggregate_daily_spill_rainfall.R", "aggregate_daily_spill_rainfall.log",
  "aggregate_dry_spill_stats.R", "aggregate_dry_spill_stats.log",
  "compute_spill_stats.R", "compute_spill_stats.log",
  "aggregate_rainfall_stats.R", "aggregate_rainfall_stats.log",
  "cross_section_prior_to_sale.R", "cross_section_prior_to_sale.log",
  "house_spill_prior_to_sale.R", "house_site_prior_to_sale.log",
  "cross_section_prior_to_rental.R", "cross_section_prior_to_rental.log",
  "rental_spill_prior_to_rental.R", "rental_spill_prior_to_rental.log"
)

stale_intermediate_consumers <- tibble::tribble(
  ~artifact, ~consumer, ~role,
  "spill_site_grid_lookup", "scripts/R/02_data_cleaning/clean_rainfall_data.R", "producer regenerated for works-grain NGR set",
  "spill_site_grid_lookup", "scripts/R/03_data_enrichment/identify_dry_spills.R", "event-level rainfall lookup for dry-spill classification",
  "spill_site_grid_lookup", "scripts/R/03_data_enrichment/aggregate_daily_spill_rainfall.R", "site-day rainfall coverage join",
  "spill_site_grid_lookup", "scripts/R/03_data_enrichment/aggregate_rainfall_stats.R", "continuous site-period rainfall aggregation",
  "spill_site_grid_lookup", "scripts/R/09_analysis/07_dry_spills/dry_spill_method_figure.R", "method figure displays selected site grid cells",
  "agg_spill_stats_mo", "scripts/R/04_feature_engineering/compute_spill_stats.R", "producer; DuckDB dat_mo cache invalidated with refresh_db = TRUE",
  "agg_spill_stats_mo", "scripts/R/09_analysis/00_data_load/load_data_sewage.R", "analysis loader monthly spill treatment metrics",
  "agg_spill_stats_mo", "scripts/R/09_analysis/00_data_load/load_data_sewage.py", "Python analysis loader monthly spill treatment metrics",
  "agg_spill_stats_qtr", "scripts/R/04_feature_engineering/compute_spill_stats.R", "producer; DuckDB dat_qtr cache invalidated with refresh_db = TRUE",
  "agg_spill_stats_qtr", "scripts/R/09_analysis/00_data_load/load_data_sewage.R", "analysis loader quarterly spill treatment metrics",
  "agg_spill_stats_qtr", "scripts/R/09_analysis/00_data_load/load_data_sewage.py", "Python analysis loader quarterly spill treatment metrics",
  "spill_blocks_rainfall_yr", "scripts/R/03_data_enrichment/aggregate_rainfall_stats.R", "producer refreshed from works-grain lookup",
  "spill_blocks_rainfall_yr", "scripts/R/09_analysis/07_dry_spills/dry_spill_method_figure.R", "dry-spill method figure block rainfall source",
  "spill_blocks_rainfall_mo", "scripts/R/03_data_enrichment/aggregate_rainfall_stats.R", "producer refreshed from works-grain lookup",
  "spill_blocks_rainfall_qt", "scripts/R/03_data_enrichment/aggregate_rainfall_stats.R", "producer refreshed from works-grain lookup",
  "rainfall_agg_mo", "scripts/R/03_data_enrichment/aggregate_rainfall_stats.R", "producer refreshed in same run as spill_blocks_rainfall_*",
  "rainfall_agg_mo", "scripts/R/09_analysis/01_descriptive/dry_spill_rainfall_monthly_per_site_combined.R", "monthly rainfall descriptive figure"
)

path_exists <- function(path) {
  file.exists(path) || dir.exists(path)
}

old_path <- function(rel_path) {
  file.path(baseline_dir, rel_path)
}

new_path <- function(rel_path) {
  file.path(new_dir, rel_path)
}

open_output <- function(path) {
  if (!path_exists(path)) {
    stop(glue::glue("Missing output: {path}"), call. = FALSE)
  }
  arrow::open_dataset(path, format = "parquet")
}

field_type <- function(ds, col) {
  ds$schema$GetFieldByName(col)$type$ToString()
}

schema_df <- function(ds, artifact, source) {
  tibble::tibble(
    artifact = artifact,
    source = source,
    column = names(ds),
    type = vapply(names(ds), function(col) field_type(ds, col), character(1))
  )
}

scalar_count <- function(ds, expr) {
  ds %>%
    dplyr::summarise(value = {{ expr }}) %>%
    dplyr::collect() %>%
    dplyr::pull(.data$value)
}

distinct_count <- function(ds, col) {
  if (!col %in% names(ds)) {
    return(NA_real_)
  }
  col_sym <- rlang::sym(col)
  scalar_count(ds, dplyr::n_distinct(!!col_sym))
}

min_value <- function(ds, col) {
  if (!col %in% names(ds)) {
    return(NA)
  }
  col_sym <- rlang::sym(col)
  scalar_count(ds, suppressWarnings(min(!!col_sym, na.rm = TRUE)))
}

max_value <- function(ds, col) {
  if (!col %in% names(ds)) {
    return(NA)
  }
  col_sym <- rlang::sym(col)
  scalar_count(ds, suppressWarnings(max(!!col_sym, na.rm = TRUE)))
}

distinct_values <- function(ds, col) {
  if (!col %in% names(ds)) {
    return(NA_character_)
  }
  col_sym <- rlang::sym(col)
  values <- ds %>%
    dplyr::distinct(!!col_sym) %>%
    dplyr::collect() %>%
    dplyr::pull(!!col_sym)
  paste(sort(unique(as.character(values))), collapse = ";")
}

summarise_artifact <- function(path, artifact, source) {
  ds <- open_output(path)
  date_col <- intersect(c("date", "start_time", "end_time"), names(ds))[1]
  period_col <- intersect(c("year", "month_id", "qtr_id", "qtr_id_transfer"), names(ds))[1]

  tibble::tibble(
    artifact = artifact,
    source = source,
    path = path,
    rows = scalar_count(ds, dplyr::n()),
    columns = length(names(ds)),
    site_ids = distinct_count(ds, "site_id"),
    house_ids = distinct_count(ds, "house_id"),
    rental_ids = distinct_count(ds, "rental_id"),
    date_or_time_col = dplyr::coalesce(date_col, NA_character_),
    date_or_time_min = as.character(if (!is.na(date_col)) min_value(ds, date_col) else NA),
    date_or_time_max = as.character(if (!is.na(date_col)) max_value(ds, date_col) else NA),
    period_col = dplyr::coalesce(period_col, NA_character_),
    period_min = as.character(if (!is.na(period_col)) min_value(ds, period_col) else NA),
    period_max = as.character(if (!is.na(period_col)) max_value(ds, period_col) else NA),
    radius_values = distinct_values(ds, "radius"),
    distance_min = as.numeric(if ("distance_m" %in% names(ds)) min_value(ds, "distance_m") else NA),
    distance_max = as.numeric(if ("distance_m" %in% names(ds)) max_value(ds, "distance_m") else NA)
  )
}

type_is_numeric <- function(type) {
  stringr::str_detect(type, "int|uint|float|double|decimal")
}

type_is_logical <- function(type) {
  stringr::str_detect(type, "^bool")
}

metric_columns <- function(ds) {
  schema <- schema_df(ds, artifact = "x", source = "x")
  metric_pattern <- paste(
    c("spill", "rainfall", "distance", "missing", "within_radius", "n_spill", "count", "hrs", "hours", "days"),
    collapse = "|"
  )
  schema %>%
    dplyr::filter(
      stringr::str_detect(.data$column, metric_pattern),
      type_is_numeric(.data$type) | type_is_logical(.data$type)
    ) %>%
    dplyr::pull(.data$column)
}

summarise_metric <- function(ds, artifact, source, col) {
  col_sym <- rlang::sym(col)
  type <- field_type(ds, col)

  if (type_is_logical(type)) {
    out <- ds %>%
      dplyr::summarise(
        rows = dplyr::n(),
        non_missing = sum(!is.na(!!col_sym)),
        na_rows = sum(is.na(!!col_sym)),
        true_rows = sum(!!col_sym, na.rm = TRUE),
        false_rows = sum(!is.na(!!col_sym) & !(!!col_sym)),
        total = NA_real_,
        mean = NA_real_,
        min = NA_real_,
        max = NA_real_
      ) %>%
      dplyr::collect()
  } else {
    out <- ds %>%
      dplyr::summarise(
        rows = dplyr::n(),
        non_missing = sum(!is.na(!!col_sym)),
        na_rows = sum(is.na(!!col_sym)),
        true_rows = NA_real_,
        false_rows = NA_real_,
        total = sum(!!col_sym, na.rm = TRUE),
        mean = mean(!!col_sym, na.rm = TRUE),
        min = suppressWarnings(min(!!col_sym, na.rm = TRUE)),
        max = suppressWarnings(max(!!col_sym, na.rm = TRUE))
      ) %>%
      dplyr::collect()
  }

  out %>%
    dplyr::mutate(
      artifact = artifact,
      source = source,
      column = col,
      type = type,
      .before = 1
    ) %>%
    dplyr::mutate(
      dplyr::across(
        c(
          rows, non_missing, na_rows, true_rows,
          false_rows, total, mean, min, max
        ),
        as.numeric
      )
    )
}

summarise_metrics <- function(path, artifact, source) {
  ds <- open_output(path)
  cols <- metric_columns(ds)
  if (length(cols) == 0) {
    return(tibble::tibble())
  }
  dplyr::bind_rows(lapply(cols, function(col) {
    summarise_metric(ds, artifact, source, col)
  }))
}

grouped_distinct_count <- function(ds, group_col, value_col, out_col) {
  if (!all(c(group_col, value_col) %in% names(ds))) {
    return(tibble::tibble())
  }
  group_sym <- rlang::sym(group_col)
  value_sym <- rlang::sym(value_col)
  ds %>%
    dplyr::group_by(!!group_sym) %>%
    dplyr::summarise("{out_col}" := dplyr::n_distinct(!!value_sym), .groups = "drop") %>%
    dplyr::collect()
}

grouped_true_count <- function(ds, group_col, value_col, out_col) {
  if (!all(c(group_col, value_col) %in% names(ds))) {
    return(tibble::tibble())
  }
  group_sym <- rlang::sym(group_col)
  value_sym <- rlang::sym(value_col)
  ds %>%
    dplyr::group_by(!!group_sym) %>%
    dplyr::summarise("{out_col}" := sum(!!value_sym, na.rm = TRUE), .groups = "drop") %>%
    dplyr::collect()
}

summarise_radius <- function(path, artifact, source) {
  ds <- open_output(path)
  if (!"radius" %in% names(ds)) {
    return(tibble::tibble())
  }
  radius_sym <- rlang::sym("radius")
  base <- ds %>%
    dplyr::group_by(!!radius_sym) %>%
    dplyr::summarise(rows = dplyr::n(), .groups = "drop") %>%
    dplyr::collect()

  for (id_col in c("site_id", "house_id", "rental_id")) {
    id_counts <- grouped_distinct_count(ds, "radius", id_col, paste0(id_col, "s"))
    if (nrow(id_counts) > 0) {
      base <- dplyr::left_join(base, id_counts, by = "radius")
    }
  }

  for (flag_col in c("site_missing", "has_missing_site", "within_radius")) {
    flag_counts <- grouped_true_count(ds, "radius", flag_col, paste0(flag_col, "_true_rows"))
    if (nrow(flag_counts) > 0) {
      base <- dplyr::left_join(base, flag_counts, by = "radius")
    }
  }

  base %>%
    dplyr::mutate(artifact = artifact, source = source, .before = 1)
}

safe_read_parquet <- function(path, col_select = NULL) {
  if (is.null(col_select)) {
    return(arrow::read_parquet(path))
  }
  arrow::read_parquet(path, col_select = tidyselect::any_of(col_select))
}

collect_distinct_values <- function(path, col) {
  ds <- open_output(path)
  if (!col %in% names(ds)) {
    return(character())
  }
  col_sym <- rlang::sym(col)
  ds %>%
    dplyr::distinct(!!col_sym) %>%
    dplyr::collect() %>%
    dplyr::pull(!!col_sym) %>%
    as.character() %>%
    unique()
}

summarise_membership_churn <- function(artifact, old_path, new_path, col) {
  old_values <- collect_distinct_values(old_path, col)
  new_values <- collect_distinct_values(new_path, col)

  tibble::tibble(
    artifact = artifact,
    key = col,
    old_count = length(old_values),
    new_count = length(new_values),
    retained = length(intersect(old_values, new_values)),
    old_only = length(setdiff(old_values, new_values)),
    new_only = length(setdiff(new_values, old_values)),
    gross_membership_churn = length(setdiff(old_values, new_values)) +
      length(setdiff(new_values, old_values))
  )
}

collect_site_ngr_keys <- function(path) {
  ds <- open_output(path)
  if (!all(c("site_id", "ngr") %in% names(ds))) {
    return(character())
  }
  ds %>%
    dplyr::distinct(.data$site_id, .data$ngr) %>%
    dplyr::collect() %>%
    dplyr::mutate(key = paste(.data$site_id, .data$ngr, sep = "||")) %>%
    dplyr::pull(.data$key) %>%
    unique()
}

summarise_site_ngr_churn <- function(artifact, old_path, new_path) {
  old_values <- collect_site_ngr_keys(old_path)
  new_values <- collect_site_ngr_keys(new_path)

  tibble::tibble(
    artifact = artifact,
    key = "site_id+ngr",
    old_count = length(old_values),
    new_count = length(new_values),
    retained = length(intersect(old_values, new_values)),
    old_only = length(setdiff(old_values, new_values)),
    new_only = length(setdiff(new_values, old_values)),
    gross_membership_churn = length(setdiff(old_values, new_values)) +
      length(setdiff(new_values, old_values))
  )
}

summarise_period_grid <- function(artifact, source, path, period_col) {
  ds <- open_output(path)
  if (!period_col %in% names(ds) || !"site_id" %in% names(ds)) {
    return(tibble::tibble())
  }
  period_sym <- rlang::sym(period_col)
  out <- ds %>%
    dplyr::summarise(
      rows = dplyr::n(),
      site_ids = dplyr::n_distinct(.data$site_id),
      periods = dplyr::n_distinct(!!period_sym),
      period_min = min(!!period_sym, na.rm = TRUE),
      period_max = max(!!period_sym, na.rm = TRUE)
    ) %>%
    dplyr::collect()
  out %>%
    dplyr::mutate(
      artifact = artifact,
      source = source,
      period_col = period_col,
      complete_grid_rows = .data$site_ids * .data$periods,
      complete_grid = .data$rows == .data$complete_grid_rows,
      .before = 1
    )
}

summarise_lookup <- function(path, source) {
  lookup <- arrow::read_parquet(path)
  tibble::tibble(
    source = source,
    rows = nrow(lookup),
    site_ids = if ("site_id" %in% names(lookup)) dplyr::n_distinct(lookup$site_id) else NA_integer_,
    ngrs = if ("ngr" %in% names(lookup)) dplyr::n_distinct(lookup$ngr) else NA_integer_,
    site_ngr_pairs = if (all(c("site_id", "ngr") %in% names(lookup))) {
      nrow(dplyr::distinct(lookup, .data$site_id, .data$ngr))
    } else {
      NA_integer_
    },
    center_rows = if ("is_center" %in% names(lookup)) sum(lookup$is_center, na.rm = TRUE) else NA_integer_,
    grid_cells = if (all(c("x_idx", "y_idx") %in% names(lookup))) {
      nrow(dplyr::distinct(lookup, .data$x_idx, .data$y_idx))
    } else {
      NA_integer_
    }
  )
}

summarise_event_lookup_coverage <- function() {
  events_path <- here::here(
    "data", "processed", "matched_events_annual_data",
    "matched_events_annual_data.parquet"
  )
  events <- arrow::read_parquet(events_path, col_select = c("site_id", "ngr", "year"))
  unique_sites <- arrow::read_parquet(
    here::here("data", "processed", "unique_spill_sites.parquet"),
    col_select = c("site_id", "ngr", "easting", "northing")
  )
  eligible_sites <- unique_sites %>%
    dplyr::filter(!is.na(.data$easting), !is.na(.data$northing)) %>%
    dplyr::distinct(.data$site_id, .data$ngr)
  old_lookup <- arrow::read_parquet(
    old_path("rainfall/spill_site_grid_lookup.parquet"),
    col_select = c("site_id", "ngr")
  ) %>%
    dplyr::distinct(.data$site_id, .data$ngr)
  new_lookup <- arrow::read_parquet(
    new_path("rainfall/spill_site_grid_lookup.parquet"),
    col_select = c("site_id", "ngr")
  ) %>%
    dplyr::distinct(.data$site_id, .data$ngr)

  summarise_one_denominator <- function(event_df, denominator) {
    dplyr::bind_rows(
      tibble::tibble(
        denominator = denominator,
        lookup_source = "old_baseline",
        match_key = "ngr",
        total_events = nrow(event_df),
        mapped_events = sum(event_df$ngr %in% old_lookup$ngr, na.rm = TRUE)
      ),
      tibble::tibble(
        denominator = denominator,
        lookup_source = "new_ch10",
        match_key = "ngr",
        total_events = nrow(event_df),
        mapped_events = sum(event_df$ngr %in% new_lookup$ngr, na.rm = TRUE)
      ),
      tibble::tibble(
        denominator = denominator,
        lookup_source = "old_baseline",
        match_key = "site_id+ngr",
        total_events = nrow(event_df),
        mapped_events = nrow(dplyr::semi_join(event_df, old_lookup, by = c("site_id", "ngr")))
      ),
      tibble::tibble(
        denominator = denominator,
        lookup_source = "new_ch10",
        match_key = "site_id+ngr",
        total_events = nrow(event_df),
        mapped_events = nrow(dplyr::semi_join(event_df, new_lookup, by = c("site_id", "ngr")))
      )
    ) %>%
      dplyr::mutate(
        unmapped_events = .data$total_events - .data$mapped_events,
        unmapped_share = .data$unmapped_events / .data$total_events
      )
  }

  in_scope_events <- dplyr::semi_join(events, eligible_sites, by = c("site_id", "ngr"))
  dropped_site_events <- dplyr::anti_join(events, eligible_sites, by = c("site_id", "ngr"))

  list(
    coverage = dplyr::bind_rows(
      summarise_one_denominator(events, "all_matched_events"),
      summarise_one_denominator(in_scope_events, "coordinate_valid_unique_spill_sites")
    ),
    inventory = tibble::tibble(
      total_events = nrow(events),
      coordinate_valid_unique_site_events = nrow(in_scope_events),
      out_of_coordinate_valid_unique_site_events = nrow(dropped_site_events),
      out_of_coordinate_valid_unique_site_share = nrow(dropped_site_events) / nrow(events)
    )
  )
}

read_dropped_guard <- function(guard, path) {
  if (!file.exists(path)) {
    return(tibble::tibble(
      guard = guard,
      path = path,
      site_id = integer(),
      ngr = character(),
      missing_easting = logical(),
      missing_northing = logical()
    ))
  }
  readr::read_csv(path, show_col_types = FALSE) %>%
    dplyr::mutate(
      guard = guard,
      path = path,
      .before = 1
    )
}

attribute_removed_column <- function(artifact, column) {
  dplyr::case_when(
    artifact == "dry_spills" & stringr::str_detect(column, "_cat_") ~
      "event-level rainfall category helper removed; canonical dry_spills keeps continuous rainfall indicators only",
    artifact == "dry_spills" & stringr::str_detect(column, "rainfall_1cell_d0123") ~
      "event-level single-cell d0123 rainfall indicator removed from canonical export",
    stringr::str_detect(column, "^rain_cat_") ~
      "wide rainfall-category count/hour aggregate removed; canonical dry aggregates keep dry-spill count/hour metrics",
    stringr::str_detect(column, "^dry_spill_.*r0_d0123") ~
      "r0 d0123 dry metric removed from aggregate schema",
    column %in% c("spill_count_ea", "spill_hrs_ea") ~
      "legacy EA spill metric replaced by crosswalk-labelled spill_count_ea_crosswalk/spill_hrs_ea_crosswalk",
    column %in% c(
      "activity_reference", "asset_type", "bathing_water", "data_source",
      "edm_commission_date", "edm_operation_percent", "group_id_annual",
      "group_id_event", "key_length", "match_key", "match_method",
      "match_quality", "match_type", "ngr", "ngr_og", "no_full_years_edm_data",
      "permit_reference_ea", "permit_reference_wa_sc", "receiving_water_name",
      "shellfish_water", "site_name_ea", "site_name_wa_sc", "tie",
      "wfd_waterbody_id_cycle_2"
    ) ~
      "site/event metadata removed from aggregate grid; metadata remains in upstream site and event tables",
    TRUE ~ "removed by CH9 dry-spill aggregate schema consolidation"
  )
}

required_paths <- dplyr::bind_rows(
  artifact_specs %>%
    dplyr::transmute(
      artifact,
      source = "old_baseline",
      path = old_path(.data$old_rel)
    ),
  artifact_specs %>%
    dplyr::transmute(
      artifact,
      source = "new_ch10",
      path = new_path(.data$new_rel)
    )
) %>%
  dplyr::mutate(exists = vapply(.data$path, path_exists, logical(1)))

readr::write_csv(required_paths, file.path(evidence_dir, "ch10_required_paths.csv"))

if (any(!required_paths$exists)) {
  missing_paths <- required_paths %>% dplyr::filter(!.data$exists)
  stop(
    glue::glue("Missing required CH10 artifact(s): {paste(missing_paths$path, collapse = ', ')}"),
    call. = FALSE
  )
}

schemas <- dplyr::bind_rows(lapply(seq_len(nrow(required_paths)), function(i) {
  row <- required_paths[i, ]
  message("Reading schema: ", row$artifact, " / ", row$source)
  schema_df(open_output(row$path), row$artifact, row$source)
}))

column_diff <- schemas %>%
  tidyr::pivot_wider(
    names_from = "source",
    values_from = "type"
  ) %>%
  dplyr::mutate(
    in_old = !is.na(.data$old_baseline),
    in_new = !is.na(.data$new_ch10),
    status = dplyr::case_when(
      .data$in_old & .data$in_new & .data$old_baseline == .data$new_ch10 ~ "unchanged",
      .data$in_old & .data$in_new & .data$old_baseline != .data$new_ch10 ~ "type_changed",
      .data$in_old & !.data$in_new ~ "removed",
      !.data$in_old & .data$in_new ~ "added"
    )
  ) %>%
  dplyr::arrange(.data$artifact, .data$column)

artifact_summary <- dplyr::bind_rows(lapply(seq_len(nrow(required_paths)), function(i) {
  row <- required_paths[i, ]
  message("Summarising artifact: ", row$artifact, " / ", row$source)
  summarise_artifact(row$path, row$artifact, row$source)
}))

artifact_summary_diff <- artifact_summary %>%
  dplyr::select(
    .data$artifact, .data$source, .data$rows, .data$columns, .data$site_ids,
    .data$house_ids, .data$rental_ids, .data$radius_values,
    .data$date_or_time_min, .data$date_or_time_max,
    .data$period_min, .data$period_max,
    .data$distance_min, .data$distance_max
  ) %>%
  tidyr::pivot_wider(
    names_from = "source",
    values_from = -c(artifact, source),
    names_glue = "{.value}_{source}"
  ) %>%
  dplyr::mutate(
    row_delta = .data$rows_new_ch10 - .data$rows_old_baseline,
    column_delta = .data$columns_new_ch10 - .data$columns_old_baseline,
    site_id_delta = .data$site_ids_new_ch10 - .data$site_ids_old_baseline,
    house_id_delta = .data$house_ids_new_ch10 - .data$house_ids_old_baseline,
    rental_id_delta = .data$rental_ids_new_ch10 - .data$rental_ids_old_baseline
  ) %>%
  dplyr::arrange(.data$artifact)

metric_artifacts <- c(
  "dry_spills", "agg_spill_daily", "agg_spill_dry_yr", "agg_spill_dry_mo",
  "agg_spill_dry_qtr", "agg_spill_stats_mo", "agg_spill_stats_qtr",
  "spill_blocks_rainfall_yr", "spill_blocks_rainfall_mo",
  "spill_blocks_rainfall_qt"
)

metric_paths <- required_paths %>%
  dplyr::filter(.data$artifact %in% metric_artifacts)

metric_summary <- dplyr::bind_rows(lapply(seq_len(nrow(metric_paths)), function(i) {
  row <- metric_paths[i, ]
  message("Summarising metrics: ", row$artifact, " / ", row$source)
  summarise_metrics(row$path, row$artifact, row$source)
}))

radius_summary <- dplyr::bind_rows(lapply(seq_len(nrow(required_paths)), function(i) {
  row <- required_paths[i, ]
  message("Summarising radius: ", row$artifact, " / ", row$source)
  summarise_radius(row$path, row$artifact, row$source)
}))

new_unique_sites_path <- here::here("data", "processed", "unique_spill_sites.parquet")
old_unique_sites_path <- file.path(output_root, "baseline_old_pipeline", "unique_spill_sites.parquet")
new_unique_sites <- arrow::read_parquet(new_unique_sites_path)
old_unique_sites <- arrow::read_parquet(old_unique_sites_path)

coordinate_summary <- dplyr::bind_rows(
  old_unique_sites %>%
    dplyr::summarise(
      source = "old_baseline",
      unique_sites = dplyr::n(),
      missing_easting = sum(is.na(.data$easting)),
      missing_northing = sum(is.na(.data$northing)),
      missing_either_coordinate = sum(is.na(.data$easting) | is.na(.data$northing)),
      missing_ngr = sum(is.na(.data$ngr))
    ),
  new_unique_sites %>%
    dplyr::summarise(
      source = "new_ch10",
      unique_sites = dplyr::n(),
      missing_easting = sum(is.na(.data$easting)),
      missing_northing = sum(is.na(.data$northing)),
      missing_either_coordinate = sum(is.na(.data$easting) | is.na(.data$northing)),
      missing_ngr = sum(is.na(.data$ngr))
    )
)

new_unique_site_ids <- new_unique_sites %>%
  dplyr::pull(.data$site_id) %>%
  as.integer()

missing_coord_site_ids <- new_unique_sites %>%
  dplyr::filter(is.na(.data$easting) | is.na(.data$northing)) %>%
  dplyr::pull(.data$site_id) %>%
  as.integer()

site_key_contracts <- dplyr::bind_rows(lapply(seq_len(nrow(artifact_specs)), function(i) {
  spec <- artifact_specs[i, ]
  path <- new_path(spec$new_rel)
  ds <- open_output(path)
  if (!"site_id" %in% names(ds)) {
    return(tibble::tibble())
  }
  site_ids <- ds %>%
    dplyr::distinct(.data$site_id) %>%
    dplyr::collect() %>%
    dplyr::pull(.data$site_id) %>%
    as.integer()
  non_missing_site_ids <- unique(site_ids[!is.na(site_ids)])
  site_id_na_rows <- ds %>%
    dplyr::summarise(site_id_na_rows = sum(is.na(.data$site_id))) %>%
    dplyr::collect() %>%
    dplyr::pull(.data$site_id_na_rows) %>%
    as.numeric()
  tibble::tibble(
    artifact = spec$artifact,
    distinct_site_ids = length(non_missing_site_ids),
    site_id_na_rows = site_id_na_rows,
    unknown_nonmissing_site_ids = length(setdiff(non_missing_site_ids, new_unique_site_ids)),
    missing_coord_site_ids_in_output = length(intersect(non_missing_site_ids, missing_coord_site_ids)),
    spatial_site_output = spec$spatial_site_output
  ) %>%
    dplyr::mutate(
      contract_ok = .data$unknown_nonmissing_site_ids == 0L &
        (!.data$spatial_site_output | .data$missing_coord_site_ids_in_output == 0L)
    )
}))

membership_churn <- dplyr::bind_rows(lapply(seq_len(nrow(artifact_specs)), function(i) {
  spec <- artifact_specs[i, ]
  path <- new_path(spec$new_rel)
  ds <- open_output(path)
  churn_tables <- list()
  if ("site_id" %in% names(ds)) {
    churn_tables <- c(
      churn_tables,
      list(summarise_membership_churn(spec$artifact, old_path(spec$old_rel), path, "site_id"))
    )
  }
  if ("ngr" %in% names(ds)) {
    churn_tables <- c(
      churn_tables,
      list(summarise_membership_churn(spec$artifact, old_path(spec$old_rel), path, "ngr"))
    )
  }
  if (all(c("site_id", "ngr") %in% names(ds))) {
    churn_tables <- c(
      churn_tables,
      list(summarise_site_ngr_churn(spec$artifact, old_path(spec$old_rel), path))
    )
  }
  dplyr::bind_rows(churn_tables)
}))

period_grid_specs <- tibble::tribble(
  ~artifact, ~period_col,
  "agg_spill_dry_yr", "year",
  "agg_spill_dry_mo", "month_id",
  "agg_spill_dry_qtr", "qtr_id",
  "agg_spill_stats_mo", "month_id",
  "agg_spill_stats_qtr", "qtr_id",
  "spill_blocks_rainfall_yr", "year",
  "spill_blocks_rainfall_mo", "month_id",
  "spill_blocks_rainfall_qt", "qtr_id"
)

period_grid_summary <- dplyr::bind_rows(lapply(seq_len(nrow(period_grid_specs)), function(i) {
  spec <- period_grid_specs[i, ]
  artifact_row <- artifact_specs %>% dplyr::filter(.data$artifact == spec$artifact)
  dplyr::bind_rows(
    summarise_period_grid(spec$artifact, "old_baseline", old_path(artifact_row$old_rel), spec$period_col),
    summarise_period_grid(spec$artifact, "new_ch10", new_path(artifact_row$new_rel), spec$period_col)
  )
}))

period_grid_diff <- period_grid_summary %>%
  dplyr::select(
    .data$artifact, .data$source, .data$period_col, .data$rows,
    .data$site_ids, .data$periods, .data$period_min, .data$period_max,
    .data$complete_grid
  ) %>%
  tidyr::pivot_wider(
    names_from = "source",
    values_from = -c(artifact, source),
    names_glue = "{.value}_{source}"
  ) %>%
  dplyr::mutate(
    row_delta = .data$rows_new_ch10 - .data$rows_old_baseline,
    site_id_delta = .data$site_ids_new_ch10 - .data$site_ids_old_baseline,
    period_delta = .data$periods_new_ch10 - .data$periods_old_baseline
  ) %>%
  dplyr::arrange(.data$artifact)

lookup_summary <- dplyr::bind_rows(
  summarise_lookup(old_path("rainfall/spill_site_grid_lookup.parquet"), "old_baseline"),
  summarise_lookup(new_path("rainfall/spill_site_grid_lookup.parquet"), "new_ch10")
)

event_lookup_diagnostics <- summarise_event_lookup_coverage()
event_lookup_coverage <- event_lookup_diagnostics$coverage
event_inventory_summary <- event_lookup_diagnostics$inventory

dropped_guard_specs <- tibble::tribble(
  ~guard, ~path,
  "clean_rainfall_data", here::here("output", "log", "clean_rainfall_data_dropped_spill_sites.csv"),
  "10km_site_house_sale_match", here::here("output", "log", "10km_site_house_sale_match_dropped_spill_sites.csv"),
  "10km_site_rental_match", here::here("output", "log", "10km_site_rental_match_dropped_spill_sites.csv")
)

dropped_site_guards <- dplyr::bind_rows(lapply(seq_len(nrow(dropped_guard_specs)), function(i) {
  read_dropped_guard(dropped_guard_specs$guard[i], dropped_guard_specs$path[i])
}))

dropped_site_guard_summary <- dropped_site_guards %>%
  dplyr::group_by(.data$guard, .data$path) %>%
  dplyr::summarise(
    dropped_rows = dplyr::n(),
    dropped_site_ids = dplyr::n_distinct(.data$site_id),
    site_ids = paste(sort(unique(.data$site_id)), collapse = ", "),
    .groups = "drop"
  )

removed_column_attribution <- column_diff %>%
  dplyr::filter(.data$status == "removed") %>%
  dplyr::mutate(
    attribution = attribute_removed_column(.data$artifact, .data$column)
  ) %>%
  dplyr::arrange(.data$artifact, .data$column)

core_removed_column_attribution <- removed_column_attribution %>%
  dplyr::filter(.data$artifact %in% c(
    "dry_spills", "agg_spill_dry_yr", "agg_spill_dry_mo", "agg_spill_dry_qtr"
  ))

removed_column_attribution_summary <- removed_column_attribution %>%
  dplyr::count(.data$artifact, .data$attribution, name = "columns") %>%
  dplyr::arrange(.data$artifact, dplyr::desc(.data$columns), .data$attribution)

core_removed_column_attribution_summary <- core_removed_column_attribution %>%
  dplyr::count(.data$artifact, .data$attribution, name = "columns") %>%
  dplyr::arrange(.data$artifact, dplyr::desc(.data$columns), .data$attribution)

extract_last_run <- function(lines) {
  starts <- grep("Script started", lines)
  if (length(starts) == 0) {
    return(lines)
  }
  lines[seq.int(tail(starts, 1), length(lines))]
}

extract_last_match <- function(lines, pattern) {
  matches <- stringr::str_match(lines, pattern)
  values <- matches[, 2]
  values <- values[!is.na(values)]
  if (length(values) == 0) {
    return(NA_character_)
  }
  tail(values, 1)
}

normalise_number <- function(x) {
  ifelse(is.na(x), NA_real_, as.numeric(gsub(",", "", x)))
}

summarise_consumer_log <- function(script, log_rel) {
  path <- here::here("output", "log", log_rel)
  if (!file.exists(path)) {
    return(tibble::tibble(
      script = script,
      log_path = path,
      log_exists = FALSE,
      completed = FALSE,
      last_run_lines = NA_integer_,
      start_line = NA_character_,
      final_line = NA_character_,
      unique_sites_rows = NA_real_,
      spill_lookup_rows = NA_real_,
      raw_events_rows = NA_real_,
      rows_with_missing_sites = NA_real_,
      output_rows = NA_real_
    ))
  }

  lines <- readLines(path, warn = FALSE)
  last_run <- extract_last_run(lines)
  completed <- any(stringr::str_detect(
    last_run,
    "Script completed successfully|completed successfully|Successfully processed"
  ))

  tibble::tibble(
    script = script,
    log_path = path,
    log_exists = TRUE,
    completed = completed,
    last_run_lines = length(last_run),
    start_line = dplyr::first(last_run),
    final_line = dplyr::last(last_run),
    unique_sites_rows = normalise_number(extract_last_match(
      last_run,
      "Unique spill sites loaded: ([0-9,]+) rows"
    )),
    spill_lookup_rows = normalise_number(extract_last_match(
      last_run,
      "Spill lookup data loaded: ([0-9,]+) rows"
    )),
    raw_events_rows = normalise_number(extract_last_match(
      last_run,
      "Raw spill events loaded: ([0-9,]+) rows"
    )),
    rows_with_missing_sites = normalise_number(extract_last_match(
      last_run,
      "Rows with missing sites .*: ([0-9,]+)"
    )),
    output_rows = normalise_number(extract_last_match(
      last_run,
      "(?:database created|aggregation complete|Aggregation complete).*: ([0-9,]+) rows"
    ))
  )
}

consumer_log_summary <- dplyr::bind_rows(lapply(seq_len(nrow(consumer_logs)), function(i) {
  summarise_consumer_log(consumer_logs$script[i], consumer_logs$log_rel[i])
}))

readr::write_csv(column_diff, file.path(evidence_dir, "ch10_column_diff.csv"))
readr::write_csv(artifact_summary, file.path(evidence_dir, "ch10_artifact_summary.csv"))
readr::write_csv(artifact_summary_diff, file.path(evidence_dir, "ch10_artifact_summary_diff.csv"))
readr::write_csv(metric_summary, file.path(evidence_dir, "ch10_metric_summary.csv"))
readr::write_csv(radius_summary, file.path(evidence_dir, "ch10_radius_summary.csv"))
readr::write_csv(coordinate_summary, file.path(evidence_dir, "ch10_coordinate_summary.csv"))
readr::write_csv(site_key_contracts, file.path(evidence_dir, "ch10_site_key_contracts.csv"))
readr::write_csv(membership_churn, file.path(evidence_dir, "ch10_membership_churn.csv"))
readr::write_csv(period_grid_summary, file.path(evidence_dir, "ch10_period_grid_summary.csv"))
readr::write_csv(period_grid_diff, file.path(evidence_dir, "ch10_period_grid_diff.csv"))
readr::write_csv(lookup_summary, file.path(evidence_dir, "ch10_spill_site_grid_lookup_summary.csv"))
readr::write_csv(event_lookup_coverage, file.path(evidence_dir, "ch10_event_lookup_coverage.csv"))
readr::write_csv(event_inventory_summary, file.path(evidence_dir, "ch10_event_inventory_summary.csv"))
readr::write_csv(stale_intermediate_consumers, file.path(evidence_dir, "ch10_stale_intermediate_consumers.csv"))
readr::write_csv(dropped_site_guards, file.path(evidence_dir, "ch10_dropped_site_guards.csv"))
readr::write_csv(dropped_site_guard_summary, file.path(evidence_dir, "ch10_dropped_site_guard_summary.csv"))
readr::write_csv(removed_column_attribution, file.path(evidence_dir, "ch10_removed_column_attribution.csv"))
readr::write_csv(removed_column_attribution_summary, file.path(evidence_dir, "ch10_removed_column_attribution_summary.csv"))
readr::write_csv(core_removed_column_attribution, file.path(evidence_dir, "ch10_removed_column_attribution_316_core.csv"))
readr::write_csv(core_removed_column_attribution_summary, file.path(evidence_dir, "ch10_removed_column_attribution_316_core_summary.csv"))
readr::write_csv(consumer_log_summary, file.path(evidence_dir, "ch10_consumer_log_summary.csv"))

format_table <- function(x) {
  paste(capture.output(print(x, n = Inf, width = Inf)), collapse = "\n")
}

artifact_presence_ok <- all(required_paths$exists)
consumer_logs_ok <- all(consumer_log_summary$log_exists & consumer_log_summary$completed)
site_contracts_ok <- all(site_key_contracts$contract_ok)
in_scope_lookup_coverage_ok <- event_lookup_coverage %>%
  dplyr::filter(
    .data$denominator == "coordinate_valid_unique_spill_sites",
    .data$lookup_source == "new_ch10",
    .data$match_key == "site_id+ngr"
  ) %>%
  dplyr::pull(.data$unmapped_events)
in_scope_lookup_coverage_ok <- length(in_scope_lookup_coverage_ok) == 1L &&
  isTRUE(in_scope_lookup_coverage_ok == 0)
coordinate_guard_ok <- nrow(dropped_site_guard_summary) == nrow(dropped_guard_specs) &&
  all(dropped_site_guard_summary$dropped_site_ids == 21L)
removed_columns <- column_diff %>%
  dplyr::filter(.data$status == "removed")
core_removed_columns <- removed_columns %>%
  dplyr::filter(.data$artifact %in% c(
    "dry_spills", "agg_spill_dry_yr", "agg_spill_dry_mo", "agg_spill_dry_qtr"
  ))
type_changed_columns <- column_diff %>%
  dplyr::filter(.data$status == "type_changed")

blockers <- character()
if (!artifact_presence_ok) {
  blockers <- c(blockers, "At least one old or new CH10 artifact is missing.")
}
if (!consumer_logs_ok) {
  blockers <- c(blockers, "At least one U10 consumer or regeneration log does not show successful completion.")
}
if (!site_contracts_ok) {
  blockers <- c(blockers, "At least one regenerated site-keyed output has unknown non-missing site_ids or retained no-coordinate sites in a spatial output.")
}
if (!in_scope_lookup_coverage_ok) {
  blockers <- c(blockers, "The regenerated lookup does not fully cover coordinate-valid unique_spill_sites events by site_id + ngr.")
}
if (!coordinate_guard_ok) {
  blockers <- c(blockers, "Coordinate guard dropped-site CSVs are missing or do not record the accepted 21 dropped sites.")
}
if (nrow(type_changed_columns) > 0) {
  blockers <- c(blockers, "At least one artifact column changed Arrow type.")
}

key_rows <- artifact_summary_diff %>%
  dplyr::select(
    artifact,
    rows_old_baseline,
    rows_new_ch10,
    row_delta,
    site_ids_old_baseline,
    site_ids_new_ch10,
    site_id_delta,
    house_ids_old_baseline,
    house_ids_new_ch10,
    rental_ids_old_baseline,
    rental_ids_new_ch10
  )

column_change_summary <- column_diff %>%
  dplyr::count(.data$artifact, .data$status, name = "columns") %>%
  tidyr::pivot_wider(
    names_from = "status",
    values_from = "columns",
    values_fill = 0
  ) %>%
  dplyr::arrange(.data$artifact)

spatial_contract_table <- site_key_contracts %>%
  dplyr::filter(.data$spatial_site_output) %>%
  dplyr::arrange(.data$artifact)

report_lines <- c(
  "# CH10 site-keyed consumer diff report",
  "",
  glue::glue("Generated: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}"),
  "",
  "## Executive checks",
  "",
  glue::glue("- Required artifact presence: {artifact_presence_ok}."),
  glue::glue("- U10 consumer/regeneration logs completed: {consumer_logs_ok}."),
  glue::glue("- Regenerated site-key contracts: {site_contracts_ok}."),
  glue::glue("- In-scope event lookup coverage by `site_id + ngr`: {in_scope_lookup_coverage_ok}."),
  glue::glue("- Coordinate guard dropped-site records: {coordinate_guard_ok}."),
  glue::glue("- Column type changes: {nrow(type_changed_columns)}."),
  glue::glue("- Core dry-schema removed columns: {nrow(core_removed_columns)}."),
  glue::glue("- Removed columns across all compared artifacts: {nrow(removed_columns)}."),
  glue::glue("- Blockers: {if (length(blockers) == 0) 'none' else paste(blockers, collapse = '; ')}."),
  "",
  "## Interpretation",
  "",
  "- The regenerated outputs are compared against the old-pipeline site-keyed baseline captured before the CH8/CH9 merge rebuild.",
  "- Non-missing site ids in spatial lookup and site-level prior outputs should belong to the regenerated works-grain `unique_spill_sites` inventory and should not contain works with missing easting or northing.",
  "- `NA` site ids are reported separately because the lookup and panel scripts intentionally retain no-match property rows.",
  "- Daily and dry aggregate outputs may retain works with missing coordinates because those artifacts are site-keyed statistical grids, not distance-based spatial matches.",
  "- Row and site count changes are expected where old outlet-level site ids collapsed to works-grain ids. Dry aggregate totals additionally reflect CH9's rebuilt dry-spill grids and zero-fill behavior for periods without dry-spill matches.",
  "",
  "## Consumer Runs",
  "",
  "```",
  format_table(consumer_log_summary %>%
    dplyr::select(
      script, completed, unique_sites_rows,
      spill_lookup_rows, raw_events_rows,
      rows_with_missing_sites, output_rows
    )),
  "```",
  "",
  "## Artifact Row And Id Diffs",
  "",
  "```",
  format_table(key_rows),
  "```",
  "",
  "## Column Change Summary",
  "",
  "```",
  format_table(column_change_summary),
  "```",
  "",
  "## Removed Column Attribution",
  "",
  "```",
  format_table(removed_column_attribution_summary),
  "```",
  "",
  "The row-level list of removed columns and attributions is in `ch10_removed_column_attribution.csv`.",
  "The core 316 dry-schema removed columns are separately listed in `ch10_removed_column_attribution_316_core.csv`.",
  "",
  "## Coordinate Summary",
  "",
  "```",
  format_table(coordinate_summary),
  "```",
  "",
  "## Coordinate Guard Drops",
  "",
  "```",
  format_table(dropped_site_guard_summary),
  "```",
  "",
  "The 21 coordinate-less works are retained as an explicit guard drop in the rainfall, sale-distance, and rental-distance guard logs.",
  "",
  "## Spatial Site Contracts",
  "",
  "```",
  format_table(spatial_contract_table),
  "```",
  "",
  "## Rainfall Lookup Refresh",
  "",
  "```",
  format_table(lookup_summary),
  "```",
  "",
  "## Event-Level Lookup Coverage",
  "",
  "```",
  format_table(event_inventory_summary),
  "```",
  "",
  "```",
  format_table(event_lookup_coverage),
  "```",
  "",
  "For all matched events, the old/new NGR-only unmapped shares are shown to make the prior 4.5% regression auditable. For coordinate-valid `unique_spill_sites` events, the regenerated lookup covers all events by `site_id + ngr`; the remaining all-event gap is outside the regenerated works inventory.",
  "",
  "## Period Grid Expansion",
  "",
  "```",
  format_table(period_grid_diff),
  "```",
  "",
  "The monthly dry and spill-stat grids expand from 36 to 48 periods; quarterly grids expand from 12 to 16 periods.",
  "",
  "## Gross Membership Churn",
  "",
  "```",
  format_table(membership_churn %>%
    dplyr::filter(.data$artifact %in% c(
      "spill_site_grid_lookup", "agg_spill_dry_mo", "agg_spill_stats_mo",
      "spill_blocks_rainfall_mo", "spill_house_lookup", "spill_rental_lookup"
    )) %>%
    dplyr::arrange(.data$artifact, .data$key)),
  "```",
  "",
  "## Refreshed Stale Intermediate Consumers",
  "",
  "```",
  format_table(stale_intermediate_consumers),
  "```",
  "",
  "## Radius Summary",
  "",
  "```",
  format_table(radius_summary),
  "```",
  "",
  "## Evidence Files",
  "",
  "- `ch10_required_paths.csv`",
  "- `ch10_artifact_summary.csv`",
  "- `ch10_artifact_summary_diff.csv`",
  "- `ch10_column_diff.csv`",
  "- `ch10_metric_summary.csv`",
  "- `ch10_radius_summary.csv`",
  "- `ch10_coordinate_summary.csv`",
  "- `ch10_site_key_contracts.csv`",
  "- `ch10_membership_churn.csv`",
  "- `ch10_period_grid_summary.csv`",
  "- `ch10_period_grid_diff.csv`",
  "- `ch10_spill_site_grid_lookup_summary.csv`",
  "- `ch10_event_lookup_coverage.csv`",
  "- `ch10_event_inventory_summary.csv`",
  "- `ch10_stale_intermediate_consumers.csv`",
  "- `ch10_dropped_site_guards.csv`",
  "- `ch10_dropped_site_guard_summary.csv`",
  "- `ch10_removed_column_attribution.csv`",
  "- `ch10_removed_column_attribution_summary.csv`",
  "- `ch10_removed_column_attribution_316_core.csv`",
  "- `ch10_removed_column_attribution_316_core_summary.csv`",
  "- `ch10_consumer_log_summary.csv`"
)

writeLines(report_lines, file.path(evidence_dir, "ch10_site_keyed_consumer_diff_report.md"))

if (length(blockers) > 0) {
  stop(glue::glue("CH10 diff report found blockers: {paste(blockers, collapse = '; ')}"), call. = FALSE)
}

message("CH10 diff report written to: ", file.path(evidence_dir, "ch10_site_keyed_consumer_diff_report.md"))
