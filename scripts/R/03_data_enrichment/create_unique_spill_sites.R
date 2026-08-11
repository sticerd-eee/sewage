# ==============================================================================
# Create Canonical Spill-Site Inventory
# ==============================================================================
#
# Purpose: Build one metadata row per Canonical Spill Site in the Annual Return
#          Lookup and attach its containing Site Group ID.
#
# Inputs:
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#   - data/processed/annual_return_edm.parquet
#   - data/processed/annual_return_lookup.parquet
#
# Outputs:
#   - data/processed/unique_spill_sites.parquet
#   - data/processed/unique_spill_sites.xlsx
#   - output/log/create_unique_spill_sites.log
#
# Identity contract:
#   - site_id_canonical is the unique key and canonical metadata grain.
#   - site_id is the containing Site Group and may repeat.
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
source(here::here("scripts", "R", "utils", "ngr_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "edm_commission_utils.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "glue",
  "here",
  "logger",
  "purrr",
  "rio",
  "rnrfa",
  "stringr",
  "tibble",
  "tidyr"
)

LOG_FILE <- here::here("output", "log", "create_unique_spill_sites.log")

CONFIG <- list(
  site_group_crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  annual_data_path = here::here(
    "data", "processed", "annual_return_edm.parquet"
  ),
  lookup_data_path = here::here(
    "data", "processed", "annual_return_lookup.parquet"
  ),
  years = 2021:2024,
  large_coordinate_movement_m = 1000,
  unique_spills_parquet = here::here(
    "data", "processed", "unique_spill_sites.parquet"
  ),
  unique_spills_xlsx = here::here(
    "data", "processed", "unique_spill_sites.xlsx"
  )
)

check_required_packages(REQUIRED_PACKAGES)


# Setup -----------------------------------------------------------------------

initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, library, character.only = TRUE))
}

initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

load_data <- function(file_path, label = "dataset") {
  logger::log_info("Loading {label}: {file_path}")
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"), call. = FALSE)
  }

  tryCatch(
    {
      data <- if (tolower(tools::file_ext(file_path)) == "parquet") {
        arrow::read_parquet(file_path)
      } else {
        rio::import(file_path, trust = TRUE)
      }
      logger::log_info("Loaded {label} ({nrow(data)} rows)")
      data
    },
    error = function(error) {
      stop(
        glue::glue("Failed to load {label}: {conditionMessage(error)}"),
        call. = FALSE
      )
    }
  )
}

normalise_missing_character <- function(x) {
  value <- trimws(as.character(x))
  value[value == ""] <- NA_character_
  value
}

first_non_missing <- function(x) {
  present <- which(!is.na(x))
  if (length(present) == 0L) return(x[NA_integer_][1L])
  x[present[1L]]
}

assert_required_columns <- function(data, required, label) {
  missing <- setdiff(required, names(data))
  if (length(missing) > 0L) {
    stop(
      glue::glue(
        "{label} missing required columns: {paste(missing, collapse = ', ')}"
      ),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

assert_years <- function(years) {
  years <- as.integer(years)
  if (length(years) == 0L || anyNA(years) || anyDuplicated(years)) {
    stop("Configured metadata years must be unique and non-missing.", call. = FALSE)
  }
  sort(years)
}


# Identity boundaries ---------------------------------------------------------

#' Build one fail-closed Canonical Spill Site to Site Group mapping
#' @param crosswalk_data Site Group crosswalk at group-year grain
#' @param lookup_data Annual Return Lookup defining the canonical universe
#' @return One row per site_id_canonical with its repeated Site Group site_id
build_canonical_membership <- function(crosswalk_data, lookup_data) {
  assert_required_columns(
    crosswalk_data,
    c("site_id", "water_company", "site_id_canonical_members"),
    "Site Group crosswalk"
  )
  assert_required_columns(lookup_data, "site_id", "Annual Return Lookup")

  lookup_universe <- suppressWarnings(as.integer(lookup_data$site_id))
  if (anyNA(lookup_universe) || anyDuplicated(lookup_universe)) {
    stop(
      "Annual Return Lookup site_id must be non-missing and unique.",
      call. = FALSE
    )
  }

  group_definitions <- crosswalk_data |>
    dplyr::transmute(
      site_id = suppressWarnings(as.integer(.data$site_id)),
      water_company = normalise_missing_character(.data$water_company),
      member_set = normalise_missing_character(.data$site_id_canonical_members)
    ) |>
    dplyr::distinct()

  if (anyNA(group_definitions$site_id) || anyNA(group_definitions$water_company) ||
      anyNA(group_definitions$member_set)) {
    stop(
      "Site Group membership contains missing group, company, or member values.",
      call. = FALSE
    )
  }

  inconsistent_groups <- group_definitions |>
    dplyr::count(.data$site_id, name = "n_definitions") |>
    dplyr::filter(.data$n_definitions != 1L)
  if (nrow(inconsistent_groups) > 0L) {
    stop(
      "Site Group membership has inconsistent member sets or multiple companies.",
      call. = FALSE
    )
  }

  membership <- group_definitions |>
    tidyr::separate_rows("member_set", sep = ";") |>
    dplyr::mutate(
      member_set = trimws(.data$member_set),
      site_id_canonical = suppressWarnings(as.integer(.data$member_set))
    )

  if (anyNA(membership$site_id_canonical) ||
      any(as.character(membership$site_id_canonical) != membership$member_set)) {
    stop(
      "Site Group membership contains a missing or non-integer canonical ID.",
      call. = FALSE
    )
  }

  membership <- membership |>
    dplyr::select("site_id", "site_id_canonical", "water_company") |>
    dplyr::distinct()

  ambiguous_canonical <- membership |>
    dplyr::group_by(.data$site_id_canonical) |>
    dplyr::summarise(
      n_site_groups = dplyr::n_distinct(.data$site_id),
      n_companies = dplyr::n_distinct(.data$water_company),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$n_site_groups != 1L | .data$n_companies != 1L)
  if (nrow(ambiguous_canonical) > 0L) {
    stop(
      "Canonical membership maps to multiple Site Groups or companies.",
      call. = FALSE
    )
  }

  group_contract <- membership |>
    dplyr::group_by(.data$site_id) |>
    dplyr::summarise(
      n_companies = dplyr::n_distinct(.data$water_company),
      smallest_member = min(.data$site_id_canonical),
      .groups = "drop"
    )
  if (any(group_contract$n_companies != 1L)) {
    stop("A Site Group cannot span multiple companies.", call. = FALSE)
  }
  if (any(group_contract$site_id != group_contract$smallest_member)) {
    stop(
      "Each Site Group representative must be its smallest canonical member.",
      call. = FALSE
    )
  }

  missing_membership <- setdiff(lookup_universe, membership$site_id_canonical)
  unexpected_membership <- setdiff(membership$site_id_canonical, lookup_universe)
  if (length(missing_membership) > 0L || length(unexpected_membership) > 0L) {
    stop(
      glue::glue(
        "Site Group membership coverage differs from lookup coverage: ",
        "{length(missing_membership)} missing and ",
        "{length(unexpected_membership)} unexpected canonical IDs."
      ),
      call. = FALSE
    )
  }

  membership |>
    dplyr::arrange(.data$site_id, .data$site_id_canonical)
}

#' Map Annual Return EDM rows to Canonical Spill Sites without fallback
#' @param annual_data Raw combined Annual Return EDM rows
#' @param lookup_data Annual Return Lookup
#' @param metadata_years Configured years
#' @return Annual rows with a non-missing site_id_canonical
map_annual_to_canonical_sites <- function(
    annual_data,
    lookup_data,
    metadata_years = CONFIG$years
) {
  years <- assert_years(metadata_years)
  id_columns <- paste0("site_id_", years)
  annual_fields <- c(
    "year", "water_company", "outlet_discharge_ngr", "edm_commission_date",
    "edm_operation_percent", "edm_operation_reason", "spill_hrs_ea",
    "spill_count_ea", id_columns
  )
  assert_required_columns(annual_data, annual_fields, "Annual Return EDM")
  assert_required_columns(lookup_data, c("site_id", id_columns), "Annual Return Lookup")

  lookup_ids <- suppressWarnings(as.integer(lookup_data$site_id))
  if (anyNA(lookup_ids) || anyDuplicated(lookup_ids)) {
    stop("Annual Return Lookup canonical IDs must be unique.", call. = FALSE)
  }

  mapped_by_year <- lapply(years, function(report_year) {
    id_column <- paste0("site_id_", report_year)
    lookup_year_id <- lookup_data[[id_column]]
    lookup_present <- !is.na(lookup_year_id)
    if (anyDuplicated(lookup_year_id[lookup_present])) {
      stop(
        glue::glue("Lookup has duplicated year-site IDs in {id_column}."),
        call. = FALSE
      )
    }

    rows <- annual_data |>
      dplyr::filter(as.integer(.data$year) == report_year)
    if (nrow(rows) == 0L) return(NULL)

    year_site_id <- rows[[id_column]]
    canonical_index <- match(year_site_id, lookup_year_id)
    if (anyNA(year_site_id) || anyNA(canonical_index)) {
      stop(
        glue::glue(
          "Annual Return rows for {report_year} exist without lookup coverage."
        ),
        call. = FALSE
      )
    }

    rows |>
      dplyr::transmute(
        site_id_canonical = lookup_ids[canonical_index],
        year = report_year,
        water_company = .data$water_company,
        outlet_discharge_ngr = .data$outlet_discharge_ngr,
        edm_commission_date = .data$edm_commission_date,
        edm_operation_percent = suppressWarnings(
          as.numeric(.data$edm_operation_percent)
        ),
        edm_operation_reason = .data$edm_operation_reason,
        spill_hrs_ea = suppressWarnings(as.numeric(.data$spill_hrs_ea)),
        spill_count_ea = suppressWarnings(as.numeric(.data$spill_count_ea))
      )
  })

  mapped <- dplyr::bind_rows(mapped_by_year)
  if (nrow(mapped) == 0L) {
    return(tibble::tibble(
      site_id_canonical = integer(),
      year = integer(),
      water_company = character(),
      outlet_discharge_ngr = character(),
      edm_commission_date = character(),
      edm_operation_percent = double(),
      edm_operation_reason = character(),
      spill_hrs_ea = double(),
      spill_count_ea = double()
    ))
  }
  if (anyNA(mapped$site_id_canonical)) {
    stop("Mapped annual rows contain missing canonical IDs.", call. = FALSE)
  }
  mapped
}


# Canonical metadata ----------------------------------------------------------

prepare_canonical_observations <- function(mapped_annual) {
  observations <- mapped_annual |>
    dplyr::mutate(
      site_id_canonical = as.integer(.data$site_id_canonical),
      year = as.integer(.data$year),
      water_company = normalise_missing_character(.data$water_company),
      ngr = clean_ngr(.data$outlet_discharge_ngr),
      edm_commission_date = normalise_missing_character(.data$edm_commission_date),
      edm_operation_reason = normalise_missing_character(.data$edm_operation_reason),
      edm_operation_percent = suppressWarnings(
        as.numeric(.data$edm_operation_percent)
      )
    )
  coordinates <- parse_bng_coordinates(observations$ngr)
  dplyr::bind_cols(observations, coordinates)
}

maximum_coordinate_movement <- function(easting, northing) {
  present <- !is.na(easting) & !is.na(northing)
  coordinates <- unique(cbind(easting[present], northing[present]))
  if (nrow(coordinates) <= 1L) return(0)
  as.numeric(max(stats::dist(coordinates)))
}

count_distinct_coordinates <- function(easting, northing) {
  present <- !is.na(easting) & !is.na(northing)
  nrow(unique(cbind(easting[present], northing[present])))
}

#' Build non-production validation evidence for canonical metadata histories
#' @param mapped_annual Canonically mapped annual rows
#' @param large_coordinate_movement_m Movement threshold in metres
#' @return One validation row per observed Canonical Spill Site
build_canonical_metadata_validation <- function(
    mapped_annual,
    large_coordinate_movement_m = CONFIG$large_coordinate_movement_m
) {
  observations <- prepare_canonical_observations(mapped_annual)
  observations |>
    dplyr::group_by(.data$site_id_canonical) |>
    dplyr::summarise(
      n_water_companies = dplyr::n_distinct(.data$water_company, na.rm = TRUE),
      water_company_changed = .data$n_water_companies > 1L,
      n_parseable_locations = count_distinct_coordinates(
        .data$easting,
        .data$northing
      ),
      max_coordinate_movement_m = maximum_coordinate_movement(
        .data$easting,
        .data$northing
      ),
      large_coordinate_movement =
        .data$max_coordinate_movement_m >= large_coordinate_movement_m,
      .groups = "drop"
    )
}

summarise_canonical_metadata <- function(
    mapped_annual,
    metadata_years = CONFIG$years
) {
  years <- assert_years(metadata_years)
  observations <- prepare_canonical_observations(mapped_annual)

  company <- observations |>
    dplyr::filter(!is.na(.data$water_company)) |>
    dplyr::arrange(
      .data$site_id_canonical,
      dplyr::desc(.data$year),
      .data$water_company
    ) |>
    dplyr::group_by(.data$site_id_canonical) |>
    dplyr::summarise(
      water_company = first_non_missing(.data$water_company),
      .groups = "drop"
    )

  location <- observations |>
    dplyr::filter(
      !is.na(.data$ngr),
      !is.na(.data$easting),
      !is.na(.data$northing)
    ) |>
    dplyr::arrange(
      .data$site_id_canonical,
      dplyr::desc(.data$year),
      .data$ngr
    ) |>
    dplyr::group_by(.data$site_id_canonical) |>
    dplyr::summarise(
      ngr = first_non_missing(.data$ngr),
      easting = first_non_missing(.data$easting),
      northing = first_non_missing(.data$northing),
      .groups = "drop"
    )

  availability <- observations |>
    dplyr::distinct(.data$site_id_canonical, .data$year) |>
    dplyr::mutate(available = TRUE) |>
    tidyr::complete(
      site_id_canonical = unique(observations$site_id_canonical),
      year = years,
      fill = list(available = FALSE)
    ) |>
    tidyr::pivot_wider(
      names_from = "year",
      values_from = "available",
      names_prefix = "available_year_"
    )

  no_longer_operational <- observations |>
    dplyr::mutate(
      explicit_nlo = !is.na(.data$edm_operation_reason) &
        stringr::str_detect(
          stringr::str_to_lower(.data$edm_operation_reason),
          "no longer operational"
        )
    ) |>
    dplyr::group_by(.data$site_id_canonical) |>
    dplyr::summarise(
      no_longer_operational_year = if (any(.data$explicit_nlo)) {
        min(.data$year[.data$explicit_nlo])
      } else {
        NA_integer_
      },
      .groups = "drop"
    )

  site_year_operation <- observations |>
    dplyr::filter(.data$year %in% years) |>
    dplyr::group_by(.data$site_id_canonical, .data$year) |>
    dplyr::summarise(
      operation_percent_values = dplyr::n_distinct(
        .data$edm_operation_percent,
        na.rm = TRUE
      ),
      operation_reason_values = dplyr::n_distinct(
        .data$edm_operation_reason,
        na.rm = TRUE
      ),
      edm_operation_percent = if (.data$operation_percent_values == 1L) {
        first_non_missing(.data$edm_operation_percent)
      } else {
        NA_real_
      },
      edm_operation_reason = if (.data$operation_reason_values == 1L) {
        first_non_missing(.data$edm_operation_reason)
      } else {
        NA_character_
      },
      edm_operation_percent_conflict = .data$operation_percent_values > 1L,
      edm_operation_reason_conflict = .data$operation_reason_values > 1L,
      .groups = "drop"
    ) |>
    dplyr::select(
      "site_id_canonical",
      "year",
      "edm_operation_percent",
      "edm_operation_reason",
      "edm_operation_percent_conflict",
      "edm_operation_reason_conflict"
    ) |>
    tidyr::complete(
      site_id_canonical = unique(observations$site_id_canonical),
      year = years,
      fill = list(
        edm_operation_percent_conflict = FALSE,
        edm_operation_reason_conflict = FALSE
      )
    )

  operation_percent <- site_year_operation |>
    dplyr::select(
      "site_id_canonical",
      "year",
      "edm_operation_percent"
    ) |>
    tidyr::pivot_wider(
      names_from = "year",
      values_from = "edm_operation_percent",
      names_prefix = "edm_operation_percent_"
    )
  operation_percent_conflict <- site_year_operation |>
    dplyr::select(
      "site_id_canonical",
      "year",
      "edm_operation_percent_conflict"
    ) |>
    tidyr::pivot_wider(
      names_from = "year",
      values_from = "edm_operation_percent_conflict",
      names_prefix = "edm_operation_percent_conflict_"
    )
  operation_reason <- site_year_operation |>
    dplyr::select(
      "site_id_canonical",
      "year",
      "edm_operation_reason"
    ) |>
    tidyr::pivot_wider(
      names_from = "year",
      values_from = "edm_operation_reason",
      names_prefix = "edm_operation_reason_"
    )
  operation_reason_conflict <- site_year_operation |>
    dplyr::select(
      "site_id_canonical",
      "year",
      "edm_operation_reason_conflict"
    ) |>
    tidyr::pivot_wider(
      names_from = "year",
      values_from = "edm_operation_reason_conflict",
      names_prefix = "edm_operation_reason_conflict_"
    )

  commission <- observations |>
    dplyr::select(
      "site_id_canonical",
      "year",
      "edm_commission_date"
    ) |>
    dplyr::group_by(.data$site_id_canonical) |>
    dplyr::group_modify(~ resolve_commission_history(
      texts = .x$edm_commission_date,
      report_years = .x$year
    )) |>
    dplyr::ungroup()

  purrr::reduce(
    list(
      company,
      location,
      availability,
      no_longer_operational,
      commission,
      operation_percent,
      operation_percent_conflict,
      operation_reason,
      operation_reason_conflict
    ),
    dplyr::full_join,
    by = "site_id_canonical"
  )
}


# Assembly and output ---------------------------------------------------------

assemble_unique_sites <- function(
    lookup_data,
    membership,
    annual_metadata,
    years = CONFIG$years
) {
  years <- assert_years(years)
  availability_columns <- paste0("available_year_", years)
  percent_columns <- paste0("edm_operation_percent_", years)
  percent_conflict_columns <- paste0(
    "edm_operation_percent_conflict_",
    years
  )
  reason_columns <- paste0("edm_operation_reason_", years)
  reason_conflict_columns <- paste0("edm_operation_reason_conflict_", years)

  universe <- lookup_data |>
    dplyr::transmute(site_id_canonical = suppressWarnings(as.integer(.data$site_id)))
  if (anyNA(universe$site_id_canonical) || anyDuplicated(universe$site_id_canonical)) {
    stop("Annual Return Lookup canonical universe is not unique.", call. = FALSE)
  }

  sites <- universe |>
    dplyr::left_join(
      membership |>
        dplyr::select("site_id", "site_id_canonical"),
      by = "site_id_canonical"
    ) |>
    dplyr::left_join(annual_metadata, by = "site_id_canonical")

  if (anyNA(sites$site_id)) {
    stop("Canonical output has missing Site Group membership.", call. = FALSE)
  }

  if (!"water_company" %in% names(sites)) sites$water_company <- NA_character_
  if (!"ngr" %in% names(sites)) sites$ngr <- NA_character_
  if (!"easting" %in% names(sites)) sites$easting <- NA_real_
  if (!"northing" %in% names(sites)) sites$northing <- NA_real_
  if (!"no_longer_operational_year" %in% names(sites)) {
    sites$no_longer_operational_year <- NA_integer_
  }
  if (!"edm_commission_date" %in% names(sites)) {
    sites$edm_commission_date <- as.Date(NA)
  }
  if (!"edm_commission_date_precision" %in% names(sites)) {
    sites$edm_commission_date_precision <- NA_character_
  }
  if (!"edm_commission_resolution_status" %in% names(sites)) {
    sites$edm_commission_resolution_status <- NA_character_
  }

  for (column in availability_columns) {
    if (!column %in% names(sites)) sites[[column]] <- FALSE
  }
  for (column in percent_columns) {
    if (!column %in% names(sites)) sites[[column]] <- NA_real_
  }
  for (column in percent_conflict_columns) {
    if (!column %in% names(sites)) sites[[column]] <- FALSE
  }
  for (column in reason_columns) {
    if (!column %in% names(sites)) sites[[column]] <- NA_character_
  }
  for (column in reason_conflict_columns) {
    if (!column %in% names(sites)) sites[[column]] <- FALSE
  }

  sites <- sites |>
    dplyr::mutate(
      site_id = as.integer(.data$site_id),
      site_id_canonical = as.integer(.data$site_id_canonical),
      water_company = normalise_missing_character(.data$water_company),
      ngr = normalise_missing_character(.data$ngr),
      no_longer_operational_year = as.integer(.data$no_longer_operational_year),
      edm_commission_date = as.Date(.data$edm_commission_date),
      edm_commission_date_precision = dplyr::coalesce(
        .data$edm_commission_date_precision,
        "unknown"
      ),
      edm_commission_resolution_status = dplyr::coalesce(
        .data$edm_commission_resolution_status,
        "missing"
      ),
      dplyr::across(
        dplyr::all_of(c(
          availability_columns,
          percent_conflict_columns,
          reason_conflict_columns
        )),
        ~ tidyr::replace_na(as.logical(.x), FALSE)
      )
    ) |>
    dplyr::select(
      "site_id",
      "site_id_canonical",
      "water_company",
      "ngr",
      dplyr::all_of(availability_columns),
      "no_longer_operational_year",
      "easting",
      "northing",
      "edm_commission_date",
      "edm_commission_date_precision",
      "edm_commission_resolution_status",
      dplyr::all_of(percent_columns),
      dplyr::all_of(percent_conflict_columns),
      dplyr::all_of(reason_columns),
      dplyr::all_of(reason_conflict_columns)
    ) |>
    dplyr::arrange(.data$site_id, .data$site_id_canonical)

  if (!identical(sort(sites$site_id_canonical), sort(universe$site_id_canonical)) ||
      anyDuplicated(sites$site_id_canonical)) {
    stop("Final canonical output does not exactly cover the lookup universe.", call. = FALSE)
  }
  validate_commission_resolution(sites)
  sites
}

#' Build the complete canonical unique_spill_sites table
#' @param annual_data Annual Return EDM rows
#' @param lookup_data Annual Return Lookup
#' @param crosswalk_data Site Group crosswalk
#' @param years Configured reporting years
#' @return One row per Canonical Spill Site
build_unique_spill_sites <- function(
    annual_data,
    lookup_data,
    crosswalk_data,
    years = CONFIG$years
) {
  years <- assert_years(years)
  membership <- build_canonical_membership(crosswalk_data, lookup_data)
  mapped_annual <- map_annual_to_canonical_sites(annual_data, lookup_data, years)

  validation <- build_canonical_metadata_validation(
    mapped_annual,
    CONFIG$large_coordinate_movement_m
  )
  company_changes <- sum(validation$water_company_changed)
  coordinate_changes <- sum(validation$large_coordinate_movement)
  logger::log_info(
    "Canonical validation: {company_changes} company changes; ",
    "{coordinate_changes} coordinate movements >= ",
    "{CONFIG$large_coordinate_movement_m}m"
  )

  annual_metadata <- summarise_canonical_metadata(mapped_annual, years)
  assemble_unique_sites(lookup_data, membership, annual_metadata, years)
}

log_output_diagnostics <- function(unique_sites, years = CONFIG$years) {
  availability_columns <- paste0("available_year_", assert_years(years))
  logger::log_info("Final Canonical Spill Sites: {nrow(unique_sites)}")
  logger::log_info(
    "Distinct Site Groups: {dplyr::n_distinct(unique_sites$site_id)}"
  )
  logger::log_info(
    "Multi-member canonical rows: ",
    "{sum(duplicated(unique_sites$site_id) | duplicated(unique_sites$site_id, fromLast = TRUE))}"
  )
  for (column in availability_columns) {
    logger::log_info("{column}: {sum(unique_sites[[column]])} available sites")
  }
  logger::log_info(
    "Explicit no-longer-operational histories: ",
    "{sum(!is.na(unique_sites$no_longer_operational_year))}"
  )
}

export_data <- function(
    unique_spills_df,
    excel_output = CONFIG$unique_spills_xlsx,
    parquet_output = CONFIG$unique_spills_parquet
) {
  tryCatch(
    {
      arrow::write_parquet(unique_spills_df, parquet_output)
      logger::log_info("Exported parquet: {parquet_output}")
      rio::export(unique_spills_df, excel_output)
      logger::log_info("Exported Excel: {excel_output}")
      invisible(TRUE)
    },
    error = function(error) {
      stop(
        glue::glue("Failed to export data: {conditionMessage(error)}"),
        call. = FALSE
      )
    }
  )
}


# Main ------------------------------------------------------------------------

main <- function() {
  initialise_environment()
  initialise_logging()

  crosswalk_data <- load_data(
    CONFIG$site_group_crosswalk_path,
    "Site Group crosswalk"
  )
  annual_data <- load_data(CONFIG$annual_data_path, "Annual Return EDM")
  lookup_data <- load_data(CONFIG$lookup_data_path, "Annual Return Lookup")

  unique_sites <- build_unique_spill_sites(
    annual_data,
    lookup_data,
    crosswalk_data,
    CONFIG$years
  )
  log_output_diagnostics(unique_sites, CONFIG$years)
  export_data(unique_sites)
  logger::log_info("Processing completed successfully")
  invisible(unique_sites)
}

if (sys.nframe() == 0L) main()
