# ==============================================================================
# Shared Repeat-Transaction Pipeline
# ==============================================================================

# repeat_count is always computed over the full configured long-run input.
# Consumers that restrict the date window must regroup the mapping after their
# window filter; the persisted repeat_count must not be treated as window-local.

`%||%` <- function(x, y) if (is.null(x)) y else x

assert_repeat_dependencies <- function() {
  required <- c("arrow", "data.table", "logger", "lubridate", "tidyselect")
  missing <- required[!vapply(required, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(missing) > 0L) {
    stop(
      "Missing required packages: ", paste(missing, collapse = ", "),
      ". Install project dependencies first with `rv sync`.",
      call. = FALSE
    )
  }
}

validate_repeat_config <- function(config) {
  required <- c(
    "id_col", "date_col", "price_col", "postcode_col", "address_cols",
    "primary_address_col", "input_path", "output_path", "log_file",
    "large_group_review_path", "price_ratio_review_path", "same_day_review_path",
    "market",
    "year_min", "year_max", "key_coverage_floor", "repeat_share_floor",
    "large_group_size", "extreme_annualized_price_ratio"
  )
  missing <- required[!vapply(required, function(name) !is.null(config[[name]]), logical(1))]
  if (length(missing) > 0L) {
    stop("Missing repeat config values: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  if (!config$postcode_col %in% config$address_cols ||
      !config$primary_address_col %in% config$address_cols) {
    stop("Address columns must include postcode and primary address fields.", call. = FALSE)
  }
  if (config$key_coverage_floor < 0 || config$key_coverage_floor > 1 ||
      config$repeat_share_floor < 0 || config$repeat_share_floor > 1) {
    stop("Coverage and repeat-share floors must lie in [0, 1].", call. = FALSE)
  }
  invisible(config)
}

#' Coerce a transaction date column to Date
#'
#' Sales dates arrive as POSIXct (Arrow timestamp) and rental dates as Date
#' (Arrow date32). Date arithmetic on POSIXct yields a difftime whose unit is
#' auto-selected from the smallest difference in the vector, so a single
#' same-day pair silently reinterprets the whole column as seconds. Normalizing
#' to Date removes that dependence on the data's own contents.
as_transaction_date <- function(x) {
  if (inherits(x, "Date")) return(x)
  as.Date(x, tz = "UTC")
}

normalise_address_component <- function(x) {
  value <- enc2utf8(as.character(x))
  value[is.na(value)] <- ""
  value <- toupper(value)
  value <- gsub("[[:punct:]]+", "", value)
  value <- gsub("[[:space:]]+", " ", value)
  trimws(value)
}

build_address_key <- function(data, config) {
  validate_repeat_config(config)
  missing <- setdiff(config$address_cols, names(data))
  if (length(missing) > 0L) {
    stop("Missing address columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  components <- lapply(as.data.frame(data)[config$address_cols], normalise_address_component)
  names(components) <- config$address_cols
  postcode <- components[[config$postcode_col]]
  primary <- components[[config$primary_address_col]]
  key <- do.call(paste, c(components, sep = "|"))
  key[postcode == "" | primary == ""] <- NA_character_
  key
}

validate_repeat_input <- function(data, config) {
  required <- unique(c(
    config$id_col, config$date_col, config$price_col, config$address_cols,
    config$property_type_col %||% character(),
    config$duplicate_check_cols %||% character()
  ))
  missing <- setdiff(required, names(data))
  if (length(missing) > 0L) {
    stop("Missing repeat input columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  if (nrow(data) == 0L) stop("Repeat input must be non-empty.", call. = FALSE)

  ids <- data[[config$id_col]]
  if (anyNA(ids) || any(!nzchar(as.character(ids))) || anyDuplicated(ids)) {
    stop("Repeat input transaction ids must be non-missing and unique.", call. = FALSE)
  }

  dates <- as.Date(data[[config$date_col]])
  years <- lubridate::year(dates)
  if (anyNA(years) || any(years < config$year_min | years > config$year_max)) {
    stop(
      "Repeat input dates must fall within ", config$year_min, "-", config$year_max, ".",
      call. = FALSE
    )
  }

  duplicate_cols <- config$duplicate_check_cols %||% character()
  if (length(duplicate_cols) > 0L) {
    duplicate_input <- data.table::as.data.table(data)[, ..duplicate_cols]
    if (any(duplicated(duplicate_input, by = duplicate_cols))) {
      stop("Repeat input contains exact duplicates after cleaning.", call. = FALSE)
    }
  }
  invisible(data)
}

build_price_ratio_review <- function(keyed, config) {
  audit_cols <- c(
    "address_key", config$id_col, config$date_col, config$price_col
  )
  dt <- data.table::copy(keyed[, ..audit_cols])
  dt[, (config$date_col) := as_transaction_date(get(config$date_col))]
  data.table::setorderv(dt, c("address_key", config$date_col, config$id_col))
  dt[, `:=`(
    previous_date = data.table::shift(get(config$date_col)),
    previous_price = data.table::shift(get(config$price_col))
  ), by = address_key]
  dt[, holding_days := as.numeric(
    difftime(get(config$date_col), previous_date, units = "days")
  )]
  dt[, annualized_price_ratio := {
    ratio <- get(config$price_col) / previous_price
    data.table::fifelse(
      !is.na(ratio) & ratio > 0 & holding_days > 0,
      ratio^(365.25 / holding_days),
      NA_real_
    )
  }]
  threshold <- config$extreme_annualized_price_ratio
  dt[
    !is.na(annualized_price_ratio) &
      (annualized_price_ratio > threshold | annualized_price_ratio < 1 / threshold),
    c(
      config$id_col, "address_key", config$date_col, config$price_col,
      "previous_date", "previous_price", "holding_days", "annualized_price_ratio"
    ),
    with = FALSE
  ]
}

#' Build a repeat mapping and its diagnostics without writing files
build_repeat_mapping <- function(data, config) {
  assert_repeat_dependencies()
  validate_repeat_config(config)
  validate_repeat_input(data, config)

  dt <- data.table::as.data.table(data.table::copy(data))
  dt[, address_key := build_address_key(.SD, config)]

  input_count <- nrow(dt)
  keyed <- dt[!is.na(address_key)]
  keyed_count <- nrow(keyed)
  excluded_count <- input_count - keyed_count
  key_coverage <- keyed_count / input_count
  if (key_coverage < config$key_coverage_floor) {
    stop(
      sprintf(
        "Key coverage %.6f is below configured floor %.6f.",
        key_coverage, config$key_coverage_floor
      ),
      call. = FALSE
    )
  }

  # Two transactions at one address on one date contradict each other, so both
  # sides are treated as data errors and routed to review. repeat_count is
  # counted only over the survivors.
  keyed[, (config$date_col) := as_transaction_date(get(config$date_col))]
  keyed[, same_day_group_size := as.integer(.N), by = c("address_key", config$date_col)]
  same_day_conflicts <- keyed[
    same_day_group_size > 1L,
    c(
      config$id_col, "address_key", config$date_col, config$price_col,
      "same_day_group_size"
    ),
    with = FALSE
  ]
  keyed <- keyed[same_day_group_size == 1L]
  keyed[, same_day_group_size := NULL]
  same_day_excluded_count <- nrow(same_day_conflicts)
  mapped_count <- nrow(keyed)
  if (mapped_count == 0L) {
    stop(
      "Every keyed row was excluded as a same-day conflict; mapping would be empty.",
      call. = FALSE
    )
  }

  keyed[, repeat_id := hash_serialized_values(address_key)]
  keyed[, repeat_count := as.integer(.N), by = address_key]

  if (data.table::uniqueN(keyed$address_key) != data.table::uniqueN(keyed$repeat_id)) {
    stop("Distinct address keys did not map one-to-one to repeat ids.", call. = FALSE)
  }
  if (anyDuplicated(keyed[[config$id_col]])) {
    stop("Repeat mapping transaction ids are not unique.", call. = FALSE)
  }
  if (any(!grepl("^[0-9a-f]{16}$", keyed$repeat_id))) {
    stop("repeat_id values must be lowercase 16-character hex strings.", call. = FALSE)
  }

  group_counts <- keyed[, .(
    repeat_count = as.integer(.N),
    declared_count_min = min(repeat_count),
    declared_count_max = max(repeat_count)
  ), by = .(address_key, repeat_id)]
  if (any(group_counts$repeat_count < 1L) ||
      any(group_counts$declared_count_min != group_counts$declared_count_max) ||
      any(group_counts$repeat_count != group_counts$declared_count_min)) {
    stop("repeat_count reconciliation failed.", call. = FALSE)
  }
  group_counts[, c("declared_count_min", "declared_count_max") := NULL]

  repeat_share <- if (mapped_count == 0L) 0 else mean(keyed$repeat_count > 1L)
  if (repeat_share < config$repeat_share_floor) {
    warning(
      sprintf(
        "Repeat share %.6f is below configured floor %.6f.",
        repeat_share, config$repeat_share_floor
      ),
      call. = FALSE
    )
  }

  property_type_issues <- data.table::data.table()
  property_type_col <- config$property_type_col %||% NULL
  if (!is.null(property_type_col)) {
    property_type_issues <- keyed[
      , .(property_type_count = data.table::uniqueN(na.omit(get(property_type_col)))),
      by = .(address_key, repeat_id)
    ][property_type_count > 1L]
    if (nrow(property_type_issues) > 0L) {
      warning(
        nrow(property_type_issues),
        " repeat group(s) contain more than one property type.",
        call. = FALSE
      )
    }
  }

  if (same_day_excluded_count > 0L) {
    warning(
      same_day_excluded_count,
      " row(s) excluded as same-day repeat conflicts and routed to review.",
      call. = FALSE
    )
  }

  large_groups <- group_counts[repeat_count > as.integer(config$large_group_size)]
  price_ratio_issues <- build_price_ratio_review(keyed, config)
  if (nrow(price_ratio_issues) > 0L) {
    warning(
      nrow(price_ratio_issues),
      " extreme annualized price-ratio pair(s) routed to review.",
      call. = FALSE
    )
  }

  mapping <- keyed[, c(config$id_col, "repeat_id", "repeat_count"), with = FALSE]
  mapping[, (config$id_col) := as.character(get(config$id_col))]
  mapping[, repeat_id := as.character(repeat_id)]
  mapping[, repeat_count := as.integer(repeat_count)]

  list(
    mapping = mapping[],
    keyed_data = keyed[],
    metrics = list(
      input_count = as.integer(input_count),
      keyed_count = as.integer(keyed_count),
      excluded_count = as.integer(excluded_count),
      same_day_excluded_count = as.integer(same_day_excluded_count),
      mapped_count = as.integer(mapped_count),
      key_coverage = key_coverage,
      repeat_share = repeat_share,
      largest_group_size = if (nrow(group_counts) == 0L) 0L else max(group_counts$repeat_count)
    ),
    large_groups = large_groups[],
    property_type_issues = property_type_issues[],
    price_ratio_issues = price_ratio_issues[],
    same_day_conflicts = same_day_conflicts[]
  )
}

repeat_mapping_schema <- function(id_col) {
  arrow::schema(
    arrow::field(id_col, arrow::utf8()),
    arrow::field("repeat_id", arrow::utf8()),
    arrow::field("repeat_count", arrow::int32())
  )
}

read_repeat_manifest <- function(path) {
  if (!file.exists(path)) return(NULL)
  table <- tryCatch(
    arrow::read_parquet(path, as_data_frame = FALSE),
    error = function(e) {
      stop(
        "Existing repeat mapping is unreadable: ", path, ". ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
  metadata <- table$metadata
  if (is.null(metadata$repeat_manifest_version) ||
      metadata$repeat_manifest_version != "1") {
    return(NULL)
  }
  metadata
}

build_repeat_manifest <- function(config, metrics) {
  input_path <- normalizePath(config$input_path, mustWork = FALSE)
  input_mtime <- if (file.exists(config$input_path)) {
    format(file.info(config$input_path)$mtime, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  } else {
    "in-memory"
  }
  list(
    repeat_manifest_version = "1",
    input_path = input_path,
    input_row_count = as.character(metrics$input_count),
    input_mtime = input_mtime,
    run_timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    keyed_count = as.character(metrics$keyed_count),
    excluded_count = as.character(metrics$excluded_count),
    same_day_excluded_count = as.character(metrics$same_day_excluded_count),
    mapped_count = as.character(metrics$mapped_count),
    key_coverage = sprintf("%.8f", metrics$key_coverage),
    repeat_share = sprintf("%.8f", metrics$repeat_share),
    largest_group_size = as.character(metrics$largest_group_size),
    market = as.character(config$market),
    repeat_count_semantics = "full_long_run_input"
  )
}

emit_repeat_log <- function(level, message, log_fn = NULL) {
  if (!is.null(log_fn)) {
    log_fn(level, message)
    return(invisible(message))
  }
  switch(
    level,
    WARN = logger::log_warn(message),
    ERROR = logger::log_error(message),
    logger::log_info(message)
  )
  invisible(message)
}

log_manifest_delta <- function(previous, current, log_fn = NULL) {
  if (is.null(previous) || previous$market != current$market) {
    emit_repeat_log("INFO", "Manifest diff skipped: first generation.", log_fn)
    return(invisible(NULL))
  }
  fields <- c(
    "input_row_count", "keyed_count", "excluded_count", "same_day_excluded_count",
    "mapped_count", "key_coverage", "repeat_share", "largest_group_size"
  )
  # Index field by field: a manifest written before a field existed would
  # otherwise drop a NULL and shift every remaining pair out of alignment.
  manifest_values <- function(manifest) {
    vapply(
      fields,
      function(field) as.character(manifest[[field]] %||% "absent"),
      character(1)
    )
  }
  delta <- paste(
    sprintf("%s=%s->%s", fields, manifest_values(previous), manifest_values(current)),
    collapse = "; "
  )
  emit_repeat_log("INFO", paste("Manifest delta:", delta), log_fn)
}

write_arrow_table_atomic <- function(table, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(pattern = paste0(".", basename(path), "-"), tmpdir = dirname(path), fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  arrow::write_parquet(table, tmp)
  if (!file.rename(tmp, path)) {
    stop("Failed to promote staged parquet to ", path, ".", call. = FALSE)
  }
  invisible(path)
}

write_repeat_outputs <- function(result, config, manifest) {
  table <- arrow::Table$create(
    as.data.frame(result$mapping),
    schema = repeat_mapping_schema(config$id_col)
  )
  table$metadata <- manifest
  write_arrow_table_atomic(table, config$output_path)
  write_arrow_table_atomic(as.data.frame(result$large_groups), config$large_group_review_path)
  write_arrow_table_atomic(as.data.frame(result$price_ratio_issues), config$price_ratio_review_path)
  write_arrow_table_atomic(as.data.frame(result$same_day_conflicts), config$same_day_review_path)
  invisible(config$output_path)
}

load_repeat_input <- function(config) {
  selected <- unique(c(
    config$id_col, config$date_col, config$price_col, config$address_cols,
    config$property_type_col %||% character(),
    config$duplicate_check_cols %||% character()
  ))
  data.table::as.data.table(
    arrow::read_parquet(
      config$input_path,
      col_select = tidyselect::all_of(selected)
    )
  )
}

#' Run the configured repeat-transaction pipeline
run_repeat_transactions <- function(config, data = NULL, log_fn = NULL) {
  assert_repeat_dependencies()
  validate_repeat_config(config)
  previous_manifest_path <- config$previous_manifest_path %||% config$output_path
  previous_manifest <- read_repeat_manifest(previous_manifest_path)
  input <- if (is.null(data)) load_repeat_input(config) else data

  result <- withCallingHandlers(
    build_repeat_mapping(input, config),
    warning = function(w) {
      emit_repeat_log("WARN", conditionMessage(w), log_fn)
      invokeRestart("muffleWarning")
    }
  )
  manifest <- build_repeat_manifest(config, result$metrics)
  log_manifest_delta(previous_manifest, manifest, log_fn)
  emit_repeat_log(
    "INFO",
    sprintf(
      paste(
        "Mapped %d of %d rows; excluded %d unkeyed and %d same-day",
        "(coverage %.4f; repeat share %.4f)."
      ),
      result$metrics$mapped_count,
      result$metrics$input_count,
      result$metrics$excluded_count,
      result$metrics$same_day_excluded_count,
      result$metrics$key_coverage,
      result$metrics$repeat_share
    ),
    log_fn
  )
  write_repeat_outputs(result, config, manifest)
  result$manifest <- manifest
  invisible(result)
}
