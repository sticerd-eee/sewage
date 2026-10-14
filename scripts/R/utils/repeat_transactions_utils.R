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
  # Reject an unpublishable path before the run does any work, rather than after
  # the mappings have been built.
  if (isTRUE(config$publish)) {
    invisible(lapply(repeat_staged_output_paths(config), repeat_canonical_path))
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
  # Normalise each distinct value once and map back: identical output to
  # normalising every element, at a fraction of the memory on a 30-million-row
  # history whose address fields repeat heavily.
  x <- as.character(x)
  uniques <- unique(x)
  value <- enc2utf8(uniques)
  value[is.na(value)] <- ""
  value <- toupper(value)
  value <- gsub("[[:punct:]]+", "", value)
  value <- gsub("[[:space:]]+", " ", value)
  value <- trimws(value)
  value[match(x, uniques)]
}

build_address_key <- function(data, config) {
  validate_repeat_config(config)
  missing <- setdiff(config$address_cols, names(data))
  if (length(missing) > 0L) {
    stop("Missing address columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  # Column extraction rather than as.data.frame(): converting a wide
  # data.table copies every column, which the key never needs.
  components <- lapply(config$address_cols, function(col) {
    normalise_address_component(data[[col]])
  })
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

  # Only the columns the mapping and its reviews read are carried: the address
  # fields exist to build the key and are released once it exists, so the keyed
  # subset below copies five columns rather than the full input.
  carried <- unique(c(
    config$id_col, config$date_col, config$price_col,
    config$property_type_col %||% character()
  ))
  address_key <- build_address_key(data, config)
  input_count <- nrow(data)
  keep <- !is.na(address_key)
  # The keyed subset is built column by column from the input, then the wide
  # input is released before anything else is allocated: on the 30-million-row
  # history the address columns are the largest thing in memory and are no
  # longer needed once the key exists. `data` is this function's own reference;
  # a caller that keeps another one keeps its copy.
  keyed <- data.table::as.data.table(c(
    lapply(stats::setNames(carried, carried), function(col) data[[col]][keep]),
    list(address_key = address_key[keep])
  ))
  rm(data, address_key, keep)
  invisible(gc())
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

  # Hash each distinct key once; equal keys receive equal ids either way.
  unique_keys <- unique(keyed$address_key)
  keyed[, repeat_id := hash_serialized_values(unique_keys)[match(address_key, unique_keys)]]
  rm(unique_keys)
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
    stop("Failed to write staged parquet to ", path, ".", call. = FALSE)
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

#' The four staged outputs a run writes, in publication order
repeat_staged_output_paths <- function(config) {
  c(
    config$output_path, config$large_group_review_path,
    config$price_ratio_review_path, config$same_day_review_path
  )
}

#' Canonical destination for a staged candidate path
#'
#' Fails closed on any path that is not a candidate, so a mistyped config cannot
#' nominate an arbitrary file as a publication target.
repeat_canonical_path <- function(path) {
  canonical <- sub("_candidate\\.parquet$", ".parquet", path)
  if (identical(canonical, path)) {
    stop(
      "Repeat output path is not a candidate path and cannot be published: ",
      path,
      call. = FALSE
    )
  }
  canonical
}

#' Re-read a staged mapping and check it against the manifest it will carry
#'
#' Renaming cannot corrupt a file, so this runs before the swap rather than
#' after it: a staged mapping that disagrees with its own run aborts while the
#' canonical generation is still intact.
assert_promotable_mapping <- function(path, manifest) {
  table <- tryCatch(
    arrow::read_parquet(path, as_data_frame = FALSE),
    error = function(e) {
      stop(
        "Staged mapping is unreadable and cannot be published: ", path, ". ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
  expected <- manifest$mapped_count
  if (!is.null(expected) &&
      !identical(as.character(table$num_rows), as.character(expected))) {
    stop(
      "Staged mapping row count ", table$num_rows,
      " contradicts its manifest mapped_count ", expected, ".",
      call. = FALSE
    )
  }
  invisible(path)
}

#' Replace the canonical outputs with this run's staged candidates
#'
#' Every output is written before anything is promoted, so publication is four
#' renames executed back to back. A failure partway still leaves some outputs at
#' the new generation and some at the old, but the window is milliseconds rather
#' than the minutes it takes to write the mappings; the filesystem offers no
#' transaction that would close it entirely.
promote_repeat_outputs <- function(config, manifest, log_fn = NULL) {
  staged <- repeat_staged_output_paths(config)
  targets <- vapply(staged, repeat_canonical_path, character(1), USE.NAMES = FALSE)

  absent <- staged[!file.exists(staged)]
  if (length(absent) > 0L) {
    stop(
      "Cannot publish; staged output(s) missing: ",
      paste(absent, collapse = ", "),
      call. = FALSE
    )
  }
  assert_promotable_mapping(staged[[1L]], manifest)

  for (index in seq_along(staged)) {
    if (!file.rename(staged[[index]], targets[[index]])) {
      stop(
        "Failed to publish ", staged[[index]], " to ", targets[[index]], ".",
        call. = FALSE
      )
    }
  }
  emit_repeat_log(
    "INFO",
    sprintf(
      "Promoted %d staged output(s) to canonical paths: %s",
      length(targets), paste(basename(targets), collapse = ", ")
    ),
    log_fn
  )
  invisible(targets)
}

load_repeat_input <- function(config) {
  selected <- unique(c(
    config$id_col, config$date_col, config$price_col, config$address_cols,
    config$property_type_col %||% character(),
    config$duplicate_check_cols %||% character()
  ))
  # Materialise the Arrow vectors before data.table takes the frame (see
  # docs/solutions/logic-errors/arrow-altrep-data-table-join-nondeterminism.md).
  # data.table::setDT() converts in place; as.data.table() would copy the
  # whole materialised frame a second time.
  frame <- as.data.frame(arrow::read_parquet(
    config$input_path,
    col_select = tidyselect::all_of(selected)
  ))
  data.table::setDT(frame)
  frame[]
}

#' Build the repeat mapping from a parquet input without holding it in R
#'
#' Same contract as `build_repeat_mapping()`: every row's address key and
#' repeat id come from `build_address_key()` and `hash_serialized_values()`,
#' applied one parquet row group at a time; the grouping, same-day exclusion,
#' review tables and metrics are then computed by DuckDB over a staged copy of
#' (id, date, price, property type, key, repeat id). A 30-million-row history
#' therefore never has to sit in R as strings. Row order follows the input, so
#' the outputs match the in-memory builder value for value.
build_repeat_mapping_streamed <- function(config, stage_dir = tempfile("repeat-stage-")) {
  assert_repeat_dependencies()
  validate_repeat_config(config)
  for (pkg in c("duckdb", "DBI")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Package `", pkg, "` is required for the streamed repeat mapping.", call. = FALSE)
    }
  }
  if (!file.exists(config$input_path)) {
    stop("Repeat input not found: ", config$input_path, call. = FALSE)
  }
  reader <- arrow::ParquetFileReader$create(config$input_path)
  columns <- names(reader$GetSchema())
  property_type_col <- config$property_type_col %||% NULL
  required <- unique(c(
    config$id_col, config$date_col, config$price_col, config$address_cols,
    property_type_col %||% character(), config$duplicate_check_cols %||% character()
  ))
  missing <- setdiff(required, columns)
  if (length(missing) > 0L) {
    stop("Missing repeat input columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  if (reader$num_rows == 0) stop("Repeat input must be non-empty.", call. = FALSE)
  if (length(config$duplicate_check_cols %||% character()) > 0L) {
    stop("Exact-duplicate checks are not supported by the streamed builder.", call. = FALSE)
  }
  dir.create(stage_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(stage_dir, recursive = TRUE), add = TRUE)

  # Stage 1 (R, per row group): validate, key and hash exactly as the in-memory
  # builder does, keeping the input's row order through `rn`.
  offset <- 0
  for (g in seq_len(reader$num_row_groups) - 1L) {
    chunk <- as.data.frame(reader$ReadRowGroup(g, column_indices = match(required, columns) - 1L))
    ids <- as.character(chunk[[config$id_col]])
    if (anyNA(ids) || any(!nzchar(ids))) {
      stop("Repeat input transaction ids must be non-missing and unique.", call. = FALSE)
    }
    dates <- as_transaction_date(chunk[[config$date_col]])
    years <- lubridate::year(dates)
    if (anyNA(years) || any(years < config$year_min | years > config$year_max)) {
      stop(
        "Repeat input dates must fall within ", config$year_min, "-", config$year_max, ".",
        call. = FALSE
      )
    }
    key <- build_address_key(chunk, config)
    repeat_id <- rep(NA_character_, length(key))
    keyed <- !is.na(key)
    if (any(keyed)) repeat_id[keyed] <- hash_serialized_values(key[keyed])
    staged <- data.frame(
      rn = offset + seq_along(ids),
      id = ids,
      tx_date = dates,
      price = chunk[[config$price_col]],
      property_type = if (is.null(property_type_col)) NA_character_ else as.character(chunk[[property_type_col]]),
      address_key = key,
      repeat_id = repeat_id,
      stringsAsFactors = FALSE
    )
    arrow::write_parquet(staged, file.path(stage_dir, sprintf("part-%04d.parquet", g)))
    offset <- offset + nrow(chunk)
    rm(chunk, ids, dates, years, key, repeat_id, keyed, staged)
  }

  # Stage 2 (DuckDB): grouping, same-day exclusion, reviews and metrics.
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "PRAGMA memory_limit='4GB'")
  DBI::dbExecute(con, sprintf("PRAGMA temp_directory='%s'", file.path(stage_dir, "duckdb_temp")))
  DBI::dbExecute(con, sprintf(
    "CREATE VIEW staged AS SELECT * FROM read_parquet('%s')",
    file.path(stage_dir, "part-*.parquet")
  ))
  totals <- DBI::dbGetQuery(con, "SELECT count(*) AS n, count(DISTINCT id) AS n_ids, count(address_key) AS keyed FROM staged")
  if (totals$n_ids != totals$n) {
    stop("Repeat input transaction ids must be non-missing and unique.", call. = FALSE)
  }
  input_count <- as.integer(totals$n)
  keyed_count <- as.integer(totals$keyed)
  excluded_count <- input_count - keyed_count
  key_coverage <- keyed_count / input_count
  if (key_coverage < config$key_coverage_floor) {
    stop(
      sprintf("Key coverage %.6f is below configured floor %.6f.", key_coverage, config$key_coverage_floor),
      call. = FALSE
    )
  }
  DBI::dbExecute(con, paste(
    "CREATE TABLE keyed AS SELECT rn, id, tx_date, price, property_type, address_key, repeat_id,",
    "count(*) OVER (PARTITION BY address_key, tx_date) AS same_day_group_size",
    "FROM staged WHERE address_key IS NOT NULL"
  ))
  same_day_conflicts <- DBI::dbGetQuery(con, paste(
    "SELECT id, address_key, tx_date, price, same_day_group_size FROM keyed",
    "WHERE same_day_group_size > 1 ORDER BY rn"
  ))
  same_day_excluded_count <- nrow(same_day_conflicts)
  DBI::dbExecute(con, paste(
    "CREATE TABLE grouped AS SELECT rn, id, tx_date, price, property_type, address_key, repeat_id,",
    "count(*) OVER (PARTITION BY address_key) AS repeat_count",
    "FROM keyed WHERE same_day_group_size = 1"
  ))
  DBI::dbExecute(con, "DROP TABLE keyed")
  summary <- DBI::dbGetQuery(con, paste(
    "SELECT count(*) AS mapped, count(DISTINCT address_key) AS n_keys, count(DISTINCT repeat_id) AS n_repeat_ids,",
    "avg(CASE WHEN repeat_count > 1 THEN 1.0 ELSE 0.0 END) AS repeat_share, max(repeat_count) AS largest",
    "FROM grouped"
  ))
  mapped_count <- as.integer(summary$mapped)
  if (mapped_count == 0L) {
    stop("Every keyed row was excluded as a same-day conflict; mapping would be empty.", call. = FALSE)
  }
  if (summary$n_keys != summary$n_repeat_ids) {
    stop("Distinct address keys did not map one-to-one to repeat ids.", call. = FALSE)
  }
  mapping <- data.table::as.data.table(DBI::dbGetQuery(con, "SELECT id, repeat_id, repeat_count FROM grouped ORDER BY rn"))
  data.table::setnames(mapping, "id", config$id_col)
  mapping[, repeat_count := as.integer(repeat_count)]
  if (anyDuplicated(mapping[[config$id_col]])) {
    stop("Repeat mapping transaction ids are not unique.", call. = FALSE)
  }
  if (any(!grepl("^[0-9a-f]{16}$", unique(mapping$repeat_id)))) {
    stop("repeat_id values must be lowercase 16-character hex strings.", call. = FALSE)
  }
  group_counts <- data.table::as.data.table(DBI::dbGetQuery(con, paste(
    "SELECT address_key, repeat_id, max(repeat_count) AS repeat_count, min(rn) AS first_rn",
    "FROM grouped GROUP BY address_key, repeat_id ORDER BY first_rn"
  )))
  group_counts[, repeat_count := as.integer(repeat_count)]
  group_counts[, first_rn := NULL]
  repeat_share <- as.numeric(summary$repeat_share)
  if (repeat_share < config$repeat_share_floor) {
    warning(
      sprintf("Repeat share %.6f is below configured floor %.6f.", repeat_share, config$repeat_share_floor),
      call. = FALSE
    )
  }
  property_type_issues <- data.table::data.table()
  if (!is.null(property_type_col)) {
    property_type_issues <- data.table::as.data.table(DBI::dbGetQuery(con, paste(
      "SELECT address_key, repeat_id, count(DISTINCT property_type) AS property_type_count, min(rn) AS first_rn",
      "FROM grouped GROUP BY address_key, repeat_id HAVING count(DISTINCT property_type) > 1 ORDER BY first_rn"
    )))
    property_type_issues[, first_rn := NULL]
    property_type_issues[, property_type_count := as.integer(property_type_count)]
    if (nrow(property_type_issues) > 0L) {
      warning(nrow(property_type_issues), " repeat group(s) contain more than one property type.", call. = FALSE)
    }
  }
  if (same_day_excluded_count > 0L) {
    warning(same_day_excluded_count, " row(s) excluded as same-day repeat conflicts and routed to review.", call. = FALSE)
  }
  large_groups <- group_counts[repeat_count > as.integer(config$large_group_size)]
  threshold <- config$extreme_annualized_price_ratio
  price_ratio_issues <- data.table::as.data.table(DBI::dbGetQuery(con, sprintf(paste(
    "WITH lagged AS (",
    "  SELECT id, address_key, tx_date, price,",
    "    lag(tx_date) OVER w AS previous_date, lag(price) OVER w AS previous_price",
    "  FROM grouped WINDOW w AS (PARTITION BY address_key ORDER BY tx_date, id)",
    "), measured AS (",
    "  SELECT *, CAST(date_diff('day', previous_date, tx_date) AS DOUBLE) AS holding_days,",
    "    CAST(price AS DOUBLE) / CAST(previous_price AS DOUBLE) AS ratio",
    "  FROM lagged",
    "), annualised AS (",
    "  SELECT id, address_key, tx_date, price, previous_date, previous_price, holding_days,",
    "    CASE WHEN ratio IS NOT NULL AND ratio > 0 AND holding_days > 0",
    "         THEN pow(ratio, 365.25 / holding_days) END AS annualized_price_ratio",
    "  FROM measured",
    ")",
    "SELECT * FROM annualised",
    "WHERE annualized_price_ratio IS NOT NULL AND (annualized_price_ratio > %s OR annualized_price_ratio < %s)",
    "ORDER BY address_key, tx_date, id"
  ), format(threshold, digits = 17), format(1 / threshold, digits = 17))))
  if (nrow(price_ratio_issues) > 0L) {
    warning(nrow(price_ratio_issues), " extreme annualized price-ratio pair(s) routed to review.", call. = FALSE)
  }
  rename_review <- function(dt) {
    if (nrow(dt) == 0L && ncol(dt) == 0L) return(dt)
    data.table::setnames(dt, c("id", "tx_date"), c(config$id_col, config$date_col), skip_absent = TRUE)
    if (config$price_col != "price") data.table::setnames(dt, "price", config$price_col, skip_absent = TRUE)
    dt[]
  }
  same_day_conflicts <- data.table::as.data.table(same_day_conflicts)
  same_day_conflicts[, same_day_group_size := as.integer(same_day_group_size)]
  list(
    mapping = mapping[],
    keyed_data = NULL,
    metrics = list(
      input_count = input_count,
      keyed_count = keyed_count,
      excluded_count = as.integer(excluded_count),
      same_day_excluded_count = as.integer(same_day_excluded_count),
      mapped_count = mapped_count,
      key_coverage = key_coverage,
      repeat_share = repeat_share,
      largest_group_size = as.integer(summary$largest)
    ),
    large_groups = large_groups[],
    property_type_issues = property_type_issues[],
    price_ratio_issues = rename_review(price_ratio_issues),
    same_day_conflicts = rename_review(same_day_conflicts)
  )
}

#' Run the configured repeat-transaction pipeline
run_repeat_transactions <- function(config, data = NULL, log_fn = NULL) {
  assert_repeat_dependencies()
  validate_repeat_config(config)
  previous_manifest_path <- config$previous_manifest_path %||% config$output_path
  previous_manifest <- read_repeat_manifest(previous_manifest_path)
  # The loaded input is passed straight in so that build_repeat_mapping() holds
  # the only reference and can release it once the address key is built.
  # A parquet input is keyed row group by row group and grouped in DuckDB when
  # the configuration has no exact-duplicate check (sales). Configurations that
  # check exact duplicates (rentals) and in-memory `data` frames (tests, small
  # runs) use the in-memory builder.
  streamable <- is.null(data) &&
    length(config$duplicate_check_cols %||% character()) == 0L
  result <- withCallingHandlers(
    if (streamable) {
      build_repeat_mapping_streamed(config)
    } else {
      build_repeat_mapping(
        if (is.null(data)) load_repeat_input(config) else data,
        config
      )
    },
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
  if (isTRUE(config$publish)) {
    promote_repeat_outputs(config, manifest, log_fn)
  }
  result$manifest <- manifest
  invisible(result)
}
