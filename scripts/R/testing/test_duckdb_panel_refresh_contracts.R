# ==============================================================================
# DuckDB Panel Input Refresh Contract Tests
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(DBI)
  library(dbplyr)
  library(dplyr)
  library(duckdb)
  library(here)
  library(logger)
  library(lubridate)
})

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

relation_type <- function(con, table_name) {
  DBI::dbGetQuery(
    con,
    paste(
      "SELECT table_type FROM information_schema.tables",
      "WHERE table_schema = 'main' AND table_name = ?"
    ),
    params = list(table_name)
  )$table_type
}

write_fixture_inputs <- function(processed_dir, market, ids) {
  if (identical(market, "sales")) {
    arrow::write_parquet(
      data.frame(
        house_id = ids,
        price = c(100000L, 200000L),
        date_of_transfer = as.POSIXct(
          c("2021-01-01", "2022-01-01"), tz = "UTC"
        ),
        qtr_id = c(1, 5)
      ),
      file.path(processed_dir, "house_price.parquet")
    )
    arrow::write_parquet(
      data.frame(
        house_id = ids,
        site_id = c(10L, 20L),
        distance_m = c(100, 200)
      ),
      file.path(processed_dir, "spill_house_lookup.parquet")
    )
  } else {
    zoopla_dir <- file.path(processed_dir, "zoopla")
    dir.create(zoopla_dir, recursive = TRUE, showWarnings = FALSE)
    arrow::write_parquet(
      data.frame(
        rental_id = ids,
        listing_price = c(1000, 2000),
        rented_est = as.POSIXct(
          c("2021-01-01", "2022-01-01"), tz = "UTC"
        ),
        qtr_id = c(1, 5)
      ),
      file.path(zoopla_dir, "zoopla_rentals.parquet")
    )
    arrow::write_parquet(
      data.frame(
        rental_id = ids,
        site_id = c(10L, 20L),
        distance_m = c(100, 200)
      ),
      file.path(zoopla_dir, "spill_rental_lookup.parquet")
    )
  }
}

producer_specs <- list(
  list(
    file = "house_panel_within_radius.R",
    market = "sales",
    source_table = "house_price_data",
    lookup_table = "spill_lookup",
    id = "house_id",
    processed_dir = function(root) root
  ),
  list(
    file = "rental_panel_within_radius.R",
    market = "rentals",
    source_table = "rental_data",
    lookup_table = "rental_spill_lookup",
    id = "rental_id",
    processed_dir = function(root) file.path(root, "zoopla")
  ),
  list(
    file = "sale_panel_exp.R",
    market = "sales",
    source_table = "house_price_data",
    lookup_table = "spill_lookup",
    id = "house_id",
    processed_dir = function(root) root
  ),
  list(
    file = "rental_panel_exp.R",
    market = "rentals",
    source_table = "rental_data",
    lookup_table = "rental_spill_lookup",
    id = "rental_id",
    processed_dir = function(root) root
  )
)

for (spec in producer_specs) {
  fixture_root <- tempfile(paste0("panel-refresh-", spec$market, "-"))
  dir.create(fixture_root, recursive = TRUE)
  on.exit(unlink(fixture_root, recursive = TRUE), add = TRUE)
  write_fixture_inputs(fixture_root, spec$market, c("001", "alpha"))

  env <- new.env(parent = globalenv())
  sys.source(
    here::here(
      "scripts", "R", "06_analysis_datasets", spec$file
    ),
    envir = env
  )
  env$CONFIG$processed_dir <- spec$processed_dir(fixture_root)
  env$CONFIG$db_path <- file.path(fixture_root, "fixture.duckdb")

  configured_con <- env$connect_to_db()
  configured_temp_dir <- DBI::dbGetQuery(
    configured_con,
    "SELECT value FROM duckdb_settings() WHERE name = 'temp_directory'"
  )$value
  DBI::dbDisconnect(configured_con, shutdown = TRUE)
  expected_temp_dir <- file.path(fixture_root, "duckdb_temp")
  assert_true(
    dir.exists(expected_temp_dir) &&
      identical(
        normalizePath(configured_temp_dir, mustWork = TRUE),
        normalizePath(expected_temp_dir, mustWork = TRUE)
      ),
    paste(spec$file, "must configure an existing, stable DuckDB spill directory.")
  )

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  stale_ids <- stats::setNames(data.frame(1L), spec$id)
  DBI::dbWriteTable(con, spec$source_table, stale_ids)
  DBI::dbWriteTable(con, spec$lookup_table, stale_ids)
  DBI::dbWriteTable(con, "unrelated_table", data.frame(value = 42L))

  env$load_data_to_db(con)

  source_ids <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ", DBI::dbQuoteIdentifier(con, spec$id),
      " FROM ", DBI::dbQuoteIdentifier(con, spec$source_table),
      " ORDER BY 1"
    )
  )[[spec$id]]
  lookup_ids <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ", DBI::dbQuoteIdentifier(con, spec$id),
      " FROM ", DBI::dbQuoteIdentifier(con, spec$lookup_table),
      " ORDER BY 1"
    )
  )[[spec$id]]

  assert_true(
    identical(source_ids, c("001", "alpha")),
    paste(spec$file, "must replace a stale source table with current utf8 IDs.")
  )
  assert_true(
    identical(lookup_ids, c("001", "alpha")),
    paste(spec$file, "must replace a stale lookup table with current utf8 IDs.")
  )
  assert_true(
    identical(relation_type(con, spec$source_table), "VIEW") &&
      identical(relation_type(con, spec$lookup_table), "VIEW"),
    paste(spec$file, "must expose refreshed inputs as parquet-backed views.")
  )
  assert_true(
    DBI::dbExistsTable(con, "unrelated_table") &&
      identical(DBI::dbGetQuery(con, "SELECT value FROM unrelated_table")$value, 42L),
    paste(spec$file, "must preserve unrelated persistent DuckDB tables.")
  )

  write_fixture_inputs(fixture_root, spec$market, c("009", "beta"))
  env$load_data_to_db(con)
  refreshed_ids <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ", DBI::dbQuoteIdentifier(con, spec$id),
      " FROM ", DBI::dbQuoteIdentifier(con, spec$source_table),
      " ORDER BY 1"
    )
  )[[spec$id]]
  assert_true(
    identical(refreshed_ids, c("009", "beta")),
    paste(spec$file, "must refresh an existing parquet-backed view every run.")
  )

  if (grepl("within_radius", spec$file, fixed = TRUE)) {
    prepared <- env$prepare_tables(con)
    prepared_source <- if (identical(spec$market, "sales")) {
      prepared$house_tbl
    } else {
      prepared$rental_tbl
    }
    period_probe <- tryCatch(
      prepared_source |>
        dplyr::select(dplyr::any_of(c("year", "month", "quarter"))) |>
        utils::head(1L) |>
        dplyr::collect(),
      error = function(error) error
    )
    assert_true(
      !inherits(period_probe, "error"),
      paste(
        spec$file,
        "must derive periods from parquet timestamps without requiring DuckDB ICU.",
        if (inherits(period_probe, "error")) conditionMessage(period_probe) else ""
      )
    )
  }

  DBI::dbDisconnect(con, shutdown = TRUE)
}

cat("All DuckDB panel refresh contract tests passed.\n")
