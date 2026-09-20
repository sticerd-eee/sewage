# ==============================================================================
# Sales Profile Extension: baseline, source and invariance evidence
# ==============================================================================
#
# Purpose: Gate the 1995-2024 extension of the sales history behind explicit,
#          reproducible evidence. The script preserves a frozen baseline of the
#          current sales artifact set and results bundle, validates the raw
#          year-to-source manifest, compares candidate generations with the
#          baseline on the 2014-2024 overlap, promotes (or rolls back) the
#          sales artifact set as one unit, and compares a regenerated results
#          bundle with the baseline bundle under the plan's invariance rules.
#
# Usage (from the project root, R 4.6.0 `rv` environment):
#   Rscript scripts/R/testing/verify_sales_profile_extension.R <command>
#
# Commands:
#   baseline  Copy the canonical sales artifacts and the results bundle into the
#             baseline directory and freeze them. Refuses to overwrite a frozen
#             baseline.
#   sources   Validate every raw source in the year-to-source manifest and write
#             the source evidence tables. Read-only.
#   overlap   Compare the cleaning candidates with the baseline on the 2014-2024
#             overlap (values and stable ids) and the 2021-2024 study table.
#   mapping   Compare the repeat-mapping candidates with the baseline mapping:
#             identical repeat ids for every baseline transaction; only
#             full-history repeat counts may change.
#   promote   Replace the canonical sales artifact set with the validated
#             candidates as one unit (requires passed overlap and mapping
#             evidence for these exact candidate files).
#   rollback  Restore the canonical sales artifact set from the baseline.
#   bundle    Compare the current results bundle with the baseline bundle:
#             protected scalar cells and rental profiles must reproduce
#             (tolerance 1e-8); expanded sales profiles may differ.
#
# Every command is read-only with respect to canonical data except `promote`
# and `rollback`, which rename whole files and never edit their contents.
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this script.", call. = FALSE)
}

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(here)
})

source(here::here("scripts", "R", "utils", "hash_utils.R"), local = TRUE)

BASELINE_DIR <- here::here(
  "output", "baselines", "sales_profile_extension_2026-09-20"
)
BASELINE_MANIFEST <- "baseline_manifest.json"
EVIDENCE_TOLERANCE <- 1e-8

PROCESSED_DIR <- here::here("data", "processed")
REPEAT_DIR <- file.path(PROCESSED_DIR, "repeated_transactions")
BUNDLE_PATH <- here::here(
  "output", "regs", "long_run_public_attention",
  "long_run_public_attention_bundle.rds"
)

# The sales artifact set that moves as one unit: canonical path and the
# candidate path each generating script stages next to it.
SALES_ARTIFACTS <- tibble::tibble(
  artifact = c(
    "history", "study", "mapping", "large_groups", "price_ratios", "same_day"
  ),
  canonical = c(
    file.path(PROCESSED_DIR, "house_price_long_run.parquet"),
    file.path(PROCESSED_DIR, "house_price.parquet"),
    file.path(REPEAT_DIR, "repeated_sales.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_large_groups.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_price_ratios.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_same_day.parquet")
  ),
  candidate = c(
    file.path(PROCESSED_DIR, "house_price_long_run_candidate.parquet"),
    file.path(PROCESSED_DIR, "house_price_candidate.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_candidate.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_large_groups_candidate.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_price_ratios_candidate.parquet"),
    file.path(REPEAT_DIR, "repeated_sales_same_day_candidate.parquet")
  )
)

# ---- Shared helpers -----------------------------------------------------------

say <- function(...) cat(sprintf(...), "\n", sep = "")

fail <- function(...) stop(sprintf(...), call. = FALSE)

file_hash <- function(path) digest::digest(file = path, algo = "xxhash64")

parquet_rows <- function(path) {
  as.numeric(arrow::ParquetFileReader$create(path)$num_rows)
}

write_json <- function(x, path) {
  jsonlite::write_json(x, path, auto_unbox = TRUE, pretty = TRUE, digits = NA, null = "null", na = "null")
}

read_json <- function(path) jsonlite::read_json(path, simplifyVector = TRUE)

baseline_manifest_path <- function(dir = BASELINE_DIR) file.path(dir, BASELINE_MANIFEST)

read_baseline_manifest <- function(dir = BASELINE_DIR) {
  path <- baseline_manifest_path(dir)
  if (!file.exists(path)) {
    fail("No frozen baseline at %s. Run `baseline` before this command.", dir)
  }
  read_json(path)
}

baseline_artifact_path <- function(artifact, dir = BASELINE_DIR) {
  row <- SALES_ARTIFACTS[SALES_ARTIFACTS$artifact == artifact, ]
  file.path(dir, basename(row$canonical))
}

# Load the cleaner's manifest and cleaning functions without running its main().
load_cleaner <- function() {
  env <- new.env(parent = globalenv())
  sys.source(
    here::here("scripts", "R", "02_data_cleaning", "clean_lr_house_price_data.R"),
    envir = env
  )
  env
}

# Values compared as text so integer/double storage differences are not
# differences; missing values match only missing values.
values_match <- function(old, new) {
  both_missing <- is.na(old) & is.na(new)
  both_present <- !is.na(old) & !is.na(new)
  both_missing | (both_present & as.character(old) == as.character(new))
}

# ---- baseline -----------------------------------------------------------------

cmd_baseline <- function(dir = BASELINE_DIR) {
  if (file.exists(baseline_manifest_path(dir))) {
    fail(
      "Baseline at %s is frozen (%s exists); it is never overwritten. Use another directory only for a new, separately named baseline.",
      dir, BASELINE_MANIFEST
    )
  }
  sources <- c(stats::setNames(SALES_ARTIFACTS$canonical, SALES_ARTIFACTS$artifact), bundle = BUNDLE_PATH)
  absent <- sources[!file.exists(sources)]
  if (length(absent) > 0L) {
    fail("Cannot capture a baseline; missing: %s", paste(absent, collapse = ", "))
  }
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  rows <- lapply(names(sources), function(name) {
    src <- sources[[name]]
    dst <- file.path(dir, basename(src))
    say("copying %s -> %s", name, dst)
    if (!file.copy(src, dst, overwrite = FALSE, copy.date = TRUE)) {
      fail("Copy failed for %s (destination %s may already exist).", src, dst)
    }
    src_hash <- file_hash(src)
    dst_hash <- file_hash(dst)
    if (!identical(src_hash, dst_hash)) {
      fail("Copied %s does not hash like its source (%s vs %s).", name, dst_hash, src_hash)
    }
    list(
      artifact = name,
      source_path = src,
      baseline_path = dst,
      content_hash = src_hash,
      size_bytes = file.info(src)$size,
      n_rows = if (grepl("\\.parquet$", src)) parquet_rows(src) else NA_real_
    )
  })
  names(rows) <- names(sources)
  bundle <- readRDS(BUNDLE_PATH)
  manifest <- list(
    baseline_id = basename(dir),
    captured_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    frozen = TRUE,
    artifacts = rows,
    bundle = list(
      bundle_version = bundle$metadata$bundle_version,
      created_at = bundle$metadata$created_at,
      status = bundle$metadata$status,
      combined_fingerprint = bundle$metadata$fingerprint$combined,
      inputs = as.list(bundle$metadata$fingerprint$inputs),
      n_scalar_cells = nrow(bundle$cells),
      profile_keys = names(bundle$profiles)
    )
  )
  # The frozen record itself is written last: an interrupted capture leaves no
  # manifest, so the next run starts over instead of trusting a partial copy.
  write_json(manifest, baseline_manifest_path(dir))
  Sys.chmod(baseline_manifest_path(dir), mode = "0444")
  say("Baseline frozen at %s (%d files).", dir, length(rows))
  invisible(manifest)
}

# ---- sources ------------------------------------------------------------------

# One read of one raw source: shape, dates, ids and composition. `ids` is
# returned so that global uniqueness can be checked across sources.
inspect_lr_source <- function(path, year, source_id, cleaner) {
  raw <- data.table::fread(
    path, header = FALSE, colClasses = "character", encoding = "UTF-8",
    showProgress = FALSE, na.strings = NULL
  )
  n_cols <- ncol(raw)
  if (n_cols != length(cleaner$CONFIG$column_name_mapping)) {
    fail("Source %s has %d columns; expected %d.", source_id, n_cols, length(cleaner$CONFIG$column_name_mapping))
  }
  cleaned <- cleaner$clean_data(raw, year)
  rm(raw)
  cleaner$validate_lr_year_source(cleaned, year, source_id)
  dates <- cleaned$date_of_transfer
  months <- sort(unique(lubridate::month(dates)))
  composition <- function(col) {
    tab <- table(cleaned[[col]], useNA = "ifany")
    paste(sprintf("%s=%d", names(tab), as.integer(tab)), collapse = ";")
  }
  list(
    row = tibble::tibble(
      year = year,
      source_id = source_id,
      file = basename(path),
      size_bytes = file.info(path)$size,
      mtime_utc = format(file.info(path)$mtime, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      content_hash = file_hash(path),
      n_rows = nrow(cleaned),
      n_columns = n_cols,
      date_min = format(min(dates), "%Y-%m-%d"),
      date_max = format(max(dates), "%Y-%m-%d"),
      months_present = length(months),
      ppd_category = composition("ppd_category"),
      property_type = composition("property_type"),
      record_status = composition("record_status"),
      old_new = composition("old_new"),
      n_missing_postcode = sum(is.na(cleaned$postcode) | !nzchar(cleaned$postcode))
    ),
    ids = cleaned$transaction_id,
    months = months
  )
}

# The manifest never lists the 2014-2017 split files; reading them here is
# evidence about the split convention, not consumption. If each coexisting
# split pair is exactly a partition of its complete annual file, the same
# convention applied to 1995-2013 supports treating those pairs as complete
# annual sources.
split_pair_partition_evidence <- function(input_dir, complete_ids_by_year) {
  years <- names(complete_ids_by_year)
  rows <- lapply(years, function(year) {
    parts <- file.path(input_dir, sprintf("pp-%s-%s.csv", year, c("part1", "part2")))
    if (!all(file.exists(parts))) {
      return(NULL)
    }
    part_ids <- lapply(parts, function(p) {
      data.table::fread(p, header = FALSE, select = 1L, colClasses = "character", showProgress = FALSE)[[1L]]
    })
    complete <- complete_ids_by_year[[year]]
    union_ids <- c(part_ids[[1L]], part_ids[[2L]])
    tibble::tibble(
      year = as.integer(year),
      part1_rows = length(part_ids[[1L]]),
      part2_rows = length(part_ids[[2L]]),
      complete_rows = length(complete),
      parts_overlap = sum(part_ids[[1L]] %chin% part_ids[[2L]]),
      union_equals_complete = length(union_ids) == length(complete) &&
        !anyDuplicated(union_ids) && all(union_ids %chin% complete)
    )
  })
  dplyr::bind_rows(rows)
}

cmd_sources <- function(dir = BASELINE_DIR) {
  cleaner <- load_cleaner()
  input_dir <- cleaner$CONFIG$input_dir
  manifest <- cleaner$validate_expected_lr_files(input_dir, cleaner$CONFIG$years)
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  say("Validating %d sources for %d years from %s", nrow(manifest), length(unique(manifest$year)), input_dir)

  rows <- list()
  ids <- list()
  months_by_year <- list()
  complete_ids_by_year <- list()
  for (i in seq_len(nrow(manifest))) {
    m <- manifest[i, ]
    say("  [%d/%d] %s", i, nrow(manifest), m$file)
    res <- inspect_lr_source(m$path, m$year, m$source_id, cleaner)
    rows[[i]] <- dplyr::mutate(res$row, kind = m$kind, .after = "source_id")
    ids[[m$source_id]] <- res$ids
    key <- as.character(m$year)
    months_by_year[[key]] <- sort(unique(c(months_by_year[[key]], res$months)))
    if (m$kind == "complete_annual" && m$year %in% 2014:2017) {
      complete_ids_by_year[[key]] <- res$ids
    }
  }
  files <- dplyr::bind_rows(rows)

  all_ids <- unlist(ids, use.names = FALSE)
  dup <- duplicated(all_ids)
  if (any(dup)) {
    offenders <- unique(all_ids[dup])
    where <- vapply(ids, function(x) sum(x %chin% offenders), integer(1))
    fail(
      "%d transaction id(s) appear in more than one source: %s",
      length(offenders),
      paste(sprintf("%s (%d)", names(where)[where > 0L], where[where > 0L]), collapse = ", ")
    )
  }
  rm(all_ids, ids)

  years <- files |>
    dplyr::group_by(year, kind) |>
    dplyr::summarise(
      n_sources = dplyr::n(),
      sources = paste(source_id, collapse = "+"),
      n_rows = sum(n_rows),
      date_min = min(date_min),
      date_max = max(date_max),
      .groups = "drop"
    ) |>
    dplyr::mutate(months_present = vapply(as.character(year), function(y) length(months_by_year[[y]]), integer(1)))
  split_years <- years$year[years$kind == "split_part"]
  bad_parts <- years$year[years$kind == "split_part" & years$n_sources != 2L]
  if (length(bad_parts) > 0L) {
    fail("Split years without exactly two parts: %s", paste(bad_parts, collapse = ", "))
  }
  partition <- split_pair_partition_evidence(input_dir, complete_ids_by_year)

  utils::write.csv(files, file.path(dir, "source_files.csv"), row.names = FALSE)
  utils::write.csv(years, file.path(dir, "source_years.csv"), row.names = FALSE)
  utils::write.csv(partition, file.path(dir, "source_split_partition_evidence.csv"), row.names = FALSE)
  summary <- list(
    validated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    input_dir = input_dir,
    n_sources = nrow(files),
    n_years = nrow(years),
    n_rows_total = sum(files$n_rows),
    ids_globally_unique = TRUE,
    every_source_within_its_year = TRUE,
    split_years = split_years,
    split_pairs_partition_complete_files = if (nrow(partition) > 0L) all(partition$union_equals_complete) else NA,
    split_partition_years_checked = partition$year,
    years_with_twelve_months = years$year[years$months_present == 12L],
    provenance_note = paste(
      "1995-2013 split files carry the download mtimes recorded in source_files.csv;",
      "2014-2024 complete files are the vintage consumed by the existing cleaned history.",
      "The combined archive is not one download session and is not described as one."
    )
  )
  write_json(summary, file.path(dir, "source_validation.json"))
  print(as.data.frame(years[, c("year", "kind", "n_rows", "date_min", "date_max", "months_present")]), row.names = FALSE)
  if (nrow(partition) > 0L) print(as.data.frame(partition), row.names = FALSE)
  say("Sources validated: %d files, %s rows, ids globally unique; evidence written to %s", nrow(files), format(sum(files$n_rows), big.mark = ","), dir)
  invisible(summary)
}

# ---- overlap ------------------------------------------------------------------

# Read one column set from a parquet file as a data.table with materialized
# vectors (no Arrow ALTREP), sorted by the stable id.
read_sorted <- function(path, columns, id_col = "house_id") {
  dt <- arrow::read_parquet(path, col_select = tidyselect::all_of(columns)) |>
    as.data.frame() |>
    data.table::as.data.table()
  data.table::setorderv(dt, id_col)
  dt[]
}

compare_frames <- function(old, new, id_col, label, exclude = character()) {
  if (!identical(old[[id_col]], new[[id_col]])) {
    fail("%s: %s sets differ (baseline %d rows, candidate %d rows, %d baseline ids missing from the candidate).",
         label, id_col, nrow(old), nrow(new), sum(!old[[id_col]] %chin% new[[id_col]]))
  }
  cols <- setdiff(intersect(names(old), names(new)), c(id_col, exclude))
  missing_cols <- setdiff(setdiff(names(old), exclude), names(new))
  if (length(missing_cols) > 0L) {
    fail("%s: candidate lacks baseline column(s): %s", label, paste(missing_cols, collapse = ", "))
  }
  differences <- vapply(cols, function(col) sum(!values_match(old[[col]], new[[col]])), integer(1))
  tibble::tibble(comparison = label, column = cols, n_rows = nrow(old), n_differences = differences)
}

cmd_overlap <- function(dir = BASELINE_DIR) {
  baseline <- read_baseline_manifest(dir)
  candidate_history <- SALES_ARTIFACTS$candidate[SALES_ARTIFACTS$artifact == "history"]
  candidate_study <- SALES_ARTIFACTS$candidate[SALES_ARTIFACTS$artifact == "study"]
  for (p in c(candidate_history, candidate_study)) {
    if (!file.exists(p)) fail("Cleaning candidate missing: %s", p)
  }
  baseline_history <- baseline_artifact_path("history", dir)
  baseline_study <- baseline_artifact_path("study", dir)
  if (!identical(file_hash(baseline_history), baseline$artifacts$history$content_hash)) {
    fail("Baseline history copy no longer matches its frozen hash.")
  }

  schema_old <- arrow::ParquetFileReader$create(baseline_history)$GetSchema()
  schema_new <- arrow::ParquetFileReader$create(candidate_history)$GetSchema()
  if (!identical(names(schema_old), names(schema_new))) {
    fail("Candidate history columns differ from the baseline: %s vs %s",
         paste(names(schema_new), collapse = ","), paste(names(schema_old), collapse = ","))
  }
  columns <- names(schema_old)
  say("Comparing the 2014-2024 overlap on %d columns by stable house_id...", length(columns))
  # Column blocks keep the two 30M-row generations out of memory at once.
  blocks <- split(setdiff(columns, "house_id"), ceiling(seq_along(setdiff(columns, "house_id")) / 6))
  old_ids <- read_sorted(baseline_history, "house_id")$house_id
  new_years <- arrow::open_dataset(candidate_history) |>
    dplyr::mutate(year = lubridate::year(date_of_transfer)) |>
    dplyr::count(year) |>
    dplyr::collect() |>
    dplyr::arrange(year)
  new_overlap_ids <- arrow::open_dataset(candidate_history) |>
    dplyr::filter(date_of_transfer >= as.POSIXct("2014-01-01", tz = "UTC")) |>
    dplyr::select(house_id) |>
    dplyr::collect() |>
    as.data.frame()
  new_overlap_ids <- sort(new_overlap_ids$house_id)
  if (!identical(old_ids, new_overlap_ids)) {
    fail("Candidate 2014-2024 ids differ from the baseline: baseline %d, candidate %d, %d baseline ids absent.",
         length(old_ids), length(new_overlap_ids), sum(!old_ids %chin% new_overlap_ids))
  }
  rm(new_overlap_ids)
  results <- list()
  for (block in blocks) {
    old <- read_sorted(baseline_history, c("house_id", block))
    new <- arrow::open_dataset(candidate_history) |>
      dplyr::filter(date_of_transfer >= as.POSIXct("2014-01-01", tz = "UTC")) |>
      dplyr::select(dplyr::all_of(c("house_id", block))) |>
      dplyr::collect() |>
      as.data.frame() |>
      data.table::as.data.table()
    data.table::setorderv(new, "house_id")
    results[[length(results) + 1L]] <- compare_frames(old, new, "house_id", "history_overlap_2014_2024")
    rm(old, new)
    invisible(gc())
  }
  old <- read_sorted(baseline_study, columns)
  new <- read_sorted(candidate_study, columns)
  results[[length(results) + 1L]] <- compare_frames(old, new, "house_id", "study_2021_2024")
  rm(old, new)
  comparison <- dplyr::bind_rows(results)

  new_meta <- arrow::ParquetFileReader$create(candidate_history)$GetSchema()$metadata
  differing <- comparison[comparison$n_differences > 0L, ]
  status <- if (nrow(differing) == 0L) "passed" else "failed"
  evidence <- list(
    checked_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    status = status,
    candidate_history_hash = file_hash(candidate_history),
    candidate_study_hash = file_hash(candidate_study),
    candidate_generation_id = new_meta$cleaning_run_stamp,
    candidate_rows_by_year = new_years,
    n_overlap_rows = length(old_ids),
    comparison = comparison
  )
  utils::write.csv(comparison, file.path(dir, "overlap_comparison.csv"), row.names = FALSE)
  write_json(evidence, file.path(dir, "overlap_evidence.json"))
  print(as.data.frame(new_years), row.names = FALSE)
  if (status != "passed") {
    print(as.data.frame(differing), row.names = FALSE)
    fail("Overlap comparison failed: %d column(s) differ. The candidate is not promotable.", nrow(differing))
  }
  say("Overlap passed: %s 2014-2024 rows identical on %d columns; study table identical.", format(length(old_ids), big.mark = ","), length(columns) - 1L)
  invisible(evidence)
}

# ---- mapping ------------------------------------------------------------------

cmd_mapping <- function(dir = BASELINE_DIR) {
  baseline <- read_baseline_manifest(dir)
  cand <- stats::setNames(SALES_ARTIFACTS$candidate, SALES_ARTIFACTS$artifact)
  for (a in c("mapping", "same_day", "price_ratios", "large_groups")) {
    if (!file.exists(cand[[a]])) fail("Mapping candidate missing: %s", cand[[a]])
  }
  overlap_path <- file.path(dir, "overlap_evidence.json")
  if (!file.exists(overlap_path)) fail("Run `overlap` before `mapping`.")
  overlap <- read_json(overlap_path)
  if (!identical(overlap$status, "passed")) fail("Overlap evidence is not passed.")
  candidate_history <- SALES_ARTIFACTS$candidate[SALES_ARTIFACTS$artifact == "history"]
  if (!identical(file_hash(candidate_history), overlap$candidate_history_hash)) {
    fail("The history candidate changed since the overlap check; rerun `overlap`.")
  }
  old <- read_sorted(baseline_artifact_path("mapping", dir), c("house_id", "repeat_id", "repeat_count"))
  new <- read_sorted(cand[["mapping"]], c("house_id", "repeat_id", "repeat_count"))
  idx <- match(old$house_id, new$house_id)
  if (anyNA(idx)) {
    fail("%d baseline mapping ids are absent from the candidate mapping.", sum(is.na(idx)))
  }
  n_repeat_id_changed <- sum(old$repeat_id != new$repeat_id[idx])
  if (n_repeat_id_changed > 0L) {
    fail("%d baseline transactions received a different repeat_id in the candidate mapping.", n_repeat_id_changed)
  }
  count_delta <- new$repeat_count[idx] - old$repeat_count
  if (any(count_delta < 0L)) {
    fail("%d baseline transactions have a smaller full-history repeat_count in the candidate.", sum(count_delta < 0L))
  }
  # Same-day conflicts within the overlap are date-local and must be unchanged.
  old_same_day <- read_sorted(baseline_artifact_path("same_day", dir), "house_id")$house_id
  new_same_day <- read_sorted(cand[["same_day"]], "house_id")$house_id
  overlap_ids <- arrow::open_dataset(candidate_history) |>
    dplyr::filter(date_of_transfer >= as.POSIXct("2014-01-01", tz = "UTC")) |>
    dplyr::select(house_id) |>
    dplyr::collect()
  overlap_ids <- as.character(overlap_ids$house_id)
  new_same_day_overlap <- new_same_day[new_same_day %chin% overlap_ids]
  if (!setequal(old_same_day, new_same_day_overlap)) {
    fail("Same-day conflicts within 2014-2024 changed (baseline %d, candidate %d).", length(old_same_day), length(new_same_day_overlap))
  }
  new_overlap_mapped <- sum(new$house_id %chin% overlap_ids)
  if (new_overlap_mapped != nrow(old)) {
    fail("Candidate maps %d overlap transactions; baseline mapped %d.", new_overlap_mapped, nrow(old))
  }
  manifest <- arrow::ParquetFileReader$create(cand[["mapping"]])$GetSchema()$metadata
  evidence <- list(
    checked_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    status = "passed",
    candidate_hashes = lapply(cand[c("mapping", "large_groups", "price_ratios", "same_day")], file_hash),
    candidate_history_hash = overlap$candidate_history_hash,
    candidate_study_hash = overlap$candidate_study_hash,
    candidate_generation_id = manifest$run_timestamp,
    n_baseline_mapped = nrow(old),
    n_candidate_mapped = nrow(new),
    n_repeat_id_changed = n_repeat_id_changed,
    n_repeat_count_increased = sum(count_delta > 0L),
    max_repeat_count_increase = max(count_delta),
    n_same_day_overlap = length(new_same_day_overlap),
    n_same_day_total = length(new_same_day),
    candidate_manifest = as.list(manifest)
  )
  write_json(evidence, file.path(dir, "mapping_evidence.json"))
  say("Mapping passed: %s baseline transactions keep their repeat_id; %s gained history (max +%d); same-day overlap unchanged (%d); candidate maps %s rows.",
      format(nrow(old), big.mark = ","), format(sum(count_delta > 0L), big.mark = ","), max(count_delta), length(old_same_day), format(nrow(new), big.mark = ","))
  invisible(evidence)
}

# ---- promote / rollback ---------------------------------------------------------

cmd_promote <- function(dir = BASELINE_DIR) {
  baseline <- read_baseline_manifest(dir)
  mapping_path <- file.path(dir, "mapping_evidence.json")
  if (!file.exists(mapping_path)) fail("Run `overlap` and `mapping` before `promote`.")
  evidence <- read_json(mapping_path)
  if (!identical(evidence$status, "passed")) fail("Mapping evidence is not passed.")
  cand <- stats::setNames(SALES_ARTIFACTS$candidate, SALES_ARTIFACTS$artifact)
  canon <- stats::setNames(SALES_ARTIFACTS$canonical, SALES_ARTIFACTS$artifact)
  expected <- c(
    history = evidence$candidate_history_hash,
    study = evidence$candidate_study_hash,
    unlist(evidence$candidate_hashes)
  )
  for (a in names(canon)) {
    if (!file.exists(cand[[a]])) fail("Candidate missing for %s: %s", a, cand[[a]])
    if (!identical(file_hash(cand[[a]]), expected[[a]])) {
      fail("Candidate %s changed since its evidence was recorded; rerun the checks.", a)
    }
    if (!identical(file_hash(canon[[a]]), baseline$artifacts[[a]]$content_hash)) {
      fail("Canonical %s no longer matches the frozen baseline; refusing to promote over an unknown generation.", a)
    }
    if (!file.exists(baseline_artifact_path(a, dir))) fail("Baseline copy missing for %s.", a)
  }
  # Six renames back to back: every check above ran first, and the baseline
  # copies make the previous generation recoverable with `rollback`.
  for (a in names(canon)) {
    if (!file.rename(cand[[a]], canon[[a]])) fail("Failed to promote %s onto %s.", cand[[a]], canon[[a]])
    say("promoted %s -> %s", basename(cand[[a]]), basename(canon[[a]]))
  }
  write_json(
    list(promoted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"), hashes = as.list(expected)),
    file.path(dir, "promotion_record.json")
  )
  say("Sales artifact set promoted as one unit. Previous generation recoverable with `rollback`.")
}

cmd_rollback <- function(dir = BASELINE_DIR) {
  baseline <- read_baseline_manifest(dir)
  canon <- stats::setNames(SALES_ARTIFACTS$canonical, SALES_ARTIFACTS$artifact)
  for (a in names(canon)) {
    src <- baseline_artifact_path(a, dir)
    if (!identical(file_hash(src), baseline$artifacts[[a]]$content_hash)) fail("Baseline copy of %s is corrupt.", a)
  }
  for (a in names(canon)) {
    src <- baseline_artifact_path(a, dir)
    if (!file.copy(src, canon[[a]], overwrite = TRUE, copy.date = TRUE)) fail("Failed to restore %s.", a)
    say("restored %s", basename(canon[[a]]))
  }
  say("Canonical sales artifact set restored from the baseline.")
}

# ---- bundle -------------------------------------------------------------------

PROTECTED_CELL_EXACT <- c(
  "model_key", "market", "variant", "fe", "attention", "status", "reason", "term",
  "nobs", "n_properties", "n_clusters", "n_near_properties", "n_far_properties",
  "n_properties_both_sides_event", "n_removed_by_estimator", "n_controls_absorbed"
)
PROTECTED_CELL_NUMERIC <- c("estimate", "std_error", "t_df", "conf_low", "conf_high", "p_value")

numeric_equal <- function(a, b, tolerance = EVIDENCE_TOLERANCE) {
  a <- as.numeric(a)
  b <- as.numeric(b)
  same_na <- is.na(a) == is.na(b)
  ok <- same_na
  both <- !is.na(a) & !is.na(b)
  ok[both] <- abs(a[both] - b[both]) <= tolerance * pmax(1, abs(a[both]))
  ok
}

# Sample ids retained by a fitted key, expanded from the compact bundle storage.
bundle_retained_ids <- function(bundle, market, key, expand) {
  ms <- bundle$samples[[market]]
  ms$ids[expand(ms, key)]
}

cmd_bundle <- function(dir = BASELINE_DIR) {
  baseline <- read_baseline_manifest(dir)
  old <- readRDS(file.path(dir, basename(BUNDLE_PATH)))
  new <- readRDS(BUNDLE_PATH)
  if (identical(old$metadata$fingerprint$combined, new$metadata$fingerprint$combined)) {
    fail("The current bundle is the baseline bundle itself (same fingerprint); nothing to compare.")
  }
  # The report's expansion helper is defined in the QMD; reimplement the storage
  # contract locally so this script does not depend on knitting the report.
  expand <- function(ms, key) {
    e <- ms$estimated[[key]]
    if (is.null(e) || e$n_retained == 0L) return(integer())
    if (!is.null(e$retained)) return(e$retained)
    variant <- strsplit(key, "__", fixed = TRUE)[[1L]][[2L]]
    input <- ms$variants[[variant]]
    input[!(input %in% e$removed)]
  }
  findings <- list()
  note <- function(check, passed, detail = "") {
    findings[[length(findings) + 1L]] <<- tibble::tibble(check = check, passed = passed, detail = detail)
  }

  # 1. Scalar cells: exact identity columns and statuses, numeric to tolerance.
  oc <- dplyr::arrange(old$cells, model_key)
  nc <- dplyr::arrange(new$cells, model_key)
  note("scalar_keys_identical", identical(oc$model_key, nc$model_key), sprintf("%d vs %d keys", nrow(oc), nrow(nc)))
  for (col in PROTECTED_CELL_EXACT) {
    ok <- all(values_match(oc[[col]], nc[[col]]))
    note(paste0("scalar_exact:", col), ok, if (ok) "" else paste(oc$model_key[!values_match(oc[[col]], nc[[col]])], collapse = ", "))
  }
  for (col in PROTECTED_CELL_NUMERIC) {
    ok <- numeric_equal(oc[[col]], nc[[col]])
    note(paste0("scalar_numeric:", col), all(ok), if (all(ok)) "" else paste(oc$model_key[!ok], collapse = ", "))
  }
  # 2. Scalar samples: retained ids per key and variant inputs per market.
  for (m in names(old$samples)) {
    os <- old$samples[[m]]
    ns <- new$samples[[m]]
    for (v in names(os$variants)) {
      ok <- identical(os$ids[os$variants[[v]]], ns$ids[ns$variants[[v]]])
      note(sprintf("scalar_sample_ids:%s:%s", m, v), ok, sprintf("%d vs %d rows", length(os$variants[[v]]), length(ns$variants[[v]])))
    }
    for (key in oc$model_key[oc$market == m]) {
      ok <- identical(bundle_retained_ids(old, m, key, expand), bundle_retained_ids(new, m, key, expand))
      note(paste0("scalar_retained_ids:", key), ok)
    }
    for (v in names(os$metadata)) {
      ok <- identical(os$metadata[[v]][c("n_rows", "n_properties")], ns$metadata[[v]][c("n_rows", "n_properties")])
      note(sprintf("scalar_sample_metadata:%s:%s", m, v), ok)
    }
  }
  # 3. Scalar model settings: formulas, controls, fixed-effect sizes.
  for (key in oc$model_key) {
    om <- old$models[[key]]
    nm <- new$models[[key]]
    ok <- identical(om$formula, nm$formula) && identical(om$controls, nm$controls) &&
      identical(om$controls_absorbed, nm$controls_absorbed) && identical(om$status, nm$status) &&
      isTRUE(all.equal(om$fixef_sizes, nm$fixef_sizes)) && identical(om$collin_var, nm$collin_var)
    note(paste0("scalar_model_settings:", key), ok)
  }
  # 4. Rental profiles: coefficients, covariance, df, joint tests to tolerance; sales profiles may change.
  for (key in grep("^rentals__", names(old$profiles), value = TRUE)) {
    op <- old$profiles[[key]]
    np <- new$profiles[[key]]
    note(paste0("rental_profile_status:", key), identical(op$status, np$status) && identical(op$reference_year, np$reference_year))
    ok <- identical(op$coefficients$year, np$coefficients$year) &&
      all(numeric_equal(op$coefficients$estimate, np$coefficients$estimate)) &&
      all(numeric_equal(op$coefficients$std_error, np$coefficients$std_error)) &&
      all(numeric_equal(op$coefficients$conf_low, np$coefficients$conf_low)) &&
      all(numeric_equal(op$coefficients$conf_high, np$coefficients$conf_high)) &&
      identical(op$coefficients$available, np$coefficients$available)
    note(paste0("rental_profile_coefficients:", key), ok)
    ok <- (is.null(op$vcov) && is.null(np$vcov)) ||
      (!is.null(op$vcov) && !is.null(np$vcov) && identical(dimnames(op$vcov), dimnames(np$vcov)) &&
         all(numeric_equal(as.numeric(op$vcov), as.numeric(np$vcov))))
    note(paste0("rental_profile_vcov:", key), ok)
    note(paste0("rental_profile_df_counts:", key),
         numeric_equal(op$t_df, np$t_df) && identical(op$nobs, np$nobs) && identical(op$n_properties, np$n_properties) && identical(op$n_clusters, np$n_clusters))
    oj <- op$joint_test
    nj <- np$joint_test
    note(paste0("rental_joint_test:", key),
         identical(oj$status, nj$status) && identical(oj$years, nj$years) && numeric_equal(oj$statistic, nj$statistic) &&
           numeric_equal(oj$p_value, nj$p_value) && numeric_equal(oj$df2, nj$df2) && identical(oj$df1, nj$df1))
    ok <- identical(bundle_retained_ids(old, "rentals", key, expand), bundle_retained_ids(new, "rentals", key, expand))
    note(paste0("rental_profile_retained_ids:", key), ok)
  }
  # 5. Sales profiles are expected to change: recorded, not asserted.
  sales_profile_summary <- dplyr::bind_rows(lapply(grep("^sales__", names(new$profiles), value = TRUE), function(key) {
    np <- new$profiles[[key]]
    op <- old$profiles[[key]]
    tibble::tibble(
      model_key = key, status = np$status, nobs = np$nobs, n_properties = np$n_properties,
      years = paste(range(np$coefficients$year), collapse = "-"),
      baseline_nobs = op$nobs, joint_test = np$joint_test$status,
      joint_years = paste(range(np$joint_test$years), collapse = "-")
    )
  }))
  # 6. Exclusion ledger: scalar-panel rows identical for both markets.
  ol <- old$exclusions$ledger
  nl <- new$exclusions$ledger
  if ("panel" %in% names(nl)) nl <- nl[is.na(nl$panel) | nl$panel == "scalar", ]
  ledger_cols <- c("market", "step", "n_rows_before", "n_rows_after", "n_rows_removed", "n_groups_before", "n_groups_after")
  note("scalar_exclusion_ledger", nrow(ol) == nrow(nl) && all(vapply(ledger_cols, function(c) all(values_match(ol[[c]], nl[[c]])), logical(1))),
       sprintf("%d vs %d rows", nrow(ol), nrow(nl)))
  # 7. Rental inputs unchanged; sales inputs expected to change.
  oi <- old$metadata$fingerprint$inputs
  ni <- new$metadata$fingerprint$inputs
  unchanged <- setdiff(names(oi), c("sales_history", "sales_mapping", "sales_same_day_review"))
  note("non_sales_inputs_unchanged", all(oi[unchanged] == ni[unchanged]), paste(unchanged[oi[unchanged] != ni[unchanged]], collapse = ", "))
  note("sales_inputs_changed", all(oi[c("sales_history", "sales_mapping")] != ni[c("sales_history", "sales_mapping")]))
  note("bundle_version_incremented", !identical(old$metadata$bundle_version, new$metadata$bundle_version),
       sprintf("%s -> %s", old$metadata$bundle_version, new$metadata$bundle_version))

  report <- dplyr::bind_rows(findings)
  failed <- report[!report$passed, ]
  utils::write.csv(report, file.path(dir, "bundle_comparison.csv"), row.names = FALSE)
  utils::write.csv(sales_profile_summary, file.path(dir, "bundle_sales_profiles.csv"), row.names = FALSE)
  write_json(
    list(
      checked_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      status = if (nrow(failed) == 0L) "passed" else "failed",
      baseline_fingerprint = old$metadata$fingerprint$combined,
      current_fingerprint = new$metadata$fingerprint$combined,
      n_checks = nrow(report), n_failed = nrow(failed),
      tolerance = EVIDENCE_TOLERANCE
    ),
    file.path(dir, "bundle_evidence.json")
  )
  print(as.data.frame(sales_profile_summary), row.names = FALSE)
  if (nrow(failed) > 0L) {
    print(as.data.frame(failed), row.names = FALSE)
    fail("Bundle comparison failed: %d of %d checks. Protected results did not reproduce; diagnose before accepting.", nrow(failed), nrow(report))
  }
  say("Bundle comparison passed: %d checks; 40 scalar cells, their samples and settings, both rental profiles and the scalar ledger reproduce the baseline (tolerance %g).", nrow(report), EVIDENCE_TOLERANCE)
  invisible(report)
}

# ---- Dispatch -------------------------------------------------------------------

main <- function(args = commandArgs(trailingOnly = TRUE)) {
  commands <- list(
    baseline = cmd_baseline, sources = cmd_sources, overlap = cmd_overlap,
    mapping = cmd_mapping, promote = cmd_promote, rollback = cmd_rollback, bundle = cmd_bundle
  )
  if (length(args) < 1L || !args[[1L]] %in% names(commands)) {
    fail("Usage: Rscript scripts/R/testing/verify_sales_profile_extension.R <%s>", paste(names(commands), collapse = "|"))
  }
  dir <- BASELINE_DIR
  dir_arg <- grep("^--baseline-dir=", args, value = TRUE)
  if (length(dir_arg) == 1L) dir <- sub("^--baseline-dir=", "", dir_arg)
  commands[[args[[1L]]]](dir)
}

if (sys.nframe() == 0L) main()
