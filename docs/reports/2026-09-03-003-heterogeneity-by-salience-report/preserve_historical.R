# Explicit preservation command; never sourced or executed by a report render.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report",
                  "report_storage.R"))
source(here::here("scripts", "R", "utils", "salience_group_utils.R"))

validate_historical_bundle <- function(bundle, family) {
  if (family == "overlapping") {
    stopifnot(isTRUE(bundle$settings$groups_overlap),
              identical(bundle$settings$london, "excluded"),
              bundle$settings$config$coast_rule_m == 2000L,
              setequal(names(bundle$models), c("sales", "rentals")),
              nrow(bundle$counts) == 6L,
              !anyDuplicated(bundle$counts[c("market", "group")]))
    for (i in seq_len(nrow(bundle$counts))) {
      row <- bundle$counts[i, ]
      model <- bundle$models[[row$market]][[row$group]]
      stopifnot(stats::nobs(model) == row$nobs)
      saved <- bundle$results[bundle$results$market == row$market &
                              bundle$results$group == row$group, ]
      actual <- salience_group_results(model)
      actual <- actual[match(saved$term, actual$term), ]
      stopifnot(nrow(saved) > 0L,
                isTRUE(all.equal(as.data.frame(saved[names(actual)]),
                                 as.data.frame(actual), check.attributes = FALSE)))
    }
  } else if (family == "four_strata_intensity") {
    stopifnot(all(bundle$reproduction$passed),
              all(bundle$variants$unknown_policy %in% c("exclude", "not_designated")),
              all(bundle$variants$coast_rule_m %in% c(2000, 10000)))
    for (i in seq_len(nrow(bundle$counts))) {
      row <- bundle$counts[i, ]
      stopifnot(stats::nobs(bundle$models[[row$variant]][[row$market]][[row$stratum]]) == row$nobs)
    }
  } else {
    stopifnot(family == "exclusive", nrow(bundle$results) == 30L,
              all(bundle$reproduction$passed), isTRUE(bundle$settings$drop_london),
              bundle$settings$coast_rule_m == 2000L,
              !anyDuplicated(bundle$results[c("analysis", "market", "stratum")]))
    for (i in seq_len(nrow(bundle$results))) {
      row <- bundle$results[i, ]
      stopifnot(stats::nobs(bundle$models[[row$analysis]][[row$market]][[row$stratum]]) == row$nobs)
    }
  }
  invisible(TRUE)
}

preserve_salience_history <- function() {
  root <- file.path(salience_report_root(), "historical")
  if (file.exists(file.path(root, "manifest.json"))) stop("Historical manifest already exists; never overwrite it.")
  prefixes <- c("did_trends_prior_extensive_salience", "did_articles_prior_extensive_salience",
                "did_trends_prior_salience", "did_articles_prior_salience",
                "hedonic_count_continuous_prior_salience")
  families <- stats::setNames(c(rep("four_strata_intensity", 5), rep("overlapping", 5), "exclusive"),
                            c(prefixes, paste0(prefixes, "_groups"), "salience_three_way"))
  # Validate the complete historical set before copying or declaring it usable.
  for (prefix in names(families)) {
    path <- here::here("output", "regs", paste0(prefix, ".rds"))
    if (!file.exists(path)) stop("Historical bundle unavailable: ", prefix)
    validate_historical_bundle(readRDS(path), families[[prefix]])
  }
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  copy_verified <- function(from, to) {
    if (file.exists(to)) {
      if (identical(salience_file_hash(from), salience_file_hash(to))) return(salience_file_hash(to))
      stop("Refusing to replace snapshot file: ", to)
    }
    dir.create(dirname(to), recursive = TRUE, showWarnings = FALSE)
    before <- salience_file_hash(from)
    if (!file.copy(from, to, overwrite = FALSE) || !identical(before, salience_file_hash(to)) ||
        !identical(before, salience_file_hash(from))) stop("Snapshot copy failed: ", from)
    before
  }
  artifacts <- lapply(names(families), function(prefix) {
    relative <- file.path("regs", paste0(prefix, ".rds"))
    hash <- copy_verified(here::here("output", relative), file.path(root, relative))
    list(path = relative, sha256 = hash, family = families[[prefix]], profile = "legacy_tidal",
         london = if (families[[prefix]] == "four_strata_intensity") "variant_specific" else "excluded",
         bathing = "ever_reported_2021_2024")
  })
  names(artifacts) <- names(families)
  output_files <- list.files(here::here("output"), recursive = TRUE,
                             pattern = "(salience.*\\.(tex|csv)$|^(did_(trends|articles)_prior|hedonic_count_continuous_prior).*\\.tex$)")
  exports <- lapply(output_files, function(path) {
    list(path = path, sha256 = copy_verified(here::here("output", path), file.path(root, path)))
  })
  sources <- c(list.files(here::here("scripts", "R"), pattern = "\\.R$", recursive = TRUE,
                         full.names = TRUE),
               here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report.qmd"),
               here::here("rv.lock"), here::here("rproject.toml"))
  code <- lapply(sources, function(path) {
    relative <- substring(path, nchar(here::here()) + 2L)
    list(path = relative, sha256 = copy_verified(path, file.path(root, "source", relative)))
  })
  recovery_root <- here::here("data", "processed", "recovery", "open-coast-pre-refinement")
  data_paths <- c("processed/site_characteristics/site_group_characteristics.parquet",
                  "processed/cross_section/prior_intensity_cutoffs.parquet",
                  unlist(lapply(c("sales", "rentals"), function(market) {
                    base <- file.path("processed", "cross_section", market, "prior_characteristics")
                    file.path(base, list.files(here::here("data", base),
                                               pattern = "\\.parquet$", recursive = TRUE))
                  })))
  recovery <- lapply(data_paths, function(path) {
    list(path = path, sha256 = copy_verified(here::here("data", path), file.path(recovery_root, path)))
  })
  manifest <- list(schema_version = 1L, created_at = format(Sys.time(), tz = "UTC"),
                   consumer_revision = system2("git", c("rev-parse", "HEAD"), stdout = TRUE),
                   profile = "legacy_tidal", artifacts = artifacts, exports = exports,
                   source_snapshot = code, recovery_root = normalizePath(recovery_root), recovery = recovery,
                   provenance_limit = "Historical bundles predate input hashes; original input generation cannot be retroactively proven. Recovery hashes identify the current pre-refinement artifacts.")
  stage <- tempfile("manifest-", tmpdir = root, fileext = ".json")
  jsonlite::write_json(manifest, stage, auto_unbox = TRUE, pretty = TRUE)
  jsonlite::read_json(stage)
  if (!file.rename(stage, file.path(root, "manifest.json"))) stop("Cannot finalize historical manifest.")
  invisible(manifest)
}

if (sys.nframe() == 0L) preserve_salience_history()
