source(here::here("scripts", "R", "utils", "dataset_publication_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "open_coast_contracts.R"), local = TRUE)
source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"), local = TRUE)

salience_paper_paths <- function() {
  c(file.path("05_news", c("did_trends_prior_extensive_salience.R",
    "did_articles_prior_extensive_salience.R", "did_trends_prior_salience.R",
    "did_articles_prior_salience.R")), "02_hedonic/hedonic_continuous_prior_salience.R")
}

salience_paper_prefixes <- function() {
  c("did_trends_prior_extensive_salience_groups", "did_articles_prior_extensive_salience_groups",
    "did_trends_prior_salience_groups", "did_articles_prior_salience_groups",
    "hedonic_count_continuous_prior_salience_groups")
}

salience_hash <- function(path) digest::digest(file = path, algo = "sha256")

# This common provenance inventory makes a partial data rebuild fail before
# fitting and prevents the fifth script from completing an older result set.
salience_input_provenance <- function() {
  site_path <- here::here("data", "processed", "site_characteristics", "site_group_characteristics.parquet")
  sites <- arrow::read_parquet(site_path)
  validate_open_coast_sites(sites)
  reference_path <- here::here("data", "processed", "geography", "open_coast", "reference.rds")
  reference <- readRDS(reference_path)
  validate_published_open_coast_reference(reference)
  if (!identical(single_open_coast_generation(sites, "geometry_generation"), reference$generation))
    stop("Site and shoreline generations differ.")
  if (!identical(single_open_coast_generation(sites), open_coast_site_generation(sites, reference$review$location_hash)))
    stop("Site artifact content differs from its generation.")
  generation <- single_open_coast_generation(sites)
  paths <- c(site_path, reference_path, here::here("data", "processed", c("house_price.parquet", "spill_house_lookup.parquet")),
    here::here("data", "processed", "zoopla", c("zoopla_rentals.parquet", "spill_rental_lookup.parquet")))
  for (market in c("sales", "rentals")) {
    root <- here::here("data", "processed", "cross_section", market)
    companion <- arrow::open_dataset(file.path(root, "prior_characteristics"))
    validate_open_coast_companion_manifest(file.path(root, "prior_characteristics"), generation)
    generations <- companion |> dplyr::select("radius", "site_generation") |>
      dplyr::distinct() |> dplyr::collect()
    if (!identical(sort(as.integer(generations$radius)), c(250L,500L,1000L)) ||
        !identical(single_open_coast_generation(generations), generation)) {
      stop("Incomplete or incompatible companion generation for ", market, ".")
    }
    for (folder in c("prior_characteristics", if (market == "sales") "prior_to_sale" else "prior_to_rental")) {
      paths <- c(paths, list.files(file.path(root, folder), pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE))
    }
  }
  # Include each script's attention source without importing or executing main().
  for (path in salience_paper_paths()) {
    env <- new.env(parent = globalenv())
    sys.source(here::here("scripts", "R", "09_analysis", path), env)
    if (!is.null(env$CONFIG$articles_path)) paths <- c(paths, env$CONFIG$articles_path)
  }
  paths <- sort(unique(paths))
  if (any(!file.exists(paths))) stop("Missing salience provenance input.")
  inputs <- stats::setNames(vapply(paths, salience_hash, ""),
    substring(paths, nchar(here::here()) + 2L))
  code_paths <- c(here::here("scripts", "R", "09_analysis", salience_paper_paths()),
    here::here("scripts", "R", "utils", c("salience_group_utils.R", "open_coast_contracts.R", "salience_publication_utils.R",
      "script_setup.R", "dataset_publication_utils.R")),
    here::here("scripts", "R", "09_analysis", c("utils_table_formatting.R", "05_news/extensive_margin_news_utils.R")),
    here::here("rv.lock"))
  code <- stats::setNames(vapply(code_paths, salience_hash, ""),
    substring(code_paths, nchar(here::here()) + 2L))
  payload <- list(site_generation = generation, inputs = inputs, code = code)
  c(payload, list(generation = digest::digest(payload, algo = "sha256")))
}

validate_salience_result <- function(result, prefix) {
  if (!prefix %in% salience_paper_prefixes() || !isTRUE(result$settings$groups_overlap) ||
      !identical(result$settings$profile, "open_coast") ||
      !identical(result$settings$london, "included") ||
      !identical(result$settings$bathing_policy, "ever_2124")) stop("Incompatible refined headline result.")
  generation <- result$settings$provenance$generation
  if (length(generation) != 1L || !grepl("^[a-f0-9]{64}$", generation)) stop("Missing result provenance.")
  provenance <- result$settings$provenance
  if (!length(provenance$inputs) || !length(provenance$code) ||
      !identical(generation, digest::digest(provenance[setdiff(names(provenance), "generation")], algo = "sha256")))
    stop("Result provenance identity mismatch.")
  if (!identical(result$settings$config$output_prefix, prefix)) stop("Model specification mismatch.")
  expected <- as.vector(outer(c("sales", "rentals"), c("bathing", "coastal", "inland"), paste, sep = ":"))
  keys <- paste(result$counts$market, result$counts$group, sep = ":")
  if (anyDuplicated(keys) || !setequal(keys, expected)) stop("Incomplete or duplicate model cells.")
  if (anyDuplicated(result$results[c("market", "group", "term")])) stop("Duplicate coefficient keys.")
  for (market in c("sales", "rentals")) for (group in c("bathing", "coastal", "inland")) {
    model <- result$models[[market]][[group]]
    row <- result$counts[result$counts$market == market & result$counts$group == group, ]
    config <- result$settings$config
    expected_terms <- if (grepl("hedonic", prefix)) "spill_count_weekly_avg" else {
      exposure <- if (grepl("extensive", prefix)) "near_bin" else "spill_count_weekly_avg"
      c(exposure, paste(exposure, config$attention, sep = ":"))
    }
    if (inherits(model, "salience_unavailable")) {
      coefficients <- result$results[result$results$market == market & result$results$group == group, ]
      if (!identical(row$fit_status, "unavailable") || !identical(row$reason, model$reason) ||
          !nzchar(model$reason) || !is.na(row$nobs) || any(!is.na(coefficients$estimate)) ||
          !setequal(coefficients$term, expected_terms) || !setequal(model$terms, expected_terms) ||
          any(!is.na(as.matrix(coefficients[c("estimate", "std_error", "conf_low", "conf_high", "p_value", "nobs")]))))
        stop("Unavailable result has stale or unexplained values.")
      next
    }
    if (!identical(row$fit_status, "available")) stop("Incompatible fit status.")
    if (is.null(model) || model$nobs != row$nobs ||
        row$n_estimation != row$nobs + row$n_removed_by_estimator) stop("Model/sample accounting mismatch.")
    coefficients <- result$results[result$results$market == market & result$results$group == group, ]
    if (!nrow(coefficients) || any(coefficients$nobs != model$nobs)) stop("Missing or stale coefficients.")
    if (!setequal(coefficients$term, expected_terms)) stop("Missing focal coefficients.")
    actual <- salience_group_results(model)
    matched <- actual[match(coefficients$term, actual$term), names(actual)]
    if (!isTRUE(all.equal(as.data.frame(coefficients[names(actual)]), as.data.frame(matched),
                         check.attributes = FALSE))) stop("Coefficients disagree with saved model.")
  }
  invisible(result)
}

publish_salience_result <- function(result, prefix, output_root, table_writer) {
  validate_salience_result(result, prefix)
  generation <- result$settings$provenance$generation
  root <- file.path(output_root, "salience-generations", generation)
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  stage <- tempfile(paste0(prefix, "-"), tmpdir = root)
  dir.create(stage)
  on.exit(unlink(stage, recursive = TRUE), add = TRUE)
  saveRDS(result, file.path(stage, paste0(prefix, ".rds")))
  utils::write.csv(result$results, file.path(stage, paste0(prefix, "_results.csv")), row.names = FALSE)
  utils::write.csv(result$counts, file.path(stage, paste0(prefix, "_cell_counts.csv")), row.names = FALSE)
  table_writer(result, file.path(stage, paste0(prefix, ".tex")))
  files <- list.files(stage, full.names = TRUE)
  receipt <- list(prefix = prefix, generation = generation, artifacts = lapply(files, function(path)
    list(path = basename(path), sha256 = salience_hash(path))))
  validate_stage <- function(path) {
    validate_salience_result(readRDS(file.path(path, paste0(prefix, ".rds"))), prefix)
    for (entry in receipt$artifacts) if (!identical(salience_hash(file.path(path, entry$path)), entry$sha256))
      stop("Result artifact changed during publication.")
  }
  jsonlite::write_json(receipt, file.path(stage, "manifest.json"), auto_unbox = TRUE, pretty = TRUE)
  target <- file.path(root, prefix)
  # A completed or partially completed generation is immutable. A retry may
  # finish its manifest, but must not replace files referenced by an old one.
  if (dir.exists(target)) {
    saved <- jsonlite::read_json(file.path(target, "manifest.json"))
    if (!identical(saved$generation, generation) || !identical(saved$prefix, prefix) ||
        !setequal(vapply(saved$artifacts, `[[`, "", "path"), basename(files)))
      stop("Existing result generation has an incompatible receipt.")
    for (entry in saved$artifacts) {
      if (!identical(salience_hash(file.path(target, entry$path)), entry$sha256))
        stop("Existing result generation is corrupt; refusing to overwrite it.")
    }
    validate_salience_result(readRDS(file.path(target, paste0(prefix, ".rds"))), prefix)
  } else {
    publish_validated_dataset(stage, target, validate_stage)
  }
  receipts <- file.path(root, salience_paper_prefixes(), "manifest.json")
  if (!all(file.exists(receipts))) return(invisible(result))
  entries <- lapply(receipts, jsonlite::read_json)
  if (any(vapply(entries, function(x) !identical(x$generation, generation), logical(1)))) stop("Mixed result generations.")
  # Validate all five saved bundles before replacing any conventional exports.
  for (entry in entries) {
    validate_salience_result(readRDS(file.path(root, entry$prefix, paste0(entry$prefix, ".rds"))), entry$prefix)
    for (artifact in entry$artifacts) {
      from <- file.path(root, entry$prefix, artifact$path)
      if (!identical(salience_hash(from), artifact$sha256)) stop("Saved result hash mismatch.")
    }
  }
  for (entry in entries) for (artifact in entry$artifacts) {
    folder <- if (endsWith(artifact$path, ".rds")) "regs" else if (endsWith(artifact$path, ".tex")) "tables" else "logs"
    dir.create(file.path(output_root, folder), recursive = TRUE, showWarnings = FALSE)
    target <- file.path(output_root, folder, artifact$path)
    candidate <- paste0(target, ".candidate")
    if (!file.copy(file.path(root, entry$prefix, artifact$path), candidate, overwrite = TRUE)) stop("Cannot stage result export.")
    expected_hash <- artifact$sha256
    publish_validated_file(candidate, target, function(path) {
      if (!identical(salience_hash(path), expected_hash)) stop("Result export hash mismatch.")
    })
  }
  manifest <- list(schema_version = 1L, status = "complete", generation = generation,
    profile = "open_coast", london = "included", bathing_policy = "ever_2124", family = "overlapping",
    results = entries)
  candidate <- file.path(output_root, "salience-current.json.candidate")
  jsonlite::write_json(manifest, candidate, auto_unbox = TRUE, pretty = TRUE)
  publish_validated_file(candidate, file.path(output_root, "salience-current.json"), function(path) {
    observed <- jsonlite::read_json(path)
    if (!identical(observed$generation, generation) || length(observed$results) != 5L) stop("Incomplete current result manifest.")
  })
  invisible(result)
}
