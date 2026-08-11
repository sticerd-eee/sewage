############################################################
# Reconcile Site Group Consumers
# Project: Sewage
############################################################

# Public seams:
#   * audit_site_grain_manifest(): every live grain reader is classified.
#   * reconcile_consumer_artifact(): keyed membership and value diff with
#     duplicate/fanout detection.
#   * run_fixture_reconciliation(): focused U5 integration coverage without
#     reading or publishing production outputs.

suppressPackageStartupMessages({
  library(dplyr)
  library(here)
  library(readr)
  library(tibble)
  library(tidyr)
})

MANIFEST_HELPER_PATH <- here::here(
  "scripts", "R", "testing", "site_grain_consumer_manifest.R"
)

normalise_repo_path <- function(path) {
  sub("^\\./", "", gsub("\\\\", "/", path))
}

discover_grain_token_files <- function(root = here::here()) {
  script_candidates <- list.files(
    file.path(root, "scripts", "R"),
    pattern = "\\.(R|Rmd|qmd)$",
    recursive = TRUE,
    full.names = TRUE
  )
  book_candidates <- c(
    list.files(
      file.path(root, "book"),
      pattern = "\\.qmd$",
      recursive = FALSE,
      full.names = TRUE
    ),
    list.files(
      file.path(root, "book", "data_clean_documentation"),
      pattern = "\\.qmd$",
      recursive = TRUE,
      full.names = TRUE
    )
  )
  candidates <- c(script_candidates, book_candidates)
  excluded <- grepl(
    "(^|/)(book/(_freeze|\\.quarto)|docs/plans)(/|$)",
    normalise_repo_path(sub(paste0("^", root, "/?"), "", candidates))
  )
  candidates <- candidates[!excluded]
  has_token <- vapply(candidates, function(path) {
    text <- paste(readLines(path, warn = FALSE), collapse = "\n")
    grepl(
      "unique_spill_sites|site_group_crosswalk|site_works_crosswalk|works_crosswalk",
      text
    )
  }, logical(1))
  sort(normalise_repo_path(sub(paste0("^", root, "/?"), "", candidates[has_token])))
}

audit_site_grain_manifest <- function(
    manifest_helper_path = MANIFEST_HELPER_PATH,
    root = here::here()) {
  manifest_env <- new.env(parent = globalenv())
  sys.source(manifest_helper_path, envir = manifest_env)
  manifest <- manifest_env$site_grain_consumer_manifest() |>
    mutate(path = normalise_repo_path(.data$path))

  required_columns <- c("path", "classification", "owner", "reason")
  missing_columns <- setdiff(required_columns, names(manifest))
  if (length(missing_columns) > 0L) {
    stop(
      "Consumer manifest is missing columns: ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  if (anyDuplicated(manifest$path)) {
    stop("Consumer manifest paths must be unique.", call. = FALSE)
  }
  valid_classes <- c(
    "active_canonical", "active_site_group", "historical_only", "non_reader"
  )
  if (any(!manifest$classification %in% valid_classes)) {
    stop("Consumer manifest contains an invalid classification.", call. = FALSE)
  }
  active <- filter(
    manifest,
    .data$classification %in% c("active_canonical", "active_site_group")
  )
  if (any(is.na(active$owner) | active$owner == "")) {
    stop("Every active reader must have an owning implementation unit.", call. = FALSE)
  }

  discovered <- discover_grain_token_files(root)
  unclassified <- setdiff(discovered, manifest$path)
  stale <- setdiff(manifest$path, discovered)
  if (length(unclassified) > 0L) {
    stop(
      "Unclassified grain-token file(s): ",
      paste(unclassified, collapse = ", "),
      call. = FALSE
    )
  }
  if (length(stale) > 0L) {
    stop(
      "Manifest entries no longer contain a grain token: ",
      paste(stale, collapse = ", "),
      call. = FALSE
    )
  }

  group_unique_readers <- manifest |>
    filter(.data$classification == "active_site_group") |>
    pull(.data$path)
  stale_group_reader <- group_unique_readers[vapply(group_unique_readers, function(path) {
    grepl(
      "unique_spill_sites",
      paste(readLines(file.path(root, path), warn = FALSE), collapse = "\n")
    )
  }, logical(1))]
  if (length(stale_group_reader) > 0L) {
    stop(
      "Active Site Group reader(s) still reference canonical unique_spill_sites: ",
      paste(stale_group_reader, collapse = ", "),
      call. = FALSE
    )
  }

  manifest |>
    count(.data$classification, .data$owner, name = "files") |>
    arrange(.data$classification, .data$owner)
}

assert_unique_artifact_key <- function(data, key, label) {
  missing_key <- setdiff(key, names(data))
  if (length(missing_key) > 0L) {
    stop(label, " is missing key column(s): ", paste(missing_key, collapse = ", "), call. = FALSE)
  }
  if (any(!stats::complete.cases(data[key]))) {
    stop(label, " has missing key values.", call. = FALSE)
  }
  if (anyDuplicated(data[key])) {
    stop(label, " is not unique on ", paste(key, collapse = " + "), ".", call. = FALSE)
  }
  invisible(data)
}

reconcile_consumer_artifact <- function(
    baseline, candidate, key, values, artifact) {
  baseline <- tibble::as_tibble(baseline)
  candidate <- tibble::as_tibble(candidate)
  assert_unique_artifact_key(baseline, key, paste0(artifact, " baseline"))
  assert_unique_artifact_key(candidate, key, paste0(artifact, " candidate"))

  missing_values <- setdiff(values, intersect(names(baseline), names(candidate)))
  if (length(missing_values) > 0L) {
    stop(
      artifact, " is missing compared value column(s): ",
      paste(missing_values, collapse = ", "),
      call. = FALSE
    )
  }

  membership <- full_join(
    transmute(baseline, !!!rlang::syms(key), in_baseline = TRUE),
    transmute(candidate, !!!rlang::syms(key), in_candidate = TRUE),
    by = key
  ) |>
    mutate(
      in_baseline = tidyr::replace_na(.data$in_baseline, FALSE),
      in_candidate = tidyr::replace_na(.data$in_candidate, FALSE)
    )

  value_diff <- inner_join(
    select(baseline, all_of(c(key, values))),
    select(candidate, all_of(c(key, values))),
    by = key,
    suffix = c("_baseline", "_candidate")
  )
  changed <- rep(FALSE, nrow(value_diff))
  for (value in values) {
    old <- value_diff[[paste0(value, "_baseline")]]
    new <- value_diff[[paste0(value, "_candidate")]]
    changed <- changed | !(is.na(old) & is.na(new)) &
      (is.na(old) | is.na(new) | as.character(old) != as.character(new))
  }

  removed_keys <- sum(membership$in_baseline & !membership$in_candidate)
  added_keys <- sum(!membership$in_baseline & membership$in_candidate)
  changed_rows <- sum(changed)
  tibble(
    artifact = artifact,
    baseline_rows = nrow(baseline),
    candidate_rows = nrow(candidate),
    removed_keys = removed_keys,
    added_keys = added_keys,
    changed_rows = changed_rows,
    unexplained_changes = removed_keys + added_keys + changed_rows
  )
}

run_fixture_reconciliation <- function() {
  fixtures <- list(
    property_samples = list(
      data = tibble(house_id = c("h1", "h2"), site_id = c(10L, 20L), radius = 250L,
                    spill_count = c(3, 0), distance_m = c(100, 125),
                    n_site_groups = c(1L, 1L)),
      key = c("house_id", "site_id", "radius"),
      values = c("spill_count", "distance_m", "n_site_groups")
    ),
    exposure_panel = list(
      data = tibble(site_id = c(10L, 20L), period = 2023L,
                    population = c(1000, 500), spill_total = c(3, 0)),
      key = c("site_id", "period"),
      values = c("population", "spill_total")
    ),
    map_support = list(
      data = tibble(site_id = c(10L, 20L), period = 2023L,
                    easting = c(410000, 420000), northing = c(110000, 120000),
                    spill_total = c(3, 0)),
      key = c("site_id", "period"),
      values = c("easting", "northing", "spill_total")
    ),
    target_selection = list(
      data = tibble(site_id = 10L, ngr = "SU1000010000",
                    easting = 410000, northing = 110000),
      key = "site_id",
      values = c("ngr", "easting", "northing")
    )
  )

  results <- bind_rows(lapply(names(fixtures), function(name) {
    fixture <- fixtures[[name]]
    reconcile_consumer_artifact(
      fixture$data, fixture$data, fixture$key, fixture$values, name
    )
  }))
  if (any(results$unexplained_changes != 0L)) {
    stop("Fixture reconciliation found unexplained changes.", call. = FALSE)
  }

  duplicate_target <- bind_rows(
    fixtures$target_selection$data,
    fixtures$target_selection$data
  )
  duplicate_failed <- tryCatch(
    {
      reconcile_consumer_artifact(
        fixtures$target_selection$data,
        duplicate_target,
        fixtures$target_selection$key,
        fixtures$target_selection$values,
        "target_selection"
      )
      FALSE
    },
    error = function(error) grepl("not unique", conditionMessage(error), fixed = TRUE)
  )
  if (!duplicate_failed) {
    stop("Target-selection fanout fixture did not fail.", call. = FALSE)
  }
  results
}

main <- function() {
  manifest_summary <- audit_site_grain_manifest()
  fixture_summary <- run_fixture_reconciliation()
  print(manifest_summary, n = Inf)
  print(fixture_summary, n = Inf)
  cat("Site Group consumer reconciliation passed.\n")
}

if (sys.nframe() == 0L) {
  main()
}
