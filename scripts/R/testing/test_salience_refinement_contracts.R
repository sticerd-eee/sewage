keep_paper_fixture_files <- TRUE
source(here::here("scripts", "R", "testing", "test_salience_paper_contracts.R"))
fixture_root <- root
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "run_refinement.R"))
cells <- stats::setNames(lapply(refinement_variants(), function(x) list()), refinement_variants())
legacy_cells <- coast_comparison <- list()
for (specification in names(paper_contract_fixtures)) {
  fixture <- paper_contract_fixtures[[specification]]
  env <- fixture$env
  assignment <- if (grepl("extensive|hedonic", specification)) "nearest" else "radius"
  for (market in c("sales", "rentals")) {
    data <- fixture$data[[market]]
    for (name in names(fixture$inputs[[market]])) arrow::write_parquet(
      fixture$inputs[[market]][[name]], env$MARKETS[[market]][[name]])
    earlier <- report_bathing_2021(data, sites, market, assignment, env$MARKETS[[market]]$lookup)
    stopifnot(all(!earlier$bathing | data$bathing), sum(earlier$bathing) < sum(data$bathing))
    membership <- compare_prepared_membership(data, specification, market, assignment)
    bathing <- compare_bathing_evidence(data, earlier, specification, market)
    stopifnot(sum(membership$n_properties) == nrow(data), sum(bathing$n_properties) == nrow(data),
      all(!bathing$positive_2021 | bathing$ever_positive))
    diagnostics <- compare_common_assignments(data, sites, market, specification,
      env$MARKETS[[market]]$lookup)
    stopifnot(sum(diagnostics$n_properties) > 0L)
    for (variant in refinement_variants()) {
      input <- if (variant == "bathing_2021") earlier else data
      cells[[variant]][[specification]][[market]] <- estimate_report_variant(env, input, market, variant)
    }
    legacy_data <- classify_salience_groups(data, source = assignment, profile = "legacy_tidal")
    legacy_fit <- estimate_report_variant(env, legacy_data, market, "legacy_london_excluded")
    comparison <- compare_coast_models(legacy_fit,
      cells$london_excluded[[specification]][[market]], specification, market)
    stopifnot(nrow(comparison) > 0L)
    legacy_cells[[specification]][[market]] <- legacy_fit
    coast_comparison[[paste(specification, market)]] <- comparison
    # Shuffling changes positions, not the fitted identity set, including the
    # hedonic missing-outcome removal that occurs inside the estimator.
    shuffled <- estimate_report_variant(env, data[rev(seq_len(nrow(data))), ], market, "headline")
    for (group in c("bathing", "coastal", "inland")) stopifnot(setequal(
      shuffled$observations[[group]]$fitted,
      cells$headline[[specification]][[market]]$observations[[group]]$fitted))
  }
}
flatten <- function(field) bind_rows(lapply(names(cells), function(variant) {
  bind_rows(lapply(names(cells[[variant]]), function(specification) {
    bind_rows(lapply(cells[[variant]][[specification]], `[[`, field)) |>
      mutate(variant = variant, specification = specification, .before = 1L)
  }))
}))
bundle <- list(profile = "open_coast", family = "overlapping", provenance = provenance,
               cells = cells, legacy_cells = legacy_cells, coast_comparison = bind_rows(coast_comparison),
               counts = flatten("counts"), results = flatten("results"))
validate_refinement_collection(bundle)
stopifnot(nrow(bundle$counts) == 70L)
turnover <- refinement_turnover(cells, legacy_cells)
stopifnot(nrow(turnover) == 140L,
  all(turnover$baseline_n == turnover$retained + turnover$left),
  all(turnover$variant_n == turnover$retained + turnover$entered))
bathing_coast <- turnover[turnover$comparison == "coast_definition" & turnover$group == "bathing", ]
stopifnot(nrow(bathing_coast) == 20L, all(bathing_coast$left == 0L), all(bathing_coast$entered == 0L))
bad <- bundle
bad$results <- bad$results[-1, ]
expect_error(validate_refinement_collection(bad))
bad <- bundle
bad$provenance$generation <- paste(rep("0", 64), collapse = "")
expect_error(validate_refinement_collection(bad))
bad <- bundle
bad$legacy_cells[[1]][[1]] <- NULL
expect_error(validate_refinement_collection(bad))
bad <- bundle
bad$coast_comparison$legacy_estimate[1] <- 100
expect_error(validate_refinement_collection(bad))
# A saved collection must render without estimation and reject changed files.
saved_root <- file.path(fixture_root, "refinement")
dir.create(saved_root)
saveRDS(bundle, file.path(saved_root, "collection.rds"))
jsonlite::write_json(list(status = "complete", profile = "open_coast", variants = refinement_variants(),
  generation = provenance$generation, bundle_path = "collection.rds",
  artifacts = list(list(path = "collection.rds", sha256 = salience_file_hash(file.path(saved_root, "collection.rds"))))),
  file.path(saved_root, "manifest.json"), auto_unbox = TRUE)
stopifnot(nrow(read_refinement_collection(saved_root)$counts) == 70L)
cat("changed", file = file.path(saved_root, "collection.rds"), append = TRUE)
expect_error(read_refinement_collection(saved_root))
bad <- bundle
bad$cells$headline[[1]][[1]]$settings$london <- "excluded"
expect_error(validate_refinement_collection(bad))
bad <- bundle
bad$counts <- bad$counts[-1, ]
expect_error(validate_refinement_collection(bad))
empty <- estimate_report_variant(env, mutate(data, bathing = FALSE), market, "bathing_2021")
stopifnot(empty$counts$fit_status == "unavailable", all(is.na(empty$results$estimate)),
          nzchar(empty$counts$reason), length(empty$models) == 0L)
# Shared two-site counterexample on the identical property denominator.
id <- env$MARKETS[[market]]$id
lookup <- tibble::tibble(!!id := "two-sites", site_id = c(2L, 1L), distance_m = c(10, 20))
lookup_path <- file.path(fixture_root, "two-site-lookup.parquet")
arrow::write_parquet(lookup, lookup_path)
# This property intentionally has no exposure-companion row.
comparison <- compare_common_assignments(tibble::tibble(!!id := "two-sites", region = "London"),
  sites, market, "fixture", lookup_path)
stopifnot(comparison$nearest == "inland", comparison$any_site == "coastal", comparison$n_properties == 1L)
lookup <- dplyr::bind_rows(lookup, tibble::tibble(!!id := "two-sites", site_id = 4L, distance_m = 30))
arrow::write_parquet(lookup, lookup_path)
any_2021 <- report_bathing_2021(tibble::tibble(!!id := "two-sites", bathing = TRUE),
  sites, market, "radius", lookup_path)
nearest_2021 <- report_bathing_2021(tibble::tibble(!!id := "two-sites", site_id = 2L, bathing = TRUE),
  sites, market, "nearest", lookup_path)
stopifnot(any_2021$bathing, any_2021$bathing_2021_unknown,
  !nearest_2021$bathing, !nearest_2021$bathing_2021_unknown)
unlink(fixture_root, recursive = TRUE)
cat("Refinement 70-cell grid, 2021 membership, common denominator and exact turnover contracts passed.\n")
