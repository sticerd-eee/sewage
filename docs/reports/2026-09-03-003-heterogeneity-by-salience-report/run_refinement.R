# Explicit report-owned estimation. Never calls paper main() or its exporters.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "refinement_results.R"))

membership_state <- function(data) ifelse(data$coastal, "coastal", ifelse(data$inland, "inland", "unknown"))

compare_prepared_membership <- function(data, specification, market, assignment) {
  legacy <- classify_salience_groups(data, source = assignment, profile = "legacy_tidal")
  incomplete <- if (assignment == "radius") data$n_open_coast_missing > 0L else
    data$open_coast_status != "validated"
  tibble::tibble(specification = specification, market = market, assignment = assignment,
    london = data$london, legacy = membership_state(legacy), refined = membership_state(data),
    bathing = data$bathing, incomplete_evidence = incomplete) |>
    dplyr::count(dplyr::across(dplyr::everything()), name = "n_properties")
}

compare_bathing_evidence <- function(data, earlier, specification, market) {
  tibble::tibble(specification = specification, market = market, london = data$london,
    ever_positive = data$bathing, ever_unknown = data$bath_unknown_2124,
    positive_2021 = earlier$bathing, unknown_2021 = earlier$bathing_2021_unknown) |>
    dplyr::count(dplyr::across(dplyr::everything()), name = "n_properties")
}

compare_common_assignments <- function(data, sites, market, specification, lookup_path) {
  id <- if (market == "sales") "house_id" else "rental_id"
  nearest <- nearest_salience_sites(arrow::open_dataset(lookup_path), sites, id, profile = "open_coast")
  # Same prepared transaction IDs, restricted to a nearest overflow within 250m.
  common <- data |> dplyr::select(dplyr::all_of(c(id, "region"))) |>
    dplyr::inner_join(dplyr::filter(nearest, .data$min_distance <= 250), by = id, relationship = "one-to-one")
  near <- classify_salience_groups(common, profile = "open_coast")
  # Extensive preparation includes properties outside the exposure companion's
  # universe. Derive radius evidence on these exact IDs from the full lookup;
  # restricting to companion keys would silently change the denominator.
  pairs <- arrow::open_dataset(lookup_path) |> dplyr::filter(.data$distance_m <= 250) |>
    dplyr::select(dplyr::all_of(c(id, "site_id"))) |> dplyr::collect() |>
    dplyr::semi_join(dplyr::select(common, dplyr::all_of(id)), by = id)
  if (dplyr::n_distinct(pairs[[id]], pairs$site_id) != nrow(pairs) ||
      any(!pairs$site_id %in% sites$site_id) || !setequal(pairs[[id]], common[[id]]))
    stop("Incomplete common-denominator radius evidence.")
  evidence <- pairs |> dplyr::left_join(sites, by = "site_id", relationship = "many-to-one") |>
    dplyr::summarise(n_spill_sites = dplyr::n(),
      min_open_coast_dist_m = if (all(is.na(.data$distance_to_open_coast_m))) NA_real_ else
        min(.data$distance_to_open_coast_m, na.rm = TRUE),
      max_open_coast_dist_m = if (all(is.na(.data$distance_to_open_coast_m))) NA_real_ else
        max(.data$distance_to_open_coast_m, na.rm = TRUE),
      n_open_coast_known = sum(is.finite(.data$distance_to_open_coast_m)),
      n_open_coast_missing = sum(!is.finite(.data$distance_to_open_coast_m)),
      any_bath_2124 = any(.data$bath_ever_2124), .by = dplyr::all_of(id))
  evidence$site_generation <- single_open_coast_generation(sites)
  radius <- dplyr::select(common, dplyr::all_of(c(id, "region"))) |>
    dplyr::left_join(evidence, by = id, relationship = "one-to-one") |>
    classify_salience_groups(source = "radius", profile = "open_coast")
  stopifnot(identical(near[[id]], radius[[id]]), all(!near$bathing | radius$bathing))
  tibble::tibble(specification = specification, market = market, london = near$london,
    nearest = membership_state(near), any_site = membership_state(radius),
    nearest_bathing = near$bathing, any_bathing = radius$bathing) |>
    dplyr::count(dplyr::across(dplyr::everything()), name = "n_properties")
}

refinement_turnover <- function(cells, legacy_cells) {
  dplyr::bind_rows(lapply(salience_paper_prefixes(), function(specification) {
    dplyr::bind_rows(lapply(c("sales", "rentals"), function(market) {
      dplyr::bind_rows(lapply(c("london_excluded", "bathing_2021", "coast_definition"), function(comparison) {
        coast <- comparison == "coast_definition"
        groups <- if (comparison == "bathing_2021") "bathing" else c("bathing", "coastal", "inland")
        old_cell <- if (coast) legacy_cells[[specification]][[market]] else cells$headline[[specification]][[market]]
        new_cell <- cells[[if (coast) "london_excluded" else comparison]][[specification]][[market]]
        dplyr::bind_rows(lapply(groups, function(group) {
          old <- old_cell$observations[[group]]
          new <- new_cell$observations[[group]]
          dplyr::bind_rows(lapply(c("selected", "fitted"), function(stage) tibble::tibble(
            specification = specification, market = market, comparison = comparison, group = group, stage = stage,
            baseline = if (coast) "legacy_london_excluded" else "headline",
            baseline_n = length(old[[stage]]), variant_n = length(new[[stage]]),
            retained = length(intersect(old[[stage]], new[[stage]])),
            left = length(setdiff(old[[stage]], new[[stage]])), entered = length(setdiff(new[[stage]], old[[stage]])))))
        }))
      }))
    }))
  }))
}

run_refinement <- function() {
  provenance <- salience_input_provenance()
  for (name in c("run_refinement.R", "refinement_results.R")) {
    provenance$code[[name]] <- salience_file_hash(file.path(salience_report_root(), name))
  }
  provenance$generation <- NULL
  provenance$generation <- digest::digest(provenance, algo = "sha256")
  site_path <- here::here("data", "processed", "site_characteristics", "site_group_characteristics.parquet")
  sites <- arrow::read_parquet(site_path)
  cells <- stats::setNames(lapply(refinement_variants(), function(x) list()), refinement_variants())
  membership <- assignments <- bathing_evidence <- legacy_cells <- coast_comparison <- list()
  for (path in salience_paper_paths()) {
    env <- new.env(parent = globalenv())
    sys.source(here::here("scripts", "R", "09_analysis", path), env)
    prefix <- env$CONFIG$output_prefix
    hedonic <- grepl("hedonic", path)
    extensive <- grepl("extensive", path)
    assignment <- if (hedonic || extensive) "nearest" else "radius"
    attention <- if (!hedonic) env$load_attention() else NULL
    for (market in c("sales", "rentals")) {
      message(prefix, " / ", market)
      data <- if (hedonic) env$prepare_analysis_data(market, sites) else if (extensive)
        env$prepare_analysis_data(market, attention, sites) else env$prepare_analysis_data(market, attention)
      lookup <- if (market == "sales") here::here("data", "processed", "spill_house_lookup.parquet") else
        here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
      key <- paste(prefix, market)
      membership[[key]] <- compare_prepared_membership(data, prefix, market, assignment)
      assignments[[key]] <- compare_common_assignments(data, sites, market, prefix, lookup)
      earlier <- report_bathing_2021(data, sites, market, assignment, lookup)
      bathing_evidence[[key]] <- compare_bathing_evidence(data, earlier, prefix, market)
      for (variant in refinement_variants()) {
        variant_data <- if (variant == "bathing_2021") earlier else data
        cells[[variant]][[prefix]][[market]] <- estimate_report_variant(env, variant_data, market, variant)
      }
      legacy_data <- classify_salience_groups(data, source = assignment, profile = "legacy_tidal")
      legacy_cells[[prefix]][[market]] <- estimate_report_variant(env, legacy_data, market, "legacy_london_excluded")
      coast_comparison[[key]] <- compare_coast_models(legacy_cells[[prefix]][[market]],
        cells$london_excluded[[prefix]][[market]], prefix, market)
    }
  }
  flatten <- function(field) dplyr::bind_rows(lapply(names(cells), function(variant) {
    dplyr::bind_rows(lapply(names(cells[[variant]]), function(specification) {
      dplyr::bind_rows(lapply(cells[[variant]][[specification]], function(cell) cell[[field]])) |>
        dplyr::mutate(variant = variant, specification = specification, .before = 1L)
    }))
  }))
  site_old <- classify_salience_groups(dplyr::mutate(sites, region = NA_character_), profile = "legacy_tidal")
  site_new <- classify_salience_groups(dplyr::mutate(sites, region = NA_character_), profile = "open_coast")
  bundle <- list(profile = "open_coast", family = "overlapping", provenance = provenance, cells = cells,
    legacy_cells = legacy_cells, coast_comparison = dplyr::bind_rows(coast_comparison),
    counts = flatten("counts"), results = flatten("results"), membership = dplyr::bind_rows(membership),
    assignments = dplyr::bind_rows(assignments), bathing_evidence = dplyr::bind_rows(bathing_evidence),
    turnover = refinement_turnover(cells, legacy_cells),
    site_distances = tibble::tibble(legacy_m = sites$distance_to_coast_m,
      refined_m = sites$distance_to_open_coast_m, legacy = membership_state(site_old),
      refined = membership_state(site_new)) |>
      dplyr::summarise(n_sites = dplyr::n(), n_both_known = sum(is.finite(.data$legacy_m) & is.finite(.data$refined_m)),
        median_legacy_m = if (all(is.na(.data$legacy_m))) NA_real_ else stats::median(.data$legacy_m, na.rm = TRUE),
        median_refined_m = if (all(is.na(.data$refined_m))) NA_real_ else stats::median(.data$refined_m, na.rm = TRUE),
        median_change_m = if (all(is.na(.data$refined_m - .data$legacy_m))) NA_real_ else
          stats::median(.data$refined_m - .data$legacy_m, na.rm = TRUE), .by = c("legacy", "refined")),
    sites = tibble::tibble(legacy = membership_state(site_old), refined = membership_state(site_new),
      evidence = sites$open_coast_status, bathing = sites$bath_ever_2124) |>
      dplyr::count(dplyr::across(dplyr::everything()), name = "n_sites"))
  validate_refinement_collection(bundle)
  root <- file.path(salience_report_root(), "refinement")
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  # Use a content-addressed run directory so a retry cannot mutate artifacts
  # referenced by the previous completion manifest.
  stage <- tempfile(".candidate-", tmpdir = root)
  dir.create(stage)
  on.exit(unlink(stage, recursive = TRUE), add = TRUE)
  saveRDS(bundle, file.path(stage, "collection.rds"))
  for (field in c("counts", "results", "membership", "assignments", "bathing_evidence", "turnover", "sites", "site_distances", "coast_comparison"))
    utils::write.csv(bundle[[field]], file.path(stage, paste0(field, ".csv")), row.names = FALSE)
  run_id <- paste0(provenance$generation, "-", salience_file_hash(file.path(stage, "collection.rds")))
  target <- file.path(root, run_id)
  artifacts <- lapply(list.files(stage, full.names = TRUE), function(path)
    list(path = file.path(run_id, basename(path)), sha256 = salience_file_hash(path)))
  publish_validated_dataset(stage, target, function(path) validate_refinement_collection(readRDS(file.path(path, "collection.rds"))))
  manifest <- list(status = "complete", profile = "open_coast", variants = refinement_variants(),
    generation = provenance$generation, bundle_path = file.path(run_id, "collection.rds"), artifacts = artifacts)
  candidate <- file.path(root, "manifest.json.candidate")
  jsonlite::write_json(manifest, candidate, auto_unbox = TRUE, pretty = TRUE)
  publish_validated_file(candidate, file.path(root, "manifest.json"), function(path) {
    value <- jsonlite::read_json(path)
    if (!identical(value$generation, provenance$generation)) stop("Refinement completion manifest mismatch.")
  })
  invisible(read_refinement_collection(root))
}

if (sys.nframe() == 0L) run_refinement()
