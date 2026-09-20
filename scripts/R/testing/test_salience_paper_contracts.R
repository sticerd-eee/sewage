# Exercise the five paper environments, including real parquet readers and fits.
suppressPackageStartupMessages(library(dplyr))
root <- tempfile("salience-paper-")
dir.create(root)
generation <- paste(rep("a", 64), collapse = "")
sites <- tibble::tibble(site_id = 1:4, distance_to_coast_m = c(50, 5000, 50, NA_real_),
  bath_ever_2124 = c(TRUE, TRUE, FALSE, TRUE), bath_unknown_2124 = FALSE,
  bath_status_21 = c("designated", "not_designated", "not_designated", "unknown"),
  distance_to_open_coast_m = c(2000, 2000.001, 1999.999, NA_real_),
  open_coast_status = c(rep("validated", 3), "missing_location"),
  site_generation = generation, geometry_generation = generation)
site_path <- file.path(root, "sites.parquet")
arrow::write_parquet(sites, site_path)
fixture <- expand.grid(lsoa = 1:8, month_id = c(1L,19L,20L,36L,48L),
  near = 0:1, site_id = 1:4, control = 0:1)
fixture$region <- ifelse(fixture$lsoa == 1, "London", "North West")
fixture$region[fixture$lsoa == 2] <- NA_character_
fixture$latitude <- 52
fixture$longitude <- -1
fixture$property_type <- as.character(fixture$control)
fixture$old_new <- as.character((fixture$lsoa + fixture$control) %% 2)
fixture$duration <- as.character((fixture$lsoa + fixture$month_id + fixture$control) %% 2)
fixture$bedrooms <- 1 + (fixture$lsoa + fixture$control) %% 3
fixture$bathrooms <- 1 + (fixture$lsoa + fixture$month_id + fixture$control) %% 2
fixture$count <- (seq_len(nrow(fixture)) * 7 %% 23) / 10
fixture$price <- exp(10 + .02 * fixture$count + .1 * fixture$near +
  .01 * fixture$near * (fixture$month_id >= 20) + .03 * fixture$control +
  sin(seq_len(nrow(fixture))) / 100)
fixture$listing_price <- fixture$price / 100
fixture$price[1] <- fixture$listing_price[1] <- NA_real_
attention <- tibble::tibble(month_id = 1:48, post = as.integer(month_id >= 20),
                            log_cumulative_articles = log1p(seq_len(48)^2))
paths <- c(file.path("05_news", c("did_trends_prior_extensive_salience.R",
  "did_articles_prior_extensive_salience.R", "did_trends_prior_salience.R", "did_articles_prior_salience.R")),
  "02_hedonic/hedonic_continuous_prior_salience.R")
provenance <- list(site_generation = generation, inputs = c(fixture = digest::digest(fixture, algo = "sha256")),
                   code = c(fixture = digest::digest(paths, algo = "sha256")))
provenance$generation <- digest::digest(provenance, algo = "sha256")
export_root <- file.path(root, "exports")
paper_contract_fixtures <- list()
expect_error <- function(expr) stopifnot(inherits(tryCatch(force(expr), error = identity), "error"))
for (path in paths) {
  env <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "09_analysis", path), env)
  stopifnot(identical(environment(env$prepare_analysis_data), env),
            identical(environment(env$estimate_groups), env),
            env$CONFIG$profile == "open_coast", !env$CONFIG$exclude_london)
  env$CONFIG$site_path <- site_path
  extensive <- grepl("extensive", path)
  hedonic <- grepl("hedonic", path)
  market_output <- list()
  market_data <- list()
  market_inputs <- list()
  for (market in c("sales", "rentals")) {
    id <- env$MARKETS[[market]]$id
    transactions <- fixture |> mutate(!!id := sprintf("property-%05d", row_number()))
    exposure <- transactions |> transmute(!!id := .data[[id]], radius = 250L,
      n_spill_sites = 1L, spill_count_weekly_avg = .data$count, spill_hrs_weekly_avg = .data$count * 2)
    exposure$spill_count_weekly_avg[2] <- NA_real_
    exposure$spill_hrs_weekly_avg[3] <- NA_real_
    exposure$spill_count_weekly_avg[4] <- 0
    lookup <- transactions |> transmute(!!id := .data[[id]], site_id,
      distance_m = if (extensive) ifelse(near == 1, 100, 1500) else 100)
    companion <- transactions |> left_join(sites |> select(site_id, distance_to_open_coast_m), by = "site_id") |>
      transmute(!!id := .data[[id]], radius = 250L, n_spill_sites = 1L,
        min_coast_dist_m = sites$distance_to_coast_m[site_id],
        any_bath_2124 = sites$bath_ever_2124[site_id], bath_unknown_2124 = FALSE,
        min_open_coast_dist_m = distance_to_open_coast_m, max_open_coast_dist_m = distance_to_open_coast_m,
        n_open_coast_known = as.integer(is.finite(distance_to_open_coast_m)),
        n_open_coast_missing = 1L - n_open_coast_known, site_generation = generation)
    for (name in c("transactions", "exposure", "lookup", "companion")) {
      target <- file.path(root, paste(market, name, "parquet", sep = "."))
      arrow::write_parquet(get(name), target)
      env$MARKETS[[market]][[name]] <- target
    }
    market_inputs[[market]] <- list(transactions = transactions, exposure = exposure, lookup = lookup, companion = companion)
    prepare <- function() {
      if (hedonic) env$prepare_analysis_data(market, sites) else if (extensive)
        env$prepare_analysis_data(market, attention, sites) else env$prepare_analysis_data(market, attention)
    }
    data <- prepare()
    market_data[[market]] <- data
    expected <- transactions[[id]]
    eligible <- rep(TRUE, nrow(transactions))
    if (!hedonic) eligible <- is.finite(transactions$price) &
      transactions$month_id <= if (market == "sales") 48L else 36L
    if (!extensive) eligible <- eligible & !is.na(exposure$spill_count_weekly_avg)
    if (hedonic) eligible <- eligible & !is.na(exposure$spill_hrs_weekly_avg)
    stopifnot(setequal(data[[id]], expected[eligible]), any(data$london), anyNA(data$region))
    if (extensive) stopifnot(any(data$min_distance > 1000))
    if (!extensive) stopifnot(any(data$spill_count_weekly_avg == 0))
    output <- env$estimate_groups(data, market)
    market_output[[market]] <- output
    for (group in env$CONFIG$groups) {
      model <- output$models[[group]]
      selected <- data[[id]][data[[group]]]
      stopifnot(identical(model$salience_observation_ids$selected, selected),
        length(model$salience_observation_ids$fitted) == model$nobs,
        identical(model$fixef_vars, if (hedonic) "lsoa" else c("lsoa", "month_id")),
        all((if (market == "sales") c("property_type", "old_new", "duration") else
          c("property_type", "bedrooms", "bathrooms")) %in% all.vars(model$fml)))
      type <- attr(model$cov.scaled, "type")
      stopifnot(if (hedonic) grepl("Heteroskedastic", type) else grepl("lsoa", type))
    }
    stopifnot(all(output$counts$nobs + output$counts$n_removed_by_estimator == output$counts$n_estimation))
    # London exclusion uses the same masks for audit and fit; unknown regions stay.
    env$CONFIG$exclude_london <- TRUE
    excluded <- env$estimate_groups(data, market)
    for (group in env$CONFIG$groups) stopifnot(identical(
      excluded$models[[group]]$salience_observation_ids$selected,
      data[[id]][data[[group]] & !data$london]))
    env$CONFIG$exclude_london <- FALSE
    # A valid-looking, same-count key replacement cannot silently lose a match.
    if (!extensive) {
      name <- if (hedonic) "lookup" else "companion"
      corrupted <- get(name)
      key <- match(data[[id]][1], corrupted[[id]])
      corrupted[[id]][key] <- "wrong-key"
      arrow::write_parquet(corrupted, env$MARKETS[[market]][[name]])
      expect_error(prepare())
    }
  }
  result <- list(models = lapply(market_output, `[[`, "models"),
    counts = bind_rows(lapply(market_output, `[[`, "counts")),
    results = bind_rows(lapply(market_output, `[[`, "results")),
    settings = list(config = env$CONFIG, london = "included", profile = "open_coast",
      bathing_policy = "ever_2124", groups_overlap = TRUE, provenance = provenance))
  paper_contract_fixtures[[env$CONFIG$output_prefix]] <- list(env = env, data = market_data, inputs = market_inputs)
  empty <- env$estimate_groups(dplyr::mutate(market_data$sales, bathing = FALSE), "sales")
  stopifnot(empty$counts$fit_status[empty$counts$group == "bathing"] == "unavailable",
    all(is.na(empty$results$estimate[empty$results$group == "bathing"])))
  empty_result <- result
  empty_result$models$sales <- empty$models
  empty_result$counts <- bind_rows(empty$counts, filter(result$counts, market == "rentals"))
  empty_result$results <- bind_rows(empty$results, filter(result$results, market == "rentals"))
  env$validate_salience_result(empty_result, env$CONFIG$output_prefix)
  env$export_table(empty_result, file.path(root, paste0(env$CONFIG$output_prefix, "-unavailable.tex")))
  # An interrupted actual exporter cannot complete the set or replace a prior
  # manifest. The table writer is the only injected failing boundary.
  writer <- env$export_table
  env$export_table <- function(...) stop("injected table failure")
  expect_error(env$export_results(result, export_root))
  stopifnot(!file.exists(file.path(export_root, "salience-current.json")))
  env$export_table <- writer
  env$export_results(result, export_root)
  if (!identical(path, tail(paths, 1))) stopifnot(!file.exists(file.path(export_root, "salience-current.json")))
}
manifest <- jsonlite::read_json(file.path(export_root, "salience-current.json"))
stopifnot(identical(manifest$status, "complete"), length(manifest$results) == 5L)
before <- digest::digest(file = file.path(export_root, "salience-current.json"), algo = "sha256")
snapshot <- file.path(export_root, "salience-generations", provenance$generation,
  env$CONFIG$output_prefix, paste0(env$CONFIG$output_prefix, ".rds"))
snapshot_hash <- digest::digest(file = snapshot, algo = "sha256")
env$export_results(result, export_root)
stopifnot(identical(snapshot_hash, digest::digest(file = snapshot, algo = "sha256")))
bad <- result
bad$counts <- bind_rows(bad$counts, bad$counts[1, ])
expect_error(env$export_results(bad, export_root))
stopifnot(identical(before, digest::digest(file = file.path(export_root, "salience-current.json"), algo = "sha256")))
if (!isTRUE(get0("keep_paper_fixture_files", ifnotfound = FALSE))) unlink(root, recursive = TRUE)
cat("Actual five-paper preparation, London, fitting and identity contracts passed.\n")
