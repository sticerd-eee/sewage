# Optional historical reproduction. Run explicitly; rendering never calls this.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))

reproduce_salience_history <- function() {
  assert_salience_legacy_inputs()
  definitions <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "testing", "salience_report_test_setup.R"), definitions)
  for (measure in c("trends", "articles")) {
    definitions$run_salience_extensive(measure)
    definitions$run_salience_intensive(measure)
  }
  definitions$run_salience_hedonic()
  definitions$run_exclusive_three_way()
  characteristics <- arrow::read_parquet(here::here(
    "data", "processed", "site_characteristics", "site_group_characteristics.parquet"))
  for (name in names(definitions$PAPER_SCRIPTS)) {
    script <- new.env(parent = globalenv())
    sys.source(here::here("scripts", "R", "09_analysis", definitions$PAPER_SCRIPTS[[name]]), script)
    attention_data <- if (name != "hedonic") script$load_attention() else NULL
    output <- lapply(c("sales", "rentals"), function(market) {
      args <- list(market = market, attention_data = attention_data, characteristics = characteristics)
      args <- args[intersect(names(args), names(formals(script$prepare_analysis_data)))]
      script$estimate_groups(do.call(script$prepare_analysis_data, args), market)
    })
    names(output) <- c("sales", "rentals")
    result <- list(models = lapply(output, `[[`, "models"),
                   counts = dplyr::bind_rows(lapply(output, `[[`, "counts")),
                   results = dplyr::bind_rows(lapply(output, `[[`, "results")),
                   settings = list(config = script$CONFIG, london = "excluded", groups_overlap = TRUE,
                                   profile = "legacy_tidal", reproduction = TRUE))
    original <- read_salience_snapshot(script$CONFIG$output_prefix, "overlapping")
    if (!isTRUE(all.equal(result$results, original$results, tolerance = 1e-8))) {
      stop("Historical reproduction differs from preserved results: ", name,
           ". No production or historical snapshot was replaced.")
    }
    path <- salience_report_output("regs", paste0(script$CONFIG$output_prefix, ".rds"))
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    saveRDS(result, path)
  }
  invisible(TRUE)
}

if (sys.nframe() == 0L) reproduce_salience_history()
