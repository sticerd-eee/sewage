############################################################
# Shared Script Setup Contracts
# Project: Sewage
# Date: 10/08/2026
############################################################

if (!requireNamespace("here", quietly = TRUE) ||
    !requireNamespace("logger", quietly = TRUE)) {
  stop(
    "Packages `here` and `logger` are required. Run `rv sync` first.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

aggregation_script <- here::here(
  "scripts", "R", "03_data_enrichment", "aggregate_spill_stats.R"
)
attached_before_source <- search()
aggregation_environment <- new.env(parent = globalenv())
source(aggregation_script, local = aggregation_environment)
stopifnot(identical(search(), attached_before_source))
stopifnot(is.function(aggregation_environment$initialise_environment))

assert_plain_log <- function(console) {
  log_file <- tempfile("script-setup-", fileext = ".log")
  on.exit(unlink(log_file), add = TRUE)

  setup_logging(log_file, console = console, threshold = "INFO")
  message_text <- if (console) "plain tee message" else "plain file message"
  logger::log_info(message_text)

  bytes <- readBin(log_file, what = "raw", n = file.info(log_file)$size)
  contents <- rawToChar(bytes)

  if (!grepl(message_text, contents, fixed = TRUE)) {
    stop("Persistent log did not contain the expected message.", call. = FALSE)
  }
  if (as.raw(0x1b) %in% bytes) {
    stop("Persistent log contained an ANSI escape byte.", call. = FALSE)
  }
}

assert_plain_log(console = FALSE)
assert_plain_log(console = TRUE)

cat("shared script setup contracts passed\n")
