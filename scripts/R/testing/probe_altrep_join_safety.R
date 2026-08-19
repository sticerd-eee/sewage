# Probe: which arrow-to-data.table conversion patterns give correct joins?
#
# Background: data.table joins keyed on a character column taken from an
# arrow `open_dataset() |> collect()` tibble via as.data.table() silently
# return a nondeterministic subset of the true join result on this machine
# (see docs/solutions/logic-errors/arrow-altrep-data-table-join-nondeterminism.md).
# This probe pins down, on real published data, which conversion patterns are
# safe. The metadata (right) side of every join is built through the verified
# as.data.frame() materialization so each pattern is tested in isolation.
# It reads a capped slice of one published dataset and rebuilds nothing.
suppressMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
})

root <- here::here("data", "processed", "cross_section")

collect_exposure <- function() {
  arrow::open_dataset(file.path(root, "sales/prior_to_sale")) |>
    filter(radius == 250L, n_spill_sites > 0) |>
    select(house_id, spill_count_weekly_avg) |>
    collect()
}

meta_path <- here::here("data", "processed", "house_price.parquet")
meta_tbl <- arrow::read_parquet(meta_path, col_select = c("house_id", "price"))
meta_dt <- as.data.table(as.data.frame(meta_tbl))

truth <- nrow(dplyr::inner_join(collect_exposure(), meta_tbl, by = "house_id"))
cat(sprintf("dplyr ground truth: %d rows\n\n", truth))

# Each pattern receives a freshly collected open_dataset tibble and returns
# the data.table whose house_id key is joined against the clean meta side.
patterns <- list(
  # The hazardous shape used across the pipeline before the fix.
  as_data_table_direct = function(dat) as.data.table(dat),
  # Incidental protections previously classified "uncertain" - none rebuild
  # the column, so all inherit the direct conversion's behaviour.
  copy_after_convert = function(dat) data.table::copy(as.data.table(dat)),
  setkey_after_convert = function(dat) {
    dt <- as.data.table(dat)
    setkey(dt, house_id)
    dt
  },
  subset_after_convert = function(dat) {
    dt <- as.data.table(dat)
    dt[house_id %in% dat$house_id]
  },
  # The arrow.use_altrep=FALSE guard: does NOT protect the open_dataset path.
  altrep_off_option = function(dat) {
    old <- options(arrow.use_altrep = FALSE)
    on.exit(options(old))
    as.data.table(collect_exposure())
  },
  # Verified-safe conversions.
  via_as_data_frame = function(dat) as.data.table(as.data.frame(dat)),
  via_column_extraction = function(dat) {
    data.table(house_id = dat$house_id,
               spill_count_weekly_avg = dat$spill_count_weekly_avg)
  }
)

results <- data.table(pattern = character(),
                      rows_rep1 = integer(), rows_rep2 = integer(),
                      correct = logical(), deterministic = logical())

for (name in names(patterns)) {
  rows <- integer(2)
  for (rep in 1:2) {
    dt <- patterns[[name]](collect_exposure())
    rows[rep] <- nrow(merge(dt, meta_dt, by = "house_id"))
  }
  results <- rbind(results, data.table(
    pattern = name,
    rows_rep1 = rows[1], rows_rep2 = rows[2],
    correct = all(rows == truth), deterministic = rows[1] == rows[2]
  ))
}

cat("Verdict table (correct = matches dplyr truth on both reps):\n")
print(results)

safe <- c("via_as_data_frame", "via_column_extraction")
if (all(results[pattern %in% safe, correct])) {
  cat("\nThe materializing conversions joined correctly.\n")
} else {
  cat("\nWARNING: a supposedly safe conversion did NOT join correctly:\n")
  print(results[pattern %in% safe & correct == FALSE])
}
if (any(results[!pattern %in% safe, correct])) {
  cat("NOTE: a pattern documented as hazardous joined correctly this run;\n")
  cat("the failure is nondeterministic, so treat the documentation as binding.\n")
}
