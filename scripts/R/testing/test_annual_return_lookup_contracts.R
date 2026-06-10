# ==============================================================================
# Annual Return Lookup Contract Tests
# ==============================================================================
#
# Regression net for scripts/R/03_data_enrichment/create_annual_return_lookup.R.
# Runnable standalone via Rscript; exits non-zero on the first failure.
#
# Baseline metrics (pinned 2026-06-10 from a fresh full run at commit 90c24e9,
# the pre-refactor code with the committed review fixes):
#   - 91 pre-resolution conflicted component-years across 41 components
#   - 153 resolution dropped edges (93 duplicate_year_component,
#     60 redundant_within_component)
#   - 0 post-resolution conflicted component-years
#   - 0 duplicated yearly site IDs in the final lookup
#   - per-year coverage 14470 / 14580 / 14530 / 14285 (2021-2024)
# Delta vs plan expectation (96 component-years / 98 dropped edges): the stale
# on-disk outputs predated the committed trim fix (2 extra nulled
# activity_reference placeholders), which reshapes the conflict landscape.
# The refactor must reproduce THESE pinned values exactly.
#
# ==============================================================================

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(
      paste0(
        message,
        "\nExpected: ", paste(capture.output(str(expected)), collapse = " "),
        "\nActual: ", paste(capture.output(str(actual)), collapse = " ")
      ),
      call. = FALSE
    )
  }
}

assert_error_matching <- function(expr, pattern, message) {
  err <- tryCatch(
    {
      force(expr)
      NULL
    },
    error = identity
  )
  assert_true(
    inherits(err, "error") && grepl(pattern, conditionMessage(err)),
    paste0(
      message,
      if (inherits(err, "error")) {
        paste0("\nGot error: ", conditionMessage(err))
      } else {
        "\nNo error was raised."
      }
    )
  )
}

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(stringr)
  library(purrr)
})

logger::log_threshold(logger::WARN)

lookup_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "03_data_enrichment", "create_annual_return_lookup.R"),
  local = lookup_env
)
ORIGINAL_CONFIG <- lookup_env$CONFIG

parquet_arrow_types <- function(path) {
  tbl <- arrow::read_parquet(path, as_data_frame = FALSE)
  stats::setNames(
    vapply(tbl$schema$fields, function(f) f$type$ToString(), character(1)),
    names(tbl)
  )
}

# ------------------------------------------------------------------------------
# Placeholder normalization through prepare_data_list()
# ------------------------------------------------------------------------------

placeholder_fixture <- bind_rows(
  tibble(
    year = 2021L,
    water_company = "Test Co",
    activity_reference = c(" TBC ", "tbc", "N/A ", "", "   ", "REAL-001"),
    site_name_ea = paste0("SITE-", 1:6)
  ),
  tibble(
    year = 2022L,
    water_company = "Test Co",
    activity_reference = "REAL-001",
    site_name_ea = "SITE-9"
  )
)
placeholder_parquet <- tempfile("lookup-placeholder-", fileext = ".parquet")
on.exit(unlink(placeholder_parquet), add = TRUE)
arrow::write_parquet(placeholder_fixture, placeholder_parquet)

placeholder_data_list <- lookup_env$prepare_data_list(
  years = c(2021, 2022),
  data_path = placeholder_parquet
)

assert_identical(
  placeholder_data_list$df2021$activity_reference,
  c(NA, NA, NA, NA, NA, "REAL-001"),
  "Placeholder variants (trim, case, empty, whitespace) should normalise to NA and real values should survive."
)

# Two rows whose only shared evidence is a placeholder produce zero edges.
shared_placeholder_fixture <- bind_rows(
  tibble(
    year = 2021L, water_company = "Test Co",
    activity_reference = "TBC", site_name_ea = "ALPHA"
  ),
  tibble(
    year = 2022L, water_company = "Test Co",
    activity_reference = "TBC", site_name_ea = "BETA"
  )
) %>%
  mutate(
    site_name_wa_sc = NA_character_,
    permit_reference_ea = NA_character_,
    permit_reference_wa_sc = NA_character_,
    outlet_discharge_ngr = NA_character_
  )
shared_placeholder_parquet <- tempfile("lookup-shared-", fileext = ".parquet")
on.exit(unlink(shared_placeholder_parquet), add = TRUE)
arrow::write_parquet(shared_placeholder_fixture, shared_placeholder_parquet)

shared_data_list <- lookup_env$prepare_data_list(
  years = c(2021, 2022),
  data_path = shared_placeholder_parquet
)
shared_matches <- lookup_env$run_all_windfall_matches(
  years = c(2021, 2022),
  data_list = shared_data_list
)
assert_identical(
  sum(vapply(shared_matches, nrow, integer(1))),
  0L,
  "Rows whose only shared evidence is a placeholder should produce zero deterministic matches."
)

# ------------------------------------------------------------------------------
# data_list_years() name validation
# ------------------------------------------------------------------------------

assert_identical(
  lookup_env$data_list_years(list(df2024 = tibble(), df2021 = tibble())),
  c(2021L, 2024L),
  "data_list_years() should return sorted years for valid dfYYYY names."
)
assert_error_matching(
  lookup_env$data_list_years(list(df2022x = tibble())),
  "dfYYYY",
  "data_list_years() should reject names that do not follow the dfYYYY convention."
)
assert_error_matching(
  lookup_env$data_list_years(list(tibble(), tibble())),
  "dfYYYY",
  "data_list_years() should reject unnamed data lists."
)

# ------------------------------------------------------------------------------
# build_annual_identifier_lookup() year scope
# ------------------------------------------------------------------------------

two_year_data_list <- list(
  df2021 = tibble(
    site_id_2021 = 1:2, water_company = "Test Co",
    site_name_ea = c("A", "B")
  ),
  df2022 = tibble(
    site_id_2022 = 5L, water_company = "Test Co",
    site_name_ea = "C"
  )
)
identifier_lookup <- lookup_env$build_annual_identifier_lookup(two_year_data_list)
assert_identical(
  sort(unique(identifier_lookup$year)),
  c(2021L, 2022L),
  "build_annual_identifier_lookup() should cover exactly the supplied years."
)

# ------------------------------------------------------------------------------
# Year-constrained spanning forest invariants
# ------------------------------------------------------------------------------

make_edge_fixture <- function(from, to, join_keys = "water_company|activity_reference",
                              field_priority_score = 6) {
  tibble(
    from = from,
    to = to,
    match_method = "windfall",
    match_type = "1_to_1",
    match_level = 1L,
    join_keys = join_keys,
    n_keys = str_count(join_keys, "\\|") + 1L,
    evidence_field_count = 1L,
    field_priority_score = field_priority_score,
    raw_score = 2,
    edge_priority = 100L,
    weight = 199
  )
}

chain_forest <- lookup_env$build_year_constrained_spanning_forest(
  make_edge_fixture(
    from = c("2021_1", "2022_1"),
    to = c("2022_1", "2023_1")
  )
)
assert_identical(
  nrow(chain_forest$kept_edges), 2L,
  "Linear 2021-2023 chain should keep both edges."
)
assert_identical(
  nrow(chain_forest$dropped_edges), 0L,
  "Linear 2021-2023 chain should drop no edges."
)
assert_identical(
  n_distinct(chain_forest$membership_tbl$component), 1L,
  "Linear 2021-2023 chain should form one component."
)
assert_true(
  all(chain_forest$kept_edges$year_from < chain_forest$kept_edges$year_to),
  "Kept forest edges should be chronologically oriented (year_from < year_to)."
)

triangle_forest <- lookup_env$build_year_constrained_spanning_forest(
  make_edge_fixture(
    from = c("2021_1", "2022_1", "2021_1"),
    to = c("2022_1", "2023_1", "2023_1")
  )
)
assert_identical(
  nrow(triangle_forest$kept_edges), 2L,
  "Triangle fixture should keep exactly two spanning edges."
)
assert_identical(
  triangle_forest$dropped_edges$drop_reason, "redundant_within_component",
  "Triangle fixture should drop the redundant edge with the documented reason."
)
assert_identical(
  nrow(triangle_forest$kept_edges) + nrow(triangle_forest$dropped_edges), 3L,
  "Triangle fixture kept plus dropped edges should equal the edge total."
)

duplicate_year_forest <- lookup_env$build_year_constrained_spanning_forest(
  make_edge_fixture(
    from = c("2021_1", "2021_2"),
    to = c("2022_1", "2022_1")
  )
)
assert_identical(
  duplicate_year_forest$dropped_edges$drop_reason, "duplicate_year_component",
  "Duplicate-year pair should be dropped as a duplicate_year_component."
)
assert_identical(
  duplicate_year_forest$dropped_edges$duplicate_years, "2021",
  "Duplicate-year drop should name the offending year."
)

# ------------------------------------------------------------------------------
# Schema parity: zero-row and nonzero kept/dropped forest tables
# ------------------------------------------------------------------------------

all_dropped_forest <- lookup_env$build_year_constrained_spanning_forest(
  make_edge_fixture(from = "2021_1", to = "2021_2")
)
assert_identical(
  nrow(all_dropped_forest$kept_edges), 0L,
  "A same-year-only edge set should keep no edges."
)
column_classes <- function(df) vapply(df, function(x) class(x)[1], character(1))
assert_identical(
  column_classes(all_dropped_forest$kept_edges),
  column_classes(chain_forest$kept_edges),
  "Zero-row and nonzero kept-edge tables should share column names, order, and types."
)
assert_identical(
  column_classes(chain_forest$dropped_edges),
  column_classes(all_dropped_forest$dropped_edges),
  "Zero-row and nonzero dropped-edge tables should share column names, order, and types."
)

# ------------------------------------------------------------------------------
# export_conflict_audit() idempotency
# ------------------------------------------------------------------------------

audit_dir <- tempfile("lookup-audit-")
dir.create(audit_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(audit_dir, recursive = TRUE, force = TRUE), add = TRUE)

audit_paths <- list(
  output_dir = audit_dir,
  conflict_summary_parquet = file.path(audit_dir, "summary.parquet"),
  conflict_records_parquet = file.path(audit_dir, "records.parquet"),
  conflict_edges_parquet = file.path(audit_dir, "edges.parquet"),
  resolution_kept_edges_parquet = file.path(audit_dir, "kept.parquet"),
  resolution_dropped_edges_parquet = file.path(audit_dir, "dropped.parquet"),
  conflict_excel_output = file.path(audit_dir, "conflicts.xlsx")
)

conflicted_membership <- tibble(
  vertex = c("2021_1", "2021_2", "2022_5"),
  component = 1L,
  year = c(2021L, 2021L, 2022L),
  site_id = c("1", "2", "5")
)
conflicted_edge_metadata <- tibble(
  component = 1L,
  year_from = 2021L,
  site_id_from = c("1", "2"),
  year_to = 2022L,
  site_id_to = "5",
  match_method = "windfall",
  match_type = "1_to_1",
  match_level = 1L,
  join_keys = "water_company|activity_reference",
  weight = 199
)
nonzero_audit <- lookup_env$build_lookup_conflict_audit(
  membership_tbl = conflicted_membership,
  edge_metadata = conflicted_edge_metadata,
  data_list = two_year_data_list
)
assert_true(
  nrow(nonzero_audit$summary) > 0,
  "Conflicted membership fixture should produce a nonzero conflict audit."
)
lookup_env$export_conflict_audit(nonzero_audit, paths = audit_paths)
assert_true(
  nrow(arrow::read_parquet(audit_paths$conflict_summary_parquet)) > 0,
  "Nonzero conflict audit should write nonzero summary rows."
)

clean_membership <- tibble(
  vertex = c("2021_1", "2022_5"),
  component = 1L,
  year = c(2021L, 2022L),
  site_id = c("1", "5")
)
zero_audit <- lookup_env$build_lookup_conflict_audit(
  membership_tbl = clean_membership,
  edge_metadata = conflicted_edge_metadata[1, ],
  data_list = two_year_data_list
)
assert_identical(
  nrow(zero_audit$summary), 0L,
  "Clean membership fixture should produce an empty conflict audit."
)
lookup_env$export_conflict_audit(zero_audit, paths = audit_paths)

for (key in c(
  "conflict_summary_parquet", "conflict_records_parquet",
  "conflict_edges_parquet", "resolution_kept_edges_parquet",
  "resolution_dropped_edges_parquet"
)) {
  assert_identical(
    nrow(arrow::read_parquet(audit_paths[[key]])), 0L,
    paste0("Re-export of an empty audit should overwrite ", key, " with zero rows.")
  )
}
excel_summary <- rio::import(audit_paths$conflict_excel_output, sheet = "summary")
assert_identical(
  nrow(excel_summary), 0L,
  "Re-export of an empty audit should replace the Excel workbook with empty sheets."
)

# ------------------------------------------------------------------------------
# assert_lookup_year_integrity() branches
# ------------------------------------------------------------------------------

integrity_lookup <- tibble(
  site_id_2021 = c("1", "2", NA),
  site_id_2022 = c(NA, NA, "5")
)
assert_true(
  isTRUE(lookup_env$assert_lookup_year_integrity(integrity_lookup, two_year_data_list)),
  "A complete, unique lookup should pass year-integrity assertions."
)
assert_error_matching(
  lookup_env$assert_lookup_year_integrity(
    integrity_lookup[-1, ],
    two_year_data_list
  ),
  "row-conservation",
  "Missing yearly IDs should trip the row-conservation branch."
)
assert_error_matching(
  lookup_env$assert_lookup_year_integrity(
    tibble(
      site_id_2021 = c("1", "1"),
      site_id_2022 = c("5", NA)
    ),
    two_year_data_list
  ),
  "uniqueness",
  "Duplicate yearly IDs should trip the uniqueness branch."
)
assert_error_matching(
  lookup_env$assert_lookup_year_integrity(
    integrity_lookup %>% select(-site_id_2022),
    two_year_data_list
  ),
  "missing site_id_2022",
  "A missing year column should trip the missing-column branch."
)

# ------------------------------------------------------------------------------
# score_evidence_fields() ordering
# ------------------------------------------------------------------------------

score <- function(fields) lookup_env$score_evidence_fields(fields)
assert_identical(
  score(character(0)), 0,
  "Empty evidence should score zero."
)
assert_true(
  score("site_name_wa_sc") < score("site_name_ea"),
  "Weaker name evidence should score below stronger name evidence."
)
assert_true(
  score("site_name_ea") < score("activity_reference"),
  "A weak single field should score below a strong single field."
)
assert_true(
  score(c("site_name_ea", "site_name_wa_sc")) > score("activity_reference"),
  "Two evidence fields should outrank any single field."
)

# ------------------------------------------------------------------------------
# Full builder: edge orientation, audit exports, schema parity, n_keys type
# ------------------------------------------------------------------------------

builder_dir <- tempfile("lookup-builder-")
dir.create(builder_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(builder_dir, recursive = TRUE, force = TRUE), add = TRUE)

builder_path_overrides <- as.list(stats::setNames(
  file.path(builder_dir, paste0(
    c(
      "lookup", "edge_metadata", "conflict_summary", "conflict_records",
      "conflict_edges", "resolution_kept_edges", "resolution_dropped_edges",
      "post_resolution_conflict_summary", "post_resolution_conflict_records",
      "post_resolution_conflict_edges", "post_resolution_kept_edges",
      "post_resolution_dropped_edges"
    ),
    ".parquet"
  )),
  c(
    "lookup_parquet", "edge_metadata_parquet", "conflict_summary_parquet",
    "conflict_records_parquet", "conflict_edges_parquet",
    "resolution_kept_edges_parquet", "resolution_dropped_edges_parquet",
    "post_resolution_conflict_summary_parquet",
    "post_resolution_conflict_records_parquet",
    "post_resolution_conflict_edges_parquet",
    "post_resolution_kept_edges_parquet",
    "post_resolution_dropped_edges_parquet"
  )
))
builder_path_overrides$output_dir <- builder_dir
builder_path_overrides$conflict_excel_output <-
  file.path(builder_dir, "conflicts.xlsx")
builder_path_overrides$post_resolution_conflict_excel_output <-
  file.path(builder_dir, "post_resolution_conflicts.xlsx")
builder_path_overrides$excel_output <- file.path(builder_dir, "lookup.xlsx")

lookup_env$CONFIG <- utils::modifyList(ORIGINAL_CONFIG, builder_path_overrides)

builder_data_list <- list(
  df2021 = tibble(
    site_id_2021 = 1:2, water_company = "Test Co",
    site_name_ea = c("A", "B")
  ),
  df2022 = tibble(
    site_id_2022 = 5L, water_company = "Test Co", site_name_ea = "C"
  ),
  df2023 = tibble(
    site_id_2023 = 3L, water_company = "Test Co", site_name_ea = "D"
  ),
  df2024 = tibble(
    site_id_2024 = 9L, water_company = "Test Co", site_name_ea = "D"
  )
)

# Later-year site_id column deliberately appears first in both match shapes.
windfall_shaped_matches <- tibble(
  site_id_2022 = c(5L, 5L),
  site_id_2021 = c(1L, 2L),
  match_method_2021_2022 = "windfall",
  match_type_2021_2022 = "1_to_1",
  match_level_2021_2022 = 1L,
  join_keys_2021_2022 = c(
    "water_company|activity_reference",
    "water_company|site_name_ea"
  )
)
rf_shaped_matches <- tibble(
  site_id_2024 = 9L,
  site_id_2023 = 3L,
  match_method_2023_2024 = "rf",
  match_type_2023_2024 = "one_to_one",
  join_keys_2023_2024 = "rf_probabilistic",
  match_level_2023_2024 = 5
)

builder_result <- lookup_env$build_lookup_from_matches(
  list(
    windfall = windfall_shaped_matches,
    rf = rf_shaped_matches
  ),
  data_list = builder_data_list,
  conflict_resolution = "year_constrained_forest"
)

assert_true(
  all(builder_result$edge_metadata$year_from < builder_result$edge_metadata$year_to),
  "Match dataframes with later-year columns first should still orient edges chronologically."
)
assert_true(
  any(
    builder_result$edge_metadata$match_method == "rf" &
      builder_result$edge_metadata$year_from == 2023
  ),
  "RF-shaped matches should survive edge construction with chronological orientation."
)

pre_summary <- arrow::read_parquet(
  lookup_env$CONFIG$conflict_summary_parquet
)
assert_true(
  nrow(pre_summary) > 0 && all(pre_summary$year == 2021L),
  "The shared-2022 fixture should produce a pre-resolution conflict in 2021."
)
post_summary <- arrow::read_parquet(
  lookup_env$CONFIG$post_resolution_conflict_summary_parquet
)
assert_identical(
  nrow(post_summary), 0L,
  "The year-constrained forest should clear all conflicts post-resolution."
)

# Zero-row (post) and nonzero (pre) exports of the kept/dropped edge families
# must share identical arrow schemas, pinning the n_keys integer fix.
for (family in c("kept_edges", "dropped_edges")) {
  pre_types <- parquet_arrow_types(
    lookup_env$CONFIG[[paste0("resolution_", family, "_parquet")]]
  )
  post_types <- parquet_arrow_types(
    lookup_env$CONFIG[[paste0("post_resolution_", family, "_parquet")]]
  )
  assert_identical(
    post_types, pre_types,
    paste0(
      "Zero-row and nonzero exports of the ", family,
      " family should share identical arrow schemas."
    )
  )
  assert_identical(
    unname(pre_types[["n_keys"]]), "int32",
    paste0("The ", family, " family should export n_keys as int32.")
  )
}

# Final edge-metadata schema parity between empty and nonzero builds.
empty_builder_result <- lookup_env$build_lookup_from_matches(
  list(),
  data_list = builder_data_list,
  conflict_resolution = "year_constrained_forest"
)
assert_identical(
  column_classes(empty_builder_result$edge_metadata),
  column_classes(builder_result$edge_metadata),
  "Zero-match and nonzero edge metadata should share column names, order, and types."
)

lookup_env$CONFIG <- ORIGINAL_CONFIG

# ------------------------------------------------------------------------------
# RF matching: lazy loading
# ------------------------------------------------------------------------------

rf_lazy_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "03_data_enrichment", "create_annual_return_lookup.R"),
  local = rf_lazy_env
)
assert_true(
  !exists("perform_rf_matching", envir = rf_lazy_env, inherits = FALSE),
  "RF matching functions should not be defined after sourcing the script."
)

main_fixture <- bind_rows(lapply(2021:2024, function(yr) {
  tibble(
    year = yr,
    water_company = "Test Co",
    site_name_ea = paste0("SITE-", yr, "-", 1:2),
    site_name_wa_sc = NA_character_,
    permit_reference_ea = NA_character_,
    permit_reference_wa_sc = NA_character_,
    activity_reference = NA_character_,
    outlet_discharge_ngr = NA_character_
  )
}))
main_fixture_parquet <- tempfile("lookup-main-", fileext = ".parquet")
on.exit(unlink(main_fixture_parquet), add = TRUE)
arrow::write_parquet(main_fixture, main_fixture_parquet)

main_dir <- tempfile("lookup-main-out-")
dir.create(main_dir, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(main_dir, recursive = TRUE, force = TRUE), add = TRUE)

main_overrides <- builder_path_overrides
main_overrides$output_dir <- main_dir
for (key in names(main_overrides)) {
  if (grepl("parquet$|xlsx$", main_overrides[[key]])) {
    main_overrides[[key]] <- file.path(main_dir, basename(main_overrides[[key]]))
  }
}
main_overrides$data_path <- main_fixture_parquet

rf_lazy_env$load_packages <- function() invisible(NULL)
rf_lazy_env$setup_logging <- function() invisible(NULL)
rf_lazy_env$CONFIG <- utils::modifyList(rf_lazy_env$CONFIG, main_overrides)
rf_lazy_env$main(compute_rf_matching = FALSE)

assert_true(
  !exists("perform_rf_matching", envir = rf_lazy_env, inherits = FALSE),
  "A default run (compute_rf_matching = FALSE) should never source the RF utilities."
)

# ------------------------------------------------------------------------------
# RF matching: downsampled trainer, labels, ambiguity guard, threshold
# ------------------------------------------------------------------------------

source(
  here::here("scripts", "R", "utils", "annual_return_lookup_rf_matching.R"),
  local = lookup_env
)

rf_fixture_cols <- function(df) {
  df %>%
    mutate(
      site_name_wa_sc = NA_character_,
      permit_reference_ea = NA_character_,
      permit_reference_wa_sc = NA_character_,
      activity_reference = NA_character_,
      outlet_discharge_ngr = NA_character_
    )
}
rf_left <- rf_fixture_cols(tibble(
  site_id_2023 = 1:6,
  water_company = c("Co A", "Co A", "Co A", "Co B", "Co B", "Co B"),
  site_name_ea = c(
    "ALPHA WORKS", "BRAVO WORKS", "CHARLIE WORKS",
    "DELTA WORKS", "ECHO WORKS", NA
  )
))
rf_right <- rf_fixture_cols(tibble(
  site_id_2024 = 11:16,
  water_company = c("Co A", "Co A", "Co A", "Co B", "Co B", "Co B"),
  site_name_ea = c(
    "ALPHA WORKS", "BRAVO WORKS LTD", "CHARLIE WKS",
    "DELTA WORKS", "FOXTROT STREET", NA
  )
))
rf_known_matches <- tibble(
  site_id_2023 = c(1L, 2L, 4L),
  site_id_2024 = c(11L, 12L, 14L)
)

training_pairs <- lookup_env$build_rf_training_pairs(
  rf_left, rf_right, rf_known_matches,
  hard_negative_floor = 0.95,
  easy_negative_ratio = 1,
  seed = 1
)
assert_identical(
  sum(training_pairs$training_role == "positive"), 3L,
  "The downsampled training set should contain every known-match positive."
)
hard_rows <- training_pairs %>% filter(training_role == "hard_negative")
if (nrow(hard_rows) > 0) {
  hard_sims <- as.matrix(hard_rows[lookup_env$RF_MATCH_VARS])
  hard_sims[hard_sims < 0] <- NA
  assert_true(
    all(apply(hard_sims, 1, max, na.rm = TRUE) >= 0.95),
    "Hard negatives should only be kept above the similarity floor."
  )
}
easy_by_block <- training_pairs %>%
  group_by(training_block) %>%
  summarise(
    n_easy = sum(training_role == "easy_negative"),
    n_positive = sum(training_role == "positive"),
    .groups = "drop"
  )
assert_true(
  all(easy_by_block$n_easy <= ceiling(1 * pmax(1, easy_by_block$n_positive))),
  "Easy negatives should respect the per-block sampling ratio."
)
assert_true(
  all(training_pairs$label[training_pairs$training_role == "positive"] == "match"),
  "Known-match pairs should be labelled match."
)

all_pairs <- lookup_env$build_rf_training_pairs(
  rf_left, rf_right, rf_known_matches,
  hard_negative_floor = 0.95,
  easy_negative_ratio = 100,
  seed = 1
)
na_similarity_rows <- all_pairs %>%
  filter(if_all(all_of(lookup_env$RF_MATCH_VARS), ~ .x == -1))
assert_true(
  nrow(na_similarity_rows) > 0 && all(na_similarity_rows$label == "non_match"),
  "Pairs with NA similarity on every field should be labelled non_match (poison-label guard)."
)

rf_model_fixture <- lookup_env$train_rf_linkage_model(
  rf_left, rf_right, rf_known_matches,
  num_trees = 100,
  seed = 1
)
assert_true(
  inherits(rf_model_fixture, "ranger"),
  "The downsampled trainer should return a ranger probability model."
)

# Ambiguity guard: identifier-identical groups are flagged, never linked.
ident_left <- tibble(
  site_id_2023 = 1:3,
  water_company = "Co A",
  site_name_ea = "SAME WORKS",
  site_name_wa_sc = "SAME WORKS",
  permit_reference_ea = "PERMIT-1",
  permit_reference_wa_sc = NA_character_,
  activity_reference = "ACT-1",
  outlet_discharge_ngr = "NGR-1"
)
ident_right <- ident_left %>%
  select(-site_id_2023) %>%
  mutate(site_id_2024 = 21:23, .before = 1)

guard_split <- lookup_env$flag_ambiguous_rf_proposals(
  matches = tibble(site_id_2023 = 1:3, site_id_2024 = 21:23),
  df_left = ident_left,
  df_right = ident_right,
  match_vars = lookup_env$RF_MATCH_VARS,
  site_id_left = "site_id_2023",
  site_id_right = "site_id_2024"
)
assert_identical(
  nrow(guard_split$matches), 0L,
  "Identifier-identical proposals should never be linked."
)
assert_identical(
  nrow(guard_split$ambiguous), 3L,
  "Identifier-identical proposals should be flagged as ambiguous."
)

ambiguous_run <- lookup_env$perform_rf_matching(
  ident_left, ident_right, rf_model_fixture,
  match_threshold = 0.05
)
assert_identical(
  nrow(ambiguous_run$matches), 0L,
  "An identifier-identical fixture should yield zero RF links end-to-end."
)
assert_true(
  nrow(ambiguous_run$ambiguous) > 0,
  "An identifier-identical fixture should surface a flagged ambiguous group."
)

# Threshold: 0.90 by default; override honoured.
assert_identical(
  formals(lookup_env$perform_rf_matching)$match_threshold, 0.90,
  "perform_rf_matching() should default to the validated 0.90 threshold."
)
assert_identical(
  formals(lookup_env$run_rf_matching)$match_threshold, 0.90,
  "run_rf_matching() should default to the validated 0.90 threshold."
)

threshold_left <- rf_left %>% filter(site_id_2023 %in% c(1L, 5L))
threshold_right <- rf_right %>% filter(site_id_2024 %in% c(11L, 15L))
threshold_all <- lookup_env$perform_rf_matching(
  threshold_left, threshold_right, rf_model_fixture,
  match_threshold = 0.001
)
assert_true(
  nrow(threshold_all$matches) > 0,
  "A near-zero threshold override should admit RF proposals."
)
quality_col <- grep("^match_quality_rf_", names(threshold_all$matches), value = TRUE)
max_quality <- max(threshold_all$matches[[quality_col]])
threshold_cut <- lookup_env$perform_rf_matching(
  threshold_left, threshold_right, rf_model_fixture,
  match_threshold = min(max_quality + 1e-6, 1)
)
assert_identical(
  nrow(threshold_cut$matches), 0L,
  "Proposals below the configured threshold should be excluded."
)

# ------------------------------------------------------------------------------
# Baseline metrics (integration block on refreshed data/processed outputs)
# ------------------------------------------------------------------------------

baseline_conflict_summary <- arrow::read_parquet(
  ORIGINAL_CONFIG$conflict_summary_parquet
)
assert_identical(
  nrow(baseline_conflict_summary), 91L,
  "Baseline: pre-resolution conflict audit should contain 91 conflicted component-years."
)
assert_identical(
  n_distinct(baseline_conflict_summary$component), 41L,
  "Baseline: pre-resolution conflict audit should span 41 conflicted components."
)

baseline_dropped <- arrow::read_parquet(
  ORIGINAL_CONFIG$resolution_dropped_edges_parquet
)
assert_identical(
  nrow(baseline_dropped), 153L,
  "Baseline: conflict resolution should drop 153 edges in conflicted components."
)
assert_identical(
  sum(baseline_dropped$drop_reason == "duplicate_year_component"), 93L,
  "Baseline: 93 dropped edges should be duplicate_year_component drops."
)

baseline_post_summary <- arrow::read_parquet(
  ORIGINAL_CONFIG$post_resolution_conflict_summary_parquet
)
assert_identical(
  nrow(baseline_post_summary), 0L,
  "Baseline: post-resolution conflict audit should be empty."
)

baseline_lookup <- arrow::read_parquet(ORIGINAL_CONFIG$lookup_parquet)
expected_coverage <- c(
  "2021" = 14470L, "2022" = 14580L, "2023" = 14530L, "2024" = 14285L
)
for (yr in names(expected_coverage)) {
  ids <- baseline_lookup[[paste0("site_id_", yr)]]
  ids <- ids[!is.na(ids)]
  assert_identical(
    length(ids), unname(expected_coverage[[yr]]),
    paste0("Baseline: ", yr, " lookup coverage should be ", expected_coverage[[yr]], ".")
  )
  assert_identical(
    anyDuplicated(as.character(ids)), 0L,
    paste0("Baseline: ", yr, " lookup IDs should contain no duplicates.")
  )
}

# Monitor-granularity characterization: within-year identifier-duplicate
# groups are distinct monitored discharge points (see CONCEPTS.md and the
# same-year-conflicts solution note). A data refresh that changes these
# counts should be noticed, not silently absorbed.
expected_duplicate_groups <- tibble(
  year = 2021:2024,
  n_groups = c(38L, 20L, 14L, 17L),
  n_rows = c(81L, 45L, 35L, 38L)
)
monitor_data_list <- lookup_env$prepare_data_list()
identifier_cols <- c(
  "water_company", "site_name_ea", "site_name_wa_sc",
  "permit_reference_ea", "permit_reference_wa_sc",
  "activity_reference", "outlet_discharge_ngr"
)
observed_duplicate_groups <- map_dfr(2021:2024, function(yr) {
  groups <- monitor_data_list[[paste0("df", yr)]] %>%
    count(across(all_of(identifier_cols)), name = "n") %>%
    filter(n > 1)
  tibble(year = yr, n_groups = nrow(groups), n_rows = sum(groups$n))
})
assert_identical(
  observed_duplicate_groups,
  expected_duplicate_groups,
  "Within-year identifier-duplicate (monitor-multiple) landscape should match the documented 89-group / 199-row characterization."
)

cat("All annual-return lookup contract tests passed.\n")
