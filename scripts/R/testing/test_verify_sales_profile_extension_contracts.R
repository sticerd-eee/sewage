# ==============================================================================
# Sales Profile Extension Verifier Contract Tests
# ==============================================================================

# Runnable standalone via plain Rscript; exits non-zero on the first failure.
# Exercises the comparison helpers and the guard rails of
# scripts/R/testing/verify_sales_profile_extension.R against small fixtures.
# The commands that read the real artifacts run only against the project data.

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

assert_error_matching <- function(expr, pattern, message) {
  err <- tryCatch({ force(expr); NULL }, error = identity)
  assert_true(
    inherits(err, "error") && grepl(pattern, conditionMessage(err)),
    paste0(
      message,
      if (inherits(err, "error")) paste0("\nGot error: ", conditionMessage(err)) else "\nNo error was raised."
    )
  )
}

suppressPackageStartupMessages({
  library(data.table)
  library(here)
})

verifier <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "testing", "verify_sales_profile_extension.R"),
  envir = verifier
)

test_dir <- tempfile("verify-contracts-")
dir.create(test_dir, recursive = TRUE)
on.exit(unlink(test_dir, recursive = TRUE), add = TRUE)

# values_match(): missing matches missing only; integer and double storage agree.
assert_true(
  identical(verifier$values_match(c(1L, NA, 3L), c(1, NA, 4)), c(TRUE, TRUE, FALSE)),
  "values_match must compare as text and treat NA/NA as a match."
)
assert_true(
  identical(verifier$values_match(c("a", NA), c("a", "b")), c(TRUE, FALSE)),
  "values_match must not match a missing value against an observed one."
)

# numeric_equal(): relative tolerance and NA agreement.
assert_true(
  identical(verifier$numeric_equal(c(1, 1e6, NA), c(1 + 1e-9, 1e6 + 1e-3, NA)), c(TRUE, TRUE, TRUE)),
  "numeric_equal must accept differences within the relative tolerance."
)
assert_true(
  identical(verifier$numeric_equal(c(1, NA), c(1 + 1e-6, 1)), c(FALSE, FALSE)),
  "numeric_equal must reject differences beyond tolerance and NA against a value."
)

# compare_frames(): id sets, missing columns and value differences.
old <- data.table(house_id = c("a", "b"), price = c(1L, 2L), street = c("X", "Y"))
same <- verifier$compare_frames(old, copy(old), "house_id", "same")
assert_true(all(same$n_differences == 0L) && setequal(same$column, c("price", "street")), "Identical frames must report zero differences per column.")
changed <- copy(old)[house_id == "b", price := 5L]
diff <- verifier$compare_frames(old, changed, "house_id", "changed")
assert_true(diff$n_differences[diff$column == "price"] == 1L && diff$n_differences[diff$column == "street"] == 0L, "A changed value must be counted in its column only.")
assert_error_matching(
  verifier$compare_frames(old, old[1L], "house_id", "ids"),
  "sets differ",
  "A different id set must fail rather than compare partially."
)
assert_error_matching(
  verifier$compare_frames(old, old[, .(house_id, price)], "house_id", "cols"),
  "lacks baseline column",
  "A candidate lacking a baseline column must fail by name."
)

# Baseline guard rails: nothing runs without a frozen baseline, and a frozen
# baseline is never overwritten.
empty_dir <- file.path(test_dir, "empty")
dir.create(empty_dir)
for (cmd in c("overlap", "mapping", "promote", "rollback", "bundle")) {
  assert_error_matching(
    verifier[[paste0("cmd_", cmd)]](empty_dir),
    "No frozen baseline",
    paste("`", cmd, "` must refuse to run without a frozen baseline.")
  )
}
frozen_dir <- file.path(test_dir, "frozen")
dir.create(frozen_dir)
writeLines("{}", file.path(frozen_dir, verifier$BASELINE_MANIFEST))
assert_error_matching(
  verifier$cmd_baseline(frozen_dir),
  "frozen",
  "A frozen baseline must never be overwritten."
)

# Promotion requires passed evidence for the exact candidate files.
evidence_dir <- file.path(test_dir, "evidence")
dir.create(evidence_dir)
verifier$write_json(list(frozen = TRUE, artifacts = list()), file.path(evidence_dir, verifier$BASELINE_MANIFEST))
assert_error_matching(
  verifier$cmd_promote(evidence_dir),
  "Run `overlap` and `mapping` before `promote`",
  "Promotion without mapping evidence must be refused."
)
verifier$write_json(list(status = "failed"), file.path(evidence_dir, "mapping_evidence.json"))
assert_error_matching(
  verifier$cmd_promote(evidence_dir),
  "not passed",
  "Promotion on failed mapping evidence must be refused."
)
assert_error_matching(
  verifier$cmd_mapping(evidence_dir),
  "Mapping candidate missing|Run `overlap` before `mapping`",
  "The mapping check must require overlap evidence first."
)

# Command dispatch rejects unknown commands.
assert_error_matching(
  verifier$main("nonsense"),
  "Usage",
  "An unknown command must print the usage and fail."
)

cat("Sales profile extension verifier contract tests passed.\n")
