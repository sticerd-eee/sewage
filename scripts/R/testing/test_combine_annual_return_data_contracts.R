# ==============================================================================
# Annual Return Combiner Contract Tests
# ==============================================================================
#
# Regression net for scripts/R/02_data_cleaning/combine_annual_return_data.R.
# Runnable standalone via Rscript; exits non-zero on the first failure.
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

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(stringr)
  library(hms)
})

combine_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "02_data_cleaning", "combine_annual_return_data.R"),
  local = combine_env
)

# Both 2021 header variants for the operational WaSC site name must populate
# the same canonical site_name_wa_sc column through clean_data().
variant_one_input <- tibble(
  water_company_name = "Test Co",
  site_name_wa_sc_operational_optional = "ALPHA WORKS",
  outlet_discharge_ngr_ea_consents_database = "SO123456"
)
variant_two_input <- tibble(
  water_company_name = "Test Co",
  site_name_wa_sc_operational_name_optional = "BETA WORKS",
  outlet_discharge_ngr_ea_consents_database = "SO654321"
)

variant_one_cleaned <- combine_env$clean_data(variant_one_input)
variant_two_cleaned <- combine_env$clean_data(variant_two_input)

assert_true(
  "site_name_wa_sc" %in% names(variant_one_cleaned),
  "Header variant site_name_wa_sc_operational_optional should map to site_name_wa_sc."
)
assert_identical(
  variant_one_cleaned$site_name_wa_sc, "ALPHA WORKS",
  "Header variant 1 should carry its value into site_name_wa_sc."
)
assert_true(
  "site_name_wa_sc" %in% names(variant_two_cleaned),
  "Header variant site_name_wa_sc_operational_name_optional should map to site_name_wa_sc."
)
assert_identical(
  variant_two_cleaned$site_name_wa_sc, "BETA WORKS",
  "Header variant 2 should carry its value into site_name_wa_sc."
)

# NA placeholder normalisation and Welsh Water renaming hold through clean_data().
na_input <- tibble(
  water_company_name = c("Dwr Cymru Welsh Water", "Test Co", NA),
  site_name_wa_sc_operational_optional = c("-", "n/a", "GAMMA WORKS"),
  outlet_discharge_ngr_ea_consents_database = "SO111111"
)
na_cleaned <- combine_env$clean_data(na_input)
assert_identical(
  nrow(na_cleaned), 2L,
  "Rows without a water company should be dropped by clean_data()."
)
assert_identical(
  na_cleaned$water_company,
  c("Welsh Water", "Test Co"),
  "Dwr Cymru Welsh Water should be normalised to Welsh Water."
)
assert_true(
  all(is.na(na_cleaned$site_name_wa_sc)),
  "Placeholder values ('-', 'n/a') should normalise to NA in clean_data()."
)

cat("All annual-return combiner contract tests passed.\n")
