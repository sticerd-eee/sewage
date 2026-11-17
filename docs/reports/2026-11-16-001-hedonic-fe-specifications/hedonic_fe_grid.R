# Hedonic regressions of log price on to-date weekly spill exposure (250m)
# across location FE x time FE x property characteristics, with four SE types
# and the within-variation in exposure left after absorbing the FEs.

suppressPackageStartupMessages({
  library(arrow); library(rio); library(dplyr); library(fixest); library(here)
})
source(here("scripts", "R", "09_analysis", "utils_table_formatting.R"))

RAD <- 250L
OUT <- here("docs", "reports", "2026-11-16-001-hedonic-fe-specifications", "results", "fe_grid.json")

sales <- import(here("data", "processed", "house_price.parquet"), trust = TRUE) |>
  select(house_id, price, qtr_id, month_id, lsoa, msoa, property_type, old_new, duration, latitude, longitude) |>
  mutate(across(c(property_type, old_new, duration), forcats::as_factor))
rentals <- import(here("data", "processed", "zoopla", "zoopla_rentals.parquet"), trust = TRUE) |>
  select(rental_id, listing_price, qtr_id, month_id, lsoa, msoa, property_type, bedrooms, bathrooms, latitude, longitude) |>
  mutate(property_type = forcats::as_factor(property_type))

prior <- function(path, id) {
  open_dataset(path) |>
    filter(radius == RAD, n_spill_sites > 0) |>
    select(all_of(c(id, "spill_count_weekly_avg", "spill_hrs_weekly_avg"))) |>
    collect() |>
    as.data.frame()
}
ps <- prior(here("data", "processed", "cross_section", "sales", "prior_to_sale"), "house_id")
pr <- prior(here("data", "processed", "cross_section", "rentals", "prior_to_rental"), "rental_id")

# District is the local authority district, taken from the LSOA name.
prep <- function(d) {
  mutate(d,
    lad = sub(" [0-9]{3}[A-Z]$", "", lsoa),
    year = 2021L + (qtr_id - 1L) %/% 4L,
    yq = qtr_id, ym = month_id,
    cq = (qtr_id - 1L) %% 4L + 1L, cm = (month_id - 1L) %% 12L + 1L
  )
}
ds <- ps |>
  inner_join(sales, by = "house_id") |>
  filter(!is.na(spill_count_weekly_avg), !is.na(spill_hrs_weekly_avg), !is.na(lsoa),
         !is.na(property_type), !is.na(old_new), !is.na(duration), !is.na(qtr_id)) |>
  mutate(log_price = log(price)) |>
  prep()
dr <- pr |>
  inner_join(rentals, by = "rental_id") |>
  filter(!is.na(spill_count_weekly_avg), !is.na(spill_hrs_weekly_avg), !is.na(lsoa),
         !is.na(property_type), !is.na(bedrooms), !is.na(bathrooms), !is.na(qtr_id)) |>
  mutate(log_price = log(listing_price)) |>
  prep()

time_fe <- c(
  "None" = "", "Year" = "year", "Year-quarter" = "yq", "Year-month" = "ym",
  "Year + calendar quarter" = "year + cq", "Year + calendar month" = "year + cm",
  "LAD x year" = "lad^year", "LAD x year-quarter" = "lad^yq", "LAD x year-month" = "lad^ym",
  "LAD x year + LAD x calendar quarter" = "lad^year + lad^cq",
  "LAD x year + LAD x calendar month" = "lad^year + lad^cm",
  "MSOA x year-quarter" = "msoa^yq"
)
loc_fe <- c("None" = "", "LAD" = "lad", "MSOA" = "msoa", "LSOA" = "lsoa")

# Location-by-time FEs that absorb the location FE are not estimated.
skip <- function(ln, tn) {
  (ln == "None" && tn == "MSOA x year-quarter") ||
    (ln == "LAD" && (startsWith(tn, "LAD x") || tn == "MSOA x year-quarter"))
}
fe_str <- function(l, t) {
  x <- c(l, t)
  x <- x[x != ""]
  if (length(x)) paste("|", paste(x, collapse = " + ")) else ""
}
v <- "spill_count_weekly_avg"
pv <- function(b, s) 2 * pnorm(-abs(b / s))

run_panel <- function(d, ctrl, label) {
  out <- list()
  for (ctl in c(TRUE, FALSE)) for (ln in names(loc_fe)) for (tn in names(time_fe)) {
    if (skip(ln, tn)) next
    fs <- fe_str(loc_fe[[ln]], time_fe[[tn]])
    rhs <- if (ctl) paste(v, "+", ctrl) else v
    m <- feols(as.formula(paste("log_price ~", rhs, fs)), d, vcov = spatial_vcov())
    b <- unname(coef(m)[v])
    ses <- c(
      conley = unname(se(m)[v]),
      robust = unname(se(summary(m, vcov = "hetero"))[v]),
      msoa = unname(se(summary(m, cluster = ~msoa))[v]),
      lsoa = unname(se(summary(m, cluster = ~lsoa))[v])
    )
    dd <- d[obs(m), ]
    r <- if (fs == "") dd[[v]] - mean(dd[[v]]) else resid(feols(as.formula(paste(v, "~ 1", fs)), dd))
    out[[length(out) + 1]] <- data.frame(
      panel = label, controls = ctl, location = ln, time = tn, n = nobs(m), coef = b,
      se_conley = ses[["conley"]], p_conley = unname(pvalue(m)[v]),
      se_robust = ses[["robust"]], p_robust = pv(b, ses[["robust"]]),
      se_msoa = ses[["msoa"]], p_msoa = pv(b, ses[["msoa"]]),
      se_lsoa = ses[["lsoa"]], p_lsoa = pv(b, ses[["lsoa"]]),
      share_var_left = var(r) / var(dd[[v]])
    )
  }
  do.call(rbind, out)
}

res <- rbind(
  run_panel(ds, "property_type + old_new + duration", "Sales"),
  run_panel(dr, "property_type + bedrooms + bathrooms", "Rentals")
)
rownames(res) <- NULL
jsonlite::write_json(res, OUT, digits = NA)
message("Wrote ", nrow(res), " specifications to ", OUT)
