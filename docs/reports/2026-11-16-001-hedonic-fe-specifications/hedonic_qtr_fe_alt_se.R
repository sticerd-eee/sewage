# Paper quarter-FE hedonic table (to-date weekly spill exposure, 250m) re-estimated
# with LSOA-clustered, MSOA-clustered, and heteroskedasticity-robust SEs.

suppressPackageStartupMessages({library(arrow); library(rio); library(dplyr); library(fixest); library(here)})
RAD <- 250L
sales <- import(here("data","processed","house_price.parquet"), trust=TRUE) |>
  select(house_id, price, qtr_id, lsoa, msoa, property_type, old_new, duration) |>
  mutate(across(c(property_type, old_new, duration), forcats::as_factor))
rentals <- import(here("data","processed","zoopla","zoopla_rentals.parquet"), trust=TRUE) |>
  select(rental_id, listing_price, qtr_id, lsoa, msoa, property_type, bedrooms, bathrooms) |>
  mutate(property_type = forcats::as_factor(property_type))
ps <- open_dataset(here("data","processed","cross_section","sales","prior_to_sale")) |>
  filter(radius == RAD, n_spill_sites > 0) |> select(house_id, spill_count_weekly_avg, spill_hrs_weekly_avg) |> collect() |> as.data.frame()
pr <- open_dataset(here("data","processed","cross_section","rentals","prior_to_rental")) |>
  filter(radius == RAD, n_spill_sites > 0) |> select(rental_id, spill_count_weekly_avg, spill_hrs_weekly_avg) |> collect() |> as.data.frame()
ds <- ps |> inner_join(sales, by="house_id") |>
  filter(!is.na(spill_count_weekly_avg), !is.na(spill_hrs_weekly_avg), !is.na(lsoa), !is.na(property_type), !is.na(old_new), !is.na(duration), !is.na(qtr_id)) |>
  mutate(log_price = log(price))
dr <- pr |> inner_join(rentals, by="rental_id") |>
  filter(!is.na(spill_count_weekly_avg), !is.na(spill_hrs_weekly_avg), !is.na(lsoa), !is.na(property_type), !is.na(bedrooms), !is.na(bathrooms), !is.na(qtr_id)) |>
  mutate(log_price = log(listing_price))
v <- "spill_count_weekly_avg"
specs <- list(c("", ""), c("ctl", "qtr_id"), c("", "msoa + qtr_id"), c("ctl", "msoa + qtr_id"), c("", "lsoa + qtr_id"), c("ctl", "lsoa + qtr_id"))
run <- function(d, ctrl, panel) {
  out <- list()
  for (k in seq_along(specs)) {
    sp <- specs[[k]]
    rhs <- if (sp[1] == "ctl") paste(v, "+", ctrl) else v
    f <- as.formula(paste("log_price ~", rhs, if (sp[2] != "") paste("|", sp[2]) else ""))
    m <- feols(f, d)
    for (st in c("robust", "msoa", "lsoa")) {
      s <- switch(st, robust = summary(m, vcov = "hetero"), msoa = summary(m, cluster = ~msoa), lsoa = summary(m, cluster = ~lsoa))
      ct <- coeftable(s)
      out[[length(out) + 1]] <- data.frame(panel = panel, col = k, se_type = st,
        coef = ct[v, 1], se = ct[v, 2], p = ct[v, 4],
        c_coef = if ("(Intercept)" %in% rownames(ct)) ct["(Intercept)", 1] else NA,
        c_se = if ("(Intercept)" %in% rownames(ct)) ct["(Intercept)", 2] else NA,
        c_p = if ("(Intercept)" %in% rownames(ct)) ct["(Intercept)", 4] else NA,
        n = nobs(m), r2a = unname(r2(m, "ar2")))
    }
  }
  do.call(rbind, out)
}
res <- rbind(run(ds, "property_type + old_new + duration", "Sales"), run(dr, "property_type + bedrooms + bathrooms", "Rentals"))
jsonlite::write_json(res, here("docs", "reports", "2026-11-16-001-hedonic-fe-specifications", "results", "qtr_fe_alt_se.json"), digits = NA)
message("Wrote ", nrow(res), " rows")
