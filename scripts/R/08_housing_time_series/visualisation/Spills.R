#!/usr/bin/env Rscript
# Tag houses with spill activity in the SAME QUARTER as sale
# Adds to households:
#   - site_spilled      (TRUE/FALSE/NA)
#   - site_spilled_date (Date of earliest episode start in that quarter)

suppressPackageStartupMessages({
  pkgs <- c("arrow","dplyr","lubridate")
  miss <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
  if (length(miss)) install.packages(miss)
  lapply(pkgs, library, character.only = TRUE)
})
options(arrow.use_threads = TRUE, dplyr.summarise.inform = FALSE)

dbg <- function(...) message(format(Sys.time(), "%H:%M:%S"), " | ", paste0(..., collapse = ""))

# ---- Paths ----
HOUSE_IN  <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/house_spill_enriched.parquet"
EVENTS_IN <- "/Users/odran/Dropbox/sewage/data/processed/matched_events_annual_data/matched_events_annual_data.parquet"
HOUSE_OUT <- "/Users/odran/Dropbox/sewage/data/processed/housing_graph_data/house_spill_enriched_quartertag.parquet"

stopifnot(file.exists(HOUSE_IN), file.exists(EVENTS_IN))

# ---- Load ----
dbg("Loading households: ", HOUSE_IN)
hh <- arrow::read_parquet(HOUSE_IN)

dbg("Loading matched events: ", EVENTS_IN)
ev <- arrow::read_parquet(EVENTS_IN) %>%
  dplyr::select(site_id, start_time, end_time, dplyr::any_of("group_id_event"))

# Coerce to POSIXct and drop invalid rows
ev$start_time <- lubridate::as_datetime(ev$start_time, tz = "UTC")
ev$end_time   <- lubridate::as_datetime(ev$end_time,   tz = "UTC")

ev <- ev %>%
  dplyr::filter(!is.na(site_id), !is.na(start_time), !is.na(end_time), end_time >= start_time)

dbg("Rows: households=", nrow(hh), " | valid event rows=", nrow(ev))

# ---- Build EPISODES ----
# 1) Collapse rows with a non-NA group_id_event to episodes
epi_gid <- ev %>%
  dplyr::filter(!is.na(group_id_event)) %>%
  dplyr::group_by(site_id, group_id_event) %>%
  dplyr::summarise(
    episode_start = min(start_time),
    episode_end   = max(end_time),
    .groups = "drop"
  )

# 2) Treat rows with NA group_id_event as single-interval episodes
epi_raw <- ev %>%
  dplyr::filter(is.na(group_id_event)) %>%
  dplyr::transmute(site_id, episode_start = start_time, episode_end = end_time)

episodes <- dplyr::bind_rows(epi_gid, epi_raw)

dbg("Episodes: ", nrow(episodes), "  |  Sites: ", dplyr::n_distinct(episodes$site_id))

# ---- Compute SALE QUARTER for each house ----
hh_q <- hh %>%
  dplyr::mutate(
    sale_date = as.Date(date_of_transfer),
    q_start   = lubridate::floor_date(sale_date, unit = "quarter"),
    q_end     = lubridate::ceiling_date(sale_date, unit = "quarter")  # exclusive end
  ) %>%
  dplyr::select(house_id, spill_site_id, sale_date, q_start, q_end)

# ---- Overlap episodes with house sale quarter (site-by-site) ----
overlaps <- hh_q %>%
  dplyr::filter(!is.na(spill_site_id)) %>%
  dplyr::inner_join(
    episodes,
    by = c("spill_site_id" = "site_id"),
    relationship = "many-to-many"  # this join is intentionally many-to-many
  ) %>%
  dplyr::mutate(
    # quarter [q_start, q_end); overlap if episode crosses that window
    overlap = (episode_end >  lubridate::as_datetime(q_start)) &
              (episode_start < lubridate::as_datetime(q_end))
  ) %>%
  dplyr::filter(overlap) %>%
  dplyr::group_by(house_id) %>%
  dplyr::summarise(
    site_spilled      = TRUE,
    site_spilled_date = as.Date(min(episode_start)),
    .groups = "drop"
  )

# ---- Attach to households (FALSE if site present but no overlap; NA if no site) ----
hh_out <- hh %>%
  dplyr::left_join(overlaps, by = "house_id") %>%
  dplyr::mutate(
    site_spilled = dplyr::case_when(
      is.na(spill_site_id) ~ NA,       # no site to compare
      !is.na(site_spilled) ~ TRUE,     # overlap found
      TRUE                 ~ FALSE     # had a site but no overlap in sale quarter
    )
  )

# ---- Save & quick peek ----
arrow::write_parquet(hh_out, HOUSE_OUT)
dbg("Wrote: ", HOUSE_OUT)
dbg("Counts — site_spilled: TRUE=", sum(hh_out$site_spilled %in% TRUE,  na.rm = TRUE),
               " | FALSE=",         sum(hh_out$site_spilled %in% FALSE, na.rm = TRUE),
               " | NA=",            sum(is.na(hh_out$site_spilled)))

print(utils::head(hh_out %>%
  dplyr::select(house_id, spill_relation, spill_site_id, site_spilled, site_spilled_date), 10))
