source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))

map_remaining_shores <- function(reference_name = "mouth_review_reference.rds",
                                 diagnostic_name = "remaining_mouth_review.rds",
                                 prefix = "remaining-shores-") {
  root <- file.path(salience_report_root(), "geometry-alignment-50-sepa")
  raw <- here::here("data", "raw", "geography", "open_coast", "classifications")
  ea <- sf::st_transform(sf::st_read(file.path(raw, "ea.geojson"), quiet = TRUE), 27700)
  nrw_c <- sf::st_read(file.path(raw, "nrw-coastal.geojson"), quiet = TRUE)
  nrw_t <- sf::st_read(file.path(raw, "nrw-transitional.geojson"), quiet = TRUE)
  bodies <- dplyr::bind_rows(dplyr::transmute(ea, water_id = water_body_id, water_name = water_body_name, type = water_body_type),
    dplyr::transmute(nrw_c, water_id = wbid, water_name = wb_name, type = "Coastal"),
    dplyr::transmute(nrw_t, water_id = wbid, water_name = wb_name, type = "Transitional"))
  reference <- readRDS(file.path(root, "private", reference_name))
  diagnostic <- readRDS(file.path(root, "private", diagnostic_name))
  ids <- diagnostic |> dplyr::count(.data$source_id, sort = TRUE) |> dplyr::pull("source_id")
  for (page in seq_len(ceiling(length(ids)/4))) {
    png(file.path(root, paste0(prefix, page, ".png")), width = 2000, height = 1800, res = 160)
    par(mfrow = c(2,2), mar = c(3,3,3,1))
    for (index in ((page-1)*4+1):min(page*4,length(ids))) {
      shore <- reference$unresolved[reference$unresolved$source_id == ids[index], ]
      bounds <- sf::st_bbox(sf::st_buffer(shore, 750))
      region <- sf::st_as_sfc(bounds)
      plot(region, col = "white", border = NA, axes = TRUE, main = paste(index, substr(ids[index],1,8)))
      water <- suppressWarnings(sf::st_crop(bodies, bounds))
      if (nrow(water)) plot(sf::st_geometry(water), col = ifelse(water$type == "Coastal", "#cceeff", "#dedede"),
                           border = "#999999", add = TRUE)
      for (kind in c("retained", "excluded")) {
        lines <- suppressWarnings(sf::st_crop(reference[[kind]], bounds))
        if (nrow(lines)) plot(sf::st_geometry(lines), col = if (kind == "retained") "#0072B2" else "#777777", add = TRUE)
      }
      plot(sf::st_geometry(shore), col = "#D55E00", lwd = 2, add = TRUE)
      legend("bottomleft", legend = unique(water$water_name), bty = "n", cex = .7)
    }
    dev.off()
  }
  invisible(ids)
}

if (sys.nframe() == 0L) map_remaining_shores()
