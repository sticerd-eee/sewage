############################################################
# National Grid Reference Utilities
# Project: Sewage
############################################################

#' Standardize grid references (NGR)
#' @param x Character vector of raw NGR strings
#' @return Cleaned NGR strings or NA
clean_ngr <- function(x) {
  x <- as.character(x)
  x <- dplyr::case_when(
    toupper(x) %in% c(
      "UNABLE TO MATCH TO CONSENTS DATABASE",
      "NOT IN CONSENTS DATABASE"
    ) ~ NA_character_,
    TRUE ~ x
  )

  x <- ifelse(
    stringr::str_detect(x, "(?i)and"),
    stringr::str_extract(x, "(?i)(?<=and).*"),
    x
  )
  x <- stringr::str_replace(x, "(?i)Not.*", "")

  x <- stringr::str_extract(x, "^[^,&/\\(]+")
  x <- stringr::str_remove_all(x, "\\s+")

  trimws(x)
}

#' Parse British National Grid coordinates from NGR strings
#' @param ngr Character vector of NGR values
#' @return Tibble with easting and northing columns
parse_bng_coordinates <- function(ngr) {
  coords <- purrr::map(
    ngr,
    ~ tryCatch(
      suppressWarnings(rnrfa::osg_parse(.x, coord_system = "BNG")),
      error = function(e) list(easting = NA_real_, northing = NA_real_)
    )
  )

  tibble::tibble(
    easting = purrr::map_dbl(coords, "easting"),
    northing = purrr::map_dbl(coords, "northing")
  )
}
