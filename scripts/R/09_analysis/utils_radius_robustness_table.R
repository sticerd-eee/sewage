# ==============================================================================
# Shared helper: cross-radius robustness summary table
# ==============================================================================
# Builds one compact "robustness to house-to-site radius" table shared by the
# intensive (hedonic / did_trends / did_articles) and extensive-margin
# public-attention scripts. Layout:
#
#                         House Sales              House Rentals
#                    250m    500m   1000m     250m    500m   1000m
#   Property controls + MSOA FE   <coef across 6 models>
#   Property controls + LSOA FE   <coef across 6 models>
#
# i.e. 6 columns (outcome x radius) under two spanners, with the two preferred
# fixed-effects specifications as stacked panels. modelsummary cannot emit
# column-group spanners AND row panels in a single call, so we build the rbind
# panels with modelsummary (output = "tinytable") and add the spanner with
# tinytable::group_tt(j = ...). The resulting talltblr is then patched with the
# same idiom used for the per-radius tables (label, colsep/font, notes, X[c]).
#
# `models_by_radius`: named list keyed "250m"/"500m"/"1000m"; each element a list
#   with slots `sale_msoa`, `sale_lsoa`, `rent_msoa`, `rent_lsoa` (fitted models).
# `coef_map`: single-entry named vector mapping the treatment/interaction term.
# `custom_notes`: full `note{}={...},` string in sub()-replacement form (use \\\\).
# `escape`: FALSE when coef_map labels contain LaTeX math.

write_radius_robustness_table <- function(models_by_radius,
                                          radii,
                                          coef_map,
                                          custom_notes,
                                          label,
                                          title,
                                          output_path,
                                          fmt = 2,
                                          escape = TRUE) {
  rad_names <- paste0(radii, "m")
  n_rad <- length(radii)

  # Sales radii then rentals radii, for one FE spec.
  grab <- function(fe) {
    sales <- lapply(rad_names, function(r) models_by_radius[[r]][[paste0("sale_", fe)]])
    rents <- lapply(rad_names, function(r) models_by_radius[[r]][[paste0("rent_", fe)]])
    stats::setNames(c(sales, rents), c(rad_names, rad_names))
  }

  panels <- list(
    "Property controls + MSOA FE" = grab("msoa"),
    "Property controls + LSOA FE" = grab("lsoa")
  )

  tab <- modelsummary::modelsummary(
    panels,
    shape = "rbind",
    output = "tinytable",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt,
    coef_map = coef_map,
    gof_map = tibble::tribble(
      ~raw  , ~clean         , ~fmt,
      "nobs", "Observations" ,    0
    ),
    escape = escape,
    notes = " ",
    title = title
  )

  # Column-group spanners: cols 2..(1+n) = House Sales, next n = House Rentals.
  tab <- tinytable::group_tt(tab, j = list(
    "House Sales"   = 2:(1 + n_rad),
    "House Rentals" = (2 + n_rad):(1 + 2 * n_rad)
  ))

  tmp <- tempfile(fileext = ".tex")
  tinytable::save_tt(tab, tmp, overwrite = TRUE)
  latex <- paste(readLines(tmp), collapse = "\n")

  # Patches (same idiom as the per-radius tables).
  latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", latex)
  latex <- sub(
    "caption=\\{([^}]*)\\},",
    paste0("caption={\\1},\nlabel={", label, "},"),
    latex
  )
  latex <- sub(
    "(\\{\\s*%% tabularray inner open\\n)",
    "\\1colsep=2pt,\ncells   = {font = \\\\fontsize{8pt}{9pt}\\\\selectfont},\n",
    latex
  )
  latex <- sub("note\\{\\}=\\{\\s*\\},", custom_notes, latex)
  latex <- gsub("Q\\[\\]", "X[c] ", latex)
  latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", latex)

  writeLines(latex, output_path)
  invisible(output_path)
}
