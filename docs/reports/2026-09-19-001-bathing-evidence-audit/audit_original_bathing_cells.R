.libPaths(c("/Users/jacopoolivieri/projects/sewage/rv/library/4.6/arm64", .libPaths()))
library(dplyr)
library(readr)
library(readxl)
library(xml2)

source_root <- "/Users/jacopoolivieri/projects/sewage"
output_dir <- "/private/tmp/salience-bathing-audit"
trace <- read_csv(file.path(output_dir, "first_positive_2024_raw_history.csv"), show_col_types = FALSE)
workbooks <- file.path(source_root, "data/raw/edm_data", paste0(2021:2024, "_annual_return_edm.xlsx"))

clean_scalar_column <- function(values) {
  values[!is.na(values) & (values == "-" | grepl("n/a|#n/a|#na", tolower(values)))] <- NA_character_
  as.character(type.convert(values, as.is = TRUE))
}
excel_column <- function(index) {
  result <- ""
  while (index > 0L) {
    result <- paste0(LETTERS[(index - 1L) %% 26L + 1L], result)
    index <- (index - 1L) %/% 26L
  }
  result
}
read_member_xml <- function(path, member) {
  connection <- unz(path, member, open = "rb")
  on.exit(close(connection))
  read_xml(connection)
}
sheet_xml_paths <- function(path) {
  workbook <- read_member_xml(path, "xl/workbook.xml")
  rels <- read_member_xml(path, "xl/_rels/workbook.xml.rels")
  sheets <- xml_find_all(workbook, "//*[local-name()='sheet']")
  relationships <- xml_find_all(rels, "//*[local-name()='Relationship']")
  rel_ids <- xml_attr(relationships, "Id")
  targets <- xml_attr(relationships, "Target")
  sheet_ids <- vapply(sheets, function(sheet) xml_find_chr(sheet, "string(@*[local-name()='id'])"), character(1))
  targets <- targets[match(sheet_ids, rel_ids)]
  targets <- ifelse(startsWith(targets, "/"), substring(targets, 2L), paste0("xl/", targets))
  stats::setNames(targets, xml_attr(sheets, "name"))
}

audits <- lapply(seq_along(workbooks), function(index) {
  year_value <- 2020L + index
  workbook <- workbooks[[index]]
  cat(format(Sys.time()), "Reading original workbook", year_value, "\n")
  sheets <- excel_sheets(workbook)
  raw_rows <- bind_rows(lapply(sheets, function(sheet_name) {
    raw <- read_excel(workbook, sheet = sheet_name, skip = 1, col_types = "text", na = character(), trim_ws = FALSE, .name_repair = "minimal")
    clean_names <- janitor::make_clean_names(names(raw))
    company_column <- which(clean_names == "water_company_name")
    bath_column <- which(grepl("^bathing_water", clean_names))
    permit_column <- which(clean_names == "ea_permit_reference_ea_consents_database")
    site_column <- which(clean_names == "site_name_ea_consents_database")
    stopifnot(length(company_column) == 1L, length(bath_column) == 1L, length(permit_column) == 1L, length(site_column) == 1L)
    tibble(
      workbook_path = workbook, sheet = sheet_name, excel_row = seq_len(nrow(raw)) + 2L,
      bathing_column = excel_column(bath_column), bathing_header = names(raw)[bath_column],
      original_bathing_value = raw[[bath_column]],
      expected_cleaned_bathing_value = clean_scalar_column(raw[[bath_column]]),
      raw_company = raw[[company_column]],
      source_company = clean_scalar_column(raw[[company_column]]),
      source_permit = clean_scalar_column(raw[[permit_column]]),
      source_site_name = clean_scalar_column(raw[[site_column]])
    ) |>
      filter(!is.na(source_company)) |>
      mutate(source_company = if_else(source_company == "Dwr Cymru Welsh Water", "Welsh Water", source_company))
  })) |>
    mutate(year = year_value, annual_site_id = row_number())
  current_trace <- filter(trace, year == year_value)
  joined <- current_trace |>
    left_join(raw_rows, by = c("year", "annual_site_id"), relationship = "many-to-one") |>
    mutate(excel_cell = paste0(bathing_column, excel_row))
  same <- function(left, right) (is.na(left) & is.na(right)) | (!is.na(left) & !is.na(right) & left == right)
  stopifnot(!anyNA(joined$workbook_path), all(same(joined$water_company, joined$source_company)), all(same(joined$permit_reference_ea, joined$source_permit)), all(same(joined$site_name_ea, joined$source_site_name)))
  joined <- joined |>
    mutate(matches_current_parquet = same(bathing_water, expected_cleaned_bathing_value))
  stopifnot(all(joined$matches_current_parquet))
  sheet_paths <- sheet_xml_paths(workbook)
  shared_string_document <- read_member_xml(workbook, "xl/sharedStrings.xml")
  shared_strings <- xml_text(xml_find_all(shared_string_document, "//*[local-name()='si']"))
  result <- bind_rows(lapply(unique(joined$sheet), function(sheet_name) {
    rows <- filter(joined, sheet == sheet_name)
    document <- read_member_xml(workbook, sheet_paths[[sheet_name]])
    cells <- xml_find_all(document, "//*[local-name()='sheetData']/*[local-name()='row']/*[local-name()='c']")
    refs <- xml_attr(cells, "r")
    cell_indices <- match(rows$excel_cell, refs)
    rows$xml_cell_present <- !is.na(cell_indices)
    rows$xml_cell_type <- vapply(cell_indices, function(cell_index) if (is.na(cell_index)) NA_character_ else xml_attr(cells[[cell_index]], "t"), character(1))
    rows$xml_cached_value <- vapply(cell_indices, function(cell_index) if (is.na(cell_index)) NA_character_ else xml_find_chr(cells[[cell_index]], "string(*[local-name()='v'])"), character(1))
    rows$xml_formula <- vapply(cell_indices, function(cell_index) if (is.na(cell_index)) NA_character_ else xml_find_chr(cells[[cell_index]], "string(*[local-name()='f'])"), character(1))
    rows$xml_shared_string_text <- NA_character_
    shared_indices <- which(rows$xml_cell_type %in% "s")
    rows$xml_shared_string_text[shared_indices] <- shared_strings[as.integer(rows$xml_cached_value[shared_indices]) + 1L]
    rows |>
      mutate(original_cell_class = case_when(
        !xml_cell_present ~ "absent_blank_cell",
        xml_cell_type %in% "e" ~ "excel_error_cell",
        xml_cell_type %in% "s" & !is.na(xml_shared_string_text) & xml_shared_string_text == "" ~ "shared_string_empty_cell",
        !is.na(original_bathing_value) & is.na(expected_cleaned_bathing_value) ~ "text_marker_standardized_to_NA",
        is.na(original_bathing_value) & xml_cached_value == "" & (is.na(xml_formula) | xml_formula == "") ~ "present_empty_cell",
        is.na(original_bathing_value) ~ "other_missing_readxl_value",
        original_bathing_value %in% c("0", "Not Applicable", "no", "No") ~ "explicit_negative_literal",
        TRUE ~ "named_or_other_literal"
      ))
  }))
  cat(format(Sys.time()), "Validated original workbook", year_value, "\n")
  result
})
audit <- bind_rows(audits) |> arrange(site_id, year, annual_site_id)
write_csv(audit, file.path(output_dir, "first_positive_2024_original_cell_audit.csv"))
summary <- audit |>
  count(year, original_cell_class, original_bathing_value, name = "n_member_rows") |>
  arrange(year, original_cell_class, desc(n_member_rows))
write_csv(summary, file.path(output_dir, "first_positive_2024_original_cell_summary.csv"))
write_csv(audit |> count(year, original_cell_class, name = "n_member_rows"), file.path(output_dir, "first_positive_2024_original_cell_classes.csv"))
write_csv(tibble(path = workbooks, resolved_path = normalizePath(workbooks), bytes = as.numeric(file.info(workbooks)$size), modified = as.character(file.info(workbooks)$mtime), md5 = unname(tools::md5sum(workbooks))), file.path(output_dir, "original_workbook_source_metadata.csv"))
writeLines(c(
  "Read-only audit of original workbook cells for the97 Site Groups first observed positive in2024. Entire builder was never evaluated.",
  "Original annual IDs were traced by sheet order and retained company rows, then checked against company, permit, site-name anchors and current processed bathing values.",
  "Original cell references are inspected in workbook XML to distinguish absent/empty cells, Excel errors, and textual missing markers standardized by the cleaner.",
  "The cleaner treats '-' or any text matching n/a|#n/a|#na (case-insensitive) as NA, then runs type.convert. The later Site Group parser treats NA as not_designated.",
  "Original blanks/markers do not by themselves prove absence of official designation or a later effective date."
), file.path(output_dir, "original_workbook_scope.txt"))
print(summary, n = Inf, width = Inf)
