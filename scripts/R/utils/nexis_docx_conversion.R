############################################################
# Nexis DOCX Conversion
# Project: Sewage
# Date: 14/07/2026
# Author: Ana Werneck
############################################################

#this script provides a function to convert Nexis .docx output to tabular data
#(counterpart to nexis_pdf_conversion.R, for use when LexisNexis is exported as
#one article per .docx file rather than as combined PDFs)

#  extract_nexis_zips()  - unzip the downloaded .docx exports under collision-safe
#                          names (needed because macOS is case-insensitive)
#  nexis_docx_to_table() - read the extracted .docx files into an article-level table

#extract Nexis .docx exports from their .zip downloads under sequential names,
#so headlines differing only by case cannot overwrite each other on macOS
extract_nexis_zips <- function(zip_paths, dest_dir) {
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  k <- 0
  for (zip_path in zip_paths) {
    entries <- utils::unzip(zip_path, list = TRUE)$Name
    entries <- entries[grepl("\\.docx$", entries, ignore.case = TRUE)]
    for (entry in entries) {
      k <- k + 1
      tmp <- file.path(tempdir(), "nexis", sprintf("%04d", k))
      utils::unzip(zip_path, files = entry, exdir = tmp)
      file.copy(file.path(tmp, entry),
                file.path(dest_dir, sprintf("%04d.docx", k)), overwrite = TRUE)
    }
  }
}
 
#reading .docx nexis output, and converting it to clean dataframe
#(note: unlike the PDF version this takes a FOLDER, since each article is its
#own file; the files themselves give the article boundaries)
nexis_docx_to_table <- function(docx_folder_path){
 
  #initiate an empty dataframe
  news <- NULL
 
  #a counter to print progress (useful for long lists)
  counter <- 0
 
  #a date paragraph starts with a month name, has an optional day, then a year.
  #matches all the formats seen in the data:
  #  "November 28, 2010"  "March 12 1989, Sunday"  "July 2024"
  #  "July 4, 2026 Saturday 2:57 PM GMT"
  month_pat <- "(January|February|March|April|May|June|July|August|September|October|November|December)"
  date_regex <- paste0("^", month_pat, "\\s+(?:(\\d{1,2})[, ]+)?(\\d{4})")
 
  #list all .docx files in the folder
  #(ignore.case = TRUE because LexisNexis writes uppercase .DOCX)
  docx_file_path_list <- list.files(docx_folder_path, pattern = "\\.docx$",
                                    full.names = TRUE, ignore.case = TRUE)
 
  for (i in docx_file_path_list) {
 
    #counter update
    counter <- counter + 1
 
    #wrap each file so one bad file doesn't abort the whole run
    tryCatch({
 
      #load the file as a paragraph-level table (one row per paragraph)
      summary_i <- officer::docx_summary(officer::read_docx(i))
      style_i <- summary_i$style_name
      #trim so marker comparisons (== "Body" etc.) are exact
      text_i <- trimws(summary_i$text)
 
      #headline: the paragraph tagged with style "heading 1"
      head_idx_i <- which(style_i == "heading 1")[1]
      heading_i <- text_i[head_idx_i]
 
      #date: the first non-blank paragraph after the headline that LOOKS like a
      #date. Found by content, not fixed position, because some files omit the
      #source line - which would otherwise shift the date onto the wrong row.
      after_head_i <- which(nzchar(text_i) & seq_along(text_i) > head_idx_i)
      is_date_i <- stringr::str_detect(text_i[after_head_i], stringr::regex(date_regex, ignore_case = TRUE))
      date_pos_i <- after_head_i[which(is_date_i)[1]]   #row of the date paragraph
 
      if (is.na(date_pos_i)) {
        #no date-like paragraph found - leave both blank (rare; worth inspecting)
        date_i <- as.POSIXct(NA)
        source_i <- NA_character_
      } else {
        #parse the date: month + optional day + year (assign day 1 if no day)
        m_i <- stringr::str_match(text_i[date_pos_i], stringr::regex(date_regex, ignore_case = TRUE))
        day_i <- ifelse(is.na(m_i[3]), "1", m_i[3])
        date_i <- lubridate::parse_date_time(paste(m_i[2], day_i, m_i[4]), orders = "b d Y")
 
        #source: the non-blank paragraph directly above the date. If that is the
        #headline itself, the file has no source line -> NA
        prev_i <- rev(which(nzchar(text_i) & seq_along(text_i) < date_pos_i))[1]
        source_i <- if (prev_i == head_idx_i) NA_character_ else text_i[prev_i]
      }
 
      #article body: everything between the "Body" marker and the end markers
      #(kept in full - no content removal here)
      body_start_i <- which(text_i == "Body")[1]
      body_end_i <- min(c(
        which(stringr::str_detect(text_i, "^Load-Date:")),
        which(text_i == "End of Document")
      ))
      body_i <- text_i[(body_start_i + 1):(body_end_i - 1)]
      #join paragraphs into one string; str_squish just normalises whitespace
      #(collapses the blank-line gaps into single spaces - not content cleaning)
      body_i <- stringr::str_squish(paste(body_i, collapse = " "))
 
      #merge into a one-row dataframe (date already parsed above)
      news_i <- data.frame(
        file = basename(i),
        source = source_i,
        date = date_i,
        heading = heading_i,
        body = body_i,
        stringsAsFactors = FALSE
      )
 
      #append onto data
      news <- rbind(news, news_i)
 
    }, error = function(e) {
      message(paste("FAILED:", basename(i), "-", conditionMessage(e)))
    })
 
    print(paste("processed", counter, "file[s]", sep = " "))
  }
 
  return(news)
}
