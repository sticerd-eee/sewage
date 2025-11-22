#this script using dictionary-based NER matching to extract location data from news texts
#for this script, we use searches 8 and 9, the processed file searches_89_matched_dict.RData


#required packages
library(dplyr)
library(stringr)
library(pdftools)
library(tidyr)
library(lubridate)
library(data.table)
library(tokenizers)
library(pbapply)
library(spacyr)


#path to raw data folder:
dp_path <- "C:/Users/danan/Dropbox/news_lexisnexis/data/raw"


#path to processed data 
processed <- "C:/Users/danan/Dropbox/news_lexisnexis/data/processed"



###defining functions:


#using spaCy NER model to extract place names
statistical_rule_based_ner <- function(text) {
  # Skip empty or NA text
  if (is.na(text) || !nzchar(text)) return(NULL)
  
  # initialise spacy if not already + parse data
  parsed <- tryCatch(
    spacy_parse(text, entity = TRUE),
    error = function(e) {
      message("Initializing spacyr...")
      spacy_initialize(model = "en_core_web_sm")
      spacy_parse(text, entity = TRUE)
    }
  )
  
  # Extract named entities
  entities <- entity_extract(parsed)
  
  # Keep only location-related entities
  locs <- entities %>%
    filter(grepl("GPE|LOC", entity_type))
  
  if (nrow(locs) == 0L) return(NULL)
  
  # Keep only entities that start with a capital letter
  locs <- locs %>%
    filter(grepl("^[A-Z]", entity))
  
  # Replace underscores, trim
  locs <- locs %>%
    mutate(entity = gsub("_", " ", entity),
           entity = trimws(entity))
  
  
  locs <- locs %>%
    rowwise() %>%
    mutate(entity = {
      res <- str_extract_all(entity, "\\b[A-Z][A-Za-z\\-']*\\b")
      if (length(res) == 0 || length(res[[1]]) == 0) {
        NA_character_
      } else {
        paste(res[[1]], collapse = " ")
      }
    }) %>%
    ungroup() %>%
    filter(!is.na(entity))
  
  
  # convert to lowercase for matching
  locs <- locs %>%
    mutate(entity = tolower(entity)) %>%
    distinct(entity)
  
  # Return as data table
  if (nrow(locs) == 0L) return(NULL)
  locs <- as.data.table(locs)
  
  return(locs[])
}


#second function to match these extracted place names with a place name database
match_to_place_data <- function(entities_dt, place_dt, join_col = "place23nm", add_cols = NULL) {
  
  # return null if entities aren't found in text
  if (is.null(entities_dt) || nrow(entities_dt) == 0L) return(NULL)
  
  #standardise column names
  place_dt <- copy(place_dt)
  setnames(place_dt, old = join_col, new = "place")
  
  # set names for entity data to match
  entities_dt <- copy(entities_dt)
  
  setnames(entities_dt, old = "entity", new = "place")
  
  # Join extracted entities to place data
  matched <- place_dt[J(entities_dt$place), on = .(place), nomatch = 0L]
  
  if (nrow(matched) == 0L) return(NULL)
  
  #adds counts
  counts <- matched[, .(count = .N), by = place]
  
  
  
  # Additional attributes from place name data
  if (!is.null(add_cols)) {
    valid_cols <- intersect(add_cols, names(place_dt))
    if (length(valid_cols) > 0) {
      meta <- unique(place_dt[, c("place", valid_cols), with = FALSE])
      counts <- merge(counts, meta, by = "place", all.x = TRUE)
    }
  }
  
  
  #replace names with original again
  setnames(counts, old = "place", new = join_col)
  
  return(counts[])
}




#to clean and convert place name data into a data.table
clean_convert_place_data <- function (place_data, name_column = place) {
  #note to input column name without quotation marks
  
  place_data <- place_data %>%
    mutate({{ name_column }} := gsub("\n", " ", {{ name_column }}))%>%
    mutate({{ name_column }} := gsub("[[:punct:]]", "",{{ name_column }}))%>%
    mutate({{ name_column }} := gsub("\\s+", " ",{{ name_column }}))%>%
    mutate({{ name_column }} := trimws({{ name_column }}))%>%
    mutate({{ name_column }} := tolower({{ name_column }}))
  
  place_data <- place_data%>%
    distinct({{ name_column }}, .keep_all = T)
  
  place_dt <- as.data.table(place_data)
  
  return(place_dt)
  
}




##Getting IPN place data back in environment:

place_names <- read.csv(paste0(dp_path, "/IPN_GB_2024/IPN_GB_2024.csv"))

#turn into a data.table for fast matching
ipn_dt <- clean_convert_place_data(place_names, name_column = place23nm)



### match via spaCy name extraction

#intitialise spaCy model

spacy_initialize(model = "en_core_web_sm")


#load news data

load(paste0(processed, "/searches_89_matched_dict.RData"))


#adding a column for capitalised text (not lowercase)

news_89 <- news_89 %>%
  mutate(temp_text = paste(heading, article, sep = " "))

#extracting location name entities 

news_89$spacy_entities <- pblapply(news_89$temp_text, statistical_rule_based_ner)



#matching with IPN data:

news_89$ipn_matches_spacy <- pblapply(
  news_89$spacy_entities,
  match_to_place_data,
  place_dt = ipn_dt,
  join_col = "place23nm",
  add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)


news_89 <- news_89%>%
  select(-temp_text)



#save

save(news_89, file = paste0(processed, "/searches_89_matched_final.RData"))