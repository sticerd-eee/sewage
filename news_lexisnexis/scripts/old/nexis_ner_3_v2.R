#troubleshooting ner_3 for a working statistical NER with refinements


library(dplyr)
library(stringr)
library(pdftools)
library(tidyr)
library(lubridate)
library(data.table)
library(tokenizers)
library(pbapply)
library(spacyr)


#needed background functions



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



#reading PDF nexis output, and converting it to clean dataframe
nexis_pdf_to_table <- function(pdf_file_path_list){
  
  #initiate an empty dataframe
  news<-NULL
  
  #a counter to print progress (useful for long lists)
  counter <- 0
  
  for (i in pdf_file_path_list) {
    
    #counter update
    counter <- counter + 1
    
    #load in the file as text
    text_i <- pdf_text(i)
    #join pages together into one long chunk
    rawtext_i<- paste(text_i, collapse = " ") 
    
    #create vectors for each attribute to extract:
    #date
    dates_i <- unlist(regmatches(rawtext_i, gregexpr("Date:\\s+[A-Za-z0-9, ]+", rawtext_i)))
    
    
    #source name
    sources_i <- unlist(regmatches(
      rawtext_i,
      gregexpr("(?s)\\|\\s*Source:\\s*(.+?)(?=\\s*(\\|(\\s*Byline:|\\s*Section:)|\\d+\\b|$))", rawtext_i, perl = TRUE)
    ))
    #(note this has a limitation - source names which include a number within them only return the letters before any number)
    
    #article text + headline (as one)
    chunk_i <- unlist(regmatches(rawtext_i, gregexpr("\\d+\\.\\s.*?\\sDate:", rawtext_i)))
    
    #search terms used (query)
    search_term_i <- sub(".*Search Terms:(.*?)Search Type:.*", "\\1", rawtext_i)
    
    
    
    #merge into a dataframe:
    news_i<-data.frame(
      search_term = search_term_i,
      chunk=chunk_i,
      date=dates_i,
      source=sources_i
    )
    
    
    #replacing the first observation with the correct article
    #(because it includes other information in the PDF)
    parts_i <- strsplit(news_i$chunk[1], "1\\.", fixed = FALSE)[[1]]
    news_i$chunk[1] <- paste(parts_i[-(1:2)], collapse = "1.")
    
    
    
    #Cleaning the dataframe:
    
    #cleaning article headline and body
    news_i <- news_i %>%
      mutate(
        heading = str_split_fixed(chunk, "\\.\\.\\.", 2)[, 1],
        article = str_split_fixed(chunk, "\\.\\.\\.", 2)[, 2]
      )%>%
      subset(select = -c(chunk))%>%
      mutate(heading=substr(heading, 5, nchar(heading)))%>%
      mutate(heading=trimws(heading))%>%
      mutate(article=trimws(article))%>%
      #removing punctuation and new line regex
      mutate(heading = gsub("\n", " ", heading),
             article = gsub("\n", " ", article))%>%
      mutate(heading = gsub("[[:punct:]]", "", heading),
             article = gsub("[[:punct:]]", "", article))%>%
      mutate(heading = gsub("\\s+", " ", heading),
             article = gsub("\\s+", " ", article))
    
    #separate column, joining headline + body as lower-case for rule-based NER matching
    news_i <- news_i %>%
      mutate(text = paste(heading, article, sep = " "))
    #removes tolower() argument for better statistical NER
    
    
    #dealing with dates:
    
    news_i <- news_i %>%
      mutate(date = gsub("[[:punct:]]", "", date))%>%
      mutate(date = gsub("Date", "", date))%>%
      mutate(date = trimws(date))%>%
      mutate(date = tolower(date))
    
    news_i <- news_i %>%
      mutate(date = parse_date_time(date, orders = "b d Y"))
    
    
    #cleaning source names
    news_i <- news_i %>%
      mutate(source = gsub("| Source:", "", source))%>%
      mutate(source = gsub("\\|", "", source))%>%
      mutate(source = gsub("\\s+", " ", source))%>%
      mutate(source = trimws(source))
    
    
    #append onto data
    news <- rbind(news, news_i)
    
    print(paste("processed", counter, "PDF[s]", sep = " "))
  }
  
  return(news)
}


##Preparing data:


#path to data folder:
dp_path <- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/dp_backup/"


#applying to news data searches 8 and 9 
nexis_paths <- c("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/search_8/search_8_1.pdf", 
                 "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/search_9/search_9_1.pdf")

#converting these into a dataframe
news_data <- nexis_pdf_to_table(nexis_paths)

news_test <- news_data[1,]


place_names <- read.csv(paste0(dp_path, "raw/IPN_GB_2024/IPN_GB_2024.csv"))

ipn_dt <- clean_convert_place_data(place_names, name_column = place23nm)


#initialise spacy model
spacy_initialize(model = "en_core_web_sm")




#attempted fixed function:


statistical_rule_based_ner_4 <- function(text) {
  if (is.na(text) || !nzchar(text)) return(NULL)
  
  parsed <- tryCatch(
    spacy_parse(text, entity = TRUE),
    error = function(e) {
      message("Initializing spacyr...")
      spacy_initialize(model = "en_core_web_sm")
      spacy_parse(text, entity = TRUE)
    }
  )
  
  entities <- entity_extract(parsed)
  
  # Keep only location entities
  locs <- entities %>%
    filter(grepl("GPE|LOC", entity_type))
  
  if (nrow(locs) == 0L) return(NULL)
  
  # Keep only those that start with a capital letter (eg not "beach")
  locs <- locs %>%
    filter(grepl("^[A-Z]", entity))
  
  # Replace underscores, clean
  locs <- locs %>%
    mutate(entity = gsub("_", " ", entity),
           entity = trimws(entity))
  
  # Extract capitalised words only
  locs <- locs %>%
    rowwise() %>%
    mutate(entity = {
      words <- str_extract_all(entity, "\\b[A-Z][A-Za-z\\-']*\\b")[[1]]
      if (length(words) > 0) paste(words, collapse = " ") else NA_character_
    }) %>%
    ungroup() %>%
    filter(!is.na(entity))
  
  
  # Normalize to lowercase for matching
  locs <- locs %>%
    mutate(entity = tolower(entity)) %>%
    distinct(entity)
  
  if (nrow(locs) == 0L) return(NULL)
  as.data.table(locs)
}



news_test$spacy_entities_4 <- pblapply(news_test$text, statistical_rule_based_ner_4)
#works


news_data$spacy_entities_4 <- pblapply(news_data$text, statistical_rule_based_ner_4)
#error yet again





statistical_rule_based_ner_5 <- function(text) {
  # Skip empty or NA text
  if (is.na(text) || !nzchar(text)) return(NULL)
  
  # Try parsing; initialize if necessary
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




news_test$spacy_entities_5 <- pblapply(news_test$text, statistical_rule_based_ner_5)



news_data$spacy_entities_5 <- pblapply(news_data$text, statistical_rule_based_ner_5)


print(news_data$text[7])


#this might be the best we'll get with this for now. It's a good first pass




match_to_ipn <- function(entities_dt, place_dt, join_col = "place23nm", add_cols = NULL) {
  
  # If no entities were found
  if (is.null(entities_dt) || nrow(entities_dt) == 0L) return(NULL)
  
  # Copy and standardize column names
  place_dt <- copy(place_dt)
  setnames(place_dt, old = join_col, new = "place")
  
  # Prepare entities for matching
  entities_dt <- copy(entities_dt)
  setnames(entities_dt, old = "entity", new = "place")
  
  # Join extracted entities to place data
  matched <- place_dt[J(entities_dt$place), on = .(place), nomatch = 0L]
  
  if (nrow(matched) == 0L) return(NULL)
  
  # Count occurrences
  counts <- matched[, .(count = .N), by = place]
  
  # Add optional metadata columns
  if (!is.null(add_cols)) {
    valid_cols <- intersect(add_cols, names(place_dt))
    if (length(valid_cols) > 0) {
      meta <- unique(place_dt[, c("place", valid_cols), with = FALSE])
      counts <- merge(counts, meta, by = "place", all.x = TRUE)
    }
  }
  
  # Restore original join column name
  setnames(counts, old = "place", new = join_col)
  
  return(counts[])
}

news_test$ipn_matches <- pblapply(
  news_test$spacy_entities_5,
  match_to_ipn,
  place_dt = ipn_dt,
  join_col = "place23nm",
  add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)



news_data$ipn_matches <- pblapply(
  news_data$spacy_entities_5,
  match_to_ipn,
  place_dt = ipn_dt,
  join_col = "place23nm",
  add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)






save(news_data, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_statistical_news_searches_8_9.RData")



number_matched <- news_data %>%
  filter(ipn_matches != "NULL")
#846 out of 1335


print(news_data$article[1])
