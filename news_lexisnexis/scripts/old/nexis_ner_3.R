#script trying to apply basic statistical NER t o improve dictionary-based matches with IPN data


library(dplyr)
library(stringr)
library(pdftools)
library(tidyr)
library(lubridate)
library(data.table)
library(tokenizers)
library(pbapply)


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


#path to data folder:
dp_path <- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/dp_backup/"


#defining functions:



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





#applying to news data searches 8 and 9 
nexis_paths <- c("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/search_8/search_8_1.pdf", 
"C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/search_9/search_9_1.pdf")

#converting these into a dataframe
news_data <- nexis_pdf_to_table(nexis_paths)




#ideal: to use to pre trained spaCy model[s]

#Python SpaCy backend in R:
library(spacyr)




#trial on one text:


news_test <- news_data[1,]



spacy_initialize(model = "en_core_web_sm")
#Error in spacy_initialize(model = "en_core_web_sm") : 
#  No spaCy environment found. Use `spacy_install()` to get started.

#installed - working fine

#short trial of download:

txt <- "I travelled from Sheffield to Cambridge via March."
parsed <- spacy_parse(txt, entity = TRUE)
parsed
#works


#similar to tokenenise
parsed <- spacy_parse(news_test$text, entity = TRUE)


#location type words
locations <- parsed %>%
  filter(grepl("^GPE", entity) | grepl("^LOC", entity)) %>%
  filter(!grepl("\\s", token)) %>%     # optional: one-word names only
  distinct(token)
#still get these generic things like "beach"

locations_2 <- parsed %>%
  filter(grepl("^GPE", entity) | grepl("^LOC", entity)) %>%
  distinct(token)

locations_3 <- parsed %>%
  filter(grepl("^GPE", entity)) %>%
  distinct(token)


locations_3 <- parsed %>%
  filter(grepl("^GPE", entity))

##not the best performance. It treats each word individually,
#so even if we filter for proper nouns, we get examples like "Great" and, spearately "Britain"
#but, if we filter for propoer nouns, then remove capitals, and match to IPN
#we really could bypass the issues with "beach" "water" etc. Maybe...
#depends if "water" is capitalised within texts or not (part of another name)
#how do we keep several word place names in tact
#eg "Great Britain" should remain together

entities <- entity_extract(parsed)
entities

#oh this is cool, and useful.
#let's see what we can do with all this...
#it keeps phrases together (words that seem linked)
#but then it inserts an underscore rather than a space

#one example from the text used is "LLandudno_beach"

#will this exist within the IPN data?
#should only proper nouns be kept?
#(best to check behaviour of both options)



#looking for this one in IPN data...


llad_check <- place_names %>%
  filter(grepl("Llandudno", place23nm))
#only llandudno and llandudno junction exist, but we'd still like to match the "LLandudno beach"
#to LLandudno.
#this supports using proper nouns only. But it's just one example.



#Aim: make a function to apply statistical NER to extract likely place names, then match to place data


#firstly -> a function that just returns the spacy-given location names



statistical_rule_based_ner <- function(text) {
  # Ensures spacyr is initialized
  if (!spacyr::spacy_initialized()) {
    spacyr::spacy_initialize(model = "en_core_web_sm")
  }
  
  #ignore empty strings
  if (is.na(text) || !nzchar(text)) return(NULL)
  
 
  parsed <- spacy_parse(text, entity = TRUE)
  
  # Extract named entities
  entities <- entity_extract(parsed)
  
  
  # Keep only location names 
  locs <- entities %>%
    filter(grepl("GPE|LOC", entity_type))
  
  # replace underscores with space, cleaning
  locs <- locs %>%
    mutate(entity = gsub("_", " ", entity),
           entity = trimws(tolower(entity))) %>%
    distinct(entity)
  
  # Return as data table
  if (nrow(locs) == 0L) return(NULL)
  setDT(locs)
  
  return(locs[])
}




#ERROR



statistical_rule_based_ner <- function(text) {
  
  if (is.na(text) || !nzchar(text)) return(NULL)
  
  # try to move past errors
  parsed <- tryCatch(
    spacy_parse(text, entity = TRUE),
    error = function(e) {
      message("Initializing spacyr...")
      spacy_initialize(model = "en_core_web_sm")
      spacy_parse(text, entity = TRUE)
    }
  )
  
  
  entities <- entity_extract(parsed)
  
  # Keep only location names
  locs <- entities %>%
    filter(grepl("GPE|LOC", entity_type))
  
  # replace underscores and clean
  locs <- locs %>%
    mutate(entity = gsub("_", " ", entity),
           entity = trimws(tolower(entity))) %>%
    distinct(entity)
  
  
  if (nrow(locs) == 0L) return(NULL)
  locs <- as.data.table(locs)
  
  return(locs[])
}


#applying this to test it
news_test$spacy_entities <- pblapply(news_test$text, statistical_rule_based_ner)


#ok works well, now need to remove tolower()
#then match to IPN place names

#having as two separate functions:

library(data.table)

match_to_ipn <- function(entities_dt, place_dt, join_col = "place23nm", add_cols = NULL) {
  
  
  if (is.null(entities_dt) || nrow(entities_dt) == 0L) return(NULL)
  
  # replace column name temporarily
  place_dt <- copy(place_dt)
  setnames(place_dt, old = join_col, new = "place")
  
  
  entities_dt <- copy(entities_dt)
  setnames(entities_dt, old = "entity", new = "place")
  
  # Join extracted entities to place data
  matched <- place_dt[J(entities_dt$place), on = .(place), nomatch = 0L]
  
  if (nrow(matched) == 0L) return(NULL)
  
  # Count occurrences
  counts <- matched[, .(count = .N), by = place]
  
  # Add optional attributes from place name data
  if (!is.null(add_cols)) {
    valid_cols <- intersect(add_cols, names(place_dt))
    if (length(valid_cols) > 0) {
      meta <- unique(place_dt[, c("place", valid_cols), with = FALSE])
      counts <- merge(counts, meta, by = "place", all.x = TRUE)
    }
  }
  
  
  setnames(counts, old = "place", new = join_col)
  
  return(counts[])
}





ipn_dt <- clean_convert_place_data(place_names, name_column = place23nm)




news_test$ipn_matches <- pblapply(
  news_test$spacy_entities,
  match_to_ipn,
  place_dt = ipn_dt,
  join_col = "place23nm",
  add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)


#probably works, let's look at longer data than just the one test observation


news_data$spacy_entities <- pblapply(news_data$text, statistical_rule_based_ner)

news_data$ipn_matches <- pblapply(
  news_data$spacy_entities,
  match_to_ipn,
  place_dt = ipn_dt,
  join_col = "place23nm",
  add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)


#evidently, we need to clean up the extracted entities such that they are only the words with
#capital letters

print(news_data$ipn_matches[11])

#some good stuff, time for the last alteration (capitalisation / proper nouns only)









