#this follows from nexis_ner_1 and lexisnexis_read_clean_function scripts
#completed:
# ~ a tractable function to read PDF nexis output into a clean dataframe with good format
# ~ first attempt at matching to a location database, with a flexible function

#this script to do:
# ~ trial rule-based NER with different place name corpuses
# ~ get results in a clean output frame, export (to be plotted with Leaflet)


#required packages
library(dplyr)
library(stringr)
library(pdftools)
library(tidyr)
library(lubridate)
library(data.table)
library(tokenizers)
library(pbapply)
#for river data only:
library(sf)

#path to data folder:
dp_path <- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/dp_backup/"


###defining functions:


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
      mutate(text = paste(heading, article, sep = " "))%>%
      mutate(text = tolower(text))
    
    
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




#extracting matched place or entity names to any news article, retaining any chosen attributes
rule_based_ner <- function(text, place_dt, join_col = "place", max_ngram = 3,  add_cols = NULL) {
  
  
  original_name <- join_col
  
  
  if (join_col != "place") {
    place_dt <- copy(place_dt)  
    setnames(place_dt, old = join_col, new = "place")
  }
  
  
  
  
  text_ngrams <- unlist(lapply(1:max_ngram, function(n) {
    tokenize_ngrams(text, lowercase = TRUE, n = n)[[1]]
  }))
  
  
  text_ngrams <- text_ngrams[nzchar(text_ngrams)]
  
  
  matched <- place_dt[J(text_ngrams), on = .(place), nomatch = 0L]
  
  
  if (nrow(matched) == 0L) return(NULL)
  
  
  counts <- matched[, .(count = .N), by = place]
  
  
  #extra attributes (eg geolocation etc)
  if (!is.null(add_cols)) {
    
    #column names must exist in the place name datatable
    valid_cols <- intersect(add_cols, names(place_dt))
    
    if (length(valid_cols) > 0) {
      meta <- unique(place_dt[, c("place", valid_cols), with = FALSE])
      counts <- merge(counts, meta, by = "place", all.x = TRUE)
    }
  }
  
  
  
  if (original_name != "place") {
    setnames(counts, old = "place", new = original_name)
  }
  
  
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


clean_convert_place_data_2 <- function (place_data, name_column = place) {
  #note to input column name without quotation marks
  
  place_data <- place_data %>%
    mutate({{ name_column }} := gsub("\n", " ", {{ name_column }}))%>%
    mutate({{ name_column }} := gsub("[[:punct:]]", "",{{ name_column }}))%>%
    mutate({{ name_column }} := gsub("\\s+", " ",{{ name_column }}))%>%
    mutate({{ name_column }} := trimws({{ name_column }}))%>%
    mutate({{ name_column }} := tolower({{ name_column }}))
  
  #perhaps remove this, when two places have the same name it's best to retain both
  #place_data <- place_data%>%
  #  distinct({{ name_column }}, .keep_all = T)
  #(disabled for now ~ this may cause issues for general words like "water")
  #add in new column called ipn_duplicate_matches which allows duplciate place names
  
  place_dt <- as.data.table(place_data)
  
  return(place_dt)
  
}




###applying the functions

#file paths to search_8 and search_9 data:
#(change as needed)
nexis_paths <- c("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/search_8/search_8_1.pdf", 
                 "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/search_9/search_9_1.pdf")

#converting these into a dataframe
news_89 <- nexis_pdf_to_table(nexis_paths)

#saved this dataframe
#save(news_89, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/news_searches_8_9.RData")



## UK Place Names dataset (Index of Place Names 2024)
#https://geoportal.statistics.gov.uk/datasets/208d9884575647c29f0dd5a1184e711a/about


place_names <- read.csv(paste0(dp_path, "raw/IPN_GB_2024/IPN_GB_2024.csv"))

#turn into a data.table for fast matching
ipn_dt <- clean_convert_place_data(place_names, name_column = place23nm)


#finding matches of place names within each article:
news_89$ipn_matches <- pblapply(
  news_89$text, rule_based_ner, place_dt = ipn_dt, join_col = "place23nm", add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)


##Major Towns and Cities dataset boundaries (2015)
#https://geoportal.statistics.gov.uk/datasets/980da620a0264647bd679642f96b42c1_0/explore?location=52.516015%2C-1.437868%2C6.85


towns_cities_names <- read.csv(paste0(dp_path, "raw/Major_Towns_and_Cities_Dec_2015_Boundaries_V2_2022_140358522642712462.csv"))
#only 112 

#turn into a data.table for fast matching
town_dt <- clean_convert_place_data(towns_cities_names, name_column = TCITY15NM)

#finding matches of place names within each article:
news_89$tcity_matches <- pblapply(
  news_89$text, rule_based_ner, place_dt = town_dt, join_col = "TCITY15NM", add_cols = c("TCITY15CD", "BNG_E", "BNG_N", "LONG", "LAT", "Shape_Area", "Shape_Length", "GlobalID")
)

#many more are NULL for this dataset, which makes sense given the list is so small (112)


##Rivers data, possibly harder as it is a shapefile, rather than csv.

river_sf<-read_sf(paste0(dp_path, "raw/OSRivers_shapefile/WatercourseLink.shp"))

#turn into a data.table for fast matching
river_dt <- clean_convert_place_data(river_sf, name_column = name1)
#(takes a lot longer as river_sf is an sf object)


#dropping geometry for now
river_dt[, geometry := NULL]

#finding matches of place names within each article:(expecting few due to the nature of how rivers are
#named in the rivers data. It's their exact technical rather than colloquial name)
news_89$river_matches <- pblapply(
  news_89$text, rule_based_ner, place_dt = river_dt, join_col = "name1", add_cols = c("identifier", "startNode", "endNode", "form", "flow", "fictitious", "length", "name2", "geometry")
)

news_89$ipn_matches[[1]]


#save(news_89, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_news_searches_8_9.RData")


##2nd Option using IPN place name database

# ~ a lot of matches of one word match explicitly generic words (eg "water")
# ~ ideal would be to go through the IPN database and remove these words
# ~ but the database is 104k+ words long, so it could take a lot of time
# ~ a quick alternative is to filter out all one word places
# ~ this will remove many good matches, but will make confidence increase in the matches we get


#filter for 2 or more words
ipn_2_dt <- ipn_dt[grepl("\\s+", place23nm)]


news_89$ipn_2_matches <- pblapply(
  news_89$text, rule_based_ner, place_dt = ipn_2_dt, join_col = "place23nm", add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)

save(news_89, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_news_searches_8_9.RData")




load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_news_searches_8_9.RData")


#adding an IPN match column allowing duplicate place names to match

ipn_duplicates_dt <- clean_convert_place_data_2(place_names, name_column = place23nm)

news_89$ipn_duplicate_matches <- pblapply(
  news_89$text, rule_based_ner, place_dt = ipn_duplicates_dt, join_col = "place23nm", add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)
#takes much longer ~ 6 minutes


save(news_89, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_news_searches_8_9.RData")


View(news_89)

print(news_89$ipn_duplicate_matches)


#another approach we could take is to remove all place names in IPN which appear several times
#within IPN (as they are not unique)
#but looking at examples - such as "Sussex" - we could lose a lot of information doing this




#summarise missing values:

count_missing <- news_89%>%
  filter(news_89$ipn_matches == "NULL")
#0

count_missing <- news_89%>%
  filter(news_89$ipn_2_matches == "NULL")
#442


count_missing <- news_89%>%
  filter(news_89$river_matches == "NULL")
#568 missing


count_missing <- news_89%>%
  filter(news_89$tcity_matches == "NULL")
#833 missing

check_water <- place_names%>%
  filter(grepl("North", place23nm))


check_water <- place_names%>%
  filter(place23nm =="North")



#Q: how many articles mention a place name within IPN in the headline?

#adding a lower case headline column

news_89 <- news_89 %>%
  mutate(head_lower = tolower(heading))




news_89$ipn_heading_matches <- pblapply(
  news_89$head_lower, rule_based_ner, place_dt = ipn_dt, join_col = "place23nm", add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm", "ctyhistnm")
)



count_missing <- news_89%>%
  filter(news_89$ipn_heading_matches == "NULL")
#275 out of 1335



save(news_89, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_news_searches_8_9.RData")


