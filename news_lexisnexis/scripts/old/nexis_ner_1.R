#codes to explore Named Entity Recognition (NER) for Nexis results
#Aim is to explore firstly rule based, then statistical approaches
#ideally, location information exists for most articles


#start with "search_9" - possibly the most local coverage optimised search.
#can work with "search_8" if better


#required packages
library(dplyr)
library(stringr)
library(pdftools)
library(tidyr)
library(lubridate)
library(data.table)
library(tokenizers)
library(pbapply)


dp_path <- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/dp_backup/"



#RULE-BASED NER: place names, river names

#load in search_9 results

#needs altering for use:
nexis_path<- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/"

text<-pdf_text(paste0(nexis_path, "search_9/search_9_1.pdf"))

rawtext<- paste(text, collapse = " ")


#separate dates
dates <- unlist(regmatches(rawtext, gregexpr("Date:\\s+[A-Za-z0-9, ]+", rawtext)))

#separate source name

sources <- unlist(regmatches(
  rawtext,
  gregexpr("(?s)\\|\\s*Source:\\s*(.+?)(?=\\s*(\\|(\\s*Byline:|\\s*Section:)|\\d+\\b|$))", rawtext, perl = TRUE)
))
#this does not (yet) consider the cases where there are numbers within the source name
#In these cases, the name only shows what lies before the number

#simple cleaning for now...
sources <- gsub("[\r\n\t]+", " ", sources)
sources <- gsub("\\s+", " ", sources)
sources <- trimws(sources)   



#separate article text
chunk<-unlist(regmatches(rawtext, gregexpr("\\d+\\.\\s.*?\\sDate:", rawtext)))

#convert to dataframe
news<-data.frame(
  chunk=chunk,
  date=dates,
  source=sources
)


#replace first article (includes PDF title)
print(news$chunk[1])

news$chunk[1] <- "\n 1.    Visitors issued 'don't swim' warning at North Wales beach after 'possible\n       contamination'\n       ... plumb waste water pipes into surface water drains instead of the foul water sewerage system.\n       Successive campaigns have sought to address this issue. In 1999, major improvements were completed by\n       Dwr Cymru Welsh Water for Conwy, Deganwy, Llandudno and Colwyn Bay. Sewage is now pumped to\n       the Ganol Wastewater Treatment Works at Llandudno Junction and disinfected through ultra violet\n       treatment. The treated waste is discharged through a sea outfall at Penrhyn Bay east of Llandudno. This\n       scheme was...\n       ... will be updated via Natural Resources Wales when more water samples are taken.\" According to\n       Gogarth Mostyn representative Cllr Harry Saville, sampling revealed \"high levels\" of intestinal Enterococci.\n       As bacteria that live in animals, including humans, their presence in the Conwy estuary indicates possible\n       contamination by faecal waste. But Dwr Cymru Welsh Water said discharge data may have been\n       compromised by a \"false reading\" and that any pollution event may have originated further up the...\n       ... just once in June 2022. According to Dwr Cymru Welsh Water, a complex network of pumping stations\n       operates at West Shore. One of these manages Afon Creuddyn, a culverted watercourse that runs through\n       Llandudno and discharges at the Dale Road overflow . Periodic releases here are mostly surface water\n       only, said the company. Heavy rain may result in releases containing a highly diluted mixture of wastewater\n       and rainwater. This is permitted under environmental regulations, stressed Dwr Cymru. A...\n       ... estuary. The estuary's catchment extends around 15.5 miles 25km to Betws-y-Coed. As well as\n       sewage network overflows, the Afon Conwy and its tributaries capture seepage from sheep farming,\n       forestry and old metal mines. There are two storm overflows which discharge near West Shore itself. One,\n       the West Shore overflow, has been relatively inactive in since May, with just two small incidents. However\n       the Dale Road storm overflow has discharged 53 times in the past three months. While many of these...\n       ... Beachgoers are being warned not to go swimming at a popular beach in North Wales after faecal\n       bacteria was detected. Conwy councillors have urged bathers to avoid West Shore Beach in Llandudno\n       after receiving the latest findings from Natural Resources Wales NRW which monitors the waters for\n       pollution on Monday, August 18. Figures from Surfers Against Sewage show there have been more than 50\n       discharges from the area's Dale Road storm overflow in the last three months, North Wales Live reports...\n\n       Jurisdiction: United Kingdom of Great Britain and Northern Ireland | Date:"


#use in new function for PDF manipulation... (then remove from this script - include in the next)
parts <- strsplit(news$chunk[1], "1\\.", fixed = FALSE)[[1]]
# Join everything after the 2nd "1."
result <- paste(parts[-(1:2)], collapse = "1.")


#test this
search_term <- sub(".*Search Terms:(.*?)Search Type:.*", "\\1", rawtext)





#cleaning into headlines and article text
news <- news %>%
  mutate(
    heading = str_split_fixed(chunk, "\\.\\.\\.", 2)[, 1],
    article = str_split_fixed(chunk, "\\.\\.\\.", 2)[, 2]
  )%>%
  subset(select = -c(chunk))%>%
  mutate(heading=substr(heading, 5, nchar(heading)))%>%
  mutate(heading=trimws(heading))%>%
  mutate(article=trimws(article))


#remove \n (new line) regex, and punctuation
news <- news %>%
  mutate(heading = gsub("\n", " ", heading),
         article = gsub("\n", " ", article))%>%
  mutate(heading = gsub("[[:punct:]]", " ", heading),
         article = gsub("[[:punct:]]", " ", article))%>%
  mutate(heading = gsub("\\s+", " ", heading),
         article = gsub("\\s+", " ", article))

#removing leading or trailing spaces and captialisation (note that capitals can be good indicators of important inromation though)

news_2 <- news %>%
  mutate(heading = trimws(heading),
         article = trimws(article))%>%
  mutate(heading = tolower(heading),
         article = tolower(article))


#simple NER using a dataset of UK place names


place_names <- read.csv(paste0(dp_path, "raw/IPN_GB_2024/IPN_GB_2024.csv"))

place_names <- place_names %>%
  select(placeid, placesort, place23nm, grid1km, hlth23nm, lat, long)


#clean these strings
place_names <- place_names %>%
  mutate(place23nm = gsub("\n", " ", place23nm))%>%
  mutate(place23nm = gsub("[[:punct:]]", " ", place23nm))%>%
  mutate(place23nm = gsub("\\s+", " ", place23nm))%>%
  mutate(place23nm = trimws(place23nm))%>%
  mutate(place23nm = tolower(place23nm))





#save(news_2, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/LexisNexis/output frames/news_2.RData")

#save(place_names, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/LexisNexis/output frames/place_names.RData")



#Exact matching, for rule-based NER

#creating a column that merges back headline and text body
news_2 <- news_2 %>%
  mutate(text = paste(heading, article, sep = " "))

#how many words on average in a text extract?

news_2 <- news_2 %>%
  mutate(n_words = 1 + str_count(text, pattern = " "))

print(mean(news_2$n_words))
#356.6 words in each


#the UK Place Names data
place_names <- place_names %>%
  mutate(n_words = 1 + str_count(place23nm, pattern = " "))

print(max(place_names$n_words))
#max 11

print(mean(place_names$n_words))
#mean 1.65 


#the place names which are very long, likely aren't useful.

#the idea for an efficient matching algorithm is to 
#convert the place names into a data.table
#tokenize the article, searching the data table of place names for matches
#for each article, generating a column with a list of matched places
#(this is the basic idea, adding location information from the data etc later)



##Basic 1: match on 1 to 3 ngrams
places_vec_1 <- unique(place_names$place23nm)
places_vec_1 <- places_vec_1[sapply(strsplit(places_vec_1, " "), length) <= 3]
#59660 place names that are 3 or fewer words
place_dt_1 <- data.table(place = places_vec_1)
setkey(place_dt, place)
#(converted into a 1 variable data table)


#function for matching an article
match_place_names <- function(text, place_dt, max_ngram = 3) {
  
  text_ngrams <- unlist(lapply(1:max_ngram, function(n) {
    tokenize_ngrams(text, lowercase = TRUE, n = n)[[1]]
  }))
  
  text_ngrams <- unique(text_ngrams)  
  
  
  matched <- place_dt[J(text_ngrams), on = .(place), nomatch = 0L][, unique(place)]
  
  return(matched)
}


#(removed an updated function which split place names by word length, 
# then allowed matching. This was a mistake and didn't influence joins)

#test for match

news_test <- news_2

news_test$matched_places <- pblapply(news_test$text, match_place_names, place_dt_1)

#26s elapsed


#the only issue with the results is returned matches like "water" and "middle" which are not specific
#but these don't result from the function, they exist as individual entries in the Place Name corpus
#not allowing unigrams (1-grams) to match (or removing 1 word place names) would solve most of the issues, but possibly lose a lot of information eg "London"
#So best to experiment with other name databases



#but first, updating the function to allow other columns to join (eg location information)
#also to count instances



#adding counting instances.

match_place_names_counts <- function(text, place_dt, max_ngram = 3) {
  
  
  text_ngrams <- unlist(lapply(1:max_ngram, function(n) {
    tokenize_ngrams(text, lowercase = TRUE, n = n)[[1]]
  }))
  
 
  text_ngrams <- text_ngrams[nzchar(text_ngrams)]
  #gets rid of any NAs that might appear
  
  matched <- place_dt[J(text_ngrams), on = .(place), nomatch = 0L]

  if (nrow(matched) == 0L) return(NULL)
  
  #add the counts
  matched_counts <- matched[, .(count = .N), by = place]
  
  
  return(matched_counts)
}



news_test$matched_places_w_count <- pblapply(news_test$text, match_place_names_counts, place_dt_1)

#28s so still fast, and works well, returning a mini table for each element,
#which gives a list of the terms (place names) that occur, and how often for each


#adding in other data columns (from any place name dataset):


match_place_names_w_attributes <- function(text, place_dt, max_ngram = 3, add_cols = NULL) {
  
  
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
  
  
  return(counts[])
}


#applying this (need other columns to remain in datatable)



place_dt_2 <- as.data.table(place_names)

setnames(place_dt_2, old = "place23nm", new = "place")
place_dt_2[, place := as.character(place)]

place_dt_2 <- place_dt_2[n_words <= 3]
place_dt_2 <- unique(place_dt_2, by = "place")
#59660 so matches before.


news_test_2 <- news_test

news_test_2$matched_places_w_att <- pblapply(
  news_test_2$text,match_place_names_w_attributes, place_dt = place_dt_2, add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm")
)

#1m 32s elapsed (still fast enough)




#so now these can be plotted geographically, and interact with spill sites.
#the functions are flexible and work quickly

#2 things holding this back:

#1. hardcoding of place name variable as "place" - should change for flexibility
#2. the corpus itself has too many generic words as entries. Need to find alternatives to try.

#also TEMP - need to add a source column in the read.
#this should amend the general_nexis_read script too
#(delete when done)





#finalised function:
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
  
  
  
  
  
  
  
  
news_test_2$matched_place_names_final <- pblapply(
  news_test_2$text, rule_based_ner, place_dt = place_dt_2, join_col = "place" ,add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm")
)



#just need to check it works when the variable name is not "place" to begin with...

place_dt_3 <- as.data.table(place_names)
place_dt_3 <- place_dt_3[n_words <= 3]
place_dt_3 <- unique(place_dt_3, by = "place23nm")



news_test_2$matched_place_names_final_v2_test <- pblapply(
  news_test_2$text, rule_based_ner, place_dt = place_dt_3, join_col = "place23nm" ,add_cols = c("grid1km", "lat", "long", "placeid", "hlth23nm")
)
#adds some computational weight (renaming twice for each row)
#but still fast - 1m 32s total

#works as intented.




