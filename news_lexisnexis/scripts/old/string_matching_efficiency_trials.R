#load data

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/LexisNexis/output frames/news_2.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/LexisNexis/output frames/place_names.RData")


#aim is to search for any of the 104395 place names in all of the 804 articles,
#saving which are present in each, perhaps with an accompanying count, or
#sorting via number of times mentioned. But that is secondary, the match & extract is most important



#Prevoiusly, I had an OR style str_extract_all() function, which took 
#around an hour for just 44 articles. For 804 then, we'd expect 19 to 20 hours
#far too long and inefficient.


#before trying other approaches to do this task,
#let's count the number of words in the average article


#Also it's best to merge headlines and article body once again, in case
#places are mentioned in either

library(dplyr)
library(stringr)
library(tidyr)

#merging headline and body


news_2 <- news_2 %>%
  mutate(text = paste(heading, article, sep = " "))

#how many words on average in a text extract?

news_2 <- news_2 %>%
  mutate(n_words = 1 + str_count(text, pattern = " "))

print(mean(news_2$n_words))
#356.6 words in each


#same for place names, to determine token size

place_names <- place_names %>%
  mutate(n_words = 1 + str_count(place23nm, pattern = " "))

print(max(place_names$n_words))
#max 11

print(mean(place_names$n_words))
#mean 1.65 

#the high number of words are outliers that don't make much sense
#we will go with 1 to 3 ngram tokens



library(data.table)
library(tokenizers)
library(pbapply)


#test1: a function & implementation without counting or sorting (simplest)


news_test <- sample_n(news_2, size = 10)
#(a subsample for ease of testing, lower computational expense)



places_vec <- unique(place_names$place23nm)


places_vec <- places_vec[sapply(strsplit(places_vec, " "), length) <= 3]
#59660 place names

place_dt <- data.table(place = places_vec)
setkey(place_dt, place)



match_place_names <- function(text, place_dt, max_ngram = 3) {
  
  text_ngrams <- unlist(lapply(1:max_ngram, function(n) {
    tokenize_ngrams(text, lowercase = TRUE, n = n)[[1]]
  }))
  
  text_ngrams <- unique(text_ngrams)  
  
  
  matched <- place_dt[J(text_ngrams), on = .(place), nomatch = 0L][, unique(place)]
  
  return(matched)
}


#test

news_test$matched_places <- pblapply(news_test$text, match_place_names, place_dt)

#works efficiently, but using n-gram tokenisation loses out with false positives
#for example, matching words like "water"

#so we can make this a lot better probably, without having 1 to 3 n grams, 
#and actually keeping the place names more intact.


#mainly these are issues with unigrams from the n-grams.


#rewriting the function and application:


match_places_strict_ngram <- function(text, place_dt_by_length) {
  matched <- character(0)
  
  for (n in 1:3) {
    ngrams <- tokenize_ngrams(text, lowercase = TRUE, n = n)[[1]]
    if (length(ngrams) == 0) next
    
    place_dt <- place_dt_by_length[[as.character(n)]]
    if (is.null(place_dt)) next
    
    matches <- place_dt[J(ngrams), on = .(place), nomatch = 0L][, unique(place)]
    matched <- c(matched, matches)
  }
  
  return(unique(matched))
}



# Split place names by word count 1 to 3
place_lengths <- sapply(strsplit(place_names$place23nm, " "), length)
place_dt_by_length <- split(place_names$place23nm, place_lengths)
place_dt_by_length <- lapply(place_dt_by_length[names(place_dt_by_length) %in% c("1", "2", "3")], function(x) {
  dt <- data.table(place = unique(x))
  setkey(dt, place)
  return(dt)
})



news_test$matched_places_2 <- pblapply(
  news_test$text,
  match_places_strict_ngram,
  place_dt_by_length
)




#troubleshooting: still get words like water and middle matched
#this will either be due to an incorrect function
#or these terms actually exist alone in the place names dataset



#are they within a place name dataset


#if they are, they will include the word, and not include a space in theory
#we can check for all instances of the word anyway


#search test
water_place <- place_names%>%
  filter(grepl("water", place23nm))

water_place2 <- water_place%>%
  filter(!grepl(" ", place23nm))

#there is actually a place name that is wholly "water"
#not very helpful at all. 

#but at least our functions work in some respect
#we can alter them to apply to other name lists, eg waterbody, towns, cities
#we can also alter the function to include, via lookup, geolocators for all mentioned places


#let's just see how long it takes to apply the function to the whole data

news_full <- news_2

news_full$matched_places_2 <- pblapply(
  news_full$text,
  match_places_strict_ngram,
  place_dt_by_length
)
#08s only
#it's quite useful as a starting base, to work up from



#one thing we can say, is that for each list, those that match >1 word ngrams
#are more likely to be places
#so let's run our function, with only 2 word place names


place_lengths_2 <- sapply(strsplit(place_names$place23nm, " "), length)
place_dt_by_length_2 <- split(place_names$place23nm, place_lengths)
place_dt_by_length_2 <- lapply(place_dt_by_length_2[names(place_dt_by_length_2) %in% c("2")], function(x) {
  dt_2 <- data.table(place = unique(x))
  setkey(dt_2, place)
  return(dt_2)
})


news_full$matched_places_3 <- pblapply(
  news_full$text,
  match_places_strict_ngram,
  place_dt_by_length_2
)


#this is much better, still a lot of negligible cases*, but one way that definitely yields better results
#* like "north west" and "the lake" "the town" - these are not useful. BUt exist in the data, 
#* #so again, it's an issue with the corpus
#this actually makes a lot of articles locatable
#we just have to add in the geocodes, and - firstly
#check how many are empty vectors





