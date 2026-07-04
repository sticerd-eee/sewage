#this script provides a function to convert Nexis PDF output to tabular data

#required packages
library(dplyr)
library(stringr)
library(pdftools)
library(tidyr)
library(lubridate)





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

