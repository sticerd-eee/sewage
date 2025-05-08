#cleaner script:


rm(list=ls())
gc()

#this script aims to fully match our consents database to our spills data
#it is v2 for this complete attempt

library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(fuzzyjoin)

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/consents_all.RData")
#this is consents_all from the EA consents database

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/raw_spill.RData")
#this is all spill data merged

#check<-raw_spill%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#yep no missing values anymore


#getting only anglian spills data
anglian<-raw_spill%>%
  filter(wasc=="anglian")
#315,114


#2021 and 2022 both have permit reference numbers
#2023 doesn't
#so we will match unique permit references to consents data
#then use this to match 2023 based on name

#some string cleaning in consents data
consents_all<-consents_all%>%
  mutate(COMPANY_NAME=trimws(COMPANY_NAME))%>%
  #edit to remove all extra spaces not just double spaces
  mutate(COMPANY_NAME=gsub(" +"," ",COMPANY_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=trimws(DISCHARGE_SITE_NAME))%>%
  #same here
  mutate(DISCHARGE_SITE_NAME=gsub(" +", " ", DISCHARGE_SITE_NAME))%>%
  #and remove all punctuation from site names
  mutate(DISCHARGE_SITE_NAME = gsub("[[:punct:]]", "", DISCHARGE_SITE_NAME))%>%
  mutate(PERMIT_NUMBER=trimws(PERMIT_NUMBER))%>%
  mutate(ISSUED_DATE=as.POSIXct(ISSUED_DATE, format = "%Y-%m-%d"))


#only anglian permits
consentsanglian<-consents_all%>%
  filter(grepl("anglian", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(PERMIT_NUMBER, desc(ISSUED_DATE))%>%
  #basically removes old entries of an EDM to prevent extra matches
  distinct(PERMIT_NUMBER, .keep_all = TRUE)


#taking 2021 and 2022 data only
anglian2122<-anglian%>%
  filter(startdatetime<as.POSIXct("2023-01-01 00:00"))


anglianunique<-anglian2122%>%
  distinct(EAnumber, .keep_all = TRUE)%>%
  mutate(EAnumber=gsub("/", "", EAnumber))
#961 unique objects

anglianindex<-left_join(anglianunique, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
#check for missing values
check<-anglianindex%>%
  filter(is.na(DISCHARGE_SITE_NAME))
#one missing
#AWENF/2485
#NASH-WHADDON ROAD (NEW) SP
#it has a match! just without the /
#is the dash common for other permit numbers?
#NO - REMOVE THE / from these numbers
#(done) no missing anymore.
rm(check)



#so now let's match our 21 and 22 data with this knowledge

anglian2122<-anglian2122%>%
  mutate(EAnumber=gsub("/", "", EAnumber))
#138700

anglianmerged<-left_join(anglian2122, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
check<-anglianindex%>%
  filter(is.na(DISCHARGE_SITE_NAME))
#all merged well - 138700, 0 missing

rm(check)


#let's use the index to match the 2023 data

anglian2023<-anglian%>%
  filter(startdatetime>=as.POSIXct("2023-01-01 00:00"))

anglianindex<-anglianindex%>%
  select(-EAsitename, -startdatetime, -enddatetime, -wasc, -misc_code, -id, -unique_id)

#top 8 observations were included with 2022 data
#so they have permit numbers
#should take these out and merge separately:

anglian8<-anglian2023[(1:8),]
anglianmerge2i<-left_join(anglian8, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
#merged perfectly fine

#append to merge
anglianmerged<-rbind(anglianmerged, anglianmerge2i)
#138708
                         
                         

#for the 2023 data - we found matching on the EA name rather than wasc name led to more matches:

anglianindex2<-anglianindex%>%
  arrange(DISCHARGE_SITE_NAME, desc(ISSUED_DATE))%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)
#954 vs 961 objects (prevent many to many matching)
                         
                         
anglian2023<-anglian2023%>%
  mutate(site=gsub("[[:punct:]]", "", site))%>%
  mutate(site=gsub(" +", " ", site))%>%
  mutate(site=trimws(site))


anglian23merge2<-left_join(anglian2023, anglianindex2, by = c("site" = "DISCHARGE_SITE_NAME"))
#176414
check2<-anglian23merge2%>%
  filter(is.na(DISCHARGE_NGR))
#51486


checkunique<-check2%>%
  distinct(site, .keep_all = TRUE)
#468 unique objects well now i get 472 which is even more?? - should keep the script exactly the same and not change it
#that's significant

checkunique<-checkunique[,(1:9)]



anglian2023unique<-anglian2023%>%
  distinct(site, .keep_all = TRUE)
#1100 distinct sites  






#previous script showed EDM data can match better for 2023 than index created from consents
edm<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/anglianedmreturn2023.csv")%>%
  subset(select = c(X, X.1, X.2))%>%
  rename(site=X)%>%
  rename(altname=X.1)%>%
  rename(EAnumber=X.2)

#these ones match fine

edm<-edm[-(1),]

edm<-edm%>%
  mutate(site=gsub("[[:punct:]]", "", site))%>%
  mutate(site=gsub(" +", " ", site))%>%
  mutate(site=trimws(site))

anglian2023merge<-left_join(anglian2023unique, edm, by = "site")

check2<-anglian2023merge%>%
  filter(is.na(EAnumber.y))

#not all matched...
#NEED TO GO BACK TO PREVIOUS SCRIPT EXACTLY

