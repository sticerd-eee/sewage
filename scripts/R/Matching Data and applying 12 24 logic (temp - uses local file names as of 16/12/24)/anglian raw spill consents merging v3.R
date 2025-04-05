#now that missing values from DST issues etc has been solved
#these scripts aim to match every single edm to EA consents
#script 1: Anglian water.

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
  mutate(COMPANY_NAME=gsub("  "," ",COMPANY_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=trimws(DISCHARGE_SITE_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=gsub("  ", " ", DISCHARGE_SITE_NAME))%>%
  #and remove all punctuation from site names
  mutate(DISCHARGE_SITE_NAME = gsub("[[:punct:]]", "", DISCHARGE_SITE_NAME))%>%
  mutate(PERMIT_NUMBER=trimws(PERMIT_NUMBER))%>%
  mutate(ISSUED_DATE=as.POSIXct(ISSUED_DATE, format = "%Y-%m-%d"))

#only anglian permits
consentsanglian<-consents_all%>%
  filter(grepl("anglian", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(PERMIT_NUMBER, desc(ISSUED_DATE))%>%
  distinct(PERMIT_NUMBER, .keep_all = TRUE)
#6532

#taking 2021 and 2022 data only
anglian2122<-anglian%>%
  filter(startdatetime<as.POSIXct("2023-01-01 00:00"))

anglianunique<-anglian2122%>%
  distinct(EAnumber, .keep_all = TRUE)
#961 unique objects

anglianindex<-left_join(anglianunique, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
#check for missing values
check<-anglianindex%>%
  filter(is.na(DISCHARGE_SITE_NAME))
#one missing
#AWENF/2485
#NASH-WHADDON ROAD (NEW) SP

#let's look for this in consents
whaddon<-consents_all%>%
  filter(grepl("whaddon", DISCHARGE_SITE_NAME, ignore.case = TRUE))

#it has a match! just without the /
#is the dash common for other permit numbers?
#NO - REMOVE THE / from these numbers


anglianunique<-anglianunique%>%
  mutate(EAnumber=gsub("/", "", EAnumber))


anglianindex<-left_join(anglianunique, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
#check for missing values
check<-anglianindex%>%
  filter(is.na(DISCHARGE_SITE_NAME))
#none missing


#so now let's match our 21 and 22 data with this knowledge

anglian2122<-anglian2122%>%
  mutate(EAnumber=gsub("/", "", EAnumber))


anglianmerged<-left_join(anglian2122, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
check<-anglianindex%>%
  filter(is.na(DISCHARGE_SITE_NAME))
#all merged well - 138700



#let's use the index to match the 2023 data

anglian2023<-anglian%>%
  filter(startdatetime>=as.POSIXct("2023-01-01 00:00"))

anglianindex<-anglianindex%>%
  select(-EAsitename, -startdatetime, -enddatetime, -wasc, -misc_code, -id, -unique_id)


#let's see how many join on a pure site merge

anglian23merge<-left_join(anglian2023, anglianindex, by = "site")
#many to many why??
#200 extra generated


duplicate_sites<-anglianindex%>%
  group_by(site)%>%
  filter(n() > 1)%>%  
  ungroup()
#3 duplicates making 6 observations here
#hmm some have some duplicates almost - with slightly different permit references
#the location column is effectively the same for each
#so safe to take the first distinct site name for matching these 2023 observations


anglianindex<-anglianindex%>%
  distinct(site, .keep_all = TRUE)

anglian23merge<-left_join(anglian2023, anglianindex, by = "site")

#check for misses
check<-anglian23merge%>%
  filter(is.na(DISCHARGE_NGR))
#119,908 out of 138,700
#so the majority
#from manual observation - seems some misses by a few letters so fuzzy matching could work

#need to see if fuzzy matching to "site" or "DISCHARGE_SITE_NAME" (from consents database)is closer

#looking at some examples
finningham<-anglian2023%>%
  filter(grepl("finningham", site, ignore.case=TRUE))
#none

finningham<-anglian2023%>%
  filter(grepl("barton", site, ignore.case=TRUE))

bott<-anglian2023%>%
  filter(grepl("bottisham", site, ignore.case=TRUE))

#seems closer to official EA consents names tbh

#let's check these matches
anglian23merge2<-left_join(anglian2023, anglianindex, by = c("site" = "DISCHARGE_SITE_NAME"))
#many to many again

anglianindex2<-anglianindex%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)


anglian23merge2<-left_join(anglian2023, anglianindex2, by = c("site" = "DISCHARGE_SITE_NAME"))

check2<-anglian23merge2%>%
  filter(is.na(DISCHARGE_NGR))
#66345
#so around 1/3
#this is somewhat a better way to match then, but still requires some fuzzy matching
#7 have EA permit numbers so can be matched via those
#others have to do with punctuation
#so deal with these changes before seeing which really need fuzzy matching
#also we should see how many unique EDMs this actually involves

anglian2023<-anglian2023%>%
  mutate(site=gsub("[[:punct:]]", "", site))%>%
  mutate(site=gsub("  ", " ", site))%>%
  mutate(site=trimws(site))


anglian23merge2<-left_join(anglian2023, anglianindex2, by = c("site" = "DISCHARGE_SITE_NAME"))
check2<-anglian23merge2%>%
  filter(is.na(DISCHARGE_NGR))
#51719
anglian23matched2<-anglian23merge2%>%
  filter(!is.na(DISCHARGE_NGR))
#124695 of 176414 are matched

#so 1/6 of remaining missing were solved with punctuation
#still got the 7 with permit numbers

#taking these 7 out
#actually 8 (one was matched via name fine)

anglian2023i<-anglian2023[1:8,]
#these can be matched alone - or easiest to append onto the 2122 dataframe

anglian2023<-anglian2023[-(1:8),]


check2<-check2[-(1:8),]
#51711

checkunique<-check2%>%
  distinct(site, .keep_all = TRUE)
#468 unique objects
#that's significant

checkunique<-checkunique[,(1:9)]






#went back to match on consents anglian as a whole instead of just the index from previous years?
missingmerge1<-stringdist_left_join(checkunique, consentsanglian, by = c("site" = "DISCHARGE_SITE_NAME"), method = "lv", max_dist = 3, distance_col="distance")
#1183
missingmerge1i<-missingmerge1%>%
  group_by(site) %>%           
  slice_min(order_by = distance, n = 1) %>%     
  ungroup()
#643, therefore some joint "best"
#why are some distance = 0 shouldn't these have been dealt with before? by the normal left join?

#some are good, others not so much

#running into the issue we had before when trying fuzzy matching
#it bases itself on the letters exactly - not words or anything qualitative
#simply the number of permutations to make the strings equivalent
#replacing STW and PS etc with Storm Treatment works and pumping station etc respectively
#might help the matching process


#for example ALDGATEKETTON PUMPING STATION (site) got no matches distance<=3
#if we search for ALDGATEKETTON:

aldgate<-consentsanglian%>%
  filter(grepl("ALDGATE", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#there is one: KETTON ALDGATE TPS
#also with a double space which is an issue



aldgate<-anglianindex%>%
  filter(grepl("ALDGATE", site, ignore.case = TRUE))
#it was in previous years referred to as KETTON-ALDGATE TPS (obviously much closer to the EA name)


#let's focus on the ones that aren't 0 or na

missed1<-missingmerge1i%>%
  filter(distance!=0 | is.na(distance))
#401 left not exactly matched (some duplicates too)

duplicatecheck<-missed1%>%
  distinct(site, .keep_all = TRUE)
#285 actually not matched

#ok the double spaces are causing some of the inexact matches 
#thought these were supposed to have been removed??


#need to correct my code
#mutate(DISCHARGE_SITE_NAME=gsub("  ", " ", DISCHARGE_SITE_NAME))
#with
#mutate(DISCHARGE_SITE_NAME=gsub(" +", " ", DISCHARGE_SITE_NAME))
#which will remove all >1 consecutive spaces
#to help matching become easier

#for now - quick fix
consentsanglian<-consentsanglian%>%
  mutate(DISCHARGE_SITE_NAME=gsub(" +", " ", DISCHARGE_SITE_NAME))


missingmerge2<-stringdist_left_join(checkunique, consentsanglian, by = c("site" = "DISCHARGE_SITE_NAME"), method = "lv", max_dist = 3, distance_col="distance")
#1183 like before
missingmerge2i<-missingmerge2%>%
  group_by(site) %>%           
  slice_min(order_by = distance, n = 1) %>%     
  ungroup()
#643
missed2<-missingmerge2i%>%
  filter(distance!=0 | is.na(distance))
#250

df1<-missingmerge2i%>%
  filter(distance==0)

duplicatecheck2<-missed2%>%
  distinct(site, .keep_all = TRUE)
#154


#we're getting somewhere...

#ok since there aren't many left
#and some of these seem to have found their actual matches
#best to export this 154 dataframe to excel
#manually approve or disapprove in a column
#the approved ones, we keep as another dataframe here (as an index)
#the not approved ones, we manually give a match and then come back here again


library(writexl)

write_xlsx(duplicatecheck2, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/anglian unmatched1.xlsx")

approved<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/anglianmanual1.csv")
#these ones match fine that are denoted "yes"
#still missed are labelled "no"

#the others I will manually find matches in excel


#export anglian consents for manual matching
write_xlsx(consentsanglian, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/consentsanglian.xlsx")

#i'm realizing for these last missing matches I can match to anglian's 2023 EDM return (same wording in one column)
#this will give me the permit number I need to match to the consents data


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


missed<-approved%>%
  filter(approved. == "no")%>%
  subset(select = site)
#126 left (some i've manually matched - can go back to that if needed)

made<-approved%>%
  filter(approved. == "yes")
#27

merge<-left_join(missed, edm, by = "site")
#137 so some duplicate matches (can filter on permit issue dates when indexed)

#any na?

check<-merge%>%
  filter(is.na(EAnumber))
#NONE!

#so all are matched :)

#just need to filter these matches for most up to date one
# ~ do this when matched to the consents (use desc(issued date))
#and sort out the 8 from overlap
#create a full index and marge to the full data
#we also need to clean this code
#then apply 12 24 logic to our spills

#OK so now what exactly do we have?


# 21 and 22 data as matched. "anglianmerged"

#then of 2023:


#some is already matched - so append to anglian data
anglian23matched2<-anglian23matched2%>%
  rename(EAnumber=EAnumber.y)%>%
  select(-EAnumber.x, -site.y)

anglian1<-rbind(anglianmerged, anglian23matched2)

#need to make columns consistent

anglian23matched2<-anglian23matched2%>%
  select(-site.y)
anglianmerged<-anglianmerged%>%
  select(-DISCHARGE_SITE_NAME)


anglian1<-rbind(anglianmerged, anglian23matched2)
#263,395 of 16 variables
nacheck<-anglian1%>%
  filter(is.na(DISCHARGE_NGR))
rm(nacheck)
#0 missing.

#add in the 7 that were left over from 2022:
anglian2023i<-anglian2023i[-(2),]

#these can be matched by EA number - as were 2021 and 2022 data.

df0<-left_join(anglian2023i, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))%>%
  select(-DISCHARGE_SITE_NAME)
check<-df0%>%
  filter(is.na(DISCHARGE_SITE_NAME))
rm(check)

#append this data 

anglian2<-rbind(anglian1, df0)



#Now we have 3 remaining data to use
#each will combine to make an index to match the final spills "check2"



#now the 393 that matched with distance = 0 to consents instead of index
df1<-df1%>%
  arrange(site, desc(ISSUED_DATE))%>%
  distinct(site, .keep_all = TRUE)
#314 now - perfect

df1<-df1%>%
  select(-EAnumber.x, -DISCHARGE_SITE_NAME, -distance)%>%
  rename(EAnumber=PERMIT_NUMBER)


#now the 27 "approved" fuzzy matches
df2<-made%>%
  select(-approved.)

df2<-left_join(df2, consentsanglian, by = "PERMIT_NUMBER")
#27 perfect


#now the final 137 EDMs that were matched using EDM returns 2023
df3<-merge%>%
  select(-altname)%>%
  mutate(EAnumber=gsub("/", "", EAnumber))%>%
  mutate(EAnumber=gsub("AW1NF964", "AW1NF964A", EAnumber))

df3<-left_join(df3, consentsanglian, by = c("EAnumber" = "PERMIT_NUMBER"))
#one missing that is a duplicate anyway - row 98 rushbottom lane PS
#one other missing
#Foxton cambs

#foxton2<-consents_all%>%
#  filter(grepl("AW1NF964", PERMIT_NUMBER, ignore.case = TRUE))
#it matches to consents_all however it is missing an A from the end.
#need to add in that A
df3<-df3%>%
  filter(!is.na(DISCHARGE_NGR))
#136 in the end.

#now we have those 3 we need to combine them.

df1<-df1%>%
  select(-EAsitename, -wasc, -startdatetime, -enddatetime, -misc_code, -id, -unique_id)
df2<-df2%>%
  select(-DISCHARGE_SITE_NAME.x, -DISCHARGE_SITE_NAME.y)%>%
  rename(EAnumber=PERMIT_NUMBER)
df3<-df3%>%
  select(-DISCHARGE_SITE_NAME)
matchingindex<-rbind(df1, df2, df3)


matchingindex2<-matchingindex%>%
  arrange(site, desc(ISSUED_DATE))%>%
  distinct(site, .keep_all = TRUE)
#now we can match the remaining 2023 spills through this.

remaining<-check2%>%
  subset(select = c(site, wasc, startdatetime, enddatetime, misc_code, id, unique_id))

df5<-left_join(remaining, matchingindex, by = "site")
df6<-left_join(remaining, matchingindex2, by = "site")
#not many to many anymore

#check for NA
checking<-df6%>%
  filter(is.na(DISCHARGE_NGR))
#one single EDM missing
#Woodford church SSO
#manually found permit reference AWNNF837/13008A

woodford<-consents_all%>%
  filter(grepl("AWNNF83713008A", PERMIT_NUMBER, ignore.case = TRUE))
#none exist
woodford2<-consents_all%>%
  filter(grepl("woodford", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#new permit number as of 2024 - AWNNF13008

woodford <- data.frame(
  site = "WOODFORD CHURCH SSO",
  EAnumber = "AWNNF13008",
  stringsAsFactors = FALSE)
woodford<-left_join(woodford, consents_all, by = c("EAnumber" = "PERMIT_NUMBER"))
woodford<-woodford[2,]%>%
  select(-DISCHARGE_SITE_NAME)

matchingindex3<-rbind(matchingindex2, woodford)%>%
  arrange(site, desc(ISSUED_DATE))%>%
  distinct(site, .keep_all = TRUE)
#468 so makes sense

df7<-left_join(remaining, matchingindex3, by = "site")
#51711 good
#check for any NAs left
checking2<-df7%>%
  filter(is.na(DISCHARGE_NGR))
#NONE. so all are matched well.

df7<-df7%>%
  mutate(EAsitename = site)

anglian1<-rbind(anglian2, df7)
#315113 all matched. Just one row missing - this is the one from the mistake in checking 2 earlier 
#when i took out 8 instead of 7 rows.



check3<-anglian23merge2%>%
  filter(is.na(DISCHARGE_NGR))
missedrow<-check3[8,(1:9)]
df8<-left_join(missedrow, matchingindex3, by = "site")
df8<-df8%>%
  select(-EAnumber.x)

df8<-df8%>%
  mutate(EAsitename = site)
anglian2<-rbind(anglian1, df8)
#315114

#seems some are duplicated - but ignore for now??
anglian4<-anglian%>%
  distinct()
anglian3<-anglian1%>%
  distinct()

#either way the numbers align so that was the missing row



anglianmatched<-anglian2
checkfinal<-anglianmatched%>%
  filter(is.na(DISCHARGE_NGR))

#none missing.
#so let's export this data

save(anglianmatched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/anglianmatched.RData")

#anglian done... move on to 12 24 spill logic for this.

