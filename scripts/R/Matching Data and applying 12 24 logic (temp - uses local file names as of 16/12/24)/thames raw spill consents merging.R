#now that missing values from DST issues etc has been solved
#these scripts aim to match every single edm to EA consents
#script 7: Thames water.

rm(list=ls())
gc()



library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(fuzzyjoin)


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/consents_all.RData")
#this is consents_all from the EA consents database

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/raw_spill.RData")
#this is all spill data merged


thames<-raw_spill%>%
  filter(wasc=="thames")
#63802



#string cleaning
consents_all<-consents_all%>%
  mutate(COMPANY_NAME=trimws(COMPANY_NAME))%>%
  mutate(COMPANY_NAME=gsub("  "," ",COMPANY_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=trimws(DISCHARGE_SITE_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=gsub(" +", " ", DISCHARGE_SITE_NAME))%>%
  #and remove all punctuation from site names
  mutate(DISCHARGE_SITE_NAME = gsub("[[:punct:]]", "", DISCHARGE_SITE_NAME))%>%
  mutate(PERMIT_NUMBER=trimws(PERMIT_NUMBER))%>%
  mutate(ISSUED_DATE=as.POSIXct(ISSUED_DATE, format = "%Y-%m-%d"))%>%
  #also remove double spaces in company name, since south west has a space
  mutate(COMPANY_NAME = gsub(" +", " ", COMPANY_NAME))


#also remove any extra spaces and punctuation from spill data
thames<-thames%>%
  mutate(site = trimws(site))%>%
  mutate(site = gsub(" +", " ", site))%>%
  mutate(site = gsub("[[:punct:]]", "", site))



consentsthames<-consents_all%>%
  filter(grepl("thames water", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(DISCHARGE_SITE_NAME, desc(ISSUED_DATE))%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)
#3396


thamesunique<-thames%>%
  distinct(site)
#932 unique sites



match1<-left_join(thamesunique, consentsthames, by = c("site" = "DISCHARGE_SITE_NAME"))
#932 so no duplicate matches


missing1<-match1%>%
  filter(is.na(DISCHARGE_NGR))%>%
  distinct(site)
#553 unmatched

edm23<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thamesedmreturn2023.csv")
#698

#simple string cleaning
edm23<-edm23%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..EA.Consents.Database.=trimws(Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub(" +", " ", Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub("[[:punct:]]", "", Site.Name..EA.Consents.Database.))

#matching exactly with WaSC specific naming
match2<-left_join(missing1, edm23, by = c("site" = "Site.Name..WaSC.operational...optional."))
#no duplicate matches

missing2<-match2%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#548 so not maany matches


match3<-left_join(missing2, edm23, by = c("site" = "Site.Name..EA.Consents.Database."))
#548 no duplicates

missing3<-match3%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  subset(select=site)
#525 still missing



#match with EDM data from 2022 next 

edm22<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thamesedmreturn2022.csv")
#777

#simple string cleaning
edm22<-edm22%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..EA.Consents.Database.=trimws(Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub(" +", " ", Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub("[[:punct:]]", "", Site.Name..EA.Consents.Database.))



#matching exactly with WaSC specific naming
match4<-left_join(missing3, edm22, by = c("site" = "Site.Name..WaSC.operational...optional."))
#525 no duplicates


missing4<-match4%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#524 so only ONE match

#with EA name now
match5<-left_join(missing4, edm22, by = c("site" = "Site.Name..EA.Consents.Database."))
#524 no duplicates


missing5<-match5%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  select(site)
#428 so almost 100 matches here



#2021 edm data

edm21<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thamesedmreturn2021.csv")
#465

edm21<-edm21%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..EA.Consents.Database.=trimws(Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub(" +", " ", Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub("[[:punct:]]", "", Site.Name..EA.Consents.Database.))

#via WaSC naming
match6<-left_join(missing5, edm21, by = c("site" = "Site.Name..WaSC.operational...optional."))
#no duplicate matches

missing6<-match6%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  select(site)
#428 no matches at all

#now with EA name
match7<-left_join(missing6, edm21, by = c("site" = "Site.Name..EA.Consents.Database."))
#no duplicate matches


missing7<-match7%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  select(site)
#no matches



#dealing with 428 missing matches is too much for manual matching of the data

#will try to look for patterns of discrepancies that can help matching
#almost all have some abbreviation STW or CSO. Which is not consistent with consents name
#in addition - the abbreviations are not always consistent with each other
#eg DAGNALL STW vs DAGNAL WWTW

#attempt to remove all of these



roots<-missing7%>%
  mutate(root = gsub("STW", "", site))%>%
  mutate(root = gsub("WWTW", "", root))%>%
  mutate(root = gsub("CSO", "", root))%>%
  mutate(root = gsub("SPS", "", root))%>%
  mutate(root = gsub("TSPS", "", root))%>%
  mutate(root = gsub("PS", "", root))%>%
  mutate(root = gsub("SSO", "", root))%>%
  mutate(root = gsub("SWS", "", root))%>%
  mutate(root = gsub(" +", " ", root))%>%
  mutate(root = trimws(root))

#this looks a lot cleaner

consentsroot<-consentsthames%>%
  mutate(root = gsub("STW", "", DISCHARGE_SITE_NAME))%>%
  mutate(root = gsub("WWTW", "", root))%>%
  mutate(root = gsub("CSO", "", root))%>%
  mutate(root = gsub("SPS", "", root))%>%
  mutate(root = gsub("TSPS", "", root))%>%
  mutate(root = gsub("PS", "", root))%>%
  mutate(root = gsub("SSO", "", root))%>%
  mutate(root = gsub("SWS", "", root))%>%
  mutate(root = gsub("storm treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("sewage treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("wastewater treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("waste water treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("water treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("combined sewer overflow", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("combined sewer", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("pumping station", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub(" +", " ", root))%>%
  mutate(root = trimws(root))


match8<-left_join(roots, consentsroot, by = "root")
#444 some duplicate matches
match8<-match8%>%
  distinct(site, .keep_all = TRUE)
#428


missing8<-match8%>%
  filter(is.na(DISCHARGE_NGR))%>%
  select(site, root)
#294 so around 100 matches made



#294 is still far too many to manually match:

#fuzzy matching with roots

#but before this - some names have some more cleaning we can do to the root
#some include a location (i assume that's what it is)
#two capital letters followed by 8 numbers, or has an extra letter on the end, or has 7 letters and a letter
#remove this as manually I can't search these locations via discharge_ngr they dont seem to match,
#DISCHARGE NGR is 2 letters followed by 10 numbers
#also not present in consents data


root2<-missing8%>%
  mutate(root = gsub("[A-Za-z]{2}\\d{7,8}[A-Za-z]?", "", root))%>%
  mutate(root = gsub(" +", " ", root))%>%
  mutate(root = trimws(root))

mutate(text_column = gsub("[A-Za-z]{2}\\d{8}", "", text_column))


#fuzzy matching
#stricter with distance as roots taken meant dist>=3 was poor matches
match9<-stringdist_left_join(root2, consentsroot, by = "root", method = "lv", max_dist = 2, distance_col="distance")%>%
  group_by(site) %>%           
  slice_min(order_by = distance, n = 1) %>%     
  ungroup()



#some of these are good but a LARGE PROPORTION MISSING...


#let's try matching exactly with 2023 EDM data first

edmroot<-edm23%>%
  mutate(root = gsub("STW", "", Site.Name..EA.Consents.Database.))%>%
  mutate(root = gsub("WWTW", "", root))%>%
  mutate(root = gsub("CSO", "", root))%>%
  mutate(root = gsub("SPS", "", root))%>%
  mutate(root = gsub("TSPS", "", root))%>%
  mutate(root = gsub("PS", "", root))%>%
  mutate(root = gsub("SSO", "", root))%>%
  mutate(root = gsub("SWS", "", root))%>%
  mutate(root = gsub("storm treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("sewage treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("wastewater treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("waste water treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("water treatment works", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("combined sewer overflow", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("combined sewer", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub("pumping station", "", root, ignore.case = TRUE))%>%
  mutate(root = gsub(" +", " ", root))%>%
  mutate(root = trimws(root))%>%
  mutate(root2 = gsub("STW", "", Site.Name..WaSC.operational...optional.))%>%
  mutate(root2 = gsub("WWTW", "", root2))%>%
  mutate(root2 = gsub("CSO", "", root2))%>%
  mutate(root2 = gsub("SPS", "", root2))%>%
  mutate(root2 = gsub("TSPS", "", root2))%>%
  mutate(root2 = gsub("PS", "", root2))%>%
  mutate(root2 = gsub("SSO", "", root2))%>%
  mutate(root2 = gsub("SWS", "", root2))%>%
  mutate(root2 = gsub("storm treatment works", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("sewage treatment works", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("wastewater treatment works", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("waste water treatment works", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("water treatment works", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("combined sewer overflow", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("combined sewer", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub("pumping station", "", root2, ignore.case = TRUE))%>%
  mutate(root2 = gsub(" +", " ", root2))%>%
  mutate(root2 = trimws(root2))

edmroot1<-edmroot%>%
  distinct(root, .keep_all = TRUE)

edmroot2<-edmroot%>%
  distinct(root2, .keep_all = TRUE)

match9x<-left_join(root2, edmroot1, by = "root")

missing9x<-match9x%>%
  filter(is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  select(site, root)
#258


match9x2<-left_join(missing9x, edmroot2, by = c("root" = "root2"))

missing9x2<-match9x2%>%
  filter(is.na(Outlet.Discharge.NGR..EA.Consents.Database.))
#257 this second one was not worth it


#let's just export missing 9x and try to manually match all these:

library(writexl)


write_xlsx(missing9x, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thames unmatched1.xlsx")



brighton<-consents_all%>%
  filter(grepl("brighton", DISCHARGE_SITE_NAME, ignore.case=TRUE))

#there are SIGNIFICANT TYPOS IN THAMES WATER DATA
#IN ADDITION THE DATA QUALITY IS POORER
#SOME DISCHARGE SITES HAVE NO LOCATION DATA AVAILABLE
#(not found in consents, or EDM return data - only referenced in 2019 or 2020 - with no location or permit information)

weald<-thames%>%
  filter(grepl("weald", site, ignore.case = TRUE))
#yes some are EFWEALD BRIDGE...
#just poor typos


#all matched now
#over 100 via Excel's fuzzy match add-on (& manually approved)
#over 100 just manually matched - some using EA data service platform:
#https://environment.data.gov.uk/public-register/water-discharges/registration/TH-EPRAB3890AS-001?__pageState=result-water-discharge-consents
#(used when naming made matches with data unclear)
#also - a significant number did not have permit numbers (but did have location data)
#4 observations had no matches whatsoever (explained above in typos comment)

#inital 12 manually matched
match10<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thamesmanual1.csv")
match10<-match10[(1:12),]

#excel fuzzy matching
match11<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thamesfuzzytrial2.csv")%>%
  filter(approved=="yes")

#manual matched from not approved fuzzy matches
match12<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/thamesmanual2.csv")

#in total match10, 11 and 12 combine to give the 258 discharge sites that were missing from matching in R

df1<-match1%>%
  filter(!is.na(DISCHARGE_NGR))%>%
  select(-ISSUED_DATE, -REVOCATION_DATE)
#379

df2<-match2%>%
  filter(!is.na(Site.Name..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#5

df3<-match3%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#23

df4<-match4%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#1

df5<-match5%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#96

df6<-match8%>%
  filter(!is.na(DISCHARGE_NGR))%>%
  select(-ISSUED_DATE, -REVOCATION_DATE, -root)
#134


df7<-match9x%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#36

df8<-match10%>%
  mutate(COMPANY_NAME="Thames Water")
#12

df9<-match11%>%
  mutate(COMPANY_NAME="Thames Water")%>%
  rename(PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#132

df10<-match12%>%
  mutate(COMPANY_NAME="Thames Water")
#114


thames_list <- list(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10)

thamesindex <- do.call(bind_rows, thames_list)%>%
  distinct(site, .keep_all = TRUE)


#932 as needed


thamesmatched<-left_join(thames, thamesindex, by = "site")%>%
  mutate(DISCHARGE_NGR = ifelse(DISCHARGE_NGR=="N/A", NA, DISCHARGE_NGR))%>%
  mutate(RECEIVING_WATER = ifelse(RECEIVING_WATER=="N/A", NA, RECEIVING_WATER))

checking<-thamesmatched%>%
  filter(is.na(DISCHARGE_NGR))
#209 spills from the 3 sites that lack geospatial reference

save(thamesmatched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/thamesmatched.RData")





##now apply 12 24 spill count method


rm(list=ls())
gc()



library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(fuzzyjoin)

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/thamesmatched.RData")
#63802 raw spill entries


thamesmatched<-thamesmatched%>%
  distinct()
#63800

TBC<-thamesmatched%>%
  filter(grepl("anomaly", PERMIT_NUMBER, ignore.case = TRUE) | grepl("tbc", PERMIT_NUMBER, ignore.case = TRUE) | grepl("temp", PERMIT_NUMBER, ignore.case = TRUE) | is.na(PERMIT_NUMBER))



thamesmatched<-thamesmatched%>%
  mutate(id=ifelse(grepl("anomaly", PERMIT_NUMBER, ignore.case = TRUE) | grepl("tbc", PERMIT_NUMBER, ignore.case = TRUE) | grepl("temp", PERMIT_NUMBER, ignore.case = TRUE) | is.na(PERMIT_NUMBER), site, PERMIT_NUMBER))

#so since temporary permit references aren't guaranteed to stay the same across years - we take the names in this case




create_blocks<-function(spills){
  #sort spills by their starttime
  spills <- spills %>% arrange(id, startdatetime)
  
  #create an empty dataframe to store 12 24 counted spills
  blocks<-data.frame(
    id=character(),
    block_start = as.POSIXct(character()),
    block_end = as.POSIXct(character()),
    stringsAsFactors = FALSE
  )
  
  #initialise a counter to track progress
  counter<-0
  
  unique_ids <- unique(spills$id)
  for (current_id in unique_ids) {
    spills_id <- spills %>% filter(id == current_id)
    
    #Initialise block variables - needed for 12/24 logic
    current_block_start <- NULL
    current_block_end <- NULL
    
    for(i in 1:nrow(spills_id)) {
      
      #counter for progress tracking
      counter <- counter + 1
      
      # Print count every 20 rows
      if (counter %% 20 == 0) {
        cat(" Processed:", counter, "\n")
      }
      
      #sets some variables representing spill start and stop times to the relevant data
      spill_start <- spills_id$startdatetime[i]
      spill_end <- spills_id$enddatetime[i]
      
      #now applying current logic
      if (is.null(current_block_start)) {
        # this is the case this is the first spill
        current_block_start <- spill_start
        current_block_end <- spill_start + hours(12)  #originally given as min(spill_start+hours(12), spill_end) - but this doesn't make sense right?
        #so this makes the block end 12 hours after the start
        #ADDED LOGIC
        # ~ deals with edge case that the first spill end lies in DST transition time
        if(is.na(current_block_end)) {
          current_block_end<-spill_start + hours(13)
        }
        #if the spill extends beyond this 12 hours...
        #a while loop to keep adding 24 hours until the block encompasses our spill
        while (current_block_end < spill_end) {
          #CHANGED logic
          new_end <- current_block_end + hours(24)
          #again for edge cases that any of these interim block ends lie in DST transition time
          if (is.na(new_end)) {
            current_block_end <- current_block_end + hours(25)
          } else {
            current_block_end <- new_end
          }
          #so in theory, this should attempt to add 24 hours, but if that would lead to NA
          #then it adds 25 hours, 
          #then resumes adding 24 hours after that if needed
          #hopefully this works, dealing with DST elegantly
        }
        blocks <- rbind(blocks, data.frame(id=current_id, block_start = current_block_start, block_end = current_block_end))
      } else {
        #now we go to the cases that are not the first spill
        #for these we have to consider the three possible cases:
        # ~ that they will be fully encompasses by the previous blocks
        # ~ they cause an extention to the previous blocks
        # ~ they are beyond 24 hours after the previous spill block, and so start a new one entirely
        safe_new_end<-current_block_end + hours(24)
        if (is.na(safe_new_end)){
          safe_new_end <- current_block_end + hours(25)
        }
        
        if (spill_end <= current_block_end) {
          # Spill is completely within the previous blocks
          next #skip this spill as it's already  accounted for
        } else if (spill_start <= safe_new_end) {
          # Spill extends the current block ~ since this logic means
          #these observations have an end beyond the previous block end,
          #but also start before 24 hours after the previous block end
          while (current_block_end < spill_end) {
            #NEW ADDITION again to tackle DST missing values (edge cases)
            new_end <- current_block_end + hours(24)
            if (is.na(new_end)) {
              current_block_end <- current_block_end + hours(25)
            } else {
              current_block_end <- new_end
            }
          }
          blocks$block_end[nrow(blocks)] <- current_block_end 
          #^ this may not be what I want, this seems just to amend our current block
          #although - logically it doesn't matter for my next steps tbf
          #*yes this is valid since we split blocks with a separate function
        } else {
          # Start a new block ~ this is the case we are beyond 24 hours since last block
          current_block_start <- spill_start
          current_block_end <- spill_start + hours(12)#NOTE *** fixed 05/08/24
          #NEW LOGIC FOR EDGE CASES CASUED BY DST CHANGE
          if (is.na(current_block_end)) {
            current_block_end <- spill_start + hours(13)  # Adjust to skip DST gap
          }
          while (current_block_end < spill_end) {
            #ALL NEW LOGIC AGAIN FOR DST EDGE CASES
            new_end <- current_block_end + hours(24)
            if (is.na(new_end)) {
              current_block_end <- current_block_end + hours(25)  
            } else {
              current_block_end <- new_end
            }
          }
          blocks <- rbind(blocks, data.frame(id=current_id, block_start = current_block_start, block_end = current_block_end))
        }
      }
    }
  }
  return(blocks)
  
}




split_blocksv1<-function(blocks){
  
  #creates an empty dataframe to store initial for loop
  firstsplit<-data.frame(
    id=character(),
    block_start = as.POSIXct(character()),
    block_end = as.POSIXct(character()),
    duration = numeric(),
    stringsAsFactors = FALSE
  )
  
  #initialise empty variables for the loops
  current_block_start <- NULL
  current_block_end <- NULL
  current_id<-NULL
  
  counter<-0
  
  #loop through our blocks to separate first 12 hour spill blocks
  for(i in 1:nrow(blocks)) {
    
    #counter for progress tracking
    counter <- counter + 1
    
    # Print count every 20 rows
    if (counter %% 20 == 0) {
      cat(" Row number:", counter, "\n")
    }
    
    #assigns variables to individual observations
    current_id<-blocks$id[i]
    current_block_start<-blocks$block_start[i]
    current_block_end<-blocks$block_end[i]
    
    #gives first spill end (always 12 hours)
    #NEW LOGIC NEEDED - if not 12 hours (if NA) then add 13 hours
    firstinterval_end<-blocks$block_start[i]+hours(12)
    
    #deal with the edge case that the first interval (spill) ends in a "missing" time due to DST change
    if (is.na(firstinterval_end)) {
      firstinterval_end <- current_block_start + hours(13)  
    }
    
    
    #set first duration to 12 - as it's 12 hours, even if we add 13 hours (that's only to adjust for DST change)
    firstinterval_duration<-12
    
    #give the remainder duration - for use later in splitting this further
    remainder_duration<-difftime(current_block_end, firstinterval_end, units = "hours")
    #DONT NEED NEW LOGIC HERE AS BLOCK END WILL ALWAYS BE A TRUE VALUE - given create_blocks function
    
    #append 12 hour interval and remainder into dataframe
    firstsplit<-rbind(firstsplit, data.frame(id=current_id, block_start=current_block_start, block_end=firstinterval_end, duration = firstinterval_duration))
    firstsplit<-rbind(firstsplit, data.frame(id=current_id, block_start=firstinterval_end, block_end=current_block_end, duration = remainder_duration))
  }
  
  #separate 12 hour spills from remainder
  
  first12<-firstsplit%>%
    filter(duration==12)
  
  #for remainder - updated filter to remove negatives, not just 0 and 12
  remainder<-firstsplit%>%
    filter(duration!=12 & duration > 0)
  
  
  #create an empty dataframe for second loop
  remainder_split<-data.frame(
    id=character(),
    block_start = as.POSIXct(character()),
    block_end = as.POSIXct(character()),
    duration = numeric(),
    stringsAsFactors = FALSE
  )
  
  
  for(i in 1:nrow(remainder)){
    #loop for splitting up the remainder into the 24 hour spills
    current_id<-remainder$id[i]
    current_block_start<-remainder$block_start[i]
    current_block_end<-remainder$block_end[i]
    
    #this gives the times that separate spills within our remainder
    times <- seq(from = current_block_start, to = current_block_end, by = "24 hours")
    #NEW LOGIC NEEDED - here an interim value could = NA due to DST change
    
    
    
    
    
    
    #this adds in the final endtime - however this logic may cause inconsistency in daylight savings
    if (times[length(times)] != current_block_end) {
      times <- c(times, current_block_end)
    }
    
    #empty dataframe for results of the interval splits within the loop, as we're generating and saving several observations per observation
    intervals <- data.frame(
      id = current_id,
      block_start = times[-length(times)],
      block_end = times[-1],
      duration = NA, #to ensure rbind() can operate correctly (same no. of columns)
      stringsAsFactors = FALSE
    )
    
    remainder_split<-rbind(remainder_split, intervals)
  }
  
  #merge 12 hour blocks with 24 hour blocks
  miniblock<-rbind(first12, remainder_split)%>%
    arrange(id, block_start)
  
  return(miniblock)
}


thamesblocks1<-create_blocks(thamesmatched)
#14050

thamesblocks2<-split_blocksv1(thamesblocks1)
#38667



thamesmatched2<-thamesmatched%>%
  select(-startdatetime, -enddatetime)%>%
  distinct(id, .keep_all = TRUE)
#644 (vs 900+ distinct names)



thamesclean<-left_join(thamesblocks2, thamesmatched2, by = "id")

checking<-thamesclean%>%
  filter(is.na(site))
#0 so all matched



thamesclean<-thamesclean%>%
  select(- EAnumber, - id)%>%
  mutate(duration = replace_na(duration, 24))%>%
  select(-EAsitename, -misc_code, -unique_id, wasc)%>%
  rename(permit_reference = PERMIT_NUMBER)%>%
  rename(postcode = ADD_OF_DISCHARGE_SITE_PCODE)%>%
  select(-DISCHARGE_SITE_NAME, -root)



#load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/welshclean.RData")

save(thamesclean, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/thamesclean.RData")
