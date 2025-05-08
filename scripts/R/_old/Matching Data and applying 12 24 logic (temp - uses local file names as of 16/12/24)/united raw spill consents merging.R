#now that missing values from DST issues etc has been solved
#these scripts aim to match every single edm to EA consents
#script 8: UNited Utilities

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

united<-raw_spill%>%
  filter(wasc=="united")
#2532926

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
united<-united%>%
  mutate(site = trimws(site))%>%
  mutate(site = gsub(" +", " ", site))%>%
  mutate(site = gsub("[[:punct:]]", "", site))


consentsunited<-consents_all%>%
  filter(grepl("united utilities", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(DISCHARGE_SITE_NAME, desc(ISSUED_DATE))%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)
#4843



unitedunique<-united%>%
  distinct(site)
#1997


match1<-left_join(unitedunique, consentsunited, by = c("site" = "DISCHARGE_SITE_NAME"))
#1997


missing1<-match1%>%
  filter(is.na(DISCHARGE_NGR))%>%
  distinct(site)
#183 missing only


edm23<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/unitededmreturn2023.csv")
#2264


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
#188 some duplicate matches
match2<-match2%>%
  distinct(site, .keep_all = TRUE)

missing2<-match2%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#151 left


match3<-left_join(missing2, edm23, by = c("site" = "Site.Name..EA.Consents.Database."))
#224 so duplicates

match3<-match3%>%
  distinct(site, .keep_all = TRUE)


missing3<-match3%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  subset(select=site)
#10 left

#10 is sufficiently small to manually match

library(writexl)


write_xlsx(missing3, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/united unmatched1.xlsx")


#actually they seem to match with 2021 data:


edm21<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/unitededmreturn2021.csv")
#2192

edm21<-edm21%>%
  mutate(Site.Name..WaSC.operational.name...optional.=trimws(Site.Name..WaSC.operational.name...optional.))%>%
  mutate(Site.Name..WaSC.operational.name...optional.=gsub(" +", " ", Site.Name..WaSC.operational.name...optional.))%>%
  mutate(Site.Name..WaSC.operational.name...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational.name...optional.))%>%
  mutate(Site.Name..EA.Consents.Database.=trimws(Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub(" +", " ", Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub("[[:punct:]]", "", Site.Name..EA.Consents.Database.))


match4<-left_join(missing3, edm21, by = c("site" = "Site.Name..EA.Consents.Database."))

missing4<-match4%>%
  filter(is.na(Site.Name..WaSC.operational.name...optional.))%>%
  subset(select=site)
#5 missing

darwen<-consentsunited%>%
  filter(grepl("darwen", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#change to DARWEN WWTW

hanover<-consentsunited%>%
  filter(grepl("hanover", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#hanover street cso

bradley<-consentsunited%>%
  filter(grepl("bradley", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#BRADLEY LANE CSO MH830

ravens<-consentsunited%>%
  filter(grepl("ravens", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#RAVENS BRIDGE PUMPING STATION

rishton<-consentsunited%>%
  filter(grepl("rishton", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#RISHTON TANK CSO

rm(darwen, hanover, bradley, ravens, rishton)

missing4<-missing4%>%
  mutate(match=c("DARWEN WWTW", "HANOVER STREET CSO", "BRADLEY LANE CSO MH830", "RAVENS BRIDGE PUMPING STATION", "RISHTON TANK CSO"))

match5<-left_join(missing4, consentsunited, by = c("match" = "DISCHARGE_SITE_NAME"))%>%
  select(-match)

#all matched

df1<-match1%>%
  filter(!is.na(DISCHARGE_NGR))
#1814

df2<-match2%>%
  filter(!is.na(Site.Name..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#32

df3<-match3%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#141

df4<-match4%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)
#5

df5<-match5
#5


united_list <- list(df1, df2, df3, df4, df5)

unitedindex <- do.call(bind_rows, united_list)%>%
  distinct(site, .keep_all = TRUE)
#1997

check1<-unitedindex%>%
  filter(is.na(DISCHARGE_NGR))
#no missing from index


unitedmatched<-left_join(united, unitedindex, by = "site")

check2<-unitedmatched%>%
  filter(is.na(DISCHARGE_NGR))
#all  matched

#export/save:

save(unitedmatched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/unitedmatched.RData")





##now apply 12 24 spill count method


rm(list=ls())
gc()



library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(fuzzyjoin)

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/unitedmatched.RData")
#2,532,926

unitedmatched<-unitedmatched%>%
  distinct()
#1,813,530

TBC<-unitedmatched%>%
  filter(grepl("anomaly", PERMIT_NUMBER, ignore.case = TRUE) | grepl("tbc", PERMIT_NUMBER, ignore.case = TRUE) | grepl("temp", PERMIT_NUMBER, ignore.case = TRUE) | is.na(PERMIT_NUMBER))

unique(TBC$PERMIT_NUMBER)
#20072
#all are "TBC"

unitedmatched<-unitedmatched%>%
  mutate(id=ifelse(grepl("tbc", PERMIT_NUMBER, ignore.case = TRUE), site, PERMIT_NUMBER))

#oh the site name is also TBC - but all have the same location
#so we can conclude it is the same site 

unique(TBC$DISCHARGE_NGR)
#yes all same site


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


ublocks1<-create_blocks(unitedmatched)
#120425

ublocks2<-split_blocksv1(ublocks1)


unitedmatched2<-unitedmatched%>%
  select(-startdatetime, -enddatetime)%>%
  distinct(id, .keep_all = TRUE)
#1982 unique sites via id (permit reference)

unitedclean<-left_join(ublocks2, unitedmatched2, by = "id")

checking<-unitedclean%>%
  filter(is.na(site))
#0 so all matched



unitedclean<-unitedclean%>%
  select(- EAnumber, - id)%>%
  mutate(duration = replace_na(duration, 24))%>%
  select(-EAsitename, -misc_code, -unique_id, wasc)%>%
  rename(permit_reference = PERMIT_NUMBER)%>%
  rename(postcode = ADD_OF_DISCHARGE_SITE_PCODE)%>%
  select(-ISSUED_DATE, -REVOCATION_DATE)




save(unitedclean, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/unitedclean.RData")

#272,485 spills

