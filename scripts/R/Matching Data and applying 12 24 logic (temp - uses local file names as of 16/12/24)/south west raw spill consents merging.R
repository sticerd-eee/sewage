#now that missing values from DST issues etc has been solved
#these scripts aim to match every single edm to EA consents
#script 5: severn trent water.

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

southwest<-raw_spill%>%
  filter(wasc=="southwest")
#845451

#need to match on site name, like other wascs

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
southwest<-southwest%>%
  mutate(site = trimws(site))%>%
  mutate(site = gsub(" +", " ", site))%>%
  mutate(site = gsub("[[:punct:]]", "", site))



#south west water consents - taking the most recent permit information
consentssouthwest<-consents_all%>%
  filter(grepl("south west water", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(DISCHARGE_SITE_NAME, desc(ISSUED_DATE))%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)
#3232 - lesser than other wascs, but a much higher number of raw spills recorded



southwestunique<-southwest%>%
  distinct(site)
#1771 distinct site names in the spill data



match1<-left_join(southwestunique, consentssouthwest, by = c("site" = "DISCHARGE_SITE_NAME"))
#1771 so no duplicate matches

missing1<-match1%>%
  filter(is.na(DISCHARGE_NGR))
#673 missing 



#2023 EDM returns data:

edm23<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/southwestedmreturn2023.csv")

#1374

#simple string cleaning
edm23<-edm23%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))


#matching exactly with WaSC specific naming
match2<-left_join(missing1, edm23, by = c("site" = "Site.Name..WaSC.operational...optional."))
#682 so a few duplicate matches

match2<-match2%>%
  distinct(site, .keep_all = TRUE)
#673

missing2<-match2%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#only 76 missing


#match exactly with edm return - EA site name now
match3<-left_join(missing2, edm23, by = c("site" = "Site.Name..EA.Consents.Database."))
#76


missing3<-match3%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  subset(select=site)
#68 left missing


#match with EDM data from 2022 next 

edm22<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/southwestedmreturn2022.csv")
#1342


#simple string cleaning
edm22<-edm22%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))

#matching exactly with WaSC specific naming
match4<-left_join(missing3, edm22, by = c("site" = "Site.Name..WaSC.operational...optional."))
#68 so no duplicate matches


missing4<-match4%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#54 left missing

#now with EA name not WaSC specific name

match5<-left_join(missing4, edm22, by = c("site" = "Site.Name..EA.Consents.Database."))
#55 so 1 duplicate match
match5<-match5%>%
  distinct(site, .keep_all = TRUE)


missing5<-match5%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  select(site)
#44 left now



edm21<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/southwestedmreturn2021.csv")
#1391

edm21<-edm21%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..EA.Consents.Database.=trimws(Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub(" +", " ", Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub("[[:punct:]]", "", Site.Name..EA.Consents.Database.))


#matching exactly with WaSC specific naming
match6<-left_join(missing5, edm21, by = c("site" = "Site.Name..WaSC.operational...optional."))
#44 so no duplicate matches

missing6<-match6%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  select(site)
#44 so no matches here


#now with EA name
match7<-left_join(missing6, edm21, by = c("site" = "Site.Name..EA.Consents.Database."))
#44 no duplicate matches


missing7<-match7%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  select(site)
#39 left now


#try fuzzy matching, taking only the best match:


match8<-stringdist_left_join(missing7, consentssouthwest, by = c("site" = "DISCHARGE_SITE_NAME"), method = "lv", max_dist = 4, distance_col="distance")%>%
  group_by(site) %>%           
  slice_min(order_by = distance, n = 1) %>%     
  ungroup()
#40 so one joint match


#export to excel for approval & manual matching


library(writexl)


write_xlsx(match8, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/southwest unmatched1.xlsx")


match8<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/southwestmanual1.csv")

#all but 3 are approved

missing8<-match8%>%
  filter(approved == "no")%>%
  select(site)
#3 sites

#2 of these site names are missing. They are NA values - and so must be removed (no other way to match them)
#the third is PORTHILLY COVE

porth<-consentssouthwest%>%
  filter(grepl("porthilly", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#not matched due to PS rather than PUMPING STATION

match9 <- as.data.frame(t(missing8[3, ]))%>%
  mutate(match = "PORTHILLY COVE PUMPING STATION")%>%
  rename(site = V1)
match9<-left_join(match9, consentssouthwest, by = c("match" = "DISCHARGE_SITE_NAME"))%>%
  select(-match)



#all matched now - create index, then match spills, remove NA site names, apply 12 24 logic, clean final data


df1<-match1%>%
  filter(!is.na(DISCHARGE_NGR))%>%
  select(-ISSUED_DATE, -REVOCATION_DATE)
#1098

df2<-match2%>%
  filter(!is.na(Site.Name..EA.Consents.Database.))%>%
  select(-COMPANY_NAME, -RECEIVING_WATER, -DISCHARGE_NGR, -REC_ENV_CODE_DESCRIPTION, -PERMIT_NUMBER, -ADD_OF_DISCHARGE_SITE_PCODE, -ISSUED_DATE, - REVOCATION_DATE)%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)

#597

df3<-match3%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)

#8

df4<-match4%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)

#14

df5<-match5%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)

#10

df6<-match7%>%
  filter(!is.na(Outlet.Discharge.NGR..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER)

#5

df7<-match8%>%
  filter(approved == "yes")%>%
  select(-ISSUED_DATE, -REVOCATION_DATE)
#37

df8<-match9%>%
  select(-ISSUED_DATE, -REVOCATION_DATE)
#1

sw_list <- list(df1, df2, df3, df4, df5, df6, df7, df8)

swindex <- do.call(bind_rows, sw_list)%>%
  select(-approved)
#1770 so just missing the NA site - which we must remove from spill data anyway

southwest<-southwest%>%
  filter(site!="NA")
#845306 vs. 845451

southwestmatched<-left_join(southwest, swindex, by = "site")

check<-southwestmatched%>%
  filter(is.na(DISCHARGE_NGR))
#no missing


save(southwestmatched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/southwestmatched.RData")


rm(list=ls())
gc()



library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(fuzzyjoin)


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/southwestmatched.RData")
#845306

southwestmatched<-southwestmatched%>%
  distinct()
#823300

#some permit numbers may be missing (unpermitted discharge sites):
TBC<-southwestmatched%>%
  filter(grepl("anomaly", PERMIT_NUMBER, ignore.case = TRUE) | grepl("tbc", PERMIT_NUMBER, ignore.case = TRUE) | is.na(PERMIT_NUMBER))
#37
#one single site is yet to be permitted

#therefore, again, make an id column with permit numbers for the rest, and site name for this one


southwestmatched1<-southwestmatched%>%
  mutate(PERMIT_NUMBER = ifelse(grepl("anomaly|tbc", PERMIT_NUMBER, ignore.case = TRUE), NA, PERMIT_NUMBER))%>%
  mutate(id = ifelse(is.na(PERMIT_NUMBER), site, PERMIT_NUMBER))

check<-southwestmatched1%>%
  filter(is.na(PERMIT_NUMBER))


#apply 12 24 logic to define the spills




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

swblocks1<-create_blocks(southwestmatched1)

swblocks2<-split_blocksv1(swblocks1)


southwestmatched2<-southwestmatched1%>%
  select(-startdatetime, -enddatetime)%>%
  distinct(id, .keep_all = TRUE)

southwestclean<-left_join(swblocks2, southwestmatched2, by = "id")

finalcheck<-southwestclean%>%
  filter(is.na(DISCHARGE_NGR))
#0 missing



southwestclean<-southwestclean%>%
  select(- EAnumber, - id)%>%
  mutate(duration = replace_na(duration, 24))%>%
  select(-EAsitename, -misc_code, -unique_id, wasc)%>%
  rename(permit_reference = PERMIT_NUMBER)%>%
  rename(postcode = ADD_OF_DISCHARGE_SITE_PCODE)%>%
  select(-distance, -DISCHARGE_SITE_NAME)

#131208


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/welshclean.RData")

save(southwestclean, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/southwestclean.RData")

