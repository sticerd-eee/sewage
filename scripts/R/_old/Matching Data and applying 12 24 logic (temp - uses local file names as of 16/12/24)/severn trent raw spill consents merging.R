#now that missing values from DST issues etc has been solved
#these scripts aim to match every single edm to EA consents
#script 4: severn trent water.
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


severn<-raw_spill%>%
  filter(wasc=="severntrent")
#537016

#2021 and 2022 data for severn trent has some miscellaneous code included
#does it match with permit numbers in EA consents?


consents_all<-consents_all%>%
  mutate(COMPANY_NAME=trimws(COMPANY_NAME))%>%
  mutate(COMPANY_NAME=gsub("  "," ",COMPANY_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=trimws(DISCHARGE_SITE_NAME))%>%
  mutate(DISCHARGE_SITE_NAME=gsub(" +", " ", DISCHARGE_SITE_NAME))%>%
  #and remove all punctuation from site names
  mutate(DISCHARGE_SITE_NAME = gsub("[[:punct:]]", "", DISCHARGE_SITE_NAME))%>%
  mutate(PERMIT_NUMBER=trimws(PERMIT_NUMBER))%>%
  mutate(ISSUED_DATE=as.POSIXct(ISSUED_DATE, format = "%Y-%m-%d"))

#also remove any extra spaces and punctuation from spill data
severn<-severn%>%
  mutate(site = trimws(site))%>%
  mutate(site = gsub(" +", " ", site))%>%
  mutate(site = gsub("[[:punct:]]", "", site))

#severn trent consents - taking the most recent permit information
consentssevern<-consents_all%>%
  filter(grepl("severn", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(DISCHARGE_SITE_NAME, desc(ISSUED_DATE))%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)
#6856

#check<-left_join(severn, consentssevern, by = c("misc_code" = "PERMIT_NUMBER"))%>%
#  filter(!is.na(DISCHARGE_NGR))


#the misc code does not relate to EA consents. 


severnunique<-severn%>%
  distinct(site)
#2377 sites to match


#matching exactly to EA consents

match1<-left_join(severnunique, consentssevern, by = c("site" = "DISCHARGE_SITE_NAME"))
#2377 so no duplicated matches

missing1<-match1%>%
  filter(is.na(DISCHARGE_NGR))%>%
  select(site)
#477 missing 


#2023 EDM returns data:
edm<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/severntrentedmreturn2023.csv")
#2472

#simple string cleaning
edm<-edm%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))


#matching exactly with WaSC specific naming
matched2<-left_join(missing1, edm, by = c("site" = "Site.Name..WaSC.operational...optional."))
#477 so no duplicate matches

missing2<-matched2%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#236 missing


#match exactly with edm return - EA site name now
matched3<-left_join(missing2, edm, by = c("site" = "Site.Name..EA.Consents.Database."))
#239 so some multiple matches
matched3<-matched3%>%
  distinct(site, .keep_all = TRUE)
#236


missing3<-matched3%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  subset(select=site)
#213 still missing


#match with EDM data from 2022 and 2021 next 

edm22<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/severntrentedmreturn2022.csv")
#2466

#simple string cleaning
edm22<-edm22%>%
  mutate(Site.Name..WaSC.operational...optional.=trimws(Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub(" +", " ", Site.Name..WaSC.operational...optional.))%>%
  mutate(Site.Name..WaSC.operational...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational...optional.))


#matching exactly with WaSC specific naming
matched4<-left_join(missing3, edm22, by = c("site" = "Site.Name..WaSC.operational...optional."))
#213 so no duplicate matches


missing4<-matched4%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  subset(select=site)
#205 missing so only matched 8



matched5<-left_join(missing4, edm22, by = c("site" = "Site.Name..EA.Consents.Database."))
#209 so some extra matches

matched5<-matched5%>%
  distinct(site, .keep_all = TRUE)

missing5<-matched5%>%
  filter(is.na(Site.Name..WaSC.operational...optional.))%>%
  select(site)
#127 left missing


#match with EDM from 2021 returns

edm21<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/severntrentedmreturn2021.csv")
#2658

edm21<-edm21%>%
  mutate(Site.Name..WaSC.operational.name...optional.=trimws(Site.Name..WaSC.operational.name...optional.))%>%
  mutate(Site.Name..WaSC.operational.name...optional.=gsub(" +", " ", Site.Name..WaSC.operational.name...optional.))%>%
  mutate(Site.Name..WaSC.operational.name...optional.=gsub("[[:punct:]]", "", Site.Name..WaSC.operational.name...optional.))%>%
  mutate(Site.Name..EA.Consents.Database.=trimws(Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub(" +", " ", Site.Name..EA.Consents.Database.))%>%
  mutate(Site.Name..EA.Consents.Database.=gsub("[[:punct:]]", "", Site.Name..EA.Consents.Database.))


#matching exactly with WaSC specific naming
matched6<-left_join(missing5, edm21, by = c("site" = "Site.Name..WaSC.operational.name...optional."))
#127 so no extra matches

missing6<-matched6%>%
  filter(is.na(Site.Name..EA.Consents.Database.))%>%
  select(site)
#82 left

#now with EA name
matched7<-left_join(missing6, edm21, by = c("site" = "Site.Name..EA.Consents.Database."))
#111 so extra matches

matched7<-matched7%>%
  distinct(site, .keep_all = TRUE)
#82 now

missing7<-matched7%>%
  filter(is.na(Site.Name..WaSC.operational.name...optional.))%>%
  select(site)
#13 missing only

#move to match these manually

library(writexl)


write_xlsx(missing7, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/severntrent unmatched1.xlsx")


matched8<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/severnmanual1.csv")
#some are not yet permitted - so for these, we include discharge location and TBC as permit reference

matched9<-matched8%>%
  filter(!grepl("tbc", EAnumber, ignore.case = TRUE))

matched8<-matched8%>%
  filter(grepl("tbc", EAnumber, ignore.case = TRUE))

#for those with permit numbers - we join information from consents

#matched9<-left_join(matched9, consents_all, by = c("EAnumber" = "PERMIT_NUMBER"))

#why didnt these match?

matched9<-left_join(matched9, edm, by = c("EAnumber" = "EA.Permit.Reference..EA.Consents.Database."))



#all matched now so need to join these dataframes into one index:


df1<-match1%>%
  filter(!is.na(DISCHARGE_NGR))
#1900

df2<-matched2%>%
  filter(!is.na(Site.Name..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)
#241


df3<-matched3%>%
  filter(!is.na(Site.Name..WaSC.operational...optional.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)

#23

df4<-matched4%>%
  filter(!is.na(Site.Name..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(-EDM.Operation.....of.reporting.period.EDM.operational)

#8

df5<-matched5%>%
  filter(!is.na(Site.Name..WaSC.operational...optional.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(-EDM.Operation.....of.reporting.period.EDM.operational)

#78

df6<-matched6%>%
  filter(!is.na(Site.Name..EA.Consents.Database.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(-Total.Duration..hrs..all.spills.prior.to.processing.through.12.24h.count.method,
         -Counted.spills.using.12.24h.count.method,
         -EDM.Operation.....of.reporting.period.EDM.operational)

#45

df7<-matched7%>%
  filter(!is.na(Site.Name..WaSC.operational.name...optional.))%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EA.Permit.Reference..EA.Consents.Database., 
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)%>%
  select(-Total.Duration..hrs..all.spills.prior.to.processing.through.12.24h.count.method,
         -Counted.spills.using.12.24h.count.method,
         -EDM.Operation.....of.reporting.period.EDM.operational)

#69

df8<-matched8%>%
  select(-site.name)%>%
  rename(PERMIT_NUMBER = EAnumber, DISCHARGE_NGR = location)
#6

df9<-matched9%>%
  select(-site.name, - RECEIVING_WATER)%>%
  rename(COMPANY_NAME = Water.Company.Name, PERMIT_NUMBER = EAnumber,
         DISCHARGE_NGR = Outlet.Discharge.NGR..EA.Consents.Database., RECEIVING_WATER = Receiving.Water...Environment..common.name...EA.Consents.Database.)

#7

#combine into index
severn_list <- list(df1, df2, df3, df4, df5, df6, df7, df8, df9)

severnindex <- do.call(bind_rows, severn_list)
#2377 so index represents all unique sites in severn trent data


severnindex<-severnindex%>%
  select(site, COMPANY_NAME, PERMIT_NUMBER, DISCHARGE_NGR, RECEIVING_WATER, ADD_OF_DISCHARGE_SITE_PCODE, REC_ENV_CODE_DESCRIPTION,
         ISSUED_DATE, REVOCATION_DATE)


#add to the severn trent raw spill data

severnmatched<-left_join(severn, severnindex, by = "site")
#check<-severnmatched%>%
#  filter(is.na(DISCHARGE_NGR))
#no missing

save(severnmatched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/severnmatched.RData")


#transform with 12 24 spill count method:

rm(list=ls())
gc()



library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(fuzzyjoin)


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/severnmatched.RData")
#527016

severnmatched<-severnmatched%>%
  distinct()
#527016 so no duplicate entries


#there are some unpermitted discharge sites included in the data here
#therefore we need an id column to identify sites, rather than rely on Permit Numbers which don't exist for every site

#TBC<-severnmatched%>%
#  filter(grepl("anomaly", PERMIT_NUMBER, ignore.case = TRUE) | grepl("tbc", PERMIT_NUMBER, ignore.case = TRUE) | is.na(PERMIT_NUMBER))
#16490
#unique(TBC$PERMIT_NUMBER)
#all labelled TBC

severnmatched1<-severnmatched%>%
  mutate(PERMIT_NUMBER = ifelse(grepl("anomaly|tbc", PERMIT_NUMBER, ignore.case = TRUE), NA, PERMIT_NUMBER))%>%
  mutate(id = ifelse(is.na(PERMIT_NUMBER), site, PERMIT_NUMBER))

check<-severnmatched1%>%
  filter(is.na(PERMIT_NUMBER))



#apply 12 24 spill counting logic:


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

severnblocks1<-create_blocks(severnmatched1)

severnblocks2<-split_blocksv1(severnblocks1)
#156164 spills via 12 24 method


severnmatched1<-severnmatched1%>%
  select(-startdatetime, -enddatetime)

severnmatched1<-severnmatched1%>%
  distinct(site, .keep_all = TRUE)

severnmatched2<-severnmatched1%>%
  distinct(id, .keep_all = TRUE)
#2204 - likely some "different" site names had the same permit number (refer to the same site, but vary in spelling etc)


severnclean<-left_join(severnblocks2, severnmatched2, by = "id")

#finalcheck<-severnclean%>%
#  filter(is.na(DISCHARGE_NGR))
#no missing values

severnclean<-severnclean%>%
  select(-ISSUED_DATE, -REVOCATION_DATE, - EAnumber, - id)%>%
  mutate(duration = replace_na(duration, 24))%>%
  select(-EAsitename, -misc_code, -unique_id, wasc)%>%
  rename(permit_reference = PERMIT_NUMBER)%>%
  rename(postcode = ADD_OF_DISCHARGE_SITE_PCODE)


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/welshclean.RData")

save(severnclean, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/severnclean.RData")
