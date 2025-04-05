#now that missing values from DST issues etc has been solved
#these scripts aim to match every single edm to EA consents
#script 2: welsh water.

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


#welsh water only
welsh<-raw_spill%>%
  filter(wasc=="welsh")
#34134

#welsh water has no provided EA Permit Reference information - all done via name matching


#some string cleaning in consents data
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
welsh<-welsh%>%
  mutate(site = trimws(site))%>%
  mutate(site = gsub(" +", " ", site))%>%
  mutate(site = gsub("[[:punct:]]", "", site))

#how many unique sites do we have
welshunique<-welsh%>%
  distinct(site)
#only 122 sites

#so we'll match this first via exact matches with consents
#secondly with EA consents returns
#thirdly via fuzzy matching with manual verification
#and lastly manual matching of remaining


#exact matching with consents

#welsh consents - taking the most recent permit information
consentswelsh<-consents_all%>%
  filter(grepl("dwr cymru", COMPANY_NAME, ignore.case = TRUE))%>%
  arrange(DISCHARGE_SITE_NAME, desc(ISSUED_DATE))%>%
  distinct(DISCHARGE_SITE_NAME, .keep_all = TRUE)
#507

match1<-left_join(welshunique, consentswelsh, by = c("site" = "DISCHARGE_SITE_NAME"))

#checing for missing values
missing1<-match1%>%
  filter(is.na(DISCHARGE_NGR))
#only 14 missing


#exact matching with edm data

edm<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/manual matching/welshedmreturn2023.csv")
#128
edm<-edm%>%
  mutate(X.1=trimws(X.1))%>%
  mutate(X.1=gsub(" +", " ", X.1))%>%
  mutate(X.1=gsub("[[:punct:]]", "", X.1))

missing1<-missing1%>%
  subset(select = site)
  
matched2<-left_join(missing1, edm, by = c("site" = "X.1"))
#10 matched - all unpermitted - but have a unpermitted reference number, and the NGR location


missing2<-matched2%>%
  filter(is.na(X))
#4 missing


edm<-edm%>%
  mutate(X=trimws(X))%>%
  mutate(X=gsub(" +", " ", X))%>%
  mutate(X=gsub("[[:punct:]]", "", X))

missing2<-missing2%>%
  subset(select = site)

matched3<-left_join(missing2, edm, by = c("site" = "X"))

#2 left

missing3<-matched3%>%
  filter(is.na(X.2))%>%
  subset(select = site)

#will manually find these matches

#LYDBROOK:
check1<-consentswelsh%>%
  filter(grepl("lydbrook", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#matches with LYDBROOK SEWAGE TREATMENT WORKS

#WHITECROSS:
check2<-consentswelsh%>%
  filter(grepl("whitecross", DISCHARGE_SITE_NAME, ignore.case = TRUE))
#matches with WHITECROSS ROAD CSO HEREFORD

#so mutate a column for this 

missing3<-missing3%>%
  mutate(match = c("LYDBROOK SEWAGE TREATMENT WORKS", "WHITECROSS ROAD CSO HEREFORD"))

matched4<-left_join(missing3, consentswelsh, by = c("match" = "DISCHARGE_SITE_NAME"))


#so now all are matched, we can create our index by joining all of these matched dataframes
# - by removing the missing values and making the columns identical
#then use this index to match up all the spill data


df1<-match1%>%
  filter(!is.na(DISCHARGE_NGR))
#108

df2<-matched2%>%
  filter(!is.na(X.3))%>%
  rename(COMPANY_NAME = EDM.STORM.OVERFLOW.REGULATORY.ANNUAL.RETURN)%>%
  rename(PERMIT_NUMBER = X.3)%>%
  rename(DISCHARGE_NGR = X.6)%>%
  rename(RECEIVING_WATER = X.9)%>%
  mutate(ADD_OF_DISCHARGE_SITE_PCODE=NA)%>%
  mutate(REC_ENV_CODE_DESCRIPTION=NA)%>%
  mutate(ISSUED_DATE=NA)%>%
  mutate(REVOCATION_DATE=NA)%>%
  select(-X, -X.2, -X.4, -X.5, -X.7, -X.8, -X.10, -X.11, -X.12, -X.13, -X.14, -X.15, -X.16, -X.17, -X.18, -Complete.columns.U...V.when.data.in.column.T.is..90., -Enter.in.columns.W..X..Y.when.relevant.spill.frequency.data..column.Q...valid.historic..is.above.the.SOAF.threshold..pg..2.of.SOAF., -X.19, -X.20, -X.21, -X.22)
#10


#join these
welshindex<-rbind(df1, df2)%>%
  mutate(DISCHARGE_NGR = gsub(" ", "", DISCHARGE_NGR))


df3<-matched3%>%
  filter(!is.na(X.3))%>%
  rename(COMPANY_NAME = EDM.STORM.OVERFLOW.REGULATORY.ANNUAL.RETURN)%>%
  rename(PERMIT_NUMBER = X.3)%>%
  rename(DISCHARGE_NGR = X.6)%>%
  rename(RECEIVING_WATER = X.9)%>%
  mutate(ADD_OF_DISCHARGE_SITE_PCODE=NA)%>%
  mutate(REC_ENV_CODE_DESCRIPTION=NA)%>%
  mutate(ISSUED_DATE=NA)%>%
  mutate(REVOCATION_DATE=NA)%>%
  select(-X.1, -X.2, -X.4, -X.5, -X.7, -X.8, -X.10, -X.11, -X.12, -X.13, -X.14, -X.15, -X.16, -X.17, -X.18, -Complete.columns.U...V.when.data.in.column.T.is..90., -Enter.in.columns.W..X..Y.when.relevant.spill.frequency.data..column.Q...valid.historic..is.above.the.SOAF.threshold..pg..2.of.SOAF., -X.19, -X.20, -X.21, -X.22)%>%
  mutate(DISCHARGE_NGR = gsub(" ", "", DISCHARGE_NGR))
#2


df4<-matched4%>%
  select(-match)
#2

welshindex<-rbind(welshindex, df3, df4)
#122 as needed 

welshmatched<-left_join(welsh, welshindex, by = "site")

checkfinal<-welshmatched%>%
  filter(is.na(DISCHARGE_NGR))
#none missing
rm(checkfinal)


#now to make columns consistent with anglian water
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/anglianmatched.RData")

welshmatched<-welshmatched%>%
  mutate(EAnumber=PERMIT_NUMBER)%>%
  select(-PERMIT_NUMBER)

checking<-rbind(anglianmatched, welshmatched)
#match columns

save(welshmatched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/welshmatched.RData")



#done matching - move onto applying 12 24 spills


rm(list=ls())
gc()

#12 24 processing to welsh raw data


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)



load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/matched/welshmatched.RData")
#34134

welshmatched<-welshmatched%>%
  arrange(EAnumber, startdatetime)

#remove any duplicate rows
welshmatched<-welshmatched%>%
  distinct()
#25323



welsh<-welshmatched%>%
  mutate(id=EAnumber)



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


block1<-create_blocks(welsh)

block2<-split_blocksv1(block1)


df1<-welshmatched%>%
  distinct(EAnumber, .keep_all = TRUE)%>%
  select(-startdatetime, -enddatetime)

welsh1224<-left_join(block2, df1, by = c("id" = "EAnumber"))

check<-welsh1224%>%
  filter(is.na(DISCHARGE_NGR))

welshclean<-welsh1224%>%
  select(-ISSUED_DATE, -REVOCATION_DATE, -id.y)%>%
  mutate(duration = replace_na(duration, 24))%>%
  select(-EAsitename, -misc_code, -unique_id, wasc)%>%
  rename(permit_reference = id)%>%
  rename(postcode = ADD_OF_DISCHARGE_SITE_PCODE)

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/anglianclean.RData")


save(welshclean, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/welshclean.RData")



library(writexl)

write_xlsx(welshclean, path = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/welshclean.xlsx")

