gc()
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)



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


anglianblocks<-create_blocks(anglian)

#perfect

save(anglianblocks, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/anglianblocks.RData")



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
      cat(" Processed:", counter, "\n")
    }
    
    #assigns variables to individual observations
    current_id<-blocks$id[i]
    current_block_start<-blocks$block_start[i]
    current_block_end<-blocks$block_end[i]
    #gives first spill end (always 12 hours)
    firstinterval_end<-blocks$block_start[i]+hours(12)
    #NEW LOGIC NEEDED - if not 12 hours (if NA) then add 13 hours
    firstinterval_duration<-12
    #now we wish to see "duration" - to check function works properly
    remainder_duration<-difftime(current_block_end, firstinterval_end, units = "hours")
    #DONT NEED NEW LOGIC HERE AS BLOCK END WILL ALWAYS BE A TRUE VALUE - given previous code
    
    #append 12 hour interval and remainder into dataframe
    firstsplit<-rbind(firstsplit, data.frame(id=current_id, block_start=current_block_start, block_end=firstinterval_end, duration = firstinterval_duration))
    firstsplit<-rbind(firstsplit, data.frame(id=current_id, block_start=firstinterval_end, block_end=current_block_end, duration = remainder_duration))
  }
  
  #separate 12 hour spills from remainder
  
  first12<-firstsplit%>%
    filter(duration==12)
  
  remainder<-firstsplit%>%
    filter(duration!=12 & duration!= 0)
  
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
