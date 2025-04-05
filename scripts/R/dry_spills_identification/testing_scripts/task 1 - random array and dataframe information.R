rm(list = ls())
gc()

#merging rainfall data to sewage spills data

#Task 1:
#AIM: To retrieve array information, based on two columns in a dataframe.
#(uses random array and dataframe values)

library(dplyr)
library(tidyr)
library(stringr)

#making a false rainfall array:

rainfall <- array(
  data = c(1:24),  #rainfall takes these values
  dim = c(3, 3, 8),  # Dimensions: x = 3, y = 3 ( ~ analogous to latitude longitude but simpler), time = 8
  dimnames = list(
    x = c(1, 2, 3),  # x-dimension (location)
    y = c(1, 2, 3),  # y-dimension (location)
    time = c(0, 1, 2, 3, 4, 5, 6, 7)  # Time: "hours since midnight" (for example)
  )
)

#in total this array contains 8 times (0 to 7 "hours since midnight")
#and 9 1x1m grid spaces
#so 72 data points



spill_data <- data.frame(
  time = c(0, 3, 6),  # Time points to match with array
  location = c("x1y1", "x2y3", "x3y2")  # Locations corresponding to x and y in the array
)

#3 observations total


#simple way to deal with adding array data to this dataframe:


spill_data <- spill_data %>%
  mutate(
    x = as.numeric(substr(location, 2, 2)),  
    y = as.numeric(substr(location, 4, 4))   
  )

spill_data <- spill_data %>%
  rowwise() %>%
  mutate(
    rainfall = rainfall[x, y, as.character(time)]
  )

#works fine

#so it's easy when indexing in the array matches the data:

#therefore two ways to think about matching

#working out the matching dimension values for each spill observation within the matching function
#or:
#creating new variables to match on, which match exactly to the array data




#move onto task 2.





