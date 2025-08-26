# install if needed
# install.packages("arrow")

library(arrow)
library(dplyr)

# path to your parquet
fp <- "/Users/odran/Dropbox/sewage/data/final/dat_panel_house/dat_panel_house_250.parquet"

# read it
dat <- read_parquet(fp)

# basic checks
dim(dat)           # rows x cols
names(dat)         # column names
head(dat, 10)      # first 10 rows
glimpse(dat)       # tidy overview
summary(dat)       # summary stats
