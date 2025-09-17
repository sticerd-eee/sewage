* Define the paths

** Opening Commands **

	clear all
	set linesize 200
	set more off
	set maxvar 32767
	set matsize 11000

	set emptycells drop

****************************************************

* Set working directory (to where the Python script is)
cd "/Users/s.dhingra/Dropbox/sewage/"
****************************************************


* uncomment to understand main structure of data cleaning and data construction 
/*
****************************************************

* scripts 1 to 9 process the the edm spill data for different companies and different years
* edm data is them matched to ge location 
* some of this has fuzzy matching so looking at it from script 10
****************************************************


* below change the "convert_parquet_to_dta.py" file with the name of the datset that we want to look at and convert to csv and then to dta
* converting to csv first because stata can't handle some characters
* from script 10 in R
/*
File	Description
matched_events_annual_data.parquet	Final merged dataset with individual + annual spill info
events_unmatched.parquet	Individual events that had no matching annual record
annual_unmatched.parquet	Annual records (with spills) that were not matched to any event
site_metadata.parquet	Deduplicated site-level location and permit metadata

Primary merged dataset, combining:
Individual event details (timestamped spills)
Annual stats (spill duration/counts, metadata)
Site-level location info (ngr, permit_reference, etc.)
Match metadata (match_method, match_quality, etc.)
*/ 

/*

****************************************************
* Convert parquet files to .csv via Python, then
* import and save as .dta
****************************************************

* Set Python executable (already configured)
python set exec "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3", permanently

* Step 1: Run the Python script that generates CSVs
shell python3 convert_parquet_batch_to_csv.py

* Step 2: Import each CSV and save as .dta
local files matched_events_annual_data events_unmatched annual_unmatched site_metadata

foreach f in `files' {
    local csv = "raw_to_final/`f'.csv"
    local dta = "raw_to_final/`f'.dta"

    capture confirm file "`csv'"
    if _rc {
        di as error "⚠️  File not found: `csv'. Skipping."
        continue
    }

    di "🔄 Processing `csv'"
    import delimited using "`csv'", clear stringcols(_all)
    save "`dta'", replace
    di as result "✅ Saved `dta'"
}
****************************************************

*/


pq use "data/processed/matched_events_annual_data/matched_events_annual_data.parquet", clear

* matched event annual file description (other files don't exist)
*import delimited "raw_to_final/matched_events_annual_data.csv", clear varnames(1) encoding(utf8)
* 5413633 obs 
* 14,500 siteid/10 water cos/2021-2023/17569 site name/13506 permit_ea/15741 permit_wasc
* activity_reference is what kind of discharge is legally allowed at a given site -- SO = Storm overflow and CSO = combined sewer overflow -- 365 unqiue
* asset_type is physical infrastructure type associated with a discharge point -- 17 unique
* 12,992 ngr  codes
/* ngr 
This is a British National Grid (BNG) reference, and here's what it means:
SD: The 100 km grid square identifier (each grid square has a two-letter code)
53990: The easting — how far east within the grid square (in meters)
27460: The northing — how far north within the grid square (in meters)
Together, this pinpoints a location to within 1 meter accuracy.
*/
/* unique id
site-level identifier assigned by the water company or the Environment Agency. It uniquely identifies an EDM (Event Duration Monitoring) monitoring point — such as a storm overflow, pumping station, or treatment works.
Part	Meaning
SWW	Likely the water company code – here, South West Water
0760	A unique number identifying the site (e.g. an overflow or outfall)
*/
* 65,002 missing for spill_hrs_ea and spill_count_ea
* wfd waterbody 2795 unique
/* "GB104027062980"
GB	United Kingdom (ISO country code)
1	River (surface water type)
04	EA management catchment (region code)
027062980	Unique numeric identifier for the waterbody
*/
* group ids are matches with annual data 

pq use "data/processed/matched_events_annual_data/events_unmatched.parquet", clear
* 124,368 obs 
pq use "data/processed/matched_events_annual_data/annual_unmatched.parquet", clear
* 742 obs 
pq use "data/processed/matched_events_annual_data/site_metadata.parquet", clear
* 37,691 obs 14,500 site_id

****************************************************

* script 11 has the site name and ngr and their permit type with details on effluent permits
****************************************************
****************************************************

* script 12 transforms detailed timestamped spill data from individual site spill events into time-aggregated spill statistics at multiple levels (year, quarter, month) for each site and water company. for events spannign multiple months, different month intervals are created, uses 12/24hr counting rule -- counts each spill event, merging spills closer than 12 hours into one event and adjusting for longer durations. fills missing observations with zero counts/durations to keep the time series complete
pq use "data/processed/agg_spill_stats/agg_spill_mo.parquet", clear
* 14,500 site_id 3 years at monthly aggregation
* 12,992 ngr
* has 14500*3*12 = 522,000 obs
* 70,306 obs with missings for spill_count_mo
save "raw_to_final/agg_spill_mo.dta", replace

****************************************************

* script 13 consolidates spill event data into unique site locations, providing clean geospatial coordinates and availability information across multiple years
pq use "data/processed/unique_spill_sites.parquet", clear
* 14,500 site_id 12,992 ngr
save "raw_to_final/unique_spill_sites.dta", replace

****************************************************

* script 14 contains hm land registry data on house prices-- transactions by house and includes post codes
pq use "data/processed/house_price.parquet", clear
* 3175951 transactions/houseid 
save "raw_to_final/house_price.dta", replace

****************************************************

* script 15 joins the houseid to spill sites within 10 kms 
pq use "data/processed/spill_house_lookup.parquet", clear
* 3,163,654 households and 14,500 site_id
save "raw_to_final/spill_house_lookup.dta", replace

****************************************************

* script 16 creates the data for the spills 12 mos prior to house sale
pq use "data/processed/cross_section/sales/prior_12mo/radius=250/part-0.parquet", clear
* 1,798,189 households 

****************************************************

* script 17 generates detailed spill indicators per site over time, including thresholds that describe how severe spill counts and durations are relative to typical values.
* thresholds created Calculated for each period, year, and all-time:
* thr_p50_spill_count_mo / thr_p50_spill_count_qtr: Median spill count for the period type.
* For yearly and all-time, similar variables exist but with suffixes _yr and _all.
* d_p50_spill_count_mo: 1 if spill count exceeds median monthly threshold, else 0.
pq use "data/processed/agg_spill_stats/agg_spill_stats_mo.parquet", clear
* 14,500 site_id at monthly frequency 
* 522,000 obs = 14500*3*12
* 451,694 spill_count spill_hrs 

****************************************************

* script 18 (site_panel_sales.R) aggregates house price statistics at the site level over monthly and quarterly periods for different distance radii from the site.
* period_type (monthly or quarterly) and different radii 
* summarizes house prices around each spill site, separately for different spatial scales (radii) and temporal scales (month/quarter), capturing both unweighted and distance-weighted price measures and the number of contributing houses
pq use "data/processed/dat_panel_site/sales/radius=250/period_type=monthly/part-0.parquet", clear
* 10,004 site_id * 12*3 = 360,144 obs

****************************************************

* script 19 constructs panel of house sales linked to nearby spill sites, structured by time period and radius threshold
pq use "data/processed/within_radius_panel/sales/radius=250/period_type=monthly/part-0.parquet", clear
* 10,004 site_id  172,103 house_id 
* house_id version of scrip18 data

****************************************************

* script 20 creates a general house sale-level panel dataset linking individual house sales to sewage spill sites across specified radius thresholds (250m, 500m, 1000m), but unlike script 19 which only included houses within the radius, this one includes all houses:
* Houses within the radius are tagged and linked to sites.
* Houses outside the radius are still included but flagged accordingly and not linked to any site.
* It combines these into a general panel that covers all sales, annotated with spatial relationships, for further downstream filtering or analysis.
* aggregated quarterly (by quarter-year).
* Houses that are NOT linked to any site within the radius. Mark within_radius = FALSE and distance_m = NA.

pq use "data/processed/general_house_panel_qtr/radius=250/part-0.parquet", clear
* now have all houses 3,175,951 house_id
* have 10,004 site_id for 250m 
* no price data here -- just lookup
****************************************************

cd "/Users/s.dhingra/Dropbox/sewage/"

* script create_stata-dataset 
* quarterly panel dataset of house sales within and outside the 1000m radius of spill sites.
* Each row is a house-quarter observation.
* Contains both spill exposure variables and house characteristics.
* Summarizes spill data by house and quarter for houses within the radius
* Creates zero-spill records for houses outside the radius (ensures all houses have quarterly rows, even if no spills near them).
* Generates treatment indicators based on whether spill counts or hours exceed thresholds, at the quarterly level:
* Uses thresholds from the spill data for different quantiles (median, 75th, 90th percentile, max, mean).
* Creates binary indicators for treatment status per threshold.
* Identifies the first quarter each house is treated for each threshold, producing "event study"-style timing variables. 
pq use "data/final/dat_panel_house/dat_panel_house_250.parquet", clear
* now have all houses 3,175,951 house_id
* first_t ranges from 1-12 (643,312 treated obs/108120 house_id) and has 999999 for not treated (18,136,009 obs)
* 172,103 house_id within_radius==1 

* for lpdid, event_date is same as time of first treatment 
* event_time is missing for never treated 
* treat is 0 before treatment and 1 afterwards 
* lpdid Y, time(time) unit(unit) treat(treat) pre(5) post(10) rw nocomp // no composition 

*/


****************************************************
* Processed files for constructing main data 
* spills and houe prices together
****************************************************

* script 12 transforms detailed timestamped spill data from individual site spill events into time-aggregated spill statistics at multiple levels (year, quarter, month) for each site and water company. for events spannign multiple months, different month intervals are created, uses 12/24hr counting rule -- counts each spill event, merging spills closer than 12 hours into one event and adjusting for longer durations. fills missing observations with zero counts/durations to keep the time series complete
pq use "data/processed/agg_spill_stats/agg_spill_mo.parquet", clear
* 14,500 site_id 3 years at monthly aggregation
* 12,992 ngr
* has 14500*3*12 = 522,000 obs
* 70,306 obs with missings for spill_count_mo
rename ngr ngr_original
save "raw_to_final/agg_spill_mo.dta", replace

****************************************************

* script 13 consolidates spill event data into unique site locations, providing clean geospatial coordinates and availability information across multiple years
pq use "data/processed/unique_spill_sites.parquet", clear
* 14,500 site_id 12,992 ngr
merge m:m site_id using "raw_to_final/agg_spill_mo.dta"
* all matched
drop _merge 
save "raw_to_final/unique_spill_sites.dta", replace

****************************************************
 
* script 14 contains hm land registry data on house prices-- transactions by house and includes post codes
pq use "data/processed/house_price.parquet", clear
* 3175951 transactions/houseid 
save "raw_to_final/house_price.dta", replace

****************************************************

* script 15 joins the houseid to spill sites within 10 kms 
pq use "data/processed/spill_house_lookup.parquet", clear
* 3,163,654 households and 14,500 site_id
save "raw_to_final/spill_house_lookup.dta", replace

****************************************************

joinby house_id using "raw_to_final/house_price.dta" 
joinby site_id using "raw_to_final/unique_spill_sites.dta"
