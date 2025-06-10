/******************************************************************************
************************** London School of Economics *************************
*******************************************************************************
    Author:           Jacopo Olivieri
    Project:          Sewage Project
    Date:             2025-06-09
    Script:           load_house_panel.do
 
    Description:    This script loads the house panel data from a Parquet file.
                    The data contains event information and is used for
                    further analysis in the project.
*******************************************************************************/
 
 
********************************************************************************
* 0. Setup
********************************************************************************
 
    * Create settings.
    clear all
    set more off, permanently
    set type double
    capture log close
    macro drop all
    set excelxlsxlargefile on 
 
    * Paths and globals.
    ** Run 00_stata_setup.do to set up the environment
        
    ** Current date
    global dateout = string(date(c(current_date),"DMY"),"%tdYND")

********************************************************************************
* 01.    Load data.
********************************************************************************
    
	* Import Parquet data // ssc install pq
    ** 250m buffer
    pq use "$finaldata/dat_panel_house/dat_panel_house_250.parquet", clear
    ** 500m buffer
    pq use "$finaldata/dat_panel_house/dat_panel_house_500.parquet", clear
    ** 1km buffer
    pq use "$finaldata/dat_panel_house/dat_panel_house_1000.parquet", clear
