############################################################
# Canonical / Site Group Consumer Manifest
############################################################

# Deliberately explicit: adding any .R/.qmd/.Rmd grain-token surface requires
# an owning unit and a reviewed classification here.
site_grain_consumer_manifest <- function() {
  readr::read_csv(I(
"path,classification,owner,reason
book/data_clean_documentation/01_pipeline.qmd,non_reader,U7,Pipeline documentation names the canonical output but does not read it
book/dry_spills.qmd,active_site_group,U5,Group-keyed dry-spill aggregates use Site Group coordinates
book/house_data_exploration.qmd,active_site_group,U5,Property lookup map uses Site Group coordinates
book/spill_data_exploration.qmd,active_site_group,U5,Group annual spill aggregates use Site Group coordinates
book/zoopla_data_exploration.qmd,active_site_group,U5,Rental lookup map uses Site Group coordinates
scripts/R/02_data_cleaning/clean_rainfall_data.R,active_site_group,U4,Rainfall grid uses Site Group coordinates
scripts/R/03_data_enrichment/aggregate_daily_spill_rainfall.R,active_site_group,U4,Daily rainfall panel uses Site Group availability
scripts/R/03_data_enrichment/aggregate_rainfall_stats.R,active_site_group,U4,Rainfall summaries use Site Group coordinates
scripts/R/03_data_enrichment/aggregate_spill_stats.R,active_site_group,U1,Annual aggregation reads Site Group annual status
scripts/R/03_data_enrichment/create_unique_spill_sites.R,active_canonical,U3,Canonical inventory attaches Site Group membership from crosswalk
scripts/R/03_data_enrichment/identify_dry_spills.R,active_site_group,U4,Dry-spill events use Site Group location
scripts/R/04_feature_engineering/10km_site_house_sale_match.R,active_site_group,U4,House distances use Site Group points
scripts/R/04_feature_engineering/10km_site_rental_match.R,active_site_group,U4,Rental distances use Site Group points
scripts/R/05_data_integration/merge_individ_annual_location.R,non_reader,U1,Produces the Site Group crosswalk
scripts/R/06_analysis_datasets/cross_section_prior_to_rental.R,active_site_group,U5,Rental missingness uses group annual status
scripts/R/06_analysis_datasets/cross_section_prior_to_sale.R,active_site_group,U5,Sale missingness uses group annual status
scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R,active_site_group,U5,House-site missingness uses group annual status
scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R,active_site_group,U5,Rental-site missingness uses group annual status
scripts/R/09_analysis/01_descriptive/dry_spill_rainfall_monthly_per_site_combined.R,non_reader,U5,unique_spill_sites is only a local metric name
scripts/R/09_analysis/01_descriptive/edm_commission_cumulative.R,active_canonical,U6,Commission figure counts Canonical Spill Sites
scripts/R/09_analysis/01_descriptive/edm_commission_timeline.R,active_canonical,U6,Commission figure counts Canonical Spill Sites
scripts/R/09_analysis/01_descriptive/population_exposure.R,active_site_group,U5,Exposure buffers use one point per Site Group
scripts/R/09_analysis/01_descriptive/spill_map_support_stats.R,active_site_group,U5,Map support uses one point per Site Group
scripts/R/09_analysis/01_descriptive/spill_maps.R,active_site_group,U5,Map joins group aggregates to Site Group points
scripts/R/09_analysis/01_descriptive/spill_maps_inset.R,active_site_group,U5,Inset map joins group aggregates to Site Group points
scripts/R/09_analysis/07_dry_spills/dry_spill_method_figure.R,active_site_group,U5,Target Site Group resolves to one representative point
scripts/R/testing/annual_return_treatment_asset_by_site_year.qmd,active_canonical,U5,Notebook reports Canonical Spill Site counts
scripts/R/testing/diff_aggregate_spill_stats_ch9.R,historical_only,U1,Historical pre-migration reconciliation
scripts/R/testing/investigate_partial_availability_missingness.qmd,active_canonical,U5,Notebook studies canonical availability histories
scripts/R/testing/london_total_shares_houses_spills.qmd,active_site_group,U5,London notebook uses Site Group coordinates
scripts/R/testing/missing_observation_patterns_2021_2023.qmd,active_canonical,U5,Notebook studies canonical availability histories
scripts/R/testing/reconcile_merge_rebuild.R,historical_only,U1,Historical Works-era rebuild comparison
scripts/R/testing/reconcile_site_group_consumers.R,non_reader,U5,Audits reader classifications and fixture reconciliation only
scripts/R/testing/reconcile_unique_spill_sites_grain.R,active_canonical,U3,Canonical inventory reconciliation
scripts/R/testing/site_grain_consumer_manifest.R,non_reader,U5,Declares grain-token ownership and reader classification
scripts/R/testing/test_create_unique_spill_sites_contracts.R,active_canonical,U3,Canonical inventory contract fixture and output checks
scripts/R/testing/test_edm_commission_contracts.R,non_reader,U2,Sources the canonical builder but does not read either artifact
scripts/R/testing/test_house_price_sewage_merge.Rmd,active_site_group,U5,Spatial test uses Site Group points
scripts/R/testing/test_merge_individ_annual_contracts.R,non_reader,U1,Exercises in-memory Site Group output objects
scripts/R/testing/test_merge_outputs_contracts.R,non_reader,U1,Exercises in-memory and temporary Site Group outputs
scripts/R/testing/test_regressions.Rmd,active_site_group,U5,Regression map setup uses Site Group points
scripts/R/testing/test_site_group_consumer_contracts.R,active_site_group,U4,Reads the production crosswalk for a projection integration check
scripts/R/testing/test_unmatched.Rmd,historical_only,U5,Historical embedded canonical-site producer does not read the artifacts
scripts/R/utils/merge_outputs_utils.R,non_reader,U1,Defines the crosswalk output builder
scripts/R/utils/site_group_utils.R,active_site_group,U4,Defines the authoritative Site Group artifact readers"
  ), show_col_types = FALSE)
}
