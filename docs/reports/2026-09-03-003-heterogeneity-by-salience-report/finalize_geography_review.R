# Recheck the reviewed candidate, save evidence, and issue a hash-bound review.
# This does not publish production data or run regressions.
source(here::here('docs','reports','2026-09-03-003-heterogeneity-by-salience-report','report_storage.R'))
source(here::here('scripts','R','03_data_enrichment','build_open_coast_reference.R'))

finalize_geography_review <- function() {
  requireNamespace('sf')
  root <- file.path(salience_report_root(), 'geometry-alignment-50-sepa')
  candidate <- file.path(root, 'private', 'reviewed_reference.rds')
  reference <- readRDS(candidate)
  builder <- new.env(parent = globalenv())
  sys.source(here::here('scripts','R','03_data_enrichment','build_site_group_characteristics.R'), builder)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  usable <- is.finite(locations$easting) & is.finite(locations$northing)
  points <- sf::st_as_sf(locations[usable, ], coords = c('easting','northing'), crs = 27700)
  evidence <- measure_open_coast_evidence(points, reference, reference$coverage)
  if (any(evidence$open_coast_status != 'supported_candidate')) stop('Relevant geography remains unresolved.')
  if (any(abs(evidence$distance_to_open_coast_m - 2000) <= reference$eligibility_endpoint_resolution_m))
    stop('A threshold case requires finer mouth interval review.')
  for (kind in c('retained','excluded','unresolved'))
    validate_open_coast_layer(reference[[kind]], c('LINESTRING','MULTILINESTRING'), kind)
  summary <- data.frame(status = c('validated','missing_location'), n_sites = c(sum(usable),sum(!usable)))
  write.csv(summary, file.path(root,'final_coverage_summary.csv'), row.names = FALSE)
  near <- evidence[order(abs(evidence$distance_to_open_coast_m - 2000))[1:20], ]
  write.csv(near, file.path(root,'threshold_review.csv'), row.names = FALSE)
  panel_bounds <- list(thames=c(-.2,51.35,.8,51.75),severn=c(-3.3,51.3,-2.3,52),
    morecambe=c(-3.3,53.8,-2.7,54.3),poole=c(-2.2,50.55,-1.8,50.8),
    dee=c(-3.4,53.1,-2.8,53.5),solway=c(-3.6,54.7,-2.8,55.1))
  for (name in names(panel_bounds)) {
    region <- sf::st_as_sfc(sf::st_bbox(stats::setNames(panel_bounds[[name]],c('xmin','ymin','xmax','ymax')),crs=4326)) |>
      sf::st_transform(27700)
    png(file.path(root,paste0('final-',name,'.png')),width=1400,height=1000,res=140)
    plot(region,col='white',border=NA,axes=TRUE,main=paste(name,'— reviewed physical shoreline'))
    for (kind in c('retained','excluded','unresolved')) {
      part <- suppressWarnings(sf::st_crop(reference[[kind]],sf::st_bbox(region)))
      if(nrow(part)) plot(sf::st_geometry(part),add=TRUE,
        col=c(retained='#0072B2',excluded='#999999',unresolved='#D55E00')[[kind]],lwd=1.2)
    }
    legend('bottomleft',legend=c('Eligible coast','Excluded tidal bank','Unresolved (not nearest)'),
      col=c('#0072B2','#999999','#D55E00'),lty=1,bg='white',cex=.8)
    dev.off()
  }
  nearest <- sf::st_nearest_feature(points,reference$retained)
  shore_xy <- do.call(rbind,lapply(sf::st_nearest_points(points,reference$retained[nearest,],pairwise=TRUE),function(x)tail(x,1)))
  cross_border <- data.frame(site_id=points$site_id,site_x=sf::st_coordinates(points)[,1],
    site_y=sf::st_coordinates(points)[,2],shore_x=shore_xy[,1],shore_y=shore_xy[,2],
    source_id=reference$retained$source_id[nearest],distance_m=evidence$distance_to_open_coast_m)
  # Solway audit includes both shores; coordinates make any cross-border claim inspectable.
  cross_border <- cross_border[cross_border$site_x < 350000 & cross_border$site_y > 540000,]
  write.csv(cross_border,file.path(root,'solway_nearest_shores.csv'),row.names=FALSE)
  raw <- here::here('data','raw','geography','open_coast')
  source_files <- list.files(raw,recursive=TRUE,full.names=TRUE)
  source_files <- source_files[!dir.exists(source_files)]
  code <- c(here::here('scripts','R','03_data_enrichment','build_open_coast_reference.R'),
    file.path(salience_report_root(),c('audit_open_coast_geometry.R','review_inland_shore_extensions.R',
      'review_mouth_alignment.R','apply_shore_review.R','finalize_geography_review.R')),
    file.path(root,c('manual_shore_decisions.csv','mouth_separator_extensions.csv','inland_extension_review.csv','mouth_alignment_decisions.csv')))
  sources <- lapply(c(source_files,code),function(path) list(
    path=substring(path,nchar(here::here())+2L),sha256=salience_file_hash(path)))
  review <- list(status='approved',unresolved_relevant_segments=0L,
    candidate_sha256=salience_file_hash(candidate),location_hash=open_coast_location_hash(locations),
    sources=sources,review_date=as.character(Sys.Date()),
    distance_engine=as.list(sf::sf_extSoftVersion()),crs='EPSG:27700',units='metres',
    convention='OS OpenMap Local Mean High Water (England/Wales), Mean High Water Springs (Scotland)',
    physical_displacement_m=0,eligibility_endpoint_resolution_m=reference$eligibility_endpoint_resolution_m,
    threshold_m=2000,threshold_inclusive=TRUE,
    limitations='Generalised cartography is not survey accuracy. Polygon alignment selects existing physical segments; it never buffers the membership distance. Unresolved segments remain in the reference but none is potentially nearer for the reviewed site universe. Missing coordinates remain unknown.',
    maps=paste0('final-',names(panel_bounds),'.png'))
  jsonlite::write_json(review,file.path(root,'geography_review.json'),auto_unbox=TRUE,pretty=TRUE)
  print(summary)
  invisible(review)
}
if(sys.nframe()==0L) finalize_geography_review()
