#' @export
json_to_pilr <- function(json_data, bad_metadata_ok = FALSE, type = "json") {
  if(type == "json") {
    if(class(json_data) == "response") {
      ep_data <- httr::content(json_data, as = "text")
      if (ep_data == "\n[\n]") df <- data.frame()
      else df <- flatten(jsonlite:::fromJSON(ep_data))
    } else {
      ep_data <- jsonlite::fromJSON(json_data,
                               simplifyVector = FALSE,
                               simplifyDataFrame = TRUE,
                               simplifyMatrix = FALSE)
      df <- do.call(data.frame, ep_data)
      # test
    }
  } else {
    ep_data <- json_data
  }
  
  if(length(ep_data) == 0) {
    return(data.frame())
  }
  
  ## how do we know if we have multiple datasets, or just one for a dashboard?
  ## this is not the best check...(Removed)
  ## 
  #if(!"metadata" %in% names(ep_data[[1]])) {
  #    df <- lapply(ep_data, create_data_frame, bad_metadata_ok)
  #} else {
  #    df <- create_data_frame(ep_data)
  #}
  
  ## Remove data. and metadata. from colnames
  names(df) <- gsub("metadata.", "", names(df))
  names(df) <- gsub("data.", "", names(df))
  
  # Some variables cannot be converted to a df
  # df <- tryCatch(as.data.frame(df), error=function(e) df)
  
  as.data.frame(df)
  
  message("... complete!")
  df
}
