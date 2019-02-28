
#' Return measurement data from the PiLR server. 
#'
#' 
#' This function will return a data.frame containing measurement data
#' 
#' 
#' @param endpoint The endpoint to retrieve information. 
#' 
#' @keywords measurement data
#' @import plyr
#' @export
#' @examples
#' read_pilr(endpoint)
#' 
read_pilr <- function(pilr_server, project, data_set, schema, access_code,
                      bad_metadata_ok = FALSE, value = "parsed", 
                      query_params = NULL, ...) {
  pilr_server <- check_pilr_server(pilr_server)
  project <- check_pilr_project(project)
  
  # fix notation of numeric
  if (!(is.null(query_params$offset)))
  {
    # when returning all values, start at 0
    if (value == "all")
    {
      query_params$offset <- 0
    }
    query_params$offset <- as.integer(query_params$offset)
  }
  
  if(!is.null(query_params)) {
    query_string <- do.call(construct_query_string, query_params)
  } else {
    query_string <- ""
  }
  
  ep_data <- retrieve_pilr_data(pilr_server, project, data_set,
                                schema, query_string, access_code)
  
  if(value == "raw") {
    return(ep_data)
  }
  
  # return errors if request is not successful 
  if (ep_data$status_code != 200)
  {
    stop(ep_data)
  }
  
   ret <- json_to_pilr(ep_data, bad_metadata_ok)
  
   if (value == "all")
   {
     ret2 <- ret
     # loop until returned data frame is 0 rows
     while (nrow(ret2) >= 10000)
     {
       message("Retrieved ",nrow(ret)," records")
       query_params$offset <- as.integer(nrow(ret))
       query_string <- do.call(construct_query_string, query_params)
       ep_data <- retrieve_pilr_data(pilr_server, project, data_set,
                                     schema, query_string, access_code)
       ret2 <- json_to_pilr(ep_data, bad_metadata_ok)
       ret <- rbind.fill(ret,ret2)
     }
   }
   else if (nrow(ret) >= 10000) {
     message("You have received the maximum capacity of 10000 data records. More records may exist in this dataset but must be retrieved in multiple parts.")
   }
  
  ret
}
