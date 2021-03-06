retrieve_pilr_params <- function(pilr_server, project, participant, instrument, period, access_code) {
  
  endpoint <- paste0(pilr_server, "/api/v1", "/", project, "/instrument/", instrument, "/participant/",
                     participant, "/period/", period, "/variablesAndSettings")
  
  message("Attempting to get params from ", endpoint, " ...")
  
  get_protected_content(endpoint, access_code, do_rbind = FALSE)
}
