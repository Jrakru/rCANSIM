#' List of labour related CANSIM tables
#'
#' convenience function
#'
#' @keywords  cansim
#' @export list.of.labour.cansim_tbl
#' @return list
list.of.labour.cansim_tbl <-  c(14100001, 14100017, 14100018, 14100019, 14100020, 14100021,
                                 14100022, 14100023, 14100026, 14100027, 14100028, 14100029,
                                 14100030, 14100031, 14100032, 14100033, 14100034, 14100035,
                                 14100036, 14100037, 14100042, 14100043, 14100044, 14100045,
                                 14100048, 14100049, 14100050, 14100051, 14100054, 14100055,
                                 14100056, 14100057, 14100058, 14100059, 14100060, 14100063,
                                 14100064, 14100065, 14100066, 14100067, 14100068, 14100069,
                                 14100070, 14100071, 14100072, 14100075, 14100076, 14100077,
                                 14100078, 14100080, 14100081, 14100082, 14100083, 14100084,
                                 14100085, 14100086, 14100087, 14100088, 14100089, 14100090,
                                 14100091, 14100092, 14100095, 14100096, 14100097, 14100098,
                                 14100102, 14100103, 14100104, 14100105, 14100106, 14100107,
                                 14100108, 14100109, 14100110, 14100113, 14100114, 14100117,
                                 14100118, 14100119, 14100120, 14100121, 14100122, 14100123,
                                 14100124, 14100125, 14100126, 14100127, 14100128, 14100129,
                                 14100130, 14100132, 14100133, 14100134, 14100201, 14100202,
                                 14100203, 14100204, 14100205, 14100206, 14100208, 14100209,
                                 14100210, 14100211, 14100212, 14100213, 14100214, 14100215,
                                 14100216, 14100217, 14100218, 14100219, 14100220, 14100221,
                                 14100222, 14100223, 14100224, 14100225, 14100226, 14100227,
                                 14100255, 14100286, 14100287, 14100288, 14100289, 14100291,
                                 14100292, 14100293, 14100294, 14100295, 14100296, 14100297,
                                 14100298, 14100299, 14100300, 14100301, 14100302, 14100303,
                                 14100304, 14100305, 14100306, 14100307, 14100308, 14100309,
                                 14100310, 14100311, 14100312, 14100313, 14100314, 14100315,
                                 14100316, 14100317, 14100318, 14100319, 14100320, 14100325,
                                 14100326, 14100328, 14100331, 14100332, 14100354, 14100355,
                                 14100356, 14100357, 14100358, 14100359, 14100363, 14100364,
                                 14100365, 14100366, 14100367, 14100370)

#' get POST repsonce from Statistic Canada web API
#'
#' @param url string
#' @param body string
#' @param timeout integer (in seconds) (default = 200)
#' @param retry integer (default = 3)
#' @keywords internal API
#' @return POST response

post_with_timeout_retry <- function(url,body,timeout=200,retry=3){
  response <- purrr::safely(httr::POST)(url,
                                        body=body,
                                        encode="json",
                                        httr::add_headers("Content-Type"="application/json"),
                                        httr::timeout(timeout))
  if (!is.null(response$error)){
    if (retry>0) {
      message("Got timeout from StatCan, trying again")
      Sys.sleep(1)
      response <- post_with_timeout_retry(url,body=body,timeout=timeout,retry=retry-1)
    } else {
      message("Got timeout from StatCan, giving up")
      response=response$result
    }
  } else {
    response=response$result
  }

  if (is.null(response) && retry == 0) {
    stop(sprintf("Problem downloading data, multiple timeouts.\nPlease check your network connection. If your connections is fine then StatCan servers might be down."),call.=FALSE)
  }
  response
}




#' Retrieve metadata for specified CANSIM table
#'
#' Allows for the retrieval of metadatadata from Statistics Canada APIs
#'
#' @param PID a string representing the Product ID
#'
#' @return A tibble with metadata
#'
#' @export
get_cansim_cube_metadata<- function(PID){
  url="https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata"
  body_string=paste0('[{"productId":',PID,'}]')
  response <- post_with_timeout_retry(url, body=body_string)
  if (response$status_code!=200) {
    stop("Problem downloading data, status code ",response$status_code,"\n",httr::content(response))
  }
  data <- httr::content(response, as="text", encoding = "UTF-8")
  jsonlite::fromJSON(data)
}


update_meta_tbl <-  function(id, update){
  metaX <- if(exists("meta", envir = my_env)){
               get("meta", envir = my_env)
          } else {
               NULL
          }
  # metaX <- purrr::safely(get("meta", envir = my_env))
  if(is.null(metaX)){
    bind_rows(metaX, update) %>% arrange(Product.Id) %>% assign("meta",.,envir = my_env)
  } else {
    metaX %>% filter(Product.Id != id) %>%
      bind_rows(update) %>% arrange(Product.Id) %>% assign("meta",.,envir = my_env)
  }
}


extract_vector_metadata <- function(data){

  data %>% tibble::as.tibble(validate =F) -> K
  PID <- data$object$productId

  meta1 <- K$object %>%
    select( -c("responseStatusCode","dimension", "footnote", "correction")) %>%
    data.table::setnames(old = c("productId","cansimId", "cubeTitleEn", "cubeTitleFr", "cubeStartDate", "cubeEndDate"),
                         new = c("Product.Id", "CANSIM.Id", "Cube.Title", "Cube.Title.Fr", "Start.Reference.Period", "End.Reference.Period"))


  update_meta_tbl(PID, meta1)


  K$object["dimension"]  %>%
    tibble::as.tibble(validate = FALSE) %>%
    tidyr::unnest() -> KK


  KK %>%
    select( -member) %>%
    data.table::setnames(old = c("dimensionPositionId", "dimensionNameEn", "dimensionNameFr"),
                         new = c("Dimension.ID", "Dimension.name", "Dimension.name.fr")) %>%
    assign(paste0(PID,"_dim"), . ,  envir= my_env)


  KK %>%
    tidyr::unnest() %>%
    data.table::setnames(old = c("dimensionPositionId", "dimensionNameEn", "dimensionNameFr","memberId", "memberNameEn", "memberNameFr"),
                         new = c("Dimension.ID", "Dimension.name", "Dimension.name.fr", "Member.ID", "Member.Name", "Member.Name.Fr")) %>%
    assign(paste0(PID,"_var"), . ,  envir= my_env)

}
