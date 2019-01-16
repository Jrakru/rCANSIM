my_env <- new.env(parent = emptyenv())

#' Unzip metadata CSV files
#'
#' Unzip metadata CSV files from ZIP files into a folder. The latter is created if it does not exists.
#'
#' @param zip.files.path string representing where zip files are stored
#' @param meta.files.path string representing where metadata csv files will be stored
#' @keywords directory metadata CSV
#' @export create_meta_directory_csv
#' @return NONE (side effect function)

create_meta_directory_csv <- function(zip.files.path, meta.files.path){
  old <- getwd()
  setwd(zip.files.path)

  for(f in list.files(pattern="^\\d+\\.zip")){
    f_nm <- strsplit(f,"\\.")[[1]][1]
    unzip(f, paste0(f_nm,"_MetaData.csv"), exdir=meta.files.path)
  }

  on.exit(setwd(old))
  invisible()
}



#' Initializes the meta querying system using Statistics Canada API
#'
#' read all the nessary metadata present in the provided path for the querying system
#'
#' @param path string representing where metadata csv files are stored
#' @keywords internal directory metadata cansim set
#' @export set_meta_directory_csv
#' @return NONE (side effect function)

set_meta_directory_csv <- function(path){

  # this is not the right way to import to the namespace. I am doing this out of expendiency. this should be fixed in the futur to prevent
  # name collisions.
  message("Following libraries are being loaded")
  message(paste("\t",c("data.table", "dplyr", "pryr", "purr", "lazyeval")))
  library(data.table)
  library(dplyr)
  library(pryr)
  library(purrr)
  library(lazyeval)


  cwd <- getwd()
  setwd(path)

  meta <-  data.frame()


  for(f in list.files()){
    # print(f)

    dt <- read.csv(f, nrows=1, fileEncoding="UTF-8-BOM")

    ndim <- dt$Total.number.of.dimensions

    dt2 <- read.csv(f, skip=3, nrows=ndim, fileEncoding="UTF-8-BOM")


    dt3 <- read.csv(f, skip=3+ndim+1, fileEncoding="UTF-8-BOM")
    row_stop <- which(dt3$Dimension.ID=="Symbol Legend")
    dt3 <- dt3[1:(row_stop-1),]


    meta <- rbind(meta, dt)
    nm <- strsplit(f,"\\_")[[1]][1]
    assign(paste0(nm,"_dim"), dt2, envir = my_env)
    assign(paste0(nm,"_var"), dt3, envir = my_env)
    assign("meta",meta, envir = my_env)
  }


  rm(list = c("dt", "dt2","dt3","f","ndim", "nm","row_stop"))

  on.exit(setwd(cwd))

  invisible()
}


#' Initializes the meta querying system using Statistics Canada API
#'
#' read all the nessary metadata present in the provided path for the querying system
#'
#' @param PID_List List of strings representing that tables PIDs
#' @keywords internal directory metadata cansim set
#' @export set_meta_directory_api
#' @return NONE (side effect function)

set_meta_directory_api <- function(PID_List){

  # this is not the right way to import to the namespace. I am doing this out of expendiency. this should be fixed in the futur to prevent
  # name collisions.
  message("Following libraries are being loaded")
  message(paste("\t",c("data.table", "dplyr", "pryr", "purr", "lazyeval")))
  library(data.table)
  library(dplyr)
  library(pryr)
  library(purrr)
  library(lazyeval)



  meta <-  NULL

  tictoc::tic()
  for(PID in PID_List){
    message(PID)
    extract_vector_metadata(get_cansim_cube_metadata(PID))
  }

  message("runtime: ")
  tictoc::toc()

  invisible()
}


#' Initializes the meta querying system
#'
#' read all the nessary metadata present in the provided path for the querying system
#'
#' The api method takes a list of CANSIM PIDs are extra argument
#'
#' The CSV method take a string representing the path where the metadata CSVs are stored
#'
#' @param method String (Values = c("API", "CSV")
#' @keywords internal directory metadata cansim set
#' @export set_meta_directory
#' @return NONE (side effect function)

set_meta_directory <- function(method, ...){
  if(method=="API"){
    stopifnot(is.vector(...) == TRUE)
    set_meta_directory_api(...)
  } else if (method=="CSV"){
    stopifnot(length(...)==1)
    set_meta_directory_csv(...)
  } else {
    warning("Unknown method")
    stop()
  }


}





#' Initializes the meta querying system
#'
#' read all the nessary metadata present in the provided path for the querying system
#'
#' @param NONE side effect
#' @keywords directory metadata cansim set
#' @export ls_environment
#' @return list
ls_environment <- function(){
  ls(my_env)
}

#' returns list of dimension in a given table
#'
#'
#' @param PID String or list of strings
#' @keywords metadata cansim dimension
#' @export list_meta
#' @return list
#' @examples
#' list_meta("14100370")
#' list_meta(c("14100370","14100367","14100366"))


list_meta <- function(PID){
  l <- lapply(PID, function(p){get(paste0(p,"_dim"), envir = my_env) %>% pull(Dimension.name)})
  names(l) <- PID
  l
}


#' returns list of  common dimension for a given list of PIDs
#'
#'
#' @param l list of PIDs
#' @param init initial dimenstions to compare with dimensions from PIDs provided (default = NULL)
#' @keywords metadata cansim dimension
#' @export get_common_dim
#' @return list
#' @examples
#' get_common(c("14100370","14100367","14100366"))


get_common_dim <- function(l, init =NULL){
  if(is.null(init)){
    reduce(list_meta(l),intersect)
  } else {
    reduce(list_meta(l),intersect, .init = init)
  }
}


#' returns list of PIDS containing specific fields
#'
#'
#' @param PID_list list of PIDs
#' @param dimensons list of Dimensions
#' @keywords dimension PID
#' @export get_PID_with_dimensions
#' @return list of PIDs
#' @examples
#' test_PID <- c("14100370","14100367","14100366")
#' get_PID_with_fields(test_PID,c("Age group","Geography"))

get_PID_with_dimensions <- function(PID_list, dimensions){
  ll <- sapply(PID_list, function(p){length(intersect(unlist(list_meta(p)),dimensions))==length(dimensions)})
  names(ll)[ll]
}


#' inspect the fields in PID that share the same dimension
#'
#'function returns the fields that are common and different as separate list
#'
#' @param list_dt string list of PIDs
#' @param dim string list of common dimensions across PIDs that we want inspected
#' @keywords dimension PID field inspect
#' @export inspect_fields
#' @return lists of list
#' @examples
#' list_dt <- c("14100370","14100367")
#' common_dim <- get_common_dim(list_dt)
#' inspect_fields(list_dt,common_dim)


inspect_fields <- function(list_PID,dim){
  lll<- lapply(dim,function(c){
    field.ids <- lapply(list_PID, function(l){get(paste0(l,"_dim"), envir = my_env)%>%filter(Dimension.name == c) %>% pull(Dimension.ID)})
    map2(list_PID,field.ids,  function(dt =.x, field.id =.y){get(paste0(dt,"_var"), envir = my_env)%>%filter(Dimension.ID == field.id) %>% pull(Member.Name)})
  })

  common <- lapply(lll,function(lx) {reduce(lx,intersect)})

  diff <- lapply(lll,function(lx) {reduce(lx,setdiff)})

  l_ret <-  list(dim,common, diff)
  names(l_ret) <- c("dimension","Common.Values","different.values")
  l_ret
}

#' guess the highest field in a dimension
#'
#' **this is a guess and could be incorrect**. we usually get the user to validate the guess
#'
#' @param dt string list of PIDs
#' @param dim string list of common dimensions across PIDs that we want inspected
#' @keywords dimension PID field
#' @export get_highest_level_fields_in_dimension
#' @return lists of list

get_highest_level_fields_in_dimension <- function(dt, dimension){
  lapply(dimension, function(d){
    field.id <- get(paste0(dt,"_dim"), envir = my_env)%>%filter(Dimension.name == d) %>% pull(Dimension.ID)
    as.character(get(paste0(dt,"_var"), envir = my_env)%>%filter(Dimension.ID == field.id) %>% pull(Member.Name) %>% .[[1]])
  })
}


#' returns the fields present in a dimension for a given PID
#'
#'
#' @param PID string one pid
#' @param Dim string list of common dimensions across PIDs that we want inspected
#' @keywords dimension PID field
#' @export get_dim_fields
#' @return lists of list

get_dim_fields <- function(PID,Dim){
  field.id <- get(paste0(PID,"_dim"), envir = my_env)%>%filter(Dimension.name == Dim) %>% pull(Dimension.ID)
  as.character(get(paste0(PID,"_var"), envir = my_env)%>%filter(Dimension.ID == field.id) %>% pull(Member.Name))
}

#' returns the dimension for a given PID
#'
#'
#' @param PID string one pid
#' @param Dim string list of common dimensions across PIDs that we want inspected
#' @keywords dimension PID field
#' @export get_dim_fields
#' @return lists of list

get_dimension <- function(dt){
  dt <- get(paste0(dt,"_dim"),envir = my_env)
  l <- list(dt["Dimension.ID"], dt["Dimension.name"])
  names(l) <-  c("id", "name")
  l
}

#' returns a title for a given PIDs
#'#'
#' @param PID list of strings
#' @keywords PID title descrption
#' @export get_PID_description
#' @return data.table
get_PID_description <- function(PID){
  meta %>% filter(Product.Id %in% PID) %>% select(Product.Id, Cube.Title)
}

#' returns a title for a given PIDs
#'#'
#' @param l list of strings representing PIDs
#' @keywords PID descrption different
#' @export get_different_dim
#' @return lsit
get_different_dim <- function(l){
  common <- get_common_dim(l)
  l_ret <- lapply(l, function(ll){
    setdiff(as.character(unlist(list_meta(ll))),common)
  })
  names(l_ret) <- l
  l_ret
}


#' load actual data in memory
#'
#' @param PID list of strings representing PIDs
#' @param path string where csv files are stored
#' @keywords PID load
#' @export load_table
#' @return data.table

load_table <- function(PID, path){
  for(p in PID){
    message(paste0("Attempting to load: ",p,".csv"))
    assign(paste0("tbl_", p),fread(paste0(path,p,".csv"),stringsAsFactors=TRUE),envir = .GlobalEnv)}
}


#' clean up columns names
#'
#' @param l list of strings representing column names
#' @keywords PID load
#' @export apply_alias
#' @return list

apply_alias <-  function(l){
  which(l=="Geography")
  l[which(l=="Geography")]  <- "GEO"
  toupper(l)
}

#' drop up columns
#'
#' helper function
#'
#' @param dt data.frame
#' @keywords PID load
#' @export drop_columns_dt
#' @return data.frame
drop_columns_dt <-  function(dt){
  del.list <- c("DGUID", "VECTOR", "COORDINATE", "STATUS","SYMBOL","TERMINATED")
  dt %>% select(-one_of(del.list))
}

#' wrapper function for drop_column_dt and apply_alias
#'
#' helper function
#'
#' @param PID string
#' @keywords PID load
#' @export cleanup_column_names_tbl
#' @return NONE side effect
cleanup_column_names_tbl <- function(PID){
  for(p in PID){


    # This might not work if triggered from within the function . check environment()
    expr1 <- paste0("names(", paste0("tbl_",p), ") <- apply_alias(names(", paste0("tbl_",p),"))")
    eval(rlang::parse_expr(expr1), envir =globalenv())

    nm <-  paste0("tbl_",p)
    x <- ensym(nm)
    expr2 <- expr(!!x <- drop_columns_dt(!!x))
    eval(expr2, envir = globalenv())

  }

  invisible()
}


#' wrapper function for drop_column_dt and apply_alias
#'
#' helper function
#'
#' @param PID string
#' @keywords PID load
#' @export get_available_field_combinations
#' @return data.frame
get_available_field_combinations <-  function(PID, fields){
  get(paste0("tbl_", PID)) %>%
    select(apply_alias(unlist(fields, use.names = FALSE))) %>%
    distinct() %>% map_df(sort, decreasing = FALSE) %>%
    as.data.frame()
}


#' get object from rCANSIM internal environment
#'
#' helper function
#'
#' @param object_name string
#' @keywords internal environment
#' @export get_object_rCANSIM
#' @return object

get_object_rCANSIM <- function(object_name){
  return(get(object_name, envir = my_env))
}

#' return meta table filtered for monthly freq
#'
#' helper function
#'
#' @param None Use internal enviroment
#' @keywords meta
#' @export monthly
#' @return data.table
monthly <- function(){
  get("meta", envir = my_env) %>% filter(Frequency == "Monthly")
}


#' return meta table filtered for quarterly freq
#'
#' helper function
#'
#' @param None Use internal enviroment
#' @keywords meta
#' @export quarterly
#' @return data.table
quarterly <- function(){
  get("meta", envir = my_env) %>% filter(Frequency == "Quarterly")
}



#' return meta table filtered for yearly freq
#'
#' helper function
#'
#' @param None Use internal enviroment
#' @keywords meta
#' @export yearly
#' @return data.table
yearly <- function(){
  get("meta", envir = my_env) %>% filter(Frequency == "Yearly")
}



#' return PIDs from meta table
#'
#' helper function
#'
#' @param dt data.table
#' @keywords PID
#' @export get_PID
#' @return list
get_PID <- function(dt){
  dt %>% pull(Product.Id)
}
