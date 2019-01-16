#' interactive merging of cansim tables
#'
#' interactive merging of cansim tables ( more to be added later)
#'
#' @param dt String representing PID
#' @param dt2 String representing PID
#' @param VERBOSE Boolean to trigger the extra display of information for the merge
#' @keywords merge cansim
#' @export smart_merge_pairwise
#' @import crayon
#' @import rlist
#' @return data.table


smart_merge_pairwise <-function(dt,dt2, VERBOSE = TRUE){
  library(crayon)
  library(rlist)



  list.dt <-c(dt,dt2)
  Common.dim <- get_common_dim(list.dt)
  Diff.dim <- get_different_dim(list.dt)

  #inspect fields
  i.fields <- inspect_fields(list.dt, Common.dim)

  #print stuff to screen
  if(VERBOSE){
    cat(bold(blue("Common Dimensions: \n")))
    print(Common.dim)
    cat("\n")

    cat(bold(blue("Differing Dimensions (wrt common dimensions): \n")))
    print(Diff.dim)
    cat("\n")


    cat(bold(blue("Inspecting common dimensions: \n")))
    print(i.fields)
  }



  #confirm that correct dimensions are being used.

  for(i in 1:length(Diff.dim)){

    .guess_list <-  list()

    cat(green(bold(paste0("For table: ", names(Diff.dim[i]), "\n"))))

    .available_combination_dim_fields <- get_available_field_combinations(names(Diff.dim[i]),Diff.dim[i])

    .guess_list <- lapply(unlist(Diff.dim[i],use.names = FALSE), function(f) {get_highest_level_fields_in_dimension(names(Diff.dim[i]),f)})
    .guess_list_df <- as.data.frame(.guess_list)
    names(.guess_list_df) <- unlist(Diff.dim[i],use.names = FALSE)
    # message("GUESS")
    # print(.guess_list_df)

    is.guess.valid <-function(guess, valid){
      any(sapply(apply(valid,1, list), function(ll){
        all(unlist(ll,use.names= FALSE) %in% guess)}))
    }

    valid.guess <- is.guess.valid(unlist(.guess_list, use.names = FALSE), .available_combination_dim_fields)
    # message(valid.guess)


    if(valid.guess){
      for(f.list in unlist(Diff.dim[i],use.names = FALSE)){
        for(f in f.list){
          cat(blue(bold(paste0("\t", f,"\n"))))}
        cat(italic(green("\t\t Guessed highest level: ")))
        cat(blue(get_highest_level_fields_in_dimension(names(Diff.dim[i]),f)))
        cat("\n")
      }
    } else {
      cat(blue(bold(paste0("\t", f,"\n"))))
      cat(italic(red("\t\t Could not guess highest level\n")))
    }

    cat(yellow(bold("\t\t Available fields:\n")))
    print(.available_combination_dim_fields)
    cat("\n")


    change_fields_chosen <- function(){

      #lazy scoping
      reply <- readline("Enter the value of the fields you wish to use. Type `all` to select all fields\t")
      tbl <- .available_combination_dim_fields
      l <- unlist(strsplit(reply, ","))

      if(length(l)==1){

        input <- l

        if(input == "Q"){
          print("Q")
          return(.guess_list_df)
        } else if(toupper(l[[1]])=="ALL"){
          print("ALL")
          return(tbl)
        } else {
          return(tbl[as.integer(input),,drop=FALSE])
        }

      } else {
        rows <- as.integer(l)
        return(tbl[rows,])
      }


    }



    if(valid.guess){
      menu.ans <- menu(c("no","yes"),title = "Do you want to use a different field?" )

      if(menu.ans %in% c(0,1)){
        .filter_df <- .guess_list_df
      }

      if(menu.ans == 2){
        .filter_df <- change_fields_chosen()
      }

    } else {
      .filter_df <- change_fields_chosen()
    }


    names(.filter_df) <- apply_alias(names(.filter_df))
    assign(paste0(".tmp_tbl_",i),inner_join(get(paste0("tbl_",names(Diff.dim[i]))),.filter_df))

  }

  return_tbl <- inner_join(.tmp_tbl_1, .tmp_tbl_2)
  rm(.tmp_tbl_1)
  rm(.tmp_tbl_2)

  return(return_tbl)

}

