################################################################################
#
# Summarization functions
#
################################################################################

#' Create a table summarizing the total count of all distinct trajectories
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Name of the used schema
#' @param table Name of the used table
#' @export
getDistinctTrajectoriesTable <- function(connection, dbms, schema, table = "patient_trajectories_combined") {
sql <- "SELECT trajectory, total FROM @schema.@table;"

sql <- loadRenderTranslateSql(
  dbms = dbms,
  sql = sql,
  schema = schema,
  table = table
)
resultTable <- DatabaseConnector::querySql(connection, sql)

amountTrajectories <- sum(resultTable$TOTAL)
resultTable$PERC <- paste(round(resultTable$TOTAL*100/amountTrajectories,2), "%",sep = "")

return(resultTable)
}

#' Query data tables (matching, partially matching, not matching) defined in the settings for providing inclusion statistics
#'
#' @param dataTable Result of getDistinctTrajectoriesTable function
#' @param settings The settings of generating trajectories
#' @export
outputTrajectoryStatisticsTables <- function(dataTable, settings = NULL) {
  if (is.null(settings)){
    result <- list(
      "matching" = dataTable[0,],
      "partiallyMatching" = dataTable[0,],
      "notMatching" = dataTable
    )
    return(result)
  }

  trajDefined <- unlist(lapply(settings, function(table){
  trajStates <- NULL
  if (table$TYPE[1] == 1) {
  trajStates <- c()
  for (trajectoryPresent in dataTable$TRAJECTORY) {
  trajectoryPresentAtomic <-  stringr::str_split(trajectoryPresent, pattern = "->>")[[1]]
  trajectorySelectedAtomic <- table$STATE_LABEL

  if(identical(trajectorySelectedAtomic,trajectoryPresentAtomic[table$INDEX])) {
    trajectoryCollapsed <- paste0(trajectoryPresentAtomic[1:max(table$INDEX)], collapse = "->>") #paste0(trajectorySelectedAtomic, collapse = "->>")
    trajStates <- c(trajStates, trajectoryCollapsed)
  }
  }
  }
  if (table$TYPE[1] == 0) {
    trajStates <- c() # Return vector with all relevant trajectories
    for (trajectoryPresent in dataTable$TRAJECTORY) { # We start from looping over all present trajectories
      trajectoryPresentAtomicOriginal <-  stringr::str_split(trajectoryPresent, pattern = "->>")[[1]] # Break present trajectory down to states
      trajectoryPresentAtomic <- trajectoryPresentAtomicOriginal
      i <- 0
      k <- 0 # for iterating the trajectoryPresent variable
      trajectorySelectedAtomic <- table$STATE_LABEL #stringr::str_split(trajDefined, pattern = "->>")[[1]]
      for (state in trajectorySelectedAtomic) { # Break selected trajectory down to states and loop over states
        if(state %in% trajectoryPresentAtomic){ # Check if state present in present trajectory
          index <- match(state,trajectoryPresentAtomic) # Find first occurrance index
          # if (is.na(index)){break}
          i <- i + 1
          k <- k + index
          # index == length(trajectoryPresentAtomic) |
          if (i == length(trajectorySelectedAtomic)) { # If we are observing the last element of present trajectory and the state of selected trajectory is also the last one -- return
            trajStates <- c(trajStates, paste0( trajectoryPresentAtomicOriginal[1:k], collapse = "->>") ) # Add trajectory to return vector
            break
          }
          else if (index == length(trajectoryPresentAtomic)) {break}
          trajectoryPresentAtomic <- trajectoryPresentAtomic[(match(state,trajectoryPresentAtomic)+1):length(trajectoryPresentAtomic)] # Lets keep the tail of present trajectory
          }
        else {break}
      }
    }
  }
  return(unique(trajStates))
  }))
  indexes <- 1:nrow(dataTable)
  matchingVec <- as.logical(Reduce("+",lapply(trajDefined, function(x){
    x == dataTable$TRAJECTORY
  })))
  partiallyMatchingVec <- as.logical(Reduce("+", lapply(trajDefined,function(x){
    grepl(x, dataTable$TRAJECTORY)
  })))
  if (length(matchingVec)==0) {
    matchingVec <- rep(FALSE, nrow(dataTable))
    partiallyMatchingVec <- rep(FALSE, nrow(dataTable))
  }
  result <- list(
    "matching" = dataTable[indexes[matchingVec],],
    "partiallyMatching" = dataTable[indexes[as.logical(partiallyMatchingVec - matchingVec)],],
    "notMatching" = dataTable[indexes[!partiallyMatchingVec],]
  )
  return(result)
  ##############################################################################
  #
  # Can be accessed like:
  # result[['matching']], result[['partiallyMatching']], result[['notMatching']]
  # OR
  # result$matching, result$partiallyMatching, result$notMatching
  #
  #############################################################################
}
