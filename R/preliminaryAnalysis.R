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
getDistinctTrajectoriesTable <- function(connection, dbms, schema, table = "patient_trajectories") {
sql <- "SELECT TRAJECTORY, COUNT(*) AS TOTAL FROM (SELECT SUBJECT_ID, GROUP_CONCAT(STATE,'-->') TRAJECTORY FROM (SELECT SUBJECT_ID, STATE, STATE_START_DATE FROM @schema.@table ORDER BY SUBJECT_ID, STATE_START_DATE) tmp1 GROUP BY SUBJECT_ID) tmp2 GROUP BY TRAJECTORY ORDER BY TOTAL DESC;"

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
  trajStates <- paste0(table$STATE, collapse = "-->")
  return(trajStates)
  }))
  indexes <- 1:nrow(dataTable)

  matchingVec <- as.logical(Reduce("+",lapply(trajDefined, function(x){
    x == dataTable$TRAJECTORY
  })))

  partiallyMatchingVec <- as.logical(Reduce("+", lapply(trajDefined,function(x){
    grepl(x, dataTable$TRAJECTORY)
  })))

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
