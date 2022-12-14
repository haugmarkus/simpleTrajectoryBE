################################################################################
#
# Create database, sqlite
#
################################################################################

# install.packages(c("devtools","dplyr", "RSQLite"))
# devtools::install_github("OHDSI/DatabaseConnector")
# devtools::install_github("OHDSI/SqlRender")

#' @export
createConnectionSQLite = function(){
  return(DatabaseConnector::connect(dbms = "sqlite", server = tempfile()))
}

################################################################################
#
# Create table 'patient_trajectories' into the database
#
################################################################################
#' @export
createTrajectoriesTable = function(conn, data, schema){
  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "patient_trajectories",
                                 databaseSchema = schema,
                                 data = data)
  data$TO_STATE = c(data$STATE[2:nrow(data)], "$$not_initialized$$")
  data = dplyr::select(subset(data, STATE != TO_STATE), -TO_STATE)
  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "exact_patient_trajectories",
                                 databaseSchema = schema,
                                 data = data)

}


################################################################################
#
# Load UI settings
#
################################################################################
#' @param pathToFile The path to the settings file
#' @export
loadUITrajectories = function(pathToFile = NULL,
                              settings = NA) {
  if (!is.null(pathToFile)) {
    settings <- readr::read_csv(pathToFile)
  }
  # nrTrajectories <- length(unique(settings$TRAJECTORY_ID))
  trajList = list()

  for (i in unique(settings$TRAJECTORY_ID)) {
    trajData <- dplyr::filter(settings, TRAJECTORY_ID == i)
    trajData <- dplyr::arrange(trajData, TIME)
    trajData <- dplyr::select(trajData, STATE, TIME, TYPE)
    colnames(trajData) <- c("STATE", "INDEX", "TYPE")
    trajList[[i]] <-  trajData
  }
  return(trajList)
}




