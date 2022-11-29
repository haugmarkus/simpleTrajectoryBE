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
}


################################################################################
#
# Load UI settings
#
################################################################################
#' @export
loadUITrajectories = function(pathToFile = NULL, settings = NA){
  if(!is.null(pathToFile)){
  settings <- readr::read_csv(pathToFile)
  }
  nrTrajectories <- length(unique(settings$TRAJECTORY_ID))
  trajList = list()

  for (i in 1:nrTrajectories) {
    trajData <- dplyr::filter(settings, TRAJECTORY_ID == i)
    trajData <- dplyr::arrange(trajData, INDEX)
    firstState <- dplyr::mutate(dplyr::filter(trajData, INDEX == 1), INDEX = 0)
    colnames(firstState) = c("","TO","FROM","TRAJECTORY_ID","INDEX")
    trajData <- rbind(firstState[,c(2,5)],trajData[c(3,5)])
    trajData$INDEX = trajData$INDEX+1
    colnames(trajData) <- c("STATE", "INDEX")
    trajList[[i]] <-  trajData
  }
  return(trajList)
}





