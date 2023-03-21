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

#' @param conn Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param data Imported data
#' @param schema Name of the used schema
#' @export
createTrajectoriesTable = function(conn, dbms, data, schema){
  ##############################################################################
  #
  # Lets switch the name of "STATE" in colnames to "STATE_LABEL" to
  # avoid possible DBMS problems
  #
  ##############################################################################

  names(data)[names(data) == 'STATE'] <- 'STATE_LABEL'

# We create two tables:
# 1) patient_trajectories - the table is the same as in the input file
# 2) exact_patient_trajectories - the table has excluded rows where transitions are made to the same state
# This is done to optimize the queries


  ##############################################################################
  #
  # DROP tables
  #
  ##############################################################################
  sql_drop <- "DROP TABLE IF EXISTS @schema.@table;"

  sql_drop1 <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql_drop,
    schema = schema,
    table = 'patient_trajectories'
  )
  DatabaseConnector::executeSql(connection = conn, sql_drop1)

  sql_drop2 <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql_drop,
    schema = schema,
    table = 'patient_trajectories_combined'
  )
  DatabaseConnector::executeSql(connection = conn, sql_drop2)

  ##############################################################################
  #
  # CREATE tables
  #
  ##############################################################################

  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "patient_trajectories",
                                 databaseSchema = schema,
                                 data = data)

  # data$TO_STATE = c(data$STATE_LABEL[2:nrow(data)], "$$not_initialized$$")
  # data = dplyr::select(subset(data, STATE_LABEL != TO_STATE), -TO_STATE)

  # DatabaseConnector::insertTable(connection = conn,
  #                                tableName = "exact_patient_trajectories",
  #                                databaseSchema = schema,
  #                                data = data)
  ##############################################################################
  #
  # Create trajectory statistics tables
  #
  ##############################################################################

  sql <- "CREATE TABLE @schema.@table AS SELECT trajectory, ARRAY_AGG(subject_id) AS subject_ids, count(*) AS total FROM (SELECT SUBJECT_ID, GROUP_CONCAT(STATE_LABEL, '->>') TRAJECTORY FROM (SELECT SUBJECT_ID, STATE_LABEL, STATE_START_DATE FROM @schema.patient_trajectories ORDER BY SUBJECT_ID, STATE_START_DATE, STATE_END_DATE) tmp1 GROUP BY SUBJECT_ID) aggTrajectories GROUP BY trajectory;"

  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_combined'
  )
  DatabaseConnector::executeSql(connection = conn, sql)
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
    names(settings)[names(settings) == 'STATE'] <- 'STATE_LABEL'
  }
  # nrTrajectories <- length(unique(settings$TRAJECTORY_ID))
  trajList = list()

  for (i in unique(settings$TRAJECTORY_ID)) {
    trajData <- dplyr::filter(settings, TRAJECTORY_ID == i)
    trajData <- dplyr::arrange(trajData, TIME)
    trajData <- dplyr::select(trajData, STATE_LABEL, TIME, TYPE)
    colnames(trajData) <- c("STATE_LABEL", "INDEX", "TYPE")
    trajList[[i]] <-  trajData
  }
  return(trajList)
}




