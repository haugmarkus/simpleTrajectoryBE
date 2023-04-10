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

  # Change labelnames if needed
  lookup <- c(STATE_LABEL = "STATE", GROUP_LABEL = "GROUP")
  data = dplyr::rename(data, dplyr::any_of(lookup))

  # Add missing columns if needed
  # Add column AGE if needed
  if (!("AGE" %in% colnames(data))) {
    data$AGE = 0
  }
  # Add column GENDER if needed
  if (!("GENDER" %in% colnames(data))) {
    data$GENDER = "OTHER"
  }
  # Add column GROUP_LABEL if needed
  if (!("GROUP_LABEL" %in% colnames(data))) {
    data$GROUP_LABEL = "NA"
  }
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

  sql_drop_rendered <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql_drop,
    schema = schema,
    table = 'patient_trajectories'
  )
  DatabaseConnector::executeSql(connection = conn, sql_drop_rendered)

  sql_drop_rendered <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql_drop,
    schema = schema,
    table = 'patient_trajectories_temp'
  )
  DatabaseConnector::executeSql(connection = conn, sql_drop_rendered)

  sql_drop_rendered <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql_drop,
    schema = schema,
    table = 'patient_trajectories_combined'
  )
  DatabaseConnector::executeSql(connection = conn, sql_drop_rendered)

  sql_drop_rendered <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql_drop,
    schema = schema,
    table = 'patient_trajectories_edges'
  )
  DatabaseConnector::executeSql(connection = conn, sql_drop_rendered)

  ##############################################################################
  #
  # CREATE tables
  #
  ##############################################################################

  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "patient_trajectories",
                                 databaseSchema = schema,
                                 data = data)

  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "patient_trajectories_temp",
                                 databaseSchema = schema,
                                 data = data)
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

  ##############################################################################
  #
  # Create table containing information for transitions and edges
  #
  ##############################################################################
  table = "patient_trajectories_edges"
  if(dbms == "postgresql") {
    sql <- paste("CREATE TABLE ",schema,".",table," AS WITH sorted_states AS (SELECT subject_id, state_label, state_start_date::timestamp, state_end_date::timestamp, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM  ",schema,".patient_trajectories), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG(EXTRACT(EPOCH FROM (s2.state_start_date - s1.state_start_date)/86400)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
  } else if (dbms == "sqlite") {
    sql <- paste("CREATE TABLE ",schema,".",table," AS WITH sorted_states AS (SELECT subject_id, state_label, state_start_date as state_start_date, state_end_date as state_end_date, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM ",schema,".patient_trajectories), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG((s2.state_start_date -s1.state_start_date)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(CAST(time_elapsed AS numeric)), 2)/86400 AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
    } else {return(print("Your DBMS is not supported, please contact package maintainer for an update!"))}
  # sql <- loadRenderTranslateSql(
  #   dbms = dbms
  #   sql = sql,
  #   schema = schema,
  #   table = 'patient_trajectories_edges'
  # )

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




