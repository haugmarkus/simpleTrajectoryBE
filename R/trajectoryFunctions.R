#' Load and translate SQL file or an explicit SQL query to desired dialect.
#'
#' @param sql SQL file name or SQL query
#' @param warnOnMissingParameters Should a warning be raised when parameters provided to this function do not appear in the parameterized SQL that is being rendered? By default, this is TRUE.
#' @param output Should there be a .sql file created of the result
#' @param outputFile Name of the output file
#' @keywords internal
loadRenderTranslateSql <- function(sql,
                                   dbms = "postgresql",
                                   warnOnMissingParameters = TRUE,
                                   output = FALSE,
                                   outputFile,
                                   ...) {
  if (grepl('.sql', sql)) {
    pathToSql <- paste("inst/SQL/", sql, sep = "")
    parameterizedSql <-
      readChar(pathToSql, file.info(pathToSql)$size)[1]
  }
  else {
    parameterizedSql <- sql
  }
 # FIX FOR
 # STRING_AGG(postgrsql) and GROUP_CONCAT(sqlite)
 # TODO add other distinctions
 if (dbms == "postgresql") {
   parameterizedSql = stringr::str_replace(parameterizedSql, "GROUP_CONCAT", "STRING_AGG")
 }
  else if (dbms == "sqlite") {
    parameterizedSql = stringr::str_replace(parameterizedSql, "STRING_AGG", "GROUP_CONCAT")
    parameterizedSql = stringr::str_replace(parameterizedSql, "ARRAY_AGG", "GROUP_CONCAT")
  }
  renderedSql <-
    SqlRender::render(sql = parameterizedSql, warnOnMissingParameters = warnOnMissingParameters, ...)
  renderedSql <-
    SqlRender::translate(sql = renderedSql, targetDialect = dbms)

  if (output == TRUE) {
    SqlRender::writeSql(renderedSql, outputFile)
    writeLines(paste("Created file '", outputFile, "'", sep = ""))
  }
  return(renderedSql)
}

#' Query all trajectories defined in the matching table as matches
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param trajectories The matches in the matching table
#' @export
importTrajectoryData = function(connection, dbms, schema, trajectories) {
################################################################################
# Create a set with eligible patients
################################################################################
  # eligiblePatients <- unique(unlist(returnList))

  # Drop temp table
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')
################################################################################
# Querying the data
################################################################################

  if(dbms != 'sqlite') {
  sql = loadRenderTranslateSql(
    dbms = dbms,
    sql = "SELECT * INTO @schema.patient_trajectories_temp FROM (SELECT * FROM @schema.patient_trajectories WHERE subject_id = ANY(SELECT unnest(subject_ids) from @schema.patient_trajectories_combined where trajectory IN (@trajectories)));",
    schema = schema,
    trajectories = paste0("'", paste(trajectories$TRAJECTORY, collapse = "','"), "'")
  )
  } else {
    sql = loadRenderTranslateSql(
      dbms = dbms,
      sql = "SELECT * INTO @schema.patient_trajectories_temp FROM(SELECT * FROM @schema.patient_trajectories WHERE subject_id IN (SELECT CAST(subject_id AS INTEGER) FROM (SELECT DISTINCT subject_ids FROM @schema.patient_trajectories_combined WHERE trajectory IN (@trajectories)  ) AS x WHERE ',' || x.subject_ids || ',' LIKE '%,' || CAST(patient_trajectories.subject_id AS TEXT) || ',%'));",
      schema = schema,
      trajectories = paste0("'", paste(trajectories$TRAJECTORY, collapse = "','"), "'")
    )
  }

  DatabaseConnector::executeSql(connection,
                                  sql = sql)

  sql <- "SELECT * FROM @schema.@table;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_temp'
  )
  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql)
  colnames(returnData) <- c("SUBJECT_ID", "STATE_LABEL", "STATE_START_DATE", "STATE_END_DATE", "AGE", "GENDER", "GROUP_LABEL")
  return(returnData)
}


#' Remove all instances for patient which occur before selected state occurrence
#' @param dataset Patient trajectory data
#' @param selectedState Selected state
#' @export
removeBeforeDataset <- function(dataset, selectedState) {
  if (!selectedState %in% unique(dataset$STATE_LABEL)) {
    return(dataset)
  }

  # Remove conditions before Discharge
  dataset$STATE_START_DATE <- as.Date(dataset$STATE_START_DATE)
  dataIndex <-
    dplyr::select(data.frame(dplyr::slice(
      dplyr::filter(dplyr::group_by(dataset, SUBJECT_ID), STATE_LABEL == selectedState),
      which.min(STATE_START_DATE)
    )), SUBJECT_ID, STATE_START_DATE)
  returnData <- dplyr::left_join(dataset, dataIndex, by = "SUBJECT_ID")

  # Removing states occurring before
  returnData <-
    dplyr::select(
      dplyr::filter(returnData, STATE_START_DATE.y < STATE_START_DATE.x | STATE_LABEL == selectedState),
      -STATE_START_DATE.y
    )

  colnames(returnData) <- c("SUBJECT_ID", "STATE_LABEL", "STATE_START_DATE", "STATE_END_DATE")

  return(returnData)
}

#' Remove all instances for patient which occur before selected state occurrence
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param selectedState Selected state
#' @export
removeBeforeDatasetDB <- function(connection, dbms, schema, selectedState) {
  # Drop table
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_temp')

  # If there is no such state it will return zero rows
  sql <- "SELECT * INTO @schema.patient_trajectories_temp_temp FROM (SELECT pt.subject_id as subject_id, state_label, state_start_date, state_end_date, age, gender, group_label FROM @schema.@table pt JOIN (SELECT subject_id, MIN(state_start_date) AS min_start_date FROM @schema.@table WHERE state_label = '@selectedState' GROUP BY subject_id) s ON pt.subject_id = s.subject_id WHERE pt.subject_id IN (SELECT DISTINCT subject_id FROM @schema.@table WHERE state_label = '@selectedState') AND pt.state_start_date > s.min_start_date OR (pt.state_start_date = s.min_start_date AND state_label = '@selectedState') ORDER BY pt.subject_id, pt.state_start_date, pt.state_end_date);"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_temp',
    selectedState = selectedState
  )
  DatabaseConnector::executeSql(connection = connection, sql)

  # Drop table
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')

  sql <- "ALTER TABLE @schema.@tableB RENAME TO @tableA;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    tableA = 'patient_trajectories_temp',
    tableB = 'patient_trajectories_temp_temp'
  )
  DatabaseConnector::executeSql(connection = connection, sql)

  # colnames(returnData) <- c("SUBJECT_ID", "STATE_LABEL", "STATE_START_DATE", "STATE_END_DATE", "AGE", "GENDER", "GROUP_LABEL")
  #
  # return(returnData)
}

#' Query all data about edges and mean elapsed time
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @export
getEdgesDataset <- function(connection, dbms,schema) {
  sql <- "SELECT * FROM @schema.@table;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_edges'
  )
  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql)
  colnames(returnData) <- c("FROM", "TO", "AVG_TIME_BETWEEN", "COUNT")
  return(returnData)
  }

#' Query data about edges from specified group
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param groupId Group id for patients
#' @export
queryEdgesDatasetGroup <- function(connection, dbms, schema, groupId = NULL) {
  if (is.null(groupId)) {
    if(dbms == "postgresql") {
      sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date::timestamp, state_end_date::timestamp, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM  ",schema,".patient_trajectories_temp), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG(EXTRACT(EPOCH FROM (s2.state_start_date - s1.state_start_date)/86400)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
    } else if (dbms == "sqlite") {
      sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date as state_start_date, state_end_date as state_end_date, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM ",schema,".patient_trajectories_temp), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG((s2.state_start_date -s1.state_start_date)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(CAST(time_elapsed AS numeric)), 2)/86400 AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
    } else {return(print("Your DBMS is not supported, please contact package maintainer for an update!"))}
  } else {
  if(dbms == "postgresql") {
    sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date::timestamp, state_end_date::timestamp, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM  ",schema,".patient_trajectories_temp WHERE group_label = '", groupId ,"'), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG(EXTRACT(EPOCH FROM (s2.state_start_date - s1.state_start_date)/86400)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
  } else if (dbms == "sqlite") {
    sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date as state_start_date, state_end_date as state_end_date, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM ",schema,".patient_trajectories_temp WHERE group_label = '", groupId ,"'), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG((s2.state_start_date -s1.state_start_date)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(CAST(time_elapsed AS numeric)), 2)/86400 AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
  } else {return(print("Your DBMS is not supported, please contact package maintainer for an update!"))}
}
  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql)
  colnames(returnData) <- c("FROM", "TO", "AVG_TIME_BETWEEN", "COUNT")
  return(returnData)
}
