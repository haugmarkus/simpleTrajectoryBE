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

#' #' Query patients from the database according to the exact trajectories specified in GUI
#' #'
#' #' @param connection Connection to the database (DatabaseConnector)
#' #' @param dbms The database management system
#' #' @param schema Schema in the database where the tables are located
#' #' @param svector The names of the states which form the observed trajectory
#' #' @param ivector The moments in time when the state occurs
#' #' @internal
#' exactTrajectories <- function(connection, dbms, schema, svector, ivector) {
#'   if (length(ivector) != length(svector)) {
#'     return(message("Vector length not equal!"))
#'   }
#'   else if (!all(sort(ivector) == 1:length(ivector))){
#'     return(c())
#'   }
#'   else {
#'
#'     sql = "select * from ("
#'     # Check for two same states in a row
#'     checker = 0
#'     for (index in 1:length(ivector)) {
#'       sql = paste(
#'         sql,
#'         "select distinct SUBJECT_ID
#'                from (SELECT SUBJECT_ID,
#'                             STATE_LABEL,
#'                             STATE_START_DATE,
#'                             ROW_NUMBER()
#'                             OVER (PARTITION BY SUBJECT_ID ORDER BY SUBJECT_ID, STATE_START_DATE) as s_index
#'                      FROM @schema.@table) i
#'                where s_index =",
#'         ivector[index],
#'         " and STATE_LABEL ='",
#'         svector[index],
#'         "'", sep = "")
#'
#'
#'         if (index != length(ivector)) {
#'           sql = paste(sql, "INTERSECT ")
#'         }
#'         if (index > 1 && svector[index] == svector[index-1]){
#'           checker = 1
#'         }
#'
#'     }
#'
#'     sql = paste(
#'       sql,
#'       ') as "SUBJECT_ID";'
#'     )
#'
#'     table = if (checker == 0) "exact_patient_trajectories" else "patient_trajectories"
#'
#'     sql = loadRenderTranslateSql(
#'       dbms = dbms,
#'       sql = sql,
#'       schema = schema,
#'       table = table
#'     )
#'
#'     eligiblePatients <- DatabaseConnector::querySql(connection,
#'                                                     sql = sql)
#'
#'     return(eligiblePatients)
#'   }
#' }

#' Query all trajectories defined in the matching table as matches
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param trajectories The matches in the matching table
#' @export
importTrajectoryData = function(connection, dbms, schema, trajectories) {
  # returnList = list()
  # if (nrow(trajectories) > 0) {
  #   for (i in 1:nrow(trajectories)) {
  #     trajectoryAtomic <- stringr::str_split(trajectories[i,]$TRAJECTORY, pattern = "->>")[[1]]
  #     returnList[[i]] = exactTrajectories(
  #       connection = connection,
  #       dbms = dbms,
  #       schema = schema,
  #       ivector = 1:length(trajectoryAtomic),
  #       svector = trajectoryAtomic
  #     )
  #   }
  # }
################################################################################
# Create a set with eligible patients
################################################################################
  # eligiblePatients <- unique(unlist(returnList))

################################################################################
# Querying the data
################################################################################
  if(dbms != 'sqlite') {
  sql = loadRenderTranslateSql(
    dbms = dbms,
    sql = "SELECT * FROM @schema.patient_trajectories WHERE subject_id = ANY(SELECT unnest(subject_ids) from @schema.patient_trajectories_combined where trajectory IN (@trajectories));",
    schema = schema,
    trajectories = paste0("'", paste(trajectories$TRAJECTORY, collapse = "','"), "'")
  )
  } else {
    sql = loadRenderTranslateSql(
      dbms = dbms,
      sql = "SELECT * FROM @schema.patient_trajectories WHERE subject_id IN (SELECT CAST(subject_id AS INTEGER) FROM (SELECT DISTINCT subject_ids FROM @schema.patient_trajectories_combined WHERE trajectory IN (@trajectories)  ) AS x WHERE ',' || x.subject_ids || ',' LIKE '%,' || CAST(patient_trajectories.subject_id AS TEXT) || ',%');",
      schema = schema,
      trajectories = paste0("'", paste(trajectories$TRAJECTORY, collapse = "','"), "'")
    )
  }

  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql)

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
  return(returnData)
  }


#' Remove all instances for patient which occur before selected state occurrence
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param selectedState Selected state
#' @export
removeBeforeDatasetDB <- function(connection, dbms,schema, selectedState) {
  sql <- "SELECT pt.subject_id as subject_id, state_label, state_start_date, state_end_date, age, gender, group_label FROM @schema.@table pt JOIN (SELECT subject_id, MIN(state_start_date) AS min_start_date FROM @schema.@table WHERE state_label = '@selectedState' GROUP BY subject_id) s ON pt.subject_id = s.subject_id WHERE pt.subject_id IN (SELECT DISTINCT subject_id FROM @schema.@table WHERE state_label = '@selectedState') AND pt.state_start_date > s.min_start_date OR (pt.state_start_date = s.min_start_date AND state_label = '@selectedState') ORDER BY pt.subject_id, pt.state_start_date, pt.state_end_date;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories',
    selectedState = selectedState
  )
  returnData <- DatabaseConnector::querySql(connection = connection, sql)

  colnames(returnData) <- c("SUBJECT_ID", "STATE_LABEL", "STATE_START_DATE", "STATE_END_DATE")

  return(returnData)
}
