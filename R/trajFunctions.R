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

#' Query patients from the database according to the exact trajectories specified in GUI
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param svector The names of the states which form the observed trajectory
#' @param ivector The moments in time when the state occurs
#' @export
exactTrajectories <- function(connection, dbms, schema, svector, ivector) {
  if (length(ivector) != length(svector)) {
    return(message("Vector length not equal!"))
  }
  else {

    sql = "select * from ("
    # Check for two same states in a row
    checker = 0
    for (index in 1:length(ivector)) {
      sql = paste(
        sql,
        "select distinct SUBJECT_ID
               from (SELECT SUBJECT_ID,
                            STATE,
                            STATE_START_DATE,
                            ROW_NUMBER()
                            OVER (PARTITION BY SUBJECT_ID ORDER BY SUBJECT_ID, STATE_START_DATE) as s_index
                     FROM @schema.@table) i
               where s_index =",
        ivector[index],
        " and STATE ='",
        svector[index],
        "'", sep = "")


        if (index != length(ivector)) {
          sql = paste(sql, "INTERSECT ")
        }
        if (index > 1 && svector[index] == svector[index-1]){
          checker = 1
        }

    }

    sql = paste(
      sql,
      ") as 'SUBJECT_ID';"
    )

    table = if (checker == 0) "exact_patient_trajectories" else "patient_trajectories"

    sql = loadRenderTranslateSql(
      dbms = dbms,
      sql = sql,
      schema = schema,
      table = table
    )

    eligiblePatients <- DatabaseConnector::querySql(connection,
                                                    sql = sql)

    # sql2 = loadRenderTranslateSql(
    #   dbms = dbms,
    #   sql = "SELECT * FROM @schema.patient_trajectories WHERE SUBJECT_ID IN (@eligiblePatients);",
    #   schema = schema,
    #   eligiblePatients = eligiblePatients$SUBJECT_ID
    # )
    #
    # returnData <- DatabaseConnector::querySql(connection,
    #                                     sql = sql2)

    return(eligiblePatients)
  }
}

#' Query patients from the database according to the loose trajectories specified in GUI
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param svector The names of the states which form the observed trajectory
#' @export
looseTrajectories <- function(connection, dbms, schema, svector) {
  if (length(svector) < 1) {
    return(message("Vector length smaller than 1!"))
  }
  if (length(svector) == 1) {

      sql = loadRenderTranslateSql(
        dbms = dbms,
        sql = paste("SELECT subject_id FROM @schema.patient_trajectories WHERE STATE = '",svector[1], "';", sep = ""),
        schema = schema
      )
      eligiblePatients <- DatabaseConnector::querySql(connection,
                                                      sql = sql)
#       sql2 = loadRenderTranslateSql(
#         dbms = dbms,
#         sql = "SELECT * FROM @schema.patient_trajectories WHERE SUBJECT_ID IN (@eligiblePatients);",
#         schema = schema,
#         eligiblePatients = eligiblePatients$SUBJECT_ID
#       )
#       returnData <- DatabaseConnector::querySql(connection,
#                                                 sql = sql2)
      return(eligiblePatients)
    }
  else {
    tempTableLabels = paste("SimpleBE_", 1:length(svector), "_state", sep = "")

    ############################################################################
    #
    # Let's delete the temp table if it already exists
    #
    ############################################################################

    sql = paste("DROP TABLE IF EXISTS ", tempTableLabels[1], ";", sep = "")
    DatabaseConnector::executeSql(connection,
                                  sql = sql)

    ############################################################################
    #
    # Creating the first temp table which will gather all patients
    # fulfilling the first state criteria
    #
    ############################################################################

    sql = paste(
      "CREATE TEMP TABLE ",
      tempTableLabels[1],
      " AS
                SELECT subject_id, state, MIN(s_index) as s_index FROM (
                  SELECT
                  subject_id, state,
                  ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY subject_id, state_start_date) as s_index
                  FROM @schema.patient_trajectories) a WHERE state = '",
      svector[1],
      "' GROUP BY subject_id, state;",
      sep = ""
    )

    sql = loadRenderTranslateSql(dbms = dbms,
                                 sql = sql,
                                 schema = schema)

    DatabaseConnector::executeSql(connection,
                                  sql = sql)


    for (index in 2:length(svector)) {
      ############################################################################
      #
      # Let's delete the temp table if it already exists
      #
      ############################################################################

      sql = paste("DROP TABLE IF EXISTS ", tempTableLabels[index], ";", sep = "")
      DatabaseConnector::executeSql(connection,
                                    sql = sql)

      ############################################################################
      #
      # Creating the n'th temp table which will gather all patients
      # fulfilling the n'th state criteria
      #
      ############################################################################

      sql = paste(
        "CREATE TEMP TABLE ",
        tempTableLabels[index],
        " AS
SELECT forth.subject_id as subject_id, forth.state as state, MIN(forth.s_index) as s_index from (
SELECT subject_id, state, s_index FROM (
                                                           SELECT
                                                               subject_id, state,
                                                               ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY subject_id, state_start_date) as s_index
                                                           FROM @schema.patient_trajectories) a WHERE state = '",
        svector[index],
        "') forth
INNER JOIN ",
        tempTableLabels[index - 1],
        " fro ON fro.subject_id = forth.subject_id WHERE fro.s_index < forth.s_index GROUP BY forth.subject_id, forth.state;",
        sep = ""
      )

      sql = loadRenderTranslateSql(dbms = dbms,
                                   sql = sql,
                                   schema = schema)
      DatabaseConnector::executeSql(connection,
                                    sql = sql)
    }


    eligiblePatients <- DatabaseConnector::querySql(connection,
                                                    sql = paste("SELECT subject_id FROM ", tempTableLabels[length(tempTableLabels)], ";"))

    # sql2 = loadRenderTranslateSql(
    #   dbms = dbms,
    #   sql = "SELECT * FROM @schema.patient_trajectories WHERE SUBJECT_ID IN (@eligiblePatients);",
    #   schema = schema,
    #   eligiblePatients = eligiblePatients$SUBJECT_ID
    # )
    #
    # returnData <- DatabaseConnector::querySql(connection,
    #                                           sql = sql2)

    return(eligiblePatients)
  }
}



#' Query all trajectories defined in the settings
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param settings The settings of generating trajectories
#' @export
outputAll = function(connection, dbms, schema, settings) {
  returnList = list()
  outIndex = 0
  for (traj in 1:length(settings)) {
    outIndex = outIndex + 1
    if(is.null(settings[[traj]])) {
      outIndex = outIndex - 1
      next
    }
    if (settings[[traj]]$TYPE[1] == 0) {
      # 0 means loose trajectory
      returnList[[outIndex]] = looseTrajectories(
        connection = connection,
        dbms = dbms,
        schema = schema,
        svector = settings[[traj]]$STATE
      )
    }
    if (settings[[traj]]$TYPE[1] == 1) {
      # 1 means exact trajectory
      returnList[[outIndex]] = exactTrajectories(
        connection = connection,
        dbms = dbms,
        schema = schema,
        ivector = settings[[traj]]$INDEX,
        svector = settings[[traj]]$STATE
      )
    }
  }
  ######################
  # Create set with eligible patients
  eligiblePatients <- unique(unlist(returnList))

  ######################
  # Query the data

  sql2 = loadRenderTranslateSql(
    dbms = dbms,
    sql = "SELECT * FROM @schema.patient_trajectories WHERE SUBJECT_ID IN (@eligiblePatients);",
    schema = schema,
    eligiblePatients = eligiblePatients
  )
  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql2)

  return(returnData)
}


