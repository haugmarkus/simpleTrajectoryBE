library(testthat)

pathToDriver <- './Drivers'
dbms <- "sqlite"
schema <- "main"
pathToResults <<- dirname(dirname(getwd())) #
data = readr::read_csv(paste(pathToResults,"/TestSchemaTrajectories.csv", sep =""))
connection <- createConnectionSQLite()

test_that("Quering exact trajectories from DB", {
  connection <- createConnectionSQLite()
  createTrajectoriesTable(conn = connection, data = data, schema = schema)
  pathToFile = "/inputUI.csv"
  trajSettings = loadUITrajectories((paste(pathToResults,pathToFile, sep ="")))
  result = exactTrajectories(
    connection = connection,
    dbms = dbms,
    schema = schema,
    ivector = trajSettings[[1]]$INDEX,
    svector = trajSettings[[1]]$STATE
  )
  DatabaseConnector::disconnect(connection)
  expect_equal(length(unique(result$SUBJECT_ID)), 2000)
})
#> Test passed 🥇
