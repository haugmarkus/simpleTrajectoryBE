################################################################################
#
# Example
#
################################################################################
devtools::install_github("haugmarkus/simpleTrajectoryBE")
# Loading UI output trajectory settings
library(simpleTrajectoryBE)
pathToFile = "./inputUI.csv"
trajSettings = loadUITrajectories(pathToFile)


################################################################################
#
# Create connection to database
#
################################################################################
dbExists <- F

connection <- NULL

if(dbExists) {
  ################################################################################
  #
  # Database credentials
  #
  ################################################################################
  pathToDriver <- './Drivers'
  dbms <- "postgresql" #TODO
  user <- 'mhaug' #TODO
  password <- "" #TODO
  server <- '172.17.64.158' #TODO
  database <- "coriva"
  port <- '5432' #TODO
  schema <- 'ohdsi_temp' #TODO

  connection <- createConnection(server, database, password, user, dbms, port, pathToDriver)
} else {
  dbms = "sqlite"
  schema = "main"
  # Creating connection to database
  connection <- createConnectionSQLite()
}

# Write datatable into database

data = readr::read_csv("TestSchemaTrajectories.csv")
#data = readr::read_csv("CovidTrajectoriesContinuous.csv")
data = dplyr::select(data, SUBJECT_ID, STATE, STATE_START_DATE, STATE_END_DATE)
#data = dplyr::filter(data, (STATE %in% c("START", "EXIT")))
createTrajectoriesTable(conn = connection,dbms = dbms ,schema = schema, data = data)

head(getEdgesDataset(connection, dbms,schema))

queryEdgesDatasetGroup(connection = connection, dbms = dbms,schema = schema, groupId = NULL)
queryNodesDatasetGroup(connection = connection, dbms = dbms,schema = schema, groupId = NULL)

filterBySettings(connection = connection, dbms = dbms, schema = schema, settings = trajSettings)
removeBeforeDatasetDB(connection = connection, dbms,schema, "State2")
################################################################################
#
# Trajectory inclusion statistics tables
#
################################################################################

initialTable <- simpleTrajectoryBE::getDistinctTrajectoriesTable(connection = connection,dbms = dbms,schema = schema)
matchTable <- outputTrajectoryStatisticsTables(dataTable = initialTable, settings = trajSettings)

################################################################################
#
# Output all trajectories defined in inputUI.csv
#
################################################################################
# result = outputAll(connection = conn, dbms = dbms, schema = schema, settings = trajSettings)
result = importTrajectoryData(connection = connection, dbms = dbms, schema = schema, trajectories = matchTable$matching)
nrow(result)


################################################################################
#
# Testing
#
################################################################################


# Test data
data = readr::read_csv("/home/mhaug/PCASynth.csv")
pathToFile = "./inputUITEST.csv"
trajSettings = loadUITrajectories(pathToFile)
#~/R-packages/Cohort2Trajectory/tmp/datasets/Target660patientDataPriority.csv
names(data)[names(data) == 'STATE'] <- 'STATE_LABEL'
testDBQueries <- data.frame(matrix(ncol = 5, nrow = 0))
n <- 5 # nr of runs
library(dplyr)
for ( i in (1:n)) {
  subjects <- length(unique(data$SUBJECT_ID))
  rows <- nrow(data)
  run <- system.time({
  createTrajectoriesTable(conn = connection,dbms = dbms ,schema = schema, data = data)
  initialTable <- simpleTrajectoryBE::getDistinctTrajectoriesTable(connection = connection,dbms = dbms,schema = schema)
  matchTable <- outputTrajectoryStatisticsTables(dataTable = initialTable, settings = trajSettings)
  importTrajectoryData(connection = connection, dbms = dbms, schema = schema, trajectories = matchTable$matching)
  })
  row <- t(as.data.frame(c(data.matrix(run)[1:3], subjects, rows, "queryPatients")))
  testDBQueries <- rbind(testDBQueries, row)
}
colnames(testDBQueries) <- c("user", "system", "elapsed", "subjects", "rows", "testType")
rownames(testDBQueries) = NULL
testDBQueries %>% filter(testType == 'queryPatients') %>% select(elapsed) %>% as.vector() %>% unlist() %>% as.numeric() %>% mean()

# Clicking on some edge and running before DPLYR

testRemovalQueries <- data.frame(matrix(ncol = 5, nrow = 0))
for ( i in (1:n)) {
  subjects <- length(unique(data$SUBJECT_ID))
  rows <- nrow(data)
  user.avg <- 0
  system.avg <- 0
  elapsed.avg <- 0
  for (state in unique(data$STATE_LABEL)) {
    run <- system.time({
     removeBeforeDataset(data, state)
    })
  user.avg <- user.avg + as.numeric(run[1])
  system.avg <- system.avg + as.numeric(run[2])
  elapsed.avg <- elapsed.avg + as.numeric(run[3])
  }
  row <- t(as.data.frame(c(user.avg/n,system.avg/n,elapsed.avg/n, subjects, rows, 'removeBefore')))
  testRemovalQueries <- rbind(testRemovalQueries, row)
}
colnames(testRemovalQueries) <- c("user", "system", "elapsed", "subjects", "rows", "testType")
rownames(testRemovalQueries) = NULL
mean(as.numeric(testRemovalQueries$elapsed))

dataTest <- rbind(testDBQueries,testRemovalQueries)
dataTest$dataset = 'pca'
dataTest$database = 'postgres'
result =  rbind(result, dataTest)
unique(result$database)


result %>% filter(testType == 'queryPatients') %>% select(elapsed) %>% as.vector() %>% unlist() %>% as.numeric() %>% mean()
result %>% filter(testType == 'removeBefore') %>% filter(dataset == 'pca') %>% select(elapsed) %>% as.vector() %>% unlist() %>% as.numeric() %>% mean()

trajectories = matchTable$matching
