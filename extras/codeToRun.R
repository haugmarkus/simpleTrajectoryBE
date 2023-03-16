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

# Database conf
dbms = "sqlite"
schema = "main"

# Creating connection to database

conn <- createConnectionSQLite()

# Write datatable into database

data = readr::read_csv("TestSchemaTrajectories.csv")

createTrajectoriesTable(conn = conn,dbms = dbms ,schema = schema, data = data)

################################################################################
#
# Trajectory inclusion statistics tables
#
################################################################################

initialTable <- simpleTrajectoryBE::getDistinctTrajectoriesTable(connection = conn,dbms = dbms,schema = schema)
matchTable <- outputTrajectoryStatisticsTables(dataTable = initialTable, settings = trajSettings)

# Anda andmebaasile ette matching?

################################################################################
#
# Output all trajectories defined in inputUI.csv
#
################################################################################
matchTable$matching
result = outputAll(connection = conn, dbms = dbms, schema = schema, settings = trajSettings)
result = importTrajectoryData(connection = conn, dbms = dbms, schema = schema, trajectories = matchTable$matching)
nrow(result)


################################################################################
#
# Testing
#
################################################################################


# Whole data proccessing process 16.03.2023
# Test data
data = readr::read_csv("TestSchemaTrajectories.csv")
testDBQueries <- data.frame(matrix(ncol = 4, nrow = 0))
n <- 10 # nr of runs

for ( i in (1:n)) {
  subjects <- length(unique(data$SUBJECT_ID))
  rows <- nrow(data)
  run <- system.time({
  createTrajectoriesTable(conn = conn,dbms = dbms ,schema = schema, data = data)
  initialTable <- simpleTrajectoryBE::getDistinctTrajectoriesTable(connection = conn,dbms = dbms,schema = schema)
  matchTable <- outputTrajectoryStatisticsTables(dataTable = initialTable, settings = trajSettings)
  importTrajectoryData(connection = conn, dbms = dbms, schema = schema, trajectories = matchTable$matching)
  })
  row <- t(as.data.frame(c(data.matrix(run)[1:3], subjects, rows)))
  testDBQueries <- rbind(testDBQueries, row)
}
colnames(testDBQueries) <- c("user", "system", "elapsed", "subjects", "rows")
rownames(testDBQueries) = NULL
mean(testDBQueries$elapsed) #1.3


