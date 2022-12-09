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

data = readr::read_csv("./HeartFailuregeneratedTrajectoriesDiscrete.csv")


createTrajectoriesTable(conn = conn, schema = schema, data = data)

################################################################################
#
# Using function exactTrajectories
#
# params:
# connection - connection to the database
# dbms - database management system (sqlite, psql, etc ...)
# ivector - a vector representing the order/indeces of a trajectory
# svector - a vector of state labels corresponding to ivector
#
# Example below
#
################################################################################


# ivector + svector means that at index 2 patient has state "HF1", at index 3 patient has state "HF1", ...



# For trajectory with index == 1, all the patients and their data fulfilling the criteria
result = exactTrajectories(
  connection = conn,
  dbms = dbms,
  schema = schema,
  ivector = trajSettings[[1]]$INDEX,
  svector = trajSettings[[1]]$STATE
)

head(result)


################################################################################
#
# Using function looseTrajectories
#
# params:
# connection - connection to the database
# dbms - database management system (sqlite, psql, etc ...)
# svector - a vector of state labels corresponding to ivector
#
# Example below
#
################################################################################

# For trajectory with index == 1, all the patients and their data fulfilling the criterias
result = looseTrajectories(
  connection = conn,
  dbms = dbms,
  schema = schema,
  svector = trajSettings[[2]]$STATE
)

head(result)

################################################################################
#
# Output all trajectories defined in inputUI.csv
#
#
#
################################################################################

outputAll(connection = conn, dbms = dbms, schema = schema, settings = trajSettings)
