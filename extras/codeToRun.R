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

data = readr::read_csv("./TestSchemaTrajectories.csv")

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
length(unique(result$SUBJECT_ID))


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
  svector = trajSettings[[1]]$STATE
)

head(result)
length(unique(result$SUBJECT_ID))

################################################################################
#
# Output all trajectories defined in inputUI.csv
#
#
#
################################################################################
result = outputAll(connection = conn, dbms = dbms, schema = schema, settings = trajSettings)

################################################################################
#
# Simple business process map
#
################################################################################

library(bupaR)
library(dplyr)

# I trajektoori result

data2 = result[[1]]

# Enne seda võiks küsida, et kas kasutaja tahab eristada sama nimega seisundite esinemise järjekorda
# Ehk siis siin varustane tabeli vastava infoga, mitmes kord patsiendil seisundis x olla on
data2 = dplyr::ungroup(dplyr::mutate(dplyr::group_by(data2,SUBJECT_ID, STATE),ID_REC = row_number()))
data2$STATE = paste(data2$STATE,data2$ID_REC, sep = "_")
# Näiteks kui seda HFD data'ga teha, siis läheb graph liiga suureks ja ei arvuta koguni välja ...


# Formatting dates
data2$STATE_START_DATE = bupaR::ymd_hms(paste(data2$STATE_START_DATE, "00:00:00"),tz=Sys.timezone())
data2$STATE_END_DATE = bupaR::ymd_hms(paste(data2$STATE_END_DATE, "00:00:00"),tz=Sys.timezone())
# Creating bupaR activitylog
eventlog = bupaR::simple_eventlog(eventlog = data2,
                                  case_id = "SUBJECT_ID",
                                  activity_id = "STATE",
                                  timestamp = "STATE_START_DATE")
eventlog =dplyr::rename(data2, start = STATE_START_DATE, # rename timestamp variables appropriately
                complete = STATE_END_DATE)
  # convert timestamps to
eventlog = bupaR::convert_timestamps(eventlog,columns = c("start", "complete"), format = ymd)
eventlog = bupaR::activitylog(activitylog = eventlog, case_id = "SUBJECT_ID",
              activity_id = "STATE",
              timestamps = c("start", "complete"),
              resource_id = "SUBJECT_ID"
  )
# bupaR plot
processmapR::process_map(eventlog)

