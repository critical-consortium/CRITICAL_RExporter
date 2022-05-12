
# --- Installation ---

install.packages("remotes")
library(remotes)

# Uncomment to Verify JAVA_HOME is set to jdk path
# Sys.getenv("JAVA_HOME")


remotes::install_github(repo = "critical-consortium/CRITICAL_RExporter"
               ,ref = "master"
               ,INSTALL_opts = "--no-multiarch"
)

# Uncomment to test for missing packages
# setdiff(c("rJava", "DatabaseConnector","SqlRender","zip","N3cOhdsi"), rownames(installed.packages()))

# load package
library(CriticalRExporter)


# --- Local configuration ---

# -- run config
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",  # options: oracle, postgressql, redshift, sql server, pdw, netezza, bigquery, sqlite
                                                          server = "", # name of the server
                                                          user="", # username to access server
                                                          password = "" #password for that user
                                                          )
cdmDatabaseSchema <- "" # schema for your CDM instance -- e.g. TMC_OMOP.dbo
cohortDatabaseSchema <- "" # schema with write privileges -- e.g. OHDSI.dbo
# tempDatabaseSchema <- "" # For Google BigQuery users only

outputFolder <-  paste0(getwd(), "/output/")  # directory where output will be stored. default provided

extractSqlPath <- ""  # full path of extract sql file specific to your flavor of SQL, found within the 'ExtractScripts' folder (e.g. .../CRITICAL_RExporter/ExtractScripts/CRITICAL_extract_mssql.sql)

siteAbbrev <- "TuftsMC" # site identifier

# --- Execution ---


# Extract data to pipe delimited files
CriticalRExporter::runExtraction(connectionDetails = connectionDetails,
                        sqlFilePath = extractSqlPath,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        outputFolder = outputFolder,
                        siteAbbrev = siteAbbrev
                        )


# Compress output
zip::zipr(zipfile = paste0(siteAbbrev, "_", cdmName, "_", format(Sys.Date(),"%Y%m%d"),".zip"),
          files = list.files(outputFolder, full.names = TRUE))
