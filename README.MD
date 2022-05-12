<!-----
NEW: Your output is on the clipboard!

NEW: Check the "Supress top comment" to remove this info from the output.

Conversion time: 0.423 seconds.


Using this Markdown file:

1. Paste this output into your source file.
2. See the notes and action items below regarding this conversion run.
3. Check the rendered output (headings, lists, code blocks, tables) for proper
   formatting and use a linkchecker before you publish this page.

Conversion notes:

* Docs to Markdown version 1.0?23
* Tue May 12 2020 10:08:49 GMT-0700 (PDT)
* Source doc: Untitled document
----->

**Data Prerequisites**

- You have created a table named 'CRITICAL_COHORT' containing the column 'PERSON_ID' which lists the unique PERSON_ID values of patients included in your CRITICAL cohort. The schema of where this table lives is specified within the 'cohortDatabaseSchema' variable below.



**System Prerequisites**

- A database in [Common Data Model version 5](https://github.com/OHDSI/CommonDataModel) in one of these platforms: SQL Server, Oracle, PostgreSQL, Amazon RedShift, Google BigQuery 
- R version 3.5.0 or newer (version 4 or newer is highly recommended)
- On Windows: [RTools](http://cran.r-project.org/bin/windows/Rtools/)
- [Java](http://java.com)
- JAVA_HOME environment variable set to JDK path
  ```r
    # Check current value: 
    Sys.getenv("JAVA_HOME")
    # Set to new value
    Sys.setenv(JAVA_HOME="~/yourjdkpath")
  ```
- Suggested: 25 GB of free disk space

**Installation**

First, navigate the root of this repository to find the appropriate [phenotype generation](https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/tree/master/PhenotypeScripts) and [data extraction](https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/tree/master/ExtractScripts) scripts that match your common data model and sql dialect. Save these to a convenient location as the file paths are expected as parameters below. 

To utilize the Exporter, in `R` use the following code to install the dependencies:
```r
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
```
**Troubleshooting note:** If you have an older version of R (prior to version 4), the installation may fail--the remotes package is not tolerant of warning messages, which often come up due to versioning issues. The best way to correct this is to update to the newest version of R. If you are not able to update R, you can force the installation to ignore the warning messages by setting an additional environment variable: R_REMOTES_NO_ERRORS_FROM_WARNINGS = true. 

**Local configuration**

Note: Please scroll to the bottom of this README to see examples of what the MANIFEST configuration should look like.

```r
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

extractSqlPath <- ""  # full path of extract sql file specific to your flavor of SQL, found within the 'ExtractScripts/' directory of this repository. (e.g. .../CRITICAL_RExporter/ExtractScripts/CRITICAL_extract_mssql.sql)

siteAbbrev <- "TuftsMC" # site identifier
```
**Execution**
```r
# Extract data to pipe delimited files
CRITICAL_RExporter::runExtraction(connectionDetails = connectionDetails,
                        sqlFilePath = extractSqlPath,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        outputFolder = outputFolder,
                        siteAbbrev = siteAbbrev
                        )
   
# Compress output
zip::zipr(zipfile = paste0(siteAbbrev, "_", cdmName, "_", format(Sys.Date(),"%Y%m%d"),".zip"),
          files = list.files(outputFolder, full.names = TRUE))


```
**Troubleshooting note:** The default settings of the `runExtraction` function pulls data from your database into local memory before saving it disk as delimited files. If you are experiencing RAM limitation issues on your local machine, you can avoid storing the query results in memory by leveraging the Andromeda package (set `useAndromeda=TRUE` parameter in `runExtraction()` ) which instead temporarily stores that data on disk instead of RAM.


