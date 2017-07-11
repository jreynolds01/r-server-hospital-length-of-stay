##########################################################################################################################################
## This Python script will do the following :
## 1. Standardize the continuous variables (Z-score).
## 2. Create the variable number_of_issues: the number of preidentified medical conditions.

## Input : Data set before feature engineering LengthOfStay.
## Output: Data set with new features LoS.

##########################################################################################################################################

## Compute Contexts and Packages

##########################################################################################################################################

from pandas import DataFrame
from pandas import to_numeric
import pyodbc

# Load packages.
from revoscalepy import rx_get_var_names
from revoscalepy import RxSqlServerData
from revoscalepy import rx_summary
from revoscalepy import RxInSqlServer, RxLocalSeq, rx_set_compute_context
from revoscalepy import rx_import, rx_data_step

# Load the connection string and compute context definitions.
connection_string = "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"
sql = RxInSqlServer(connection_string = connection_string)
local = RxLocalSeq()

# Set the Compute Context to local.
rx_set_compute_context(local)

##########################################################################################################################################

## Function to get the top n rows of a table stored on SQL Server.
## You can execute this function at any time during    your progress by removing the comment "#", and inputting:
##    - the table name.
##    - the number of rows you want to display.

##########################################################################################################################################

def display_head(table_name, n_rows):
    table_sql = RxSqlServerData(sql_query = "SELECT TOP({}) * FROM {}".format(n_rows, table_name), connection_string = connection_string)
    table = rx_import(table_sql)
    print(table)

# table_name = "insert_table_name"
# n_rows = 10
# display_head(table_name, n_rows)

##########################################################################################################################################

## Input: Point to the SQL table with the cleaned raw data set

##########################################################################################################################################

# TODO: Actually implement the missing flag. For now manually set the missing flag based on the results of step 1
missing = False
table_name = None
if(missing == False):
    table_name = "LengthOfStay"
else:
    table_name = "LoS0"

LengthOfStay_cleaned_sql = RxSqlServerData(table = table_name, connection_string = connection_string)


##########################################################################################################################################

## Feature Engineering:
## 1- Standardization: hematocrit, neutrophils, sodium, glucose, bloodureanitro, creatinine, bmi, pulse, respiration.
## 2- Number of preidentified medical conditions: number_of_issues.

##########################################################################################################################################

# Get the mean and standard deviation of those variables.
col_list = rx_get_var_names(LengthOfStay_cleaned_sql)
f = "+".join(col_list)
summary = rx_summary(formula = f, data = LengthOfStay_cleaned_sql, by_term = True).summary_data_frame
summary.index.name = "Name"
summary.reset_index(inplace=True)

names = ["hematocrit", "neutrophils", "sodium", "glucose", "bloodureanitro", "creatinine", "bmi", "pulse", "respiration"]
statistics = summary[summary["Name"].isin(names)]
statistics = statistics[["Name", "Mean", "StdDev"]]

# standardization transform function
def standardize(dataset, context):
    data = DataFrame(dataset)
    for n, row in statistics.iterrows():
        data[[row["Name"]]] = (data[[row["Name"]]] - row["Mean"])/row["StdDev"]
    return data

# number_of_issues transform function
def calculate_number_of_issues(dataset, context):
    data = DataFrame(dataset)
    data["number_of_issues"] = to_numeric(data["hemo"]) + to_numeric(data["dialysisrenalendstage"]) + to_numeric(data["asthma"]) + to_numeric(data["irondef"]) \
        + to_numeric(data["pneum"]) + to_numeric(data["substancedependence"]) + to_numeric(data["psychologicaldisordermajor"]) + to_numeric(data["depress"]) \
        + to_numeric(data["psychother"]) + to_numeric(data["fibrosisandother"]) + to_numeric(data["malnutrition"])
    return data

# We drop the LoS view in case the SQL Stored Procedure was executed in the same database before.
pyodbc_cnxn = pyodbc.connect(connection_string)
pyodbc_cursor = pyodbc_cnxn.cursor()
pyodbc_cursor.execute("IF OBJECT_ID ('LoS', 'V') IS NOT NULL DROP VIEW LoS ;")
pyodbc_cursor.close()
pyodbc_cnxn.commit()
pyodbc_cnxn.close()

# TODO: combine into one data step
# Standardize the cleaned table by wrapping it up in rxDataStep. Output is written to LoS_standard.
LengthOfStay_cleaned_sql = RxSqlServerData(sql_query = "SELECT * FROM [Hospital].[dbo].[{}]".format(table_name), connection_string = connection_string) # Temporary workaround
LoS_std_sql = RxSqlServerData(table = "LoS_standard", connection_string = connection_string)
rx_data_step(input_data = LengthOfStay_cleaned_sql, output_file = LoS_std_sql, overwrite = True, transform_function = standardize)

# We create a new column number_of_issues as the number of preidentified medical conditions. Output is written to LoS.
LoS_std_sql = RxSqlServerData(sql_query = "SELECT * FROM [Hospital].[dbo].[LoS_standard]", connection_string = connection_string) # Temporary workaround
LoS_sql = RxSqlServerData(table = "LoS", connection_string = connection_string)
rx_data_step(input_data = LoS_std_sql, output_file = LoS_sql, overwrite = True, transform_function = calculate_number_of_issues)

# Converting number_of_issues to character with a SQL query because as.character in rxDataStep is crashing.
pyodbc_cnxn = pyodbc.connect(connection_string)
pyodbc_cursor = pyodbc_cnxn.cursor()
pyodbc_cursor.execute("ALTER TABLE LoS ALTER COLUMN number_of_issues varchar(2);")
pyodbc_cursor.execute("ALTER TABLE LoS ALTER COLUMN lengthofstay float;")   # int -> float for regression
pyodbc_cursor.close()
pyodbc_cnxn.commit()
pyodbc_cnxn.close()