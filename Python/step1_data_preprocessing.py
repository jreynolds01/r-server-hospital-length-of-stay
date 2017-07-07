##########################################################################################################################################

## This Python script will do the following:
## 1. Upload the data set to SQL.
## 2. Determine the variables containing missing values, if any.
## 3. Clean the table: replace NAs with -1 or 'missing' (1st Method) or with the mean or mode (2nd Method).

## Input : CSV file "LengthOfStay.csv".
## Output: Cleaned raw data set LengthOfStay.

##########################################################################################################################################

## Compute Contexts and Packages

##########################################################################################################################################

# Load packages.
import os
import pyodbc
from pandas import DataFrame, isnull
from numpy import where

from revoscalepy import rx_get_var_names
from revoscalepy.datasource import RxSqlServerData, RxTextData
from revoscalepy.computecontext import RxInSqlServer, RxLocalSeq, RxComputeContext
from revoscalepy.etl import RxImport, RxDataStep
from revoscalepy.functions import RxSummary

# Load the connection string and compute context definitions.
connection_string = "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"
sql = RxInSqlServer.RxInSqlServer(connection_string = connection_string)
local = RxLocalSeq.RxLocalSeq()

# Set the Compute Context to local.
RxComputeContext.rx_set_compute_context(local)

##########################################################################################################################################

## Function to get the top n rows of a table stored on SQL Server.
## You can execute this function at any time during your progress by removing the comment "#", and inputting:
##  - the table name.
##  - the number of rows you want to display.

##########################################################################################################################################


def display_head(table_name, n_rows):
    table_sql = RxSqlServerData(sql_query = "SELECT TOP({}}) * FROM {}}".format(n_rows, table_name), connection_string = connection_string)
    table = RxImport.rx_import(table_sql)
    print(table)

# table_name = "insert_table_name"
# n_rows = 10
# display_head(table_name, n_rows)

##########################################################################################################################################

## Upload the data set to to SQL

##########################################################################################################################################

# Specify the desired column types.
# Character and Factor are converted to nvarchar(255), Integer to Integer and Numeric to Float.
col_info = {"eid": {'type': 'integer'},
            "vdate": {'type': 'character'},
            "rcount": {'type': 'character'},
            "gender": {'type': 'factor'},
            "dialysisrenalendstage": {'type': 'factor'},
            "asthma": {'type': 'factor'},
            "irondef": {'type': 'factor'},
            "pneum": {'type': 'factor'},
            "substancedependence": {'type': 'factor'},
            "psychologicaldisordermajor": {'type': 'factor'},
            "depress": {'type': 'factor'},
            "psychother": {'type': 'factor'},
            "fibrosisandother": {'type': 'factor'},
            "malnutrition": {'type': 'factor'},
            "hemo": {'type': 'factor'},
            "hematocrit": {'type': 'numeric'},
            "neutrophils": {'type': 'numeric'},
            "sodium": {'type': 'numeric'},
            "glucose": {'type': 'numeric'},
            "bloodureanitro": {'type': 'numeric'},
            "creatinine": {'type': 'numeric'},
            "bmi": {'type': 'numeric'},
            "pulse": {'type': 'numeric'},
            "respiration": {'type': 'numeric'},
            "secondarydiagnosisnonicd9": {'type': 'factor'},
            "discharged": {'type': 'character'},
            "facid": {'type': 'factor'},
            "lengthofstay": {'type': 'integer'}}


# Point to the input data set while specifying the classes.
file_path = "..\\Data"
LoS_text = RxTextData(file = os.path.join(file_path, "LengthOfStay.csv"), column_info=col_info)  # , column_classes = column_types

# Upload the table to SQL.
LengthOfStay_sql = RxSqlServerData(table = "LengthOfStay", connection_string = connection_string)
RxDataStep.rx_data_step(input_data = LoS_text, output_file = LengthOfStay_sql, overwrite = True)

##########################################################################################################################################

## Determine if LengthOfStay has missing values

##########################################################################################################################################

# First, get the names and types of the variables to be treated.
# For rxSummary to give correct info on characters, stringsAsFactors = T should be used.
LengthOfStay_sql2 = RxSqlServerData(table = "LengthOfStay", connection_string = connection_string, stringsAsFactors = True)

#col = rxCreateColInfo(LengthOfStay_sql2)    # Not yet implemented
colnames = rx_get_var_names(LengthOfStay_sql2)

# Then, get the names of the variables that actually have missing values. Assumption: no NA in eid, lengthofstay, or dates.
var = [x for x in colnames if x not in ["eid", "lengthofstay", "vdate", "discharged"]]
f = "+".join(var)
summary = RxSummary.rx_summary(formula = f, data = LengthOfStay_sql2, by_term = True).summary_data_frame
summary.index.name = "Name"
summary.reset_index(inplace=True)

var_with_NA = summary[summary["MissingObs"] > 0]

method = None
if var_with_NA.empty:
    print("No missing values.")
    print("You can move to step 2.")
    missing = False
else:
    print("Variables containing missing values are:")
    print(var_with_NA)
    print("Apply one of the methods below to fill missing values.")
    missing = True
    #method = "missing"
    method = "mean_mode"

##########################################################################################################################################

## 1st Method: NULL is replaced with "missing" (character variables) or -1 (numeric variables)

##########################################################################################################################################
# TODO: test these methods on datasets with missing data. Hospital Length of Stay has no missing data.
if method == "missing":
    print("Fill with 'missing'")

    # Get the variables types (character vs. numeric)
    char_names = []
    num_names = []
    for index, row in var_with_NA.iterrows():
        name = var_with_NA["Name"]
        if col_info[name] == "numeric" or col_info[name] == "integer":
            num_names.append(name)
        else:
            char_names.append(name)


    # # Function to replace missing values with "missing" (character variables) or -1 (numeric variables).
    def fill_NA_explicit(dataset, context):
        data = DataFrame(dataset)
        for j in range(len(char_names)):
            row_na = where(isnull(data[:,char_names[j]]) == True)
            if len(row_na) > 0:
                data[row_na, char_names[j]] = "missing"

        for j in range(len(num_names)):
            row_na = where(isnull(data[:,num_names[j]]) == True)
            if len(row_na) > 0:
                data[row_na, num_names[j]] = -1
        return data

    # # Apply this function to LeangthOfStay by wrapping it up in rxDataStep. Output is written to LoS0.
    # ## We drop the LoS0 view in case the SQL Stored Procedure was executed in the same database before.
    pyodbc_cnxn = pyodbc.connect(connection_string)
    pyodbc_cursor = pyodbc_cnxn.cursor()
    pyodbc_cursor.execute("IF OBJECT_ID ('LoS0', 'V') IS NOT NULL DROP VIEW LoS0 ;")
    pyodbc_cursor.close()
    pyodbc_cnxn.commit()
    pyodbc_cnxn.close()

    LoS0_sql = RxSqlServerData(table = "LoS0", connection_string = connection_string)
    RxDataStep.rx_data_step(input_data = LengthOfStay_sql, output_file = LoS0_sql, overwrite = True, transform_function = fill_NA_explicit)

# ##########################################################################################################################################

# ## 2nd Method: NULL is replaced with the mode (categorical variables: integer or character) or mean (continuous variables)

# ##########################################################################################################################################
if method == "mean_mode":
    print("Fill with mode and mean")

    # Get the variables types (categortical vs. continuous)
    categ_names = []
    contin_names = []
    for index, row in var_with_NA.iterrows():
        name = var_with_NA["Name"]
        if col_info[name] == "numeric":
            contin_names.append(name)
        else:
            categ_names.append(name)

    # Function to replace missing values with the mode (categorical variables) or mean (continuous variables)
    def fill_NA_mode_mean(dataset, context):
        data = DataFrame(dataset)
        for j in range(len(categ_names)):
            categ_names[j].fillna(categ_names[j].mode())
        for j in range(len(contin_names)):
            categ_names[j].fillna(categ_names[j].mean())
        return(data)

    # Apply this function to LeangthOfStay by wrapping it up in rxDataStep. Output is written to LoS0.
    # We drop the LoS0 view in case the SQL Stored Procedure was executed in the same database before.
    pyodbc_cnxn = pyodbc.connect(connection_string)
    pyodbc_cursor = pyodbc_cnxn.cursor()
    pyodbc_cursor.execute("IF OBJECT_ID ('LoS0', 'V') IS NOT NULL DROP VIEW LoS0 ;")
    pyodbc_cursor.close()
    pyodbc_cnxn.commit()
    pyodbc_cnxn.close()

    LoS0_sql = RxSqlServerData(table = "LoS0", connection_string = connection_string)
    RxDataStep.rx_data_step(input_data = LengthOfStay_sql, output_file = LoS0_sql, overwrite = True, transform_function = fill_NA_mode_mean)

