##########################################################################################################################################
## This Python script will do the following:
## 1. Split LoS into a Training LoS_Train, and a Testing set LoS_Test.
## 2. Train Random Forest (rx_dforest implementation) and Boosted Trees (rxFastTrees [work in progress] implementation) and save them to SQL.
## 3. Score the models on LoS_Test.
## 4. Evalaute the scored models.

## Input : Data set LoS
## Output: Regression Random forest and Boosted Trees saved to SQL.

##########################################################################################################################################

## Compute Contexts and Packages

##########################################################################################################################################

# Load packages.
from numpy import mean
from math import sqrt
from pandas import Series
import pyodbc

from revoscalepy import rx_dforest, rx_btrees, rx_predict, RxOdbcData, rx_get_var_info, RxSqlServerData, rx_get_var_names, RxOptions, RxXdfData
from revoscalepy.computecontext import RxInSqlServer, RxLocalSeq, RxComputeContext
from revoscalepy.etl import RxImport
from revoscalepy.functions import RxWriteObject, RxReadObject, RxSerializeModel
#from microsoftml import rx_fast_forest
#from microsoftml import rx_predict as ml_predict

# Load the connection string and compute context definitions.
connection_string = "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"
sql = RxInSqlServer.RxInSqlServer(connection_string = connection_string)
local = RxLocalSeq.RxLocalSeq()

# Set the Compute Context to local.
RxComputeContext.rx_set_compute_context(local)

##########################################################################################################################################

## Function to get the top n rows of a table stored on SQL Server.
## You can execute this function at any time during    your progress by removing the comment "#", and inputting:
##    - the table name.
##    - the number of rows you want to display.

##########################################################################################################################################

def display_head(table_name, n_rows):
    table_sql = RxSqlServerData(sql_query = "SELECT TOP({}) * FROM {}".format(n_rows, table_name), connection_string = connection_string)
    table = RxImport.rx_import(table_sql)
    print(table)

# table_name = "insert_table_name"
# n_rows = 10
# display_head(table_name, n_rows)


##########################################################################################################################################

## Input: Point to the SQL table with the data set for modeling

##########################################################################################################################################

LoS = RxSqlServerData(table = "LoS", connection_string = connection_string, strings_as_factors = True)

##########################################################################################################################################

##	Specify the type of the features before the training. The target variable is converted to integer for regression.

##########################################################################################################################################

def create_col_info(data_source, strings_as_factors = True):
    var_info = rx_get_var_info(data_source)
    column_info = {}
    for key, value in var_info.items():
        varType = value['varType']
        colInfo = {}
        if varType == 'Double':
            colInfo['type'] = 'numeric'
        if varType == 'Int':
            colInfo['type'] = 'integer'
        if varType == 'String' and strings_as_factors == True:
            colInfo['type'] = 'factor'
        elif varType == 'String':
            colInfo['type'] = 'character'
        column_info[key] = colInfo
    return column_info

# column_info = rxCreateColInfo(LoS)  # Not supported yet
column_info = create_col_info(LoS)
print(column_info)

##########################################################################################################################################

##	Split the data set into a training and a testing set

##########################################################################################################################################

# Randomly split the data into a training set and a testing set, with a splitting % p.
# p % goes to the training set, and the rest goes to the testing set. Default is 70%.

p = "70"

## Create the Train_Id table containing Lead_Id of training set.
pyodbc_cnxn = pyodbc.connect(connection_string)
pyodbc_cursor = pyodbc_cnxn.cursor()
pyodbc_cursor.execute("DROP TABLE if exists Train_Id;")
pyodbc_cursor.execute("SELECT eid INTO Train_Id FROM LoS WHERE ABS(CAST(BINARY_CHECKSUM(eid, NEWID()) as int)) % 100 < {} ;".format(p))
pyodbc_cursor.close()
pyodbc_cnxn.commit()
pyodbc_cnxn.close()

## Point to the training set. It will be created on the fly when training models.
variables_all = rx_get_var_names(LoS)
variables_to_remove = ["eid", "vdate", "discharged", "facid"]
training_variables = [x for x in variables_all if x not in variables_to_remove]
LoS_Train = RxSqlServerData(sql_query = "SELECT {} FROM LoS WHERE eid IN (SELECT eid from Train_Id)".format(', '.join(training_variables)), connection_string = connection_string, column_info = column_info)

## Point to the testing set. It will be created on the fly when testing models.
LoS_Test = RxSqlServerData(sql_query = "SELECT {} FROM LoS WHERE eid NOT IN (SELECT eid from Train_Id)".format(', '.join(training_variables)), connection_string = connection_string, column_info = column_info)

##########################################################################################################################################

##	Specify the variables to keep for the training

##########################################################################################################################################

# Write the formula after removing variables not used in the modeling.
variables_to_remove = ["eid", "vdate", "discharged", "facid"]
training_variables = [x for x in variables_all if x not in variables_to_remove and x not in "lengthofstay"]
formula = "lengthofstay ~ " + " + ".join(training_variables)
print("Formula:", formula)

##########################################################################################################################################

## Functions to automate hyperparameter tuning.

##########################################################################################################################################

def tune_rx_btrees(formula, data, n_tree_list, lr_list, min_split_list):
    print("Tuning rx_btrees")
    best_error = 1000000000.0
    best_model = None
    for nt in n_tree_list:
        for lr in lr_list:
            for ms in min_split_list:
                model = rx_btrees(formula=formula,
                                  data=data,
                                  n_tree=nt,
                                  loss_function="gaussian",
                                  learning_rate=lr,
                                  min_split=ms)
                error = model.oob_err['oob.err'][model.ntree - 1]
                print(error, nt, lr, ms)
                if error < best_error:
                    print("^ New best model!")
                    best_error = error
                    best_model = model
    return best_model

def tune_rx_dforest(formula, data, n_tree_list, min_split_list, cp_list):
    print("Tuning rx_dforest")
    best_error = 1.0
    best_model = None
    for nt in n_tree_list:
        for ms in min_split_list:
                for cp in cp_list:
                    model = rx_dforest(formula=formula,
                                       data=data,
                                       n_tree=nt,
                                       min_split=ms,
                                       cp=cp,
                                       seed=5)
                    error = model.oob_err['oob.err'][model.ntree - 1]
                    print(error, nt, ms, cp)
                    if error < best_error:
                        print("^ New best model!")
                        best_error = error
                        best_model = model
    return best_model

##########################################################################################################################################

##	Random Forest (rx_dforest implementation) Training and saving the model to SQL

##########################################################################################################################################

# Set the compute context to SQL for model training.
RxComputeContext.rx_set_compute_context(sql)
LoSImport = RxImport.rx_import(input_data=LoS_Train)    # TODO: file bug for rx_serialize_model when rx_import is not used

# Train the Random Forest.
forest_model = tune_rx_dforest(formula, LoSImport, n_tree_list = [32], min_split_list = [10, 15, 20], cp_list = [0, 0.0001])

# Save the Random Forest in SQL. The compute context is set to local in order to export the model.
RxComputeContext.rx_set_compute_context(local)

# RxSerialize for Real Time Scoring
RTS_odbc = RxOdbcData(connection_string, table = "RTS")
serialized_model = RxSerializeModel.rx_serialize_model(forest_model, realtime_scoring_only = True)
RxWriteObject.rx_write_object(RTS_odbc, key = "forest", value = serialized_model, serialize = False, overwrite = False)

##########################################################################################################################################

##	Boosted Trees (rx_btrees implementation) Training and saving the model to SQL

##########################################################################################################################################

# Set the compute context to SQL for model training.
RxComputeContext.rx_set_compute_context(sql)

# Train the Boosted Trees model.
boosted_model = tune_rx_btrees(formula, LoSImport, n_tree_list = [32], lr_list = [0.1, 0.15, 0.2], min_split_list = [10, 15, 20])

# Save the Boosted Trees in SQL. The compute context is set to Local in order to export the model.
RxComputeContext.rx_set_compute_context(local)

serialized_model = RxSerializeModel.rx_serialize_model(boosted_model, realtime_scoring_only = True)
RxWriteObject.rx_write_object(RTS_odbc, key = "boosted", value = serialized_model, serialize = False, overwrite = False)

##########################################################################################################################################

##	Fast Trees (rx_fast_trees implementation) Training and saving the model to SQL

##########################################################################################################################################

# # Set the compute context to SQL for model training.
# RxComputeContext.rx_set_compute_context(sql)
#
# # Train the Fast Trees model.
# print("Training Fast Trees")
# fast_model = rx_fast_trees(formula = formula,
#                      data = LoS_Train,
#                      num_trees = 40,
#                      method = "regression",
#                      learning_rate = 0.2,
#                      split_fraction = 5/24,
#                      feature_fraction = 1,
#                      min_split = 10)
# #
# # # Save the Boosted Trees in SQL. The compute context is set to Local in order to export the model.
# RxComputeContext.rx_set_compute_context(local)
#
# fast_model_raw = dill.dumps(fast_model)
# Fast_odbc = RxOdbcData(connection_string, table = "SavedModels")
# status = RxWriteObject.rx_write_object(Fast_odbc, key = "boosted", value = fast_model_raw, serialize = False, overwrite=False)
# #read_obj = RxReadObject.rx_read_object(Fast_odbc, key = "boosted", deserialize = False)

##########################################################################################################################################

## Regression model evaluation metrics

##########################################################################################################################################

# Write a function that computes regression performance metrics.
def evaluate_model(observed, predicted, model):
    mean_observed = mean(observed)
    se = (observed - predicted)**2
    ae = abs(observed - predicted)
    sem = (observed - mean_observed)**2
    aem = abs(observed - mean_observed)
    mae = mean(ae)
    rmse = sqrt(mean(se))
    rae = sum(ae) / sum(aem)
    rse = sum(se) / sum(sem)
    rsq = 1 - rse
    metrics = {"Mean Absolute Error": mae,
                "Root Mean Squared Error": rmse,
                "Relative Absolute Error": rae,
                "Relative Squared Error": rse,
                "Coefficient of Determination": rsq}
    print(model)
    print(metrics)
    print("Summary statistics of the absolute error")
    print(Series(abs(observed - predicted)).describe())
    return metrics

##########################################################################################################################################

##	Random Forest Scoring

##########################################################################################################################################

# Make Predictions, then import them into R.
forest_prediction_sql = RxSqlServerData(table = "Forest_Prediction", strings_as_factors = True, connection_string = connection_string)
rx_predict(forest_model, data = LoS_Test, output_data = forest_prediction_sql, type = "response", extra_vars_to_write = ["lengthofstay", "eid"], overwrite = True)

# Compute the performance metrics of the model.
forest_prediction = RxImport.rx_import(input_data = forest_prediction_sql)
forest_metrics = evaluate_model(observed = forest_prediction['lengthofstay'], predicted = forest_prediction['lengthofstay_Pred'], model = "RF")

##########################################################################################################################################

##	Boosted Trees Scoring

##########################################################################################################################################

# Make Predictions, then import them into R.
boosted_prediction_sql = RxSqlServerData(table = "Boosted_Prediction", strings_as_factors = True, connection_string = connection_string)
rx_predict(boosted_model, data = LoS_Test, output_data = boosted_prediction_sql, extra_vars_to_write = ["lengthofstay", "eid"], overwrite = True)

# Compute the performance metrics of the model.
boosted_prediction = RxImport.rx_import(input_data = boosted_prediction_sql)
boosted_metrics = evaluate_model(observed = boosted_prediction['lengthofstay'], predicted = boosted_prediction['lengthofstay_Pred'], model = "GBT")

##########################################################################################################################################

##	Fast Trees Scoring

##########################################################################################################################################

# # Make Predictions, then import them into R.
# fast_prediction_sql = RxSqlServerData(table = "Fast_Prediction", strings_as_factors = True, connection_string = connection_string)
# ml_predict(fast_model, data = LoS_Test, output_data = fast_prediction_sql, extra_vars_to_write = ["lengthofstay", "eid"], overwrite = True)
#
# # Compute the performance metrics of the model.
# fast_prediction = RxImport.rx_import_datasource(input_data = fast_prediction_sql)
# fast_metrics = evaluate_model(observed = fast_prediction['lengthofstay'], predicted = fast_prediction['lengthofstay_Pred'], model = "FT")