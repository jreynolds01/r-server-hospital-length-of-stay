-- Stored Procedure to train a Random Forest (rxDForest implementation) or Boosted Trees (rxFastTrees implementation).

-- @modelName: specify 'RF' to train a Random Forest,  or 'GBT' for Boosted Trees.
-- @dataset_name: specify the name of the featurized data set. 

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS [dbo].[train_model];
GO

CREATE PROCEDURE [train_model]   @modelName varchar(20),
								 @dataset_name varchar(max) 
AS 
BEGIN
	-- Create an empty table to be filled with the trained models.
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'Models' AND xtype = 'U')
	CREATE TABLE [dbo].[Models](
		[model_name] [varchar](30) NOT NULL default('default model'),
		[model] [varbinary](max) NOT NULL
		)

	-- Get the database name and the column information. 
	DECLARE @info varbinary(max) = (select * from [dbo].[ColInfo]);
	DECLARE @database_name varchar(max) = db_name();

	-- Train the model on the training set.	
	DELETE FROM Models WHERE model_name = @modelName;
	INSERT INTO Models (model)
	EXECUTE sp_execute_external_script @language = N'Python',
									   @script = N' 
import dill
from revoscalepy import RxInSqlServer, rx_set_compute_context, RxSqlServerData, rx_dforest, rx_btrees
##########################################################################################################################################
##	Set the compute context to SQL for faster training
##########################################################################################################################################
# Define the connection string
connection_string = "Driver=SQL Server;Server=localhost;Database=" + database_name + ";Trusted_Connection=true;"

# Set the Compute Context to SQL.
sql = RxInSqlServer(connection_string = connection_string)
rx_set_compute_context(sql)

##########################################################################################################################################
##	Get the column information.
##########################################################################################################################################
column_info = dill.loads(info)

##########################################################################################################################################
##	Point to the training set and use the column_info list to specify the types of the features.
##########################################################################################################################################
LoS_Train = RxSqlServerData(  
  sql_query = "SELECT * FROM [{}] WHERE eid IN (SELECT eid from Train_Id)".format(dataset_name),
  connection_string = connection_string, 
  column_info = column_info)

##########################################################################################################################################
##	Specify the variables to keep for the training 
##########################################################################################################################################
variables_all = [var for var in column_info]
training_variables = [x for x in variables_all if x not in ["eid", "vdate", "discharged", "facid", "lengthofstay"]]
formula = "lengthofstay ~ " + " + ".join(training_variables)

##########################################################################################################################################
## Training model based on model selection
##########################################################################################################################################
# Parameters of both models have been chosen for illustrative purposes, and can be further optimized.

if model_name == "RF":
	# Train the Random Forest.
	model = rx_dforest(formula = formula,
				data = LoS_Train,
				n_tree = 10,
				min_split = 10,
				min_bucket = 5,
				cp = 0.00005,
				seed = 5)
else:
	# Train the Gradient Boosted Trees (rx_btrees implementation).
	model = rx_btrees(formula = formula,
                     data = LoS_Train,
                     n_tree = 40,
                     method = "class",
                     loss_function="gaussian",
                     learning_rate = 0.2,
                     min_split = 10)
			   				       
OutputDataSet = DataFrame({"payload": dill.dumps(model)}, index=[0])'
, @params = N' @model_name varchar(20), @dataset_name varchar(max), @info varbinary(max), @database_name varchar(max)'
, @model_name = @modelName 
, @dataset_name =  @dataset_name
, @info = @info
, @database_name = @database_name

UPDATE Models set model_name = @modelName 
WHERE model_name = 'default model'

;
END
GO

