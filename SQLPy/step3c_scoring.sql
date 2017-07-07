-- Stored Procedure to score a data set on a trained model stored in the Models table. 

-- @modelName: specify 'RF' to use the Random Forest,  or 'GBT' for Boosted Trees.
-- @inquery: select the dataset to be scored (the testing set for Development, or the featurized data set for Production). 
-- @output: name of the table that will hold the predictions. 

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS [dbo].[score]
GO

CREATE PROCEDURE [score] @model_name varchar(20), 
						 @inquery varchar(max),
						 @output varchar(max)

AS 
BEGIN

	--	Get the trained model, the current database name and the column information.
	DECLARE @model varbinary(max) = (select model from [dbo].[Models] where model_name = @model_name);
	DECLARE @database_name varchar(max) = db_name();
	DECLARE @info varbinary(max) = (select * from [dbo].[ColInfo]);
	-- Compute the predictions. 
	EXECUTE sp_execute_external_script @language = N'Python',
     					               @script = N' 
import dill
from revoscalepy import RxSqlServerData, rx_predict
##########################################################################################################################################
##	Define the connection string
##########################################################################################################################################
connection_string = "Driver=SQL Server;Server=localhost;Database=" + database_name + ";Trusted_Connection=true;"

##########################################################################################################################################
##	Get the column information.
##########################################################################################################################################
#column_info = dill.load(info)
column_info = {"lengthofstay": {"type": "integer"},
            "asthma": {"type": "factor"},
            "irondef": {"type": "factor"},
            "gender": {"type": "factor"},
            "fibrosisandother": {"type": "factor"},
            "dialysisrenalendstage": {"type": "factor"},
            "secondarydiagnosisnonicd9": {"type": "factor"},
            "psychologicaldisordermajor": {"type": "factor"},
            "bmi": {"type": "numeric"},
            "number_of_issues": {"type": "factor"},
            "facid": {"type": "factor"}, "rcount": {"type": "factor"},
            "discharged": {"type": "factor"},
            "substancedependence": {"type": "factor"},
            "sodium": {"type": "numeric"},
            "malnutrition": {"type": "factor"},
            "neutrophils": {"type": "numeric"},
            "pulse": {"type": "numeric"},
            "respiration": {"type": "numeric"},
            "hematocrit": {"type": "numeric"},
            "glucose": {"type": "numeric"},
            "hemo": {"type": "factor"},
            "pneum": {"type": "factor"},
            "eid": {"type": "integer"},
            "psychother": {"type": "factor"},
            "vdate": {"type": "factor"},
            "bloodureanitro": {"type": "numeric"},
            "depress": {"type": "factor"},
            "creatinine": {"type": "numeric"}}

##########################################################################################################################################
## Point to the data set to score and use the column_info list to specify the types of the features.
##########################################################################################################################################
LoS_Test = RxSqlServerData(sql_query = "{}".format(inquery),
							connection_string = connection_string,
							column_info = column_info)

##########################################################################################################################################
## Random forest scoring.
##########################################################################################################################################
# The prediction results are directly written to a SQL table. 
if model_name == "RF" and len(model) > 0:
	model = dill.loads(model)

	forest_prediction_sql = RxSqlServerData(table = output, connection_string = connection_string, strings_as_factors = True)

	rx_predict(model,
			 data = LoS_Test,
			 output_data = forest_prediction_sql,
			 type = "response",
			 extra_vars_to_write = ["lengthofstay", "eid"],
			 overwrite = True)

##########################################################################################################################################
## Boosted tree scoring.
##########################################################################################################################################
# The prediction results are directly written to a SQL table.
if model_name == "GBT" and len(model) > 0:
	#from microsoftml import rx_predict as ml_predict
	model = dill.loads(model)

	boosted_prediction_sql = RxSqlServerData(table = output, connection_string = connection_string, strings_as_factors = True)

	rx_predict(model,
			data = LoS_Test,
			output_data = boosted_prediction_sql,
			extra_vars_to_write = ["lengthofstay", "eid"],
			overwrite = True)   	   	   
'
, @params = N' @model_name varchar(20), @model varbinary(max), @inquery nvarchar(max), @database_name varchar(max), @info varbinary(max), @output varchar(max)'	  
, @model_name = @model_name
, @model = @model
, @inquery = @inquery
, @database_name = @database_name
, @info = @info
, @output = @output 
;
END
GO

