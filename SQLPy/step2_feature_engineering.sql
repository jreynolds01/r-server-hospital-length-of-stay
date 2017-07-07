-- Stored procedure for feature engineering. The feature engineered table will be a View. 

-- @input: specify the name of the cleaned View to be featurized by this SP. 
-- @output: specify the name of the View that will hold the featurized data. 
-- @is_production is set to 1 for Production pipeline and to 0 for Modeling/Development. 

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS [dbo].[feature_engineering]
GO

CREATE PROCEDURE [dbo].[feature_engineering]  @input varchar(max), @output varchar(max), @is_production int
AS
BEGIN 

-- Drop the output table if it has been created in R in the same database. 
    DECLARE @sql0 nvarchar(max);
	SELECT @sql0 = N'
	IF OBJECT_ID (''' + @output + ''', ''U'') IS NOT NULL  
	DROP TABLE ' + @output ;  
	EXEC sp_executesql @sql0

-- Drop the output view if it already exists. 
	DECLARE @sql1 nvarchar(max);
	SELECT @sql1 = N'
	IF OBJECT_ID (''' + @output + ''', ''V'') IS NOT NULL  
	DROP VIEW ' + @output ;  
	EXEC sp_executesql @sql1

-- Create a View with new features:
-- 1- Standardize the health numeric variables by substracting the mean and dividing by the standard deviation. 
-- 2- Create number_of_issues variable corresponding to the total number of preidentified medical conditions. 
-- lengthofstay variable is only selected if it exists (ie. in Modeling pipeline).

	DECLARE @sql2 nvarchar(max);
	SELECT @sql2 = N'
		CREATE VIEW ' + @output + '
		AS
		SELECT eid, vdate, rcount, gender, dialysisrenalendstage, asthma, irondef, pneum, substancedependence, psychologicaldisordermajor, depress,
			   psychother, fibrosisandother, malnutrition, hemo,
		       (hematocrit - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''hematocrit''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''hematocrit'') AS hematocrit,
		       (neutrophils - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''neutrophils''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''neutrophils'') AS neutrophils,
		       (sodium - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''sodium ''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''sodium '') AS sodium,
		       (glucose - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''glucose''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''glucose'') AS glucose,
		       (bloodureanitro - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''bloodureanitro''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''bloodureanitro'') AS bloodureanitro,
		       (creatinine - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''creatinine''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''creatinine'') AS creatinine,
		       (bmi - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''bmi''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''bmi'') AS bmi,
		       (pulse - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''pulse''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''pulse'') AS pulse,
		       (respiration - (SELECT mean FROM [dbo].[Stats] WHERE variable_name = ''respiration''))/(SELECT std FROM [dbo].[Stats] WHERE variable_name = ''respiration'') AS respiration,
		       CAST((CAST(hemo as int) + CAST(dialysisrenalendstage as int) + CAST(asthma as int) + CAST(irondef as int) + CAST(pneum as int) +
			        CAST(substancedependence as int) + CAST(psychologicaldisordermajor as int) + CAST(depress as int) +
                    CAST(psychother as int) + CAST(fibrosisandother as int) + CAST(malnutrition as int)) as varchar(2)) 
               AS number_of_issues,
			   secondarydiagnosisnonicd9, discharged, facid, '+
			   (CASE WHEN @is_production = 0 THEN 'lengthofstay' else 'NULL as lengthofstay' end) + '
	    FROM ' + @input;
	EXEC sp_executesql @sql2

;
END
GO

-- Stored Procedure to get the column information (variable names, types, and levels for factors) from the data used during the deployment pipeline. 

-- @input: specify the name of the featurized data set.  

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS [dbo].[get_column_info]  
GO

CREATE PROCEDURE [get_column_info] @input varchar(max)
AS 
BEGIN
	-- Create an empty table to store the serialized column information. 
	DROP TABLE IF EXISTS [dbo].[ColInfo]
	CREATE TABLE [dbo].[ColInfo](
		[info] [varbinary](max) NOT NULL
		)

	-- Serialize the column information. 
	DECLARE @database_name varchar(max) = db_name()
	INSERT INTO ColInfo
	EXECUTE sp_execute_external_script @language = N'Python',
     					               @script = N' 
from revoscalepy import RxSqlServerData
from dill import dumps
from pandas import DataFrame

# TODO: replace with rx_get_var_info once available
col_info = {"lengthofstay": {"type": "integer"},
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

connection_string = "Driver=SQL Server;Server=localhost;Database=" + database_name + ";Trusted_Connection=true;"
LoS = RxSqlServerData(sql_query = "SELECT *  FROM [{}]".format(input),
					   connection_string = connection_string,
					   strings_as_factors = True)
OutputDataSet = DataFrame({"payload": dumps(col_info)}, index=[0])
'
, @params = N'@input varchar(max), @database_name varchar(max)'
, @input = @input
, @database_name = @database_name 
;
END
GO