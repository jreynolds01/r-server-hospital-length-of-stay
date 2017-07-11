Use Hospital
GO

DECLARE @inputData VARCHAR(max);
SET @inputData = 'SELECT eid, rcount, gender,
						dialysisrenalendstage,
						asthma,	irondef,
						pneum, substancedependence,
						psychologicaldisordermajor,
						depress, psychother,
						fibrosisandother, malnutrition,
						hemo, hematocrit, neutrophils,
						sodium,	glucose, bloodureanitro,
						creatinine,	bmi, pulse,
						respiration, secondarydiagnosisnonicd9,
						CAST(lengthofstay AS float) lengthofstay, number_of_issues
						FROM LoS WHERE eid NOT IN (SELECT eid from Train_Id)';

--- Real Time Scoring
DECLARE @forestmodel VARBINARY(max);
SELECT @forestmodel = value FROM [dbo].[RTS] WHERE id = 'forest';
exec [dbo].[sp_rxPredict] @model = @forestmodel,
						  @inputData = @inputData;

DECLARE @boostedmodel VARBINARY(max);
SELECT @boostedmodel = value FROM [dbo].[RTS] WHERE id = 'boosted';
exec [dbo].[sp_rxPredict] @model = @boostedmodel,
						  @inputData = @inputData;

DECLARE @fastmodel VARBINARY(max);
SELECT @fastmodel = value FROM [dbo].[RTS] WHERE id = 'fast';
exec [dbo].[sp_rxPredict] @model = @fastmodel,
						  @inputData = @inputData;

DECLARE @NNmodel VARBINARY(max);
SELECT @NNmodel = value FROM [dbo].[RTS] WHERE id = 'NN';
exec [dbo].[sp_rxPredict] @model = @NNmodel,
						  @inputData = @inputData;