Use Hospital
GO

--- Real Time Scoring
DECLARE @forestmodel VARBINARY(max);
SELECT @forestmodel = value FROM [dbo].[RTS] WHERE id = 'forest';
exec [dbo].[sp_rxPredict] @model = @forestmodel,
						  @inputData = 'SELECT * FROM [dbo].[LoS] WHERE eid NOT IN (SELECT eid from Train_Id)';

DECLARE @boostedmodel VARBINARY(max);
SELECT @boostedmodel = value FROM [dbo].[RTS] WHERE id = 'boosted';
exec [dbo].[sp_rxPredict] @model = @boostedmodel,
						  @inputData = 'SELECT * FROM [dbo].[LoS] WHERE eid NOT IN (SELECT eid from Train_Id)';