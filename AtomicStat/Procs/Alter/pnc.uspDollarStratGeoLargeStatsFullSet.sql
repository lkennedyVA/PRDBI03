USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [pnc].[uspDollarStratGeoLargeStatsFullSet]
	(
		@psiStatBatchLogId INT = NULL
	)
AS
BEGIN
	SET @psiStatBatchLogId = ISNULL( @psiStatBatchLogId, 0 )
	;
	TRUNCATE TABLE [pnc].[DollarStratGeoLargeStatsBulk]
	;
	INSERT INTO [pnc].[DollarStratGeoLargeStatsBulk]
		(
			 [StatBatchLogId]
			,[GeoLarge]
			,[DollarStrat]
			,[CycleDate]
			,[DollarStratGeoLargeItemAmountFloor]
			,[DollarStratGeoLargeItemAmountCeiling]
			,[DollarStratGeoLargeItemsCleared180]
			,[DollarStratGeoLargeItemAmountCleared180]
			,[DollarStratGeoLargeItemAmount180]
			,[DollarStratGeoLargeItemAmountReturned180]
			,[DollarStratGeoLargeLossRateBPS180]
			,[DollarStratGeoLargeItemsCleared90]
			,[DollarStratGeoLargeItemAmountCleared90]
			,[DollarStratGeoLargeItemAmount90]
			,[DollarStratGeoLargeItemAmountReturned90]
			,[DollarStratGeoLargeLossRateBPS90]
			,[DollarStratGeoLargeItemsCleared60]
			,[DollarStratGeoLargeItemAmountCleared60]
			,[DollarStratGeoLargeItemAmount60]
			,[DollarStratGeoLargeItemAmountReturned60]
			,[DollarStratGeoLargeLossRateBPS60]
			,[DollarStratGeoLargeItemsCleared30]
			,[DollarStratGeoLargeItemAmountCleared30]
			,[DollarStratGeoLargeItemAmount30]
			,[DollarStratGeoLargeItemAmountReturned30]
			,[DollarStratGeoLargeLossRateBPS30]
			,[InsertDatetime]
			,[UpdateDatetime]
		)
	SELECT 
			 @psiStatBatchLogId AS [StatBatchLogId]
			,[GeoLarge]
			,[DollarStrat]
			,[LastCycleDate] AS [CycleDate]
			,[DollarStratGeoLargeItemAmountFloor]
			,[DollarStratGeoLargeItemAmountCeiling]
			,[DollarStratGeoLargeItemsCleared180]
			,[DollarStratGeoLargeItemAmountCleared180]
			,[DollarStratGeoLargeItemAmount180]
			,[DollarStratGeoLargeItemAmountReturned180]
			,[DollarStratGeoLargeLossRateBPS180]
			,[DollarStratGeoLargeItemsCleared90]
			,[DollarStratGeoLargeItemAmountCleared90]
			,[DollarStratGeoLargeItemAmount90]
			,[DollarStratGeoLargeItemAmountReturned90]
			,[DollarStratGeoLargeLossRateBPS90]
			,[DollarStratGeoLargeItemsCleared60]
			,[DollarStratGeoLargeItemAmountCleared60]
			,[DollarStratGeoLargeItemAmount60]
			,[DollarStratGeoLargeItemAmountReturned60]
			,[DollarStratGeoLargeLossRateBPS60]
			,[DollarStratGeoLargeItemsCleared30]
			,[DollarStratGeoLargeItemAmountCleared30]
			,[DollarStratGeoLargeItemAmount30]
			,[DollarStratGeoLargeItemAmountReturned30]
			,[DollarStratGeoLargeLossRateBPS30]
			,[InsertDatetime]
			,[UpdateDatetime]
	FROM [DHayes].[dbo].[PNCDollarStratGeoLargeStatsFullSet]
	;
END

GO
