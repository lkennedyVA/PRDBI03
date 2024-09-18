USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [pnc].[uspDollarStratGeoSmallStatsFullSet]
	(
		@psiStatBatchLogId smallint = NULL
	)
AS
BEGIN
	SET @psiStatBatchLogId = ISNULL( @psiStatBatchLogId, 0 )
	;
	TRUNCATE TABLE [pnc].[DollarStratGeoSmallStatsBulk]
	;
	INSERT INTO [pnc].[DollarStratGeoSmallStatsBulk]
		(
			 [StatBatchLogId]
			,[GeoLarge]
			,[GeoSmall]
			,[DollarStrat]
			,[DollarStratGeoSmallItemAmountFloor]
			,[DollarStratGeoSmallItemAmountCeiling]
			,[CycleDate]
			,[DollarStratGeoSmallItemsCleared180]
			,[DollarStratGeoSmallItemAmountCleared180]
			,[DollarStratGeoSmallItemAmount180]
			,[DollarStratGeoSmallItemAmountReturned180]
			,[DollarStratGeoSmallLossRateBPS180]
			,[DollarStratGeoSmallItemsCleared90]
			,[DollarStratGeoSmallItemAmountCleared90]
			,[DollarStratGeoSmallItemAmount90]
			,[DollarStratGeoSmallItemAmountReturned90]
			,[DollarStratGeoSmallLossRateBPS90]
			,[DollarStratGeoSmallItemsCleared60]
			,[DollarStratGeoSmallItemAmountCleared60]
			,[DollarStratGeoSmallItemAmount60]
			,[DollarStratGeoSmallItemAmountReturned60]
			,[DollarStratGeoSmallLossRateBPS60]
			,[DollarStratGeoSmallItemsCleared30]
			,[DollarStratGeoSmallItemAmountCleared30]
			,[DollarStratGeoSmallItemAmount30]
			,[DollarStratGeoSmallItemAmountReturned30]
			,[DollarStratGeoSmallLossRateBPS30]
			,[InsertDatetime]
			,[UpdateDatetime]
		)
	SELECT 
			 @psiStatBatchLogId AS [StatBatchLogId]
			,[GeoLarge]
			,[GeoSmall]
			,[DollarStrat]
			,[DollarStratGeoSmallItemAmountFloor]
			,[DollarStratGeoSmallItemAmountCeiling]
			,[LastCycleDate] AS [CycleDate]
			,[DollarStratGeoSmallItemsCleared180]
			,[DollarStratGeoSmallItemAmountCleared180]
			,[DollarStratGeoSmallItemAmount180]
			,[DollarStratGeoSmallItemAmountReturned180]
			,[DollarStratGeoSmallLossRateBPS180]
			,[DollarStratGeoSmallItemsCleared90]
			,[DollarStratGeoSmallItemAmountCleared90]
			,[DollarStratGeoSmallItemAmount90]
			,[DollarStratGeoSmallItemAmountReturned90]
			,[DollarStratGeoSmallLossRateBPS90]
			,[DollarStratGeoSmallItemsCleared60]
			,[DollarStratGeoSmallItemAmountCleared60]
			,[DollarStratGeoSmallItemAmount60]
			,[DollarStratGeoSmallItemAmountReturned60]
			,[DollarStratGeoSmallLossRateBPS60]
			,[DollarStratGeoSmallItemsCleared30]
			,[DollarStratGeoSmallItemAmountCleared30]
			,[DollarStratGeoSmallItemAmount30]
			,[DollarStratGeoSmallItemAmountReturned30]
			,[DollarStratGeoSmallLossRateBPS30]
			,[InsertDatetime]
			,[UpdateDatetime]
	FROM [DHayes].[dbo].[PNCDollarStratGeoSmallStatsFullSet]
	;
END

GO
