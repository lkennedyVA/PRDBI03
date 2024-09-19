USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [pnc].[uspPNCDollarStratGeoLargeStatsFullSet_TransferToAtomicStat]
	(
		 @psiStatBatchLogId INT = NULL
		,@ptiDebug tinyint = NULL
	)
AS
BEGIN
	--SET @psiStatBatchLogId = ISNULL( @psiStatBatchLogId, 0 )
	;

	SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000)
		,@biRowCount bigint
		,@nvMessage nvarchar(4000)
	;
	
	IF @ptiDebug > 0
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + CONVERT( nvarchar(50), @psiStatBatchLogId )
		PRINT N''
	END

	DECLARE @siStatBatchLogId INT = @psiStatBatchLogId
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCDollarStratGeoLargeStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''PNCDollarStratGeoLargeStatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCDollarStratGeoLargeStatsDelta'
		,@psiPNCBatchId = @iPNCBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piPNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId OUTPUT
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;
	
	IF @ptiDebug > 0 
	BEGIN
		IF @iPNCBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
	END

	
	IF ( OBJECT_ID('tempdb..##PNCBatchStatBatchLogDescendant', 'U') IS NULL ) EXEC pnc.uspPNCBatchStatBatchLogDescendantListPopulate @psiPNCBatchId = @iPNCBatchId;


	IF @iPNCBatchDataSetRefreshLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCDollarStratGeoLargeStatsDelta x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
		--AND @siStatBatchLogId > ( SELECT MAX( StatBatchLogId ) FROM pnc.DollarStratGeoLargeStatsBulk )
		--AND NOT EXISTS( SELECT 'X' FROM ##PNCBatchStatBatchLogDescendant x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
			,@numeric16_12DeepestNegativeValue numeric(16,12) = -9999.999999999999
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [pnc].[PNCStatAtomicStatXref]
		--WHERE [ObjectName] = N'PNCDollarStratGeoLargeStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCDollarStratGeoLargeStatsDelta'
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
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.DollarStratGeoLargeStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

	END -- are there rows to process?


	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferComplete
		 @piStatBatchLogId = @siStatBatchLogId
		,@piPNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId 
		,@psiPNCBatchId = @iPNCBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @psiStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END

GO
