USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [pnc].[uspPNCDollarStratGeoSmallStatsFullSet_TransferToAtomicStat]
	(
		 @psiStatBatchLogId smallint = NULL
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

	DECLARE @siStatBatchLogId smallint = @psiStatBatchLogId
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCDollarStratGeoSmallStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''PNCDollarStratGeoSmallStatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCDollarStratGeoSmallStatsDelta'
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
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCDollarStratGeoSmallStatsDelta x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
		--AND @siStatBatchLogId > ( SELECT MAX( StatBatchLogId ) FROM pnc.DollarStratGeoSmallStatsBulk )
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
		--WHERE [ObjectName] = N'PNCDollarStratGeoSmallStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCDollarStratGeoSmallStatsDelta'
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
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.DollarStratGeoSmallStatsBulk.'
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
