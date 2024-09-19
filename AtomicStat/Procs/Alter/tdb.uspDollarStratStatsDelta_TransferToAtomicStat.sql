USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: tdb.uspDollarStratStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from TDBRisk DollarStratStatsDelta to AtomicStat.tdb.DollarStratStatsBulk.
		This does not take the horizontal rows of DollarStratStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-07-22 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [tdb].[uspDollarStratStatsDelta_TransferToAtomicStat]
	(
		 @piStatBatchLogId INT
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	)
AS
BEGIN

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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		PRINT N''
	END

	DECLARE @iStatBatchLogId INT = @piStatBatchLogId
		,@iBatchId int
		,@iBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iBatchDataSetRefreshLogId = [tdb].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( @iStatBatchLogId, N'DollarStratStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'NULL' ) + N' via [tdb].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( ' + CONVERT( nvarchar(50), @iStatBatchLogId ) + N'}, N''DollarStratStatsDelta'' )'
	END

	--EXEC [tdb].uspBatchDataSetRefreshLogTransferInitiate
	--	 @piStatBatchLogId = @iStatBatchLogId
	--	,@pnvDataSetName = N'DollarStratStatsDelta'
	--	,@piBatchId = @iBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
	--	,@piBatchDataSetRefreshLogId = @iBatchDataSetRefreshLogId OUTPUT
	--	,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
	--	,@ptiDebug = @ptiDebug
	--;
	
	IF @ptiDebug > 0 
	BEGIN
		IF @iBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
	END


	IF @iBatchDataSetRefreshLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM [TDBRisk].[dbo].[DollarStratStatsDelta] x WHERE x.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [tdb].[StatAtomicStatXref]
		--WHERE [ObjectName] = N'DollarStratStatsDelta'
		;
		--SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		--FROM TDBRisk.dbo.BatchDataSetRefreshLog 
		--WHERE StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) AND DataSetName = N'DollarStratStatsDelta'
		--;


		TRUNCATE TABLE [tdbprior].[DollarStratStatsBulk]
		;
		INSERT INTO [tdbprior].[DollarStratStatsBulk]
			(
				 StatBatchLogId
				,DollarStrat
				,ClientOrgId
				,CycleDate

				,DollarStratItemAmountFloor
				,DollarStratItemAmountCeiling
				,DollarStratAmountPresented
				,DollarStratItemsPresented
				,DollarStratReturnedAmount
				,DollarStratReturnedItems
				,DollarStratFraudReturnedAmount
				,DollarStratFraudReturnedItems
				,DollarStratTargetLossRateBPS
				,DollarStratFraudLossRateBPS

			)
		SELECT
				 StatBatchLogId
				,DollarStrat
				,ClientOrgId
				,CycleDate

				,DollarStratItemAmountFloor
				,DollarStratItemAmountCeiling
				,DollarStratAmountPresented
				,DollarStratItemsPresented
				,DollarStratReturnedAmount
				,DollarStratReturnedItems
				,DollarStratFraudReturnedAmount
				,DollarStratFraudReturnedItems
				,DollarStratTargetLossRateBPS
				,DollarStratFraudLossRateBPS
		FROM [tdb].[DollarStratStatsBulk]
		;


		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into [tdb].[DollarStratStatsBulk]'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END


		TRUNCATE TABLE [tdb].[DollarStratStatsBulk]
		;
		INSERT INTO [tdb].[DollarStratStatsBulk]
			(
				 StatBatchLogId
				,DollarStrat
				,ClientOrgId
				,CycleDate

				,DollarStratItemAmountFloor
				,DollarStratItemAmountCeiling
				,DollarStratAmountPresented
				,DollarStratItemsPresented
				,DollarStratReturnedAmount
				,DollarStratReturnedItems
				,DollarStratFraudReturnedAmount
				,DollarStratFraudReturnedItems
				,DollarStratTargetLossRateBPS
				,DollarStratFraudLossRateBPS

			)
		SELECT
			 @iStatBatchLogId AS StatBatchLogId
			,DollarStrat
			,100010 AS ClientOrgId
			,CycleDate

			,DollarStratItemAmountFloor
			,DollarStratItemAmountCeiling
			,DollarStratAmountPresented
			,DollarStratItemsPresented
			,DollarStratReturnedAmount
			,DollarStratReturnedItems
			,DollarStratFraudReturnedAmount
			,DollarStratFraudReturnedItems
			,DollarStratTargetLossRateBPS
			,DollarStratFraudLossRateBPS

		FROM [TDBRisk].[dbo].[DollarStratStatsDelta] t
		WHERE ISNULL( @iBatchDataSetRefreshLogId, 0 ) > 0
			AND t.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM [tdb].[DollarStratStatsBulk] x
					WHERE x.StatBatchLogId = @iStatBatchLogId
						AND x.DollarStrat = t.DollarStrat
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [tdb].[DollarStratStatsBulk].'
		END

	END -- are there rows to process?


	--EXEC [tdb].uspBatchDataSetRefreshLogTransferComplete
	--	 @piStatBatchLogId = @iStatBatchLogId
	--	,@piBatchDataSetRefreshLogId = @iBatchDataSetRefreshLogId 
	--	,@piBatchId = @iBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
	--	,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
	--	,@ptiDebug = @ptiDebug
	--;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
