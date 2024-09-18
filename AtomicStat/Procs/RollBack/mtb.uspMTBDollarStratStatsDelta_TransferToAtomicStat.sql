USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspMTBDollarStratStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from MTBDollarStratStatsDelta to DollarStratStatsBulk.
		This does not take the horizontal rows of MTBDollarStratStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-03-17 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [mtb].[uspMTBDollarStratStatsDelta_TransferToAtomicStat]
	(
		 @psiStatBatchLogId smallint
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + CONVERT( nvarchar(50), @psiStatBatchLogId )
		PRINT N''
	END

	DECLARE @siStatBatchLogId smallint = @psiStatBatchLogId
		,@iMTBBatchId int
		,@iMTBBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iMTBBatchDataSetRefreshLogId = mtb.ufnMTBBatchDataSetRefreshLogIdGetByBatchLogIdMTBDataSetName( @siStatBatchLogId, N'MTBDollarStratStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iMTBBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iMTBBatchDataSetRefreshLogId ), N'NULL' ) + N' via mtb.ufnMTBBatchDataSetRefreshLogIdGetByBatchLogIdMTBDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''MTBDollarStratStatsDelta'' )'
	END

	EXEC mtb.uspMTBBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvMTBDataSetName = N'MTBDollarStratStatsDelta'
		,@psiMTBBatchId = @iMTBBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piMTBBatchDataSetRefreshLogId = @iMTBBatchDataSetRefreshLogId OUTPUT
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;
	
	IF @ptiDebug > 0 
	BEGIN
		IF @iMTBBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iMTBBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
	END


	IF @iMTBBatchDataSetRefreshLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.MTBDollarStratStatsDelta x WHERE x.MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [mtb].[MTBStatAtomicStatXref]
		--WHERE [ObjectName] = N'MTBDollarStratStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.MTBBatchDataSetRefreshLog 
		WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId ) AND MTBDataSetName = N'MTBDollarStratStatsDelta'
		;


		TRUNCATE TABLE mtbprior.DollarStratStatsBulk
		;
		INSERT INTO mtbprior.DollarStratStatsBulk
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
		FROM mtb.DollarStratStatsBulk
		;


		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into mtb.DollarStratStatsBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END


		TRUNCATE TABLE mtb.DollarStratStatsBulk
		;
		INSERT INTO mtb.DollarStratStatsBulk
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
			 @siStatBatchLogId AS StatBatchLogId
			,DollarStrat
			,100008 AS ClientOrgId
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

		FROM DHayes.dbo.MTBDollarStratStatsDelta t
		WHERE ISNULL( @iMTBBatchDataSetRefreshLogId, 0 ) > 0
			AND t.MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM mtb.DollarStratStatsBulk x
					WHERE x.StatBatchLogId = @siStatBatchLogId
						AND x.DollarStrat = t.DollarStrat
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into mtb.DollarStratStatsBulk.'
		END

	END -- are there rows to process?


	EXEC mtb.uspMTBBatchDataSetRefreshLogTransferComplete
		 @piStatBatchLogId = @siStatBatchLogId
		,@piMTBBatchDataSetRefreshLogId = @iMTBBatchDataSetRefreshLogId 
		,@psiMTBBatchId = @iMTBBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @psiStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iMTBBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iMTBBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iMTBBatchDataSetRefreshLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
