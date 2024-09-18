USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCDollarStratStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from PNCDollarStratStatsDelta to DollarStratStatsBulk.
		This does not take the horizontal rows of PNCDollarStratStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-07-22 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [pnc].[uspPNCDollarStratStatsDelta_TransferToAtomicStat]
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
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCDollarStratStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''PNCDollarStratStatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCDollarStratStatsDelta'
		,@psiPNCBatchId = @iPNCBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piPNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId OUTPUT
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;
	
	IF @ptiDebug > 0 
	BEGIN
		IF @iPNCBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
	END


	IF @iPNCBatchDataSetRefreshLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCDollarStratStatsDelta x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [pnc].[PNCStatAtomicStatXref]
		--WHERE [ObjectName] = N'PNCDollarStratStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCDollarStratStatsDelta'
		;


		TRUNCATE TABLE PNCprior.DollarStratStatsBulk
		;
		INSERT INTO PNCprior.DollarStratStatsBulk
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
		FROM pnc.DollarStratStatsBulk
		;


		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into pnc.DollarStratStatsBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END


		TRUNCATE TABLE pnc.DollarStratStatsBulk
		;
		INSERT INTO pnc.DollarStratStatsBulk
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
			,100009 AS ClientOrgId
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

		FROM DHayes.dbo.PNCDollarStratStatsDelta t
		WHERE ISNULL( @iPNCBatchDataSetRefreshLogId, 0 ) > 0
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM pnc.DollarStratStatsBulk x
					WHERE x.StatBatchLogId = @siStatBatchLogId
						AND x.DollarStrat = t.DollarStrat
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.DollarStratStatsBulk.'
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
;

GO
