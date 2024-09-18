USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspMTBOnUsAccountStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from MTBOnUsAccountStatsDelta to OnUsAccountStatsBulk.
		This does not take the horizontal rows of MTBOnUsAccountStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-03-17 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [mtb].[uspMTBOnUsAccountStatsDelta_TransferToAtomicStat]
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
	SELECT @iMTBBatchDataSetRefreshLogId = mtb.ufnMTBBatchDataSetRefreshLogIdGetByBatchLogIdMTBDataSetName( @siStatBatchLogId, N'MTBOnUsAccountStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iMTBBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iMTBBatchDataSetRefreshLogId ), N'NULL' ) + N' via mtb.ufnMTBBatchDataSetRefreshLogIdGetByBatchLogIdMTBDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''MTBOnUsAccountStatsDelta'' )'
	END

	EXEC mtb.uspMTBBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvMTBDataSetName = N'MTBOnUsAccountStatsDelta'
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
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.MTBOnUsAccountStatsDelta x WHERE x.MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
			,@dateDeepestValue date = '0001-01-01'
			,@nvNullAlternativeValue nvarchar(255) = N'☠'
			,@numeric16_2DeepestNegativeValue numeric(16,2) = -99999999999999.99
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [mtb].[MTBStatAtomicStatXref]
		--WHERE [ObjectName] = N'MTBOnUsAccountStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.MTBBatchDataSetRefreshLog 
		WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId ) AND MTBDataSetName = N'MTBOnUsAccountStatsDelta'
		;

		TRUNCATE TABLE mtbprior.OnUsAccountStatsBulk
		;
		INSERT INTO mtbprior.OnUsAccountStatsBulk
			(
				 StatBatchLogId
				,AccountNumber
				,ClientOrgId
				,CycleDate
				,AccountCloseDate
				,AccountHoldCode
				,AccountRestrictionCode
				,AccountStatusDesc
				,AccountSubProductCode
				,OnUsPayerAccountAvgBalanceCycle1
				,OnUsPayerAccountCurrentBalance
			)
		SELECT
				 StatBatchLogId
				,AccountNumber
				,ClientOrgId
				,CycleDate
				,AccountCloseDate
				,AccountHoldCode
				,AccountRestrictionCode
				,AccountStatusDesc
				,AccountSubProductCode
				,OnUsPayerAccountAvgBalanceCycle1
				,OnUsPayerAccountCurrentBalance
		FROM mtb.OnUsAccountStatsBulk
		;


		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' Insert into mtb.OnUsAccountStatsBulk'
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		INSERT INTO mtb.OnUsAccountStatsBulk
			(
				 StatBatchLogId
				,AccountNumber
				,ClientOrgId
				,CycleDate
				,AccountCloseDate
				,AccountHoldCode
				,AccountRestrictionCode
				,AccountStatusDesc
				,AccountSubProductCode
				,OnUsPayerAccountAvgBalanceCycle1
				,OnUsPayerAccountCurrentBalance
			)
		SELECT
		DISTINCT
			 @siStatBatchLogId AS StatBatchLogId
			,AccountNumber
			,100008 AS ClientOrgId
			,CycleDate
			,AccountCloseDate
			,CASE WHEN AccountHoldCode IN( N'1', N'01', N'1`' ) THEN N'1' ELSE AccountHoldCode END AS AccountHoldCode
			,AccountRestrictionCode
			,AccountStatusDesc
			,AccountSubProductCode
			,AverageLedgerBalanceCycle1 AS OnUsPayerAccountAvgBalanceCycle1 -- OnUsPayerAccountAvgBalanceCycle1
			,OnUsPayerAccountCurrentBalance
		FROM DHayes.dbo.MTBOnUsAccountStatsDelta t
		WHERE ISNULL( @iMTBBatchDataSetRefreshLogId, 0 ) > 0
			AND t.MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM mtb.OnUsAccountStatsBulk x
					WHERE x.AccountNumber = t.AccountNumber
						AND x.ClientOrgId = t.ClientOrgId
						--AND x.StatBatchLogId = @siStatBatchLogId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into mtb.OnUsAccountStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		UPDATE u
		SET StatBatchLogId = @siStatBatchLogId
			,CycleDate = t.CycleDate
			,AccountCloseDate = t.AccountCloseDate
			,AccountHoldCode = CASE WHEN t.AccountHoldCode IN( N'1', N'01', N'1`' ) THEN N'1' ELSE t.AccountHoldCode END 
			,AccountRestrictionCode = t.AccountRestrictionCode
			,AccountStatusDesc = t.AccountStatusDesc
			,AccountSubProductCode = t.AccountSubProductCode
			,OnUsPayerAccountAvgBalanceCycle1 = t.AverageLedgerBalanceCycle1
			,OnUsPayerAccountCurrentBalance = t.OnUsPayerAccountCurrentBalance
		FROM DHayes.dbo.MTBOnUsAccountStatsDelta t
			INNER JOIN mtb.OnUsAccountStatsBulk u
				ON t.AccountNumber = u.AccountNumber
					AND t.ClientOrgId = u.ClientOrgId
					--AND t.HashId = u.HashId
		WHERE	( -- Do NOT test for CycleDate.
					ISNULL( u.AccountCloseDate, @dateDeepestValue ) <> ISNULL( t.AccountCloseDate, @dateDeepestValue )
					OR ISNULL( u.AccountHoldCode, @nvNullAlternativeValue ) <> ISNULL( CASE WHEN t.AccountHoldCode IN( N'1', N'01', N'1`' ) THEN N'1' ELSE t.AccountHoldCode END, @nvNullAlternativeValue )
					OR ISNULL( u.AccountRestrictionCode, @nvNullAlternativeValue ) <> ISNULL( t.AccountRestrictionCode, @nvNullAlternativeValue )
					OR ISNULL( u.AccountStatusDesc, @nvNullAlternativeValue ) <> ISNULL( t.AccountStatusDesc, @nvNullAlternativeValue )
					OR ISNULL( u.AccountSubProductCode, @nvNullAlternativeValue ) <> ISNULL( t.AccountSubProductCode, @nvNullAlternativeValue )
					OR ISNULL( u.OnUsPayerAccountAvgBalanceCycle1, @numeric16_2DeepestNegativeValue ) <> ISNULL( t.AverageLedgerBalanceCycle1, @numeric16_2DeepestNegativeValue )
					OR ISNULL( u.OnUsPayerAccountCurrentBalance, @numeric16_2DeepestNegativeValue ) <> ISNULL( t.OnUsPayerAccountCurrentBalance, @numeric16_2DeepestNegativeValue )
				)
			AND t.MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND u.StatBatchLogId < @siStatBatchLogId
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in mtb.OnUsAccountStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
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
