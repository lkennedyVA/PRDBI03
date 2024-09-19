USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCOnUsAccountStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from PNCOnUsAccountStatsDelta to OnUsAccountStatsBulk.
		This does not take the horizontal rows of PNCOnUsAccountStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-07-22 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [pnc].[uspPNCOnUsAccountStatsDelta_TransferToAtomicStat]
	(
		 @psiStatBatchLogId INT
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

	DECLARE @siStatBatchLogId INT = @psiStatBatchLogId
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;

print N'...no OnUs work until OnUs data is aligned...'
/*
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCOnUsAccountStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''PNCOnUsAccountStatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCOnUsAccountStatsDelta'
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
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCOnUsAccountStatsDelta x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
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
		--FROM [pnc].[PNCStatAtomicStatXref]
		--WHERE [ObjectName] = N'PNCOnUsAccountStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCOnUsAccountStatsDelta'
		;

		TRUNCATE TABLE PNCprior.OnUsAccountStatsBulk
		;
		INSERT INTO PNCprior.OnUsAccountStatsBulk
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
		FROM pnc.OnUsAccountStatsBulk
		;


		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' Insert into pnc.OnUsAccountStatsBulk'
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		INSERT INTO pnc.OnUsAccountStatsBulk
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
			,100009 AS ClientOrgId
			,CycleDate
			,AccountCloseDate
			,CASE WHEN AccountHoldCode IN( N'1', N'01', N'1`' ) THEN N'1' ELSE AccountHoldCode END AS AccountHoldCode
			,AccountRestrictionCode
			,AccountStatusDesc
			,AccountSubProductCode
			,AverageLedgerBalanceCycle1 AS OnUsPayerAccountAvgBalanceCycle1 -- OnUsPayerAccountAvgBalanceCycle1
			,OnUsPayerAccountCurrentBalance
		FROM DHayes.dbo.PNCOnUsAccountStatsDelta t
		WHERE ISNULL( @iPNCBatchDataSetRefreshLogId, 0 ) > 0
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM pnc.OnUsAccountStatsBulk x
					WHERE x.AccountNumber = t.AccountNumber
						AND x.ClientOrgId = t.ClientOrgId
						--AND x.StatBatchLogId = @siStatBatchLogId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.OnUsAccountStatsBulk.'
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
		FROM DHayes.dbo.PNCOnUsAccountStatsDelta t
			INNER JOIN pnc.OnUsAccountStatsBulk u
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
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND u.StatBatchLogId < @siStatBatchLogId
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in pnc.OnUsAccountStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

	END -- are there rows to process?
*/

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
