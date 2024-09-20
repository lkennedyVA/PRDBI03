USE [TDBAtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************

	Name: tdb.uspOnUsAccountStatsDelta_TransferToAtomicStat

	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from TDBRisk.dbo.CustAccountStatsDelta to TDBAtomicStat.dbo.OnUsAccountStatsBulk.
		This does not take the horizontal rows of OnUsAccountStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2020-01-06 - LSW - Based upon [m t b].[usp M T B OnUsAccountStatsDelta_TransferToAtomicStat].

*****************************************************************************************/
ALTER PROCEDURE [stat].[uspOnUsAccountStatsDelta_TransferToAtomicStat]
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
		,@nvDataFocus nvarchar(128) = N'OnUsAccountStats'
		,@nvMessage nvarchar(4000)

		,@siStatCount smallint
		,@biDataSetRowCount bigint
		,@biTargetRowCount bigint

		,@biDeepestNegativeValue bigint = -9223372036854775808
		,@dateDeepestValue date = '0001-01-01'
		,@nvNullAlternativeValue nvarchar(255) = N'☠'
		,@numeric16_2DeepestNegativeValue numeric(16,2) = -99999999999999.99
		,@numeric16_12DeepestNegativeValue numeric(16,12) = -9999.999999999999
	;
	
	IF @ptiDebug > 0
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		PRINT N''
	END

	DECLARE @iStatBatchLogId INT = @piStatBatchLogId
		,@iBatchId int
		,@iBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
--	SELECT @iBatchDataSetRefreshLogId = [stat].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( @iStatBatchLogId, @nvDataFocus + N'Delta' )
	;
	SELECT @iBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
	
--	IF @ptiDebug > 0 
--	BEGIN
--		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' ) + N' via [tdb].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( ' + CONVERT( nvarchar(50), @iStatBatchLogId ) + N'}, N''' + @nvDataFocus + N'Delta'' )'
--	END

	--EXEC [stat].uspBatchDataSetRefreshLogTransferInitiate
	--	 @piStatBatchLogId = @iStatBatchLogId
	--	,@pnvDataSetName = N'ABAAcctLengthStatsDelta'
	--	,@psiBatchId = @iBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
	--	,@piBatchDataSetRefreshLogId = @iBatchDataSetRefreshLogId OUTPUT
	--	,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
	--	,@ptiDebug = @ptiDebug
	--;
	
--	IF @ptiDebug > 0 
--	BEGIN
--		IF @iBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
--	END

	
	--IF ( OBJECT_ID('tempdb..##BatchStatBatchLogDescendant', 'U') IS NULL ) EXEC [stat].uspBatchStatBatchLogDescendantListPopulate @piBatchId = @iBatchId;

	IF ISNULL( @iStatBatchLogId, 0 ) > 0
--	IF @iBatchDataSetRefreshLogId IS NOT NULL 
--		AND EXISTS( SELECT 'X' FROM [TDBRisk].[dbo].[OnUsAccountStatsDelta] x WHERE x.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) )
		--AND @iStatBatchLogId > ( SELECT MAX( StatBatchLogId ) FROM [dbo].[ABAAcctLengthStatsBulk] )
		--AND NOT EXISTS( SELECT 'X' FROM ##BatchStatBatchLogDescendant x )
	BEGIN -- are there rows to process?

		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [tdb].[StatAtomicStatXref]
		--WHERE [ObjectName] = N'ABAAcctLengthStatsDelta'
		;
		--SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		--FROM [TDBRisk].dbo.BatchDataSetRefreshLog l
		--WHERE l.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) AND DataSetName = N'ABAAcctLengthStatsDelta'
		--;
		SELECT @biDataSetRowCount = COUNT_BIG(1)
		FROM [TDBRisk].[dbo].[CustAccountStatsFullSet] d
--		WHERE d.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
		;
		SELECT @biTargetRowCount = COUNT_BIG(1)
		FROM [dbo].[OnUsAccountStatsBulk] d
		;


		TRUNCATE TABLE [prior].[OnUsAccountStatsBulk]
		;
		INSERT INTO [prior].[OnUsAccountStatsBulk]
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
				--,OnUsPayerAccountAvgBalanceCycle1
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
				--,NULL AS OnUsPayerAccountAvgBalanceCycle1
				,OnUsPayerAccountCurrentBalance
		FROM [dbo].[OnUsAccountStatsBulk]
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'Upsert [dbo].[' + @nvDataFocus + N'Bulk]'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' raw source rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END



		UPDATE u
		SET StatBatchLogId = @iStatBatchLogId
			,CycleDate = t.CycleDate
			,AccountCloseDate = t.CustAccountCloseDate
			,AccountHoldCode = t.CustAccountHoldCode
--			,AccountRestrictionCode = t.AccountRestrictionCode
--			,AccountStatusDesc = t.AccountStatusDesc
			,AccountSubProductCode = t.CustAccountSubProductCode
--			,OnUsPayerAccountAvgBalanceCycle1 = t.AverageLedgerBalanceCycle1
			,OnUsPayerAccountCurrentBalance = t.CustAccountCurrentBalance
		FROM [TDBRisk].[dbo].[CustAccountStatsDelta] t
			INNER JOIN [dbo].[OnUsAccountStatsBulk] u
				ON t.CustomerAccountNumber = u.AccountNumber
					AND t.ClientOrgId = u.ClientOrgId
		WHERE	@biTargetRowCount > 0 -- if there are no rows in the target table, then we don't want to "update" any rows with data from the source table.
			AND t.StatBatchId = stat.ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			AND u.StatBatchLogId < @iStatBatchLogId
			AND ( -- Do NOT test for CycleDate.
					ISNULL( u.AccountCloseDate, @dateDeepestValue ) <> ISNULL( t.CustAccountCloseDate, @dateDeepestValue )
					OR ISNULL( u.AccountHoldCode, @nvNullAlternativeValue ) <> ISNULL( t.CustAccountHoldCode, @nvNullAlternativeValue )
--					OR ISNULL( u.AccountRestrictionCode, @nvNullAlternativeValue ) <> ISNULL( t.AccountRestrictionCode, @nvNullAlternativeValue )
--					OR ISNULL( u.AccountStatusDesc, @nvNullAlternativeValue ) <> ISNULL( t.AccountStatusDesc, @nvNullAlternativeValue )
					OR ISNULL( u.AccountSubProductCode, @nvNullAlternativeValue ) <> ISNULL( t.CustAccountSubProductCode, @nvNullAlternativeValue )
--					OR ISNULL( u.OnUsPayerAccountAvgBalanceCycle1, @numeric16_2DeepestNegativeValue ) <> ISNULL( t.AverageLedgerBalanceCycle1, @numeric16_2DeepestNegativeValue )
					OR ISNULL( u.OnUsPayerAccountCurrentBalance, @numeric16_2DeepestNegativeValue ) <> ISNULL( t.CustAccountCurrentBalance, @numeric16_2DeepestNegativeValue )
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in [tdb].[' + @nvDataFocus + N'Bulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END



		INSERT INTO [dbo].[OnUsAccountStatsBulk]
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
				--,OnUsPayerAccountAvgBalanceCycle1
				,OnUsPayerAccountCurrentBalance
			)
		SELECT
		DISTINCT
			 @iStatBatchLogId AS StatBatchLogId
			,t.CustomerAccountNumber AS AccountNumber
			,t.ClientOrgId
			,t.CycleDate AS CycleDate
			,t.CustAccountCloseDate AS AccountCloseDate
			,t.CustAccountHoldCode AS AccountHoldCode
			,convert( nvarchar(1), NULL ) AS AccountRestrictionCode
			,convert( nvarchar(1), NULL ) AS AccountStatusDesc
			,t.CustAccountSubProductCode AS AccountSubProductCode
			--,convert( tinyint, NULL ) AS OnUsPayerAccountAvgBalanceCycle1
			,t.CustAccountCurrentBalance AS OnUsPayerAccountCurrentBalance
		FROM [TDBRisk].[dbo].[CustAccountStatsDelta] AS t
		WHERE t.StatBatchId = stat.ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			AND NOT EXISTS
		--where @biTargetRowCount = 0 or not exists
				(
					SELECT 'X'
					FROM [dbo].[OnUsAccountStatsBulk] AS x
					WHERE x.AccountNumber = t.CustomerAccountNumber
						AND x.ClientOrgId = t.ClientOrgId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [dbo].[' + @nvDataFocus + N'Bulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

	END -- are there rows to process?


	--EXEC [tdb].[uspBatchDataSetRefreshLogTransferComplete]
	--	 @piStatBatchLogId = @iStatBatchLogId
	--	,@piBatchDataSetRefreshLogId = @iBatchDataSetRefreshLogId 
	--	,@psiBatchId = @iBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
	--	,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
	--	,@ptiDebug = @ptiDebug
	--;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @piStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
--		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
