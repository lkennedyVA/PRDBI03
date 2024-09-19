USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************

	Name: ???.uspOnUsAccountStatsDelta_TransferToAtomicStat

	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from ???Risk.dbo.OnUsAccountStatsDelta to AtomicStat.???.OnUsAccountStatsDelta.
		This does not take the horizontal rows of OnUsAccountStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2020-07-02 - VALIDRS\LWhiting - Created based upon [t d b].[uspOnUsAccountStatsDelta_TransferToAtomicStat]
		2020-07-21 - VALIDRS\LWhiting - Filled in the data gaps we left behind.

*****************************************************************************************/
ALTER PROCEDURE [mtb].[uspOnUsAccountStatsDelta_TransferToAtomicStat]
	(
		 @piStatBatchLogId INT
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	)
AS
BEGIN

	--SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000)
		,@tiDebug tinyint = CASE WHEN ISNULL( @ptiDebug, 0 ) > 0 THEN @ptiDebug + 1 ELSE 0 END
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
	
	IF @tiDebug > 0
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + @nvThisSourceCode
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug - 1 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@piStatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		--PRINT N''
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

	DECLARE @iStatBatchLogId INT = @piStatBatchLogId
		,@iBatchId int
		,@iDataSetRemainCount int
	;
	SELECT @iBatchId = [mtb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
	;

	
	IF ISNULL( @iStatBatchLogId, 0 ) > 0
	BEGIN -- are there rows to process?

		SELECT @siStatCount = 1
		;
		SELECT @biDataSetRowCount = COUNT_BIG(1)
		FROM [MTBRisk].[dbo].[CustAccountStatsDelta] d
		WHERE d.StatBatchId = [mtb].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
		;
		SELECT @biTargetRowCount = COUNT_BIG(1)
		FROM [mtb].[OnUsAccountStatsBulk] d
		;


		TRUNCATE TABLE [mtbprior].[OnUsAccountStatsBulk]
		;
		INSERT INTO [mtbprior].[OnUsAccountStatsBulk]
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
		FROM [mtb].[OnUsAccountStatsBulk]
		;

		IF @tiDebug > 0 
		BEGIN
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'Upsert [mtb].[' + @nvDataFocus + N'Bulk]'
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' raw source rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END



		SET @biRowCount = 0
		;
		UPDATE u
		SET StatBatchLogId = @iStatBatchLogId
			,CycleDate = casd.CycleDate
			,AccountCloseDate = a.AccountCloseDate
			,AccountHoldCode = a.AccountHoldCode
			,AccountRestrictionCode = aholdx.AccountRestrictionCode
			,AccountStatusDesc = astat.AccountStatusDesc
			,AccountSubProductCode = atd.AccountSubProductCode
			,OnUsPayerAccountAvgBalanceCycle1 = a.AverageLedgerBalanceCycle1
			,OnUsPayerAccountCurrentBalance = a.CurrentBalance
		FROM [MTBRisk].[dbo].[Account] AS a
			INNER JOIN [mtb].[OnUsAccountStatsBulk] u
				ON a.AccountNumber = u.AccountNumber
					AND a.ClientOrgId = u.ClientOrgId
			INNER JOIN [MTBRisk].[dbo].[AccountStatus] AS astat 
				ON a.[AccountStatusId] = astat.[AccountStatusId]
			LEFT JOIN [MTBRisk].[dbo].[CustAccountStatsDelta] AS casd
				ON a.[AccountNumber] = casd.[CustomerAccountNumber]
					AND casd.[StatBatchId] = [mtb].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
			LEFT JOIN [MTBRisk].[dbo].[AccountHoldCodeRestrictionXref] AS aholdx 
				ON a.[AccountHoldCode] = aholdx.[AccountHoldCode]
			LEFT JOIN [MTBRisk].[dbo].[AccountTypeDetail] AS atd
				ON a.[AccountTypeDetailId] = atd.[AccountTypeDetailId]
		WHERE	@biTargetRowCount > 0 -- if there are no rows in the target table, then we don't want to "update" any rows with data from the source table.
			AND casd.StatBatchId = [mtb].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
			AND u.StatBatchLogId <= @iStatBatchLogId
			AND ( 
					-- Do _NOT_ test for CycleDate.
						ISNULL( u.AccountCloseDate, @dateDeepestValue ) <> ISNULL( a.AccountCloseDate, @dateDeepestValue )
					OR ISNULL( u.AccountHoldCode, @nvNullAlternativeValue ) <> ISNULL( a.AccountHoldCode, @nvNullAlternativeValue )
					OR ISNULL( u.AccountRestrictionCode, @nvNullAlternativeValue ) <> ISNULL( aholdx.AccountRestrictionCode, @nvNullAlternativeValue )
					OR ISNULL( u.AccountStatusDesc, @nvNullAlternativeValue ) <> ISNULL( astat.AccountStatusDesc, @nvNullAlternativeValue )
					OR ISNULL( u.AccountSubProductCode, @nvNullAlternativeValue ) <> ISNULL( atd.AccountSubProductCode, @nvNullAlternativeValue )
					OR ISNULL( u.OnUsPayerAccountAvgBalanceCycle1, @numeric16_2DeepestNegativeValue ) <> ISNULL( a.AverageLedgerBalanceCycle1, @numeric16_2DeepestNegativeValue )
					OR ISNULL( u.OnUsPayerAccountCurrentBalance, @numeric16_2DeepestNegativeValue ) <> ISNULL( a.CurrentBalance, @numeric16_2DeepestNegativeValue )
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @tiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in [mtb].[' + @nvDataFocus + N'Bulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END



		SET @biRowCount = 0
		;
		INSERT INTO [mtb].[OnUsAccountStatsBulk]
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
			  @iStatBatchLogId AS [StatBatchLogId]
			, a.[AccountNumber]
			, a.[ClientOrgId]
			, ISNULL( casd.[CycleDate], a.[LastReceivedCycleDate] ) AS [CycleDate] -- TO-DO: ask Diana about how we should obtain the CycleDate
			, [AccountCloseDate]
			, a.[AccountHoldCode]
			, aholdx.[AccountRestrictionCode]
			--, a.[AccountStatusId] -- TO-DO: Comment this out or remove when done developing/troubleshooting
			, astat.[AccountStatusDesc]
			--, a.[AccountTypeDetailId] -- TO-DO: Comment this out or remove when done developing/troubleshooting
			, atd.[AccountSubProductCode]
			, a.[AverageLedgerBalanceCycle1] AS [OnUsPayerAccountAvgBalanceCycle1]
			, a.[CurrentBalance] AS [OnUsPayerAccountCurrentBalance]

		FROM [MTBRisk].[dbo].[Account] AS a
			INNER JOIN [MTBRisk].[dbo].[AccountStatus] AS astat 
				ON a.[AccountStatusId] = astat.[AccountStatusId]
			LEFT JOIN [MTBRisk].[dbo].[CustAccountStatsDelta] AS casd -- TO-DO: ask Diana about how we should obtain the CycleDate
				ON a.[AccountNumber] = casd.[CustomerAccountNumber]
					AND casd.[StatBatchId] = [mtb].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
			LEFT JOIN [MTBRisk].[dbo].[AccountHoldCodeRestrictionXref] AS aholdx 
				ON a.[AccountHoldCode] = aholdx.[AccountHoldCode]
			LEFT JOIN [MTBRisk].[dbo].[AccountTypeDetail] AS atd
				ON a.[AccountTypeDetailId] = atd.[AccountTypeDetailId]
		WHERE NOT EXISTS
		--where @biTargetRowCount = 0 or not exists
				(
					SELECT 'X'
					FROM [mtb].[OnUsAccountStatsBulk] AS x
					WHERE x.[AccountNumber] = a.[AccountNumber]
						AND x.[ClientOrgId] = a.[ClientOrgId]
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @tiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [mtb].[' + @nvDataFocus + N'Bulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

	END -- are there rows to process?


	IF @tiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@piStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @piStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
--		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + @nvThisSourceCode
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug - 1 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
