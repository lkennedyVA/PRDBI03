USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ???.uspOnUsPayerRoutingNumber_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer new data from ???Risk.dbo.OnUsPayerRoutingNumbers to AtomicStat.???.OnUsPayerRoutingNumberBulk
		when there are differences detected between ???Risk.dbo.OnUsPayerRoutingNumbers and ???.OnUsPayerRoutingNumber.
		
	History:
		2020-07-02 - VALIDRS\LWhiting - Created based upon [t d b].[uspOnUsPayerRoutingNumber_TransferToAtomicStat]

*****************************************************************************************/
ALTER PROCEDURE [mtb].[uspOnUsPayerRoutingNumber_TransferToAtomicStat]
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
		,@iDataSetRemainCount int
	;

--print N'...no OnUs work until OnUs data is aligned...'

	IF @iStatBatchLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM MTBRisk.dbo.OnUsPayerRoutingNumber x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [mtb].[StatAtomicStatXref]
		--WHERE [ObjectName] = N'ABAAcctLengthStatsDelta'
		;
		SELECT @biDataSetRowCount = COUNT(1)
		FROM [MTBRisk].[dbo].[OnUsPayerRoutingNumber]
		--WHERE ClientOrgId = 100008
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into [mtb].[OnUsPayerRoutingNumberBulk]'
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		TRUNCATE TABLE [mtbprior].[OnUsPayerRoutingNumberBulk]
		;
		INSERT INTO [mtbprior].[OnUsPayerRoutingNumberBulk]
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
			)
		SELECT 
			 [StatBatchLogId]
			,[PayerRoutingNumber]
			,[ClientOrgId]
		FROM [mtb].[OnUsPayerRoutingNumberBulk]
		;


		DECLARE @bitPerformRefresh bit = 0
		;
		--WITH cteSource AS
		--	(	
		--		SELECT
		--			 PayerRoutingNumber
		--			,BankId AS ClientOrgId
		--		FROM [MTBRisk].[dbo].[OnUsPayerRoutingNumbers]
		--			WHERE BankId = 100008
		--	)
		--,cteDif AS
		--	(
		--		SELECT
		--			 PayerRoutingNumber
		--			,ClientOrgId
		--		FROM cteSource
		--		EXCEPT
		--		SELECT
		--			 PayerRoutingNumber
		--			,ClientOrgId
		--		FROM [mtbprior].[OnUsPayerRoutingNumber]
		--	)
		--SELECT @bitPerformRefresh = CASE WHEN COUNT(1) > 0 THEN 1 ELSE 0 END
		--FROM cteDif
		--;
		--SELECT @bitPerformRefresh = 1
		--WHERE @bitPerformRefresh = 0
		--	AND NOT EXISTS( SELECT 'X' FROM [mtb].[OnUsPayerRoutingNumberBulk] )
		--;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'@bitPerformRefresh = ' + CONVERT( nvarchar(1), @bitPerformRefresh )
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		--IF @bitPerformRefresh = 1 AND EXISTS( SELECT 'X' FROM [mtb].[OnUsPayerRoutingNumberBulk] ) TRUNCATE TABLE [mtbprior].[OnUsPayerRoutingNumber]
		--;
		--INSERT INTO [mtbprior].[OnUsPayerRoutingNumber]
		--	(
		--		 StatBatchLogId
		--		,PayerRoutingNumber
		--		,ClientOrgId
		--	)
		--SELECT 
		--	 @iStatBatchLogId AS StatBatchLogId
		--	,[PayerRoutingNumber]
		--	,[ClientOrgId]
		--FROM [mtb].[OnUsPayerRoutingNumberBulk] -- [MTBRisk].[dbo].[OnUsPayerRoutingNumbers]
		--WHERE [ClientOrgId] = 100008
		--	AND @bitPerformRefresh = 1
		--ORDER BY [PayerRoutingNumber]
		--;

		--IF @bitPerformRefresh = 1 TRUNCATE TABLE [mtb].[OnUsPayerRoutingNumberBulk]
		--;
		INSERT INTO [mtb].[OnUsPayerRoutingNumberBulk]
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
			)
		SELECT 
			 @iStatBatchLogId AS StatBatchLogId
			,[PayerRoutingNumber]
			,[ClientOrgId]
		FROM [MTBRisk].[dbo].[OnUsPayerRoutingNumber] t
		WHERE NOT EXISTS
				(
					SELECT 'X'
					FROM [mtb].[OnUsPayerRoutingNumberBulk] x
					WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
				)
			-- AND [ClientOrgId] = 100008
			--	AND @bitPerformRefresh = 1
		ORDER BY [PayerRoutingNumber]
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			IF @bitPerformRefresh = 1 PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Changes detected in [MTBRisk].[dbo].[OnUsPayerRoutingNumbers].'
				ELSE PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'No changes detected in [MTBRisk].[dbo].[OnUsPayerRoutingNumbers].'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [mtb].[OnUsPayerRoutingNumberBulk].'
		END

	END -- are there rows to process?


	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @piStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
