USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [tdb].uspOnUsPayerRoutingNumber_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer new data from TDBRisk.dbo.OnUsPayerRoutingNumbers to tdb.OnUsPayerRoutingNumberBulk
		when there are differences detected between TDBRisk.dbo.OnUsPayerRoutingNumbers and tdb.OnUsPayerRoutingNumber.
		
	History:
		2020-01-12 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [tdb].[uspOnUsPayerRoutingNumber_TransferToAtomicStat]
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
		AND EXISTS( SELECT 'X' FROM TDBRisk.dbo.OnUsPayerRoutingNumber x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [tdb].[StatAtomicStatXref]
		--WHERE [ObjectName] = N'ABAAcctLengthStatsDelta'
		;
		SELECT @biDataSetRowCount = COUNT(1)
		FROM [TDBRisk].[dbo].[OnUsPayerRoutingNumber]
		--WHERE ClientOrgId = 100010
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into [tdb].[OnUsPayerRoutingNumberBulk]'
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		TRUNCATE TABLE [tdbprior].[OnUsPayerRoutingNumberBulk]
		;
		INSERT INTO [tdbprior].[OnUsPayerRoutingNumberBulk]
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
			)
		SELECT 
			 [StatBatchLogId]
			,[PayerRoutingNumber]
			,[ClientOrgId]
		FROM [tdb].[OnUsPayerRoutingNumberBulk]
		;


		DECLARE @bitPerformRefresh bit = 0
		;
		--WITH cteSource AS
		--	(	
		--		SELECT
		--			 PayerRoutingNumber
		--			,BankId AS ClientOrgId
		--		FROM [TDBRisk].[dbo].[OnUsPayerRoutingNumbers]
		--			WHERE BankId = 100010
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
		--		FROM [tdbprior].[OnUsPayerRoutingNumber]
		--	)
		--SELECT @bitPerformRefresh = CASE WHEN COUNT(1) > 0 THEN 1 ELSE 0 END
		--FROM cteDif
		--;
		--SELECT @bitPerformRefresh = 1
		--WHERE @bitPerformRefresh = 0
		--	AND NOT EXISTS( SELECT 'X' FROM [tdb].[OnUsPayerRoutingNumberBulk] )
		--;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'@bitPerformRefresh = ' + CONVERT( nvarchar(1), @bitPerformRefresh )
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		--IF @bitPerformRefresh = 1 AND EXISTS( SELECT 'X' FROM [tdb].[OnUsPayerRoutingNumberBulk] ) TRUNCATE TABLE [tdbprior].[OnUsPayerRoutingNumber]
		--;
		--INSERT INTO [tdbprior].[OnUsPayerRoutingNumber]
		--	(
		--		 StatBatchLogId
		--		,PayerRoutingNumber
		--		,ClientOrgId
		--	)
		--SELECT 
		--	 @iStatBatchLogId AS StatBatchLogId
		--	,[PayerRoutingNumber]
		--	,[ClientOrgId]
		--FROM [tdb].[OnUsPayerRoutingNumberBulk] -- [TDBRisk].[dbo].[OnUsPayerRoutingNumbers]
		--WHERE [ClientOrgId] = 100010
		--	AND @bitPerformRefresh = 1
		--ORDER BY [PayerRoutingNumber]
		--;

		--IF @bitPerformRefresh = 1 TRUNCATE TABLE [tdb].[OnUsPayerRoutingNumberBulk]
		--;
		INSERT INTO [tdb].[OnUsPayerRoutingNumberBulk]
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
			)
		SELECT 
			 @iStatBatchLogId AS StatBatchLogId
			,[PayerRoutingNumber]
			,[ClientOrgId]
		FROM [TDBRisk].[dbo].[OnUsPayerRoutingNumber] t
		WHERE NOT EXISTS
			(
				SELECT 'X'
				FROM [tdb].[OnUsPayerRoutingNumberBulk] x
				WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
			)
		--WHERE [BankId] = 100010
		--	AND @bitPerformRefresh = 1
		ORDER BY [PayerRoutingNumber]
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			IF @bitPerformRefresh = 1 PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Changes detected in [TDBRisk].[dbo].[OnUsPayerRoutingNumbers].'
				ELSE PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'No changes detected in [TDBRisk].[dbo].[OnUsPayerRoutingNumbers].'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [tdb].[OnUsPayerRoutingNumberBulk].'
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
