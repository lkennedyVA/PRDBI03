USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCOnUsPayerRoutingNumber_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer new data from DHayes.dbo.PNCOnUsPayerRoutingNumbers to pnc.OnUsPayerRoutingNumberBulk
		when there are differences detected between PNCprior.PNCOnUsPayerRoutingNumber and DHayes.dbo.PNCOnUsPayerRoutingNumbers.
		
	History:
		2019-07-22 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [pnc].[uspPNCOnUsPayerRoutingNumber_TransferToAtomicStat]
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
		,@iDataSetRemainCount int
	;

print N'...no OnUs work until OnUs data is aligned...'
/*
	IF @siStatBatchLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCOnUsPayerRoutingNumbers x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [pnc].[PNCStatAtomicStatXref]
		--WHERE [ObjectName] = N'PNCABAAcctLengthStatsDelta'
		;
		SELECT @biDataSetRowCount = COUNT(1)
		FROM DHayes.dbo.PNCOnUsPayerRoutingNumbers
		WHERE BankId = 100009
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into pnc.OnUsPayerRoutingNumberBulk'
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		TRUNCATE TABLE PNCprior.OnUsPayerRoutingNumberBulk
		;
		INSERT INTO PNCprior.OnUsPayerRoutingNumberBulk
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
			)
		SELECT 
			 [StatBatchLogId]
			,[PayerRoutingNumber]
			,[ClientOrgId]
		FROM pnc.OnUsPayerRoutingNumberBulk
		;


		DECLARE @bitPerformRefresh bit = 0
		;
		--WITH cteSource AS
		--	(	
		--		SELECT
		--			 PayerRoutingNumber
		--			,BankId AS ClientOrgId
		--		FROM [DHayes].[dbo].[PNCOnUsPayerRoutingNumbers]
		--			WHERE BankId = 100009
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
		--		FROM PNCprior.PNCOnUsPayerRoutingNumber
		--	)
		--SELECT @bitPerformRefresh = CASE WHEN COUNT(1) > 0 THEN 1 ELSE 0 END
		--FROM cteDif
		--;
		--SELECT @bitPerformRefresh = 1
		--WHERE @bitPerformRefresh = 0
		--	AND NOT EXISTS( SELECT 'X' FROM pnc.OnUsPayerRoutingNumberBulk )
		--;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'@bitPerformRefresh = ' + CONVERT( nvarchar(1), @bitPerformRefresh )
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		--IF @bitPerformRefresh = 1 AND EXISTS( SELECT 'X' FROM pnc.OnUsPayerRoutingNumberBulk ) TRUNCATE TABLE PNCprior.PNCOnUsPayerRoutingNumber
		--;
		--INSERT INTO PNCprior.PNCOnUsPayerRoutingNumber
		--	(
		--		 StatBatchLogId
		--		,PayerRoutingNumber
		--		,ClientOrgId
		--	)
		--SELECT 
		--	 @siStatBatchLogId AS StatBatchLogId
		--	,[PayerRoutingNumber]
		--	,[ClientOrgId]
		--FROM pnc.OnUsPayerRoutingNumberBulk -- [DHayes].[dbo].[PNCOnUsPayerRoutingNumbers]
		--WHERE [ClientOrgId] = 100009
		--	AND @bitPerformRefresh = 1
		--ORDER BY [PayerRoutingNumber]
		--;

		--IF @bitPerformRefresh = 1 TRUNCATE TABLE pnc.OnUsPayerRoutingNumberBulk
		--;
		INSERT INTO pnc.OnUsPayerRoutingNumberBulk
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
			)
		SELECT 
			 @siStatBatchLogId AS StatBatchLogId
			,[PayerRoutingNumber]
			,[BankId] AS ClientOrgId
		FROM [DHayes].[dbo].[PNCOnUsPayerRoutingNumbers] t
		WHERE NOT EXISTS
			(
				SELECT 'X'
				FROM pnc.OnUsPayerRoutingNumberBulk x
				WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
			)
		--WHERE [BankId] = 100009
		--	AND @bitPerformRefresh = 1
		ORDER BY [PayerRoutingNumber]
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			IF @bitPerformRefresh = 1 PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Changes detected in DHayes.dbo.PNCOnUsPayerRoutingNumbers.'
				ELSE PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'No changes detected in DHayes.dbo.PNCOnUsPayerRoutingNumbers.'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.OnUsPayerRoutingNumberBulk.'
		END

	END -- are there rows to process?
*/

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @psiStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
