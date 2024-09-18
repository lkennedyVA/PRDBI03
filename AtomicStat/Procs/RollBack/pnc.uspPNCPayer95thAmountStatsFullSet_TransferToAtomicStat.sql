USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCPayer95thAmountStatsFullSet_TransferToAtomicStat
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 447F150F-B138-44A9-9E54-36A4B1BE408C
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   PROCEDURE [pnc].[uspPNCPayer95thAmountStatsFullSet_TransferToAtomicStat]
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + CONVERT( nvarchar(50), @psiStatBatchLogId )
		PRINT N''
	END

	DECLARE @siStatBatchLogId smallint = @psiStatBatchLogId
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = PNCBatchId FROM pnc.PNCBatchStatBatchLogXref WHERE StatBatchLogId = @siStatBatchLogId
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N', N''PNCPayer95thAmountStatsFullSet'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCPayer95thAmountStatsFullSet'
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
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
		;
		SELECT @siStatCount = COUNT(1)
		FROM [pnc].[PNCStatAtomicStatXref] x
		WHERE x.[ObjectName] = N'PNCPayer95thAmountStatsFullSet'
			AND x.IsTransported = 1
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCPayer95thAmountStatsFullSet'
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Upsert PNCPayer95thAmountStatsFullSet into pnc.PayerBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N' ...estimated ' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' potential source stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		INSERT INTO pnc.PayerBulk
			(
				 StatId
				,HashId
				,StatBatchLogId
				,StatValue
				,[RoutingNumber],[AccountNumber]
				,ClientOrgId
			)
		SELECT
			 StatId
			,HashId
			,@siStatBatchLogId AS StatBatchLogId
			,t.StatValue AS StatValue -- FullSet mode _and_ Delta mode( the WHERE filters out any NULLs from being inserted )
			,[PayerRoutingNumber],[PayerAccountNumber]
			,100009 AS ClientOrgId
		FROM [pnc].[ufnPNCPayer95thAmountStatsFullSet_TranslateRowToCell]( @siStatBatchLogId ) t
		WHERE ISNULL( @iPNCBatchDataSetRefreshLogId, 0 ) > 0
			--AND t.StatValue <> N'☠' -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
			AND t.StatValue IS NOT NULL -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.PayerBulk.'
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
