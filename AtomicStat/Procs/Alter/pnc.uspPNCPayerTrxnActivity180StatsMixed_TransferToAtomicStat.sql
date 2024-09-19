USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCPayerTrxnActivity180StatsMixed_TransferToAtomicStat
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 380480C0-301C-4115-9F60-AEC2899D8168
	Generation set version: A13233E5-51BF-4F78-A24A-4989C9D7764C
	Description: Upsert pnc.PayerBulk from the relevant unpivoted stats of PNCPayerTrxnActivity180StatsFullSet.
		
	History:
		2019-07-31 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   PROCEDURE [pnc].[uspPNCPayerTrxnActivity180StatsMixed_TransferToAtomicStat]
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + CONVERT( nvarchar(50), @psiStatBatchLogId )
		PRINT N''
	END

	DECLARE @siStatBatchLogId INT = @psiStatBatchLogId
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCPayerTrxnActivity180StatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N', N''PNCPayerTrxnActivity180StatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCPayerTrxnActivity180StatsDelta'
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
		--AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCPayerTrxnActivity180StatsFullSet x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
		;
		SELECT @siStatCount = COUNT(1)
		FROM [pnc].[PNCStatAtomicStatXref] x
		WHERE x.[ObjectName] = N'PNCPayerTrxnActivity180StatsDelta'
			AND x.IsTransported = 1
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCPayerTrxnActivity180StatsDelta'
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Upsert PNCPayerTrxnActivity180StatsFullSet into pnc.PayerBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N' ...estimated ' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' potential source stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		UPDATE u -- we only perform UPDATEs in Delta mode
		SET StatBatchLogId = @siStatBatchLogId
			--,StatValue = NULLIF( t.StatValue, N'☠' )
			,StatValue = t.StatValue
		FROM [pnc].[ufnPNCPayerTrxnActivity180StatsFullSet_TranslateRowToCell]( @siStatBatchLogId ) t
			INNER JOIN pnc.PayerBulk u
				ON t.StatId = u.StatId
					AND t.HashId = u.HashId
		WHERE	ISNULL( t.StatValue, N'☠' ) <> ISNULL( u.StatValue, N'☠' )
			AND u.StatBatchLogId < @siStatBatchLogId
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in pnc.PayerBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
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
		FROM [pnc].[ufnPNCPayerTrxnActivity180StatsFullSet_TranslateRowToCell]( @siStatBatchLogId ) t
		WHERE ISNULL( @iPNCBatchDataSetRefreshLogId, 0 ) > 0
			--AND t.StatValue <> N'☠' -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
			AND t.StatValue IS NOT NULL -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM pnc.PayerBulk x
					WHERE x.StatId = t.StatId
						AND x.HashId = t.HashId
				)
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
