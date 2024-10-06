USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspMTBPayerGradeNegDelta_TransferToAtomicStat
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 52A12592-7AE6-4045-A90D-9EA6FE4631B5
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   PROCEDURE [mtb].[uspMTBPayerGradeNegDelta_TransferToAtomicStat]
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
		,@iMTBBatchId int
		,@iMTBBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iMTBBatchDataSetRefreshLogId = mtb.ufnMTBBatchDataSetRefreshLogIdGetByBatchLogIdMTBDataSetName( @siStatBatchLogId, N'MTBPayerGradeNegDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iMTBBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iMTBBatchDataSetRefreshLogId ), N'NULL' ) + N' via mtb.ufnMTBBatchDataSetRefreshLogIdGetByBatchLogIdMTBDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''MTBPayerGradeNegDelta'' )'
	END

	EXEC mtb.uspMTBBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvMTBDataSetName = N'MTBPayerGradeNegDelta'
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
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.MTBPayerGradeNegDelta x WHERE x.MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
		;
		SELECT @siStatCount = COUNT(1)
		FROM [mtb].[MTBStatAtomicStatXref]
		WHERE [ObjectName] = N'MTBPayerGradeNegDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.MTBBatchDataSetRefreshLog 
		WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @siStatBatchLogId ) AND MTBDataSetName = N'MTBPayerGradeNegDelta'
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Upsert MTBPayerGradeNegDelta into mtb.PayerBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N' ...estimated ' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' potential source stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		UPDATE u
		SET StatBatchLogId = @siStatBatchLogId
			,StatValue = NULLIF( t.StatValue, N'☠' )
		FROM [mtb].[ufnMTBPayerGradeNegDelta_TranslateRowToCell]( @siStatBatchLogId ) t
			INNER JOIN mtb.PayerBulk u
				ON t.StatId = u.StatId
					AND t.HashId = u.HashId
		WHERE	t.StatValue <> ISNULL( u.StatValue, N'☠' )
			AND u.StatBatchLogId < @siStatBatchLogId
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in mtb.PayerBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

		INSERT INTO mtb.PayerBulk
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
			,NULLIF( t.StatValue, N'☠' ) AS StatValue
			,[PayerRoutingNumber],[PayerAccountNumber]
			,100008 AS ClientOrgId
		FROM [mtb].[ufnMTBPayerGradeNegDelta_TranslateRowToCell]( @siStatBatchLogId ) t
		WHERE ISNULL( @iMTBBatchDataSetRefreshLogId, 0 ) > 0
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM mtb.PayerBulk x
					--WHERE x.StatBatchLogId = @siStatBatchLogId
					WHERE x.StatId = t.StatId
						AND x.HashId = t.HashId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into mtb.PayerBulk.'
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
