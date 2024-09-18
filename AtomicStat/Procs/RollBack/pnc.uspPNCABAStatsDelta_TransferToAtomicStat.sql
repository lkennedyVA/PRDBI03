USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCABAStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from PNCABAStatsDelta to ABAStatsBulk.
		This does not take the horizontal rows of PNCABAStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-07-22 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [pnc].[uspPNCABAStatsDelta_TransferToAtomicStat]
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
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCABAStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''PNCABAStatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCABAStatsDelta'
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
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCABAStatsDelta x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [pnc].[PNCStatAtomicStatXref]
		--WHERE [ObjectName] = N'PNCABAStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCABAStatsDelta'
		;


		TRUNCATE TABLE PNCprior.ABAStatsBulk
		;
		INSERT INTO PNCprior.ABAStatsBulk
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
				,CycleDate
				,PayerRtnNumberVolumeToDate
			)
		SELECT
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
				,CycleDate
				,PayerRtnNumberVolumeToDate
		FROM pnc.ABAStatsBulk
		;


		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into pnc.ABAStatsBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END


		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.ABAStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		UPDATE u
		SET StatBatchLogId = @siStatBatchLogId
			,CycleDate = t.CycleDate
			,PayerRtnNumberVolumeToDate = t.PayerRtnNumberVolumeToDate
		FROM DHayes.dbo.PNCABAStatsDelta t
			INNER JOIN pnc.ABAStatsBulk u
				ON t.PayerRoutingNumber = u.PayerRoutingNumber
					--AND t.HashId = u.HashId
		WHERE	ISNULL( t.PayerRtnNumberVolumeToDate, @biDeepestNegativeValue ) <> ISNULL( u.PayerRtnNumberVolumeToDate, @biDeepestNegativeValue )
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND u.StatBatchLogId < @siStatBatchLogId
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in pnc.ABAStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END


		INSERT INTO pnc.ABAStatsBulk
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,ClientOrgId
				,CycleDate
				,PayerRtnNumberVolumeToDate
			)
		SELECT
			 @siStatBatchLogId AS StatBatchLogId
			,PayerRoutingNumber
			,100009 AS ClientOrgId
			,CycleDate
			,PayerRtnNumberVolumeToDate
		FROM DHayes.dbo.PNCABAStatsDelta t
		WHERE ISNULL( @iPNCBatchDataSetRefreshLogId, 0 ) > 0
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM pnc.ABAStatsBulk x
					WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
						--AND x.StatBatchLogId = @siStatBatchLogId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
