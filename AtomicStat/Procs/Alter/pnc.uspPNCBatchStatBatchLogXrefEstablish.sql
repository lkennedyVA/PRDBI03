USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspPNCBatchStatBatchLogXrefEstablish

	Created By: VALIDRS\LWhiting
	
	Description: this stored procedure establishes a cross-ref 
		between a Stat.stat.BatchLog.BatchLogId 
		and an AtomicStat.pnc.PNCBatch.PNCBatchId,
		unless the BatchLogId already exists in 
		AtomicStat.pnc.PNCBatchStatBatchLogXref.StatBatchLogId.
		In either case, the corresponding AtomicStat.pnc.PNCBatch.PNCBatchId 
		value is returned via the output parameter @psiPNCBatchId.
		If there are no available PNCBatchId's, then @psiPNCBatchId 
		returns a NULL value.

	Example:

		DECLARE @iStatBatchLogId int = 135
			,@siPNCBatchId INT = NULL
		;
		EXEC pnc.uspPNCBatchStatBatchLogXrefEstablish @piStatBatchLogId = @iStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT 
		;
		SELECT @siPNCBatchId AS PNCBatchId
		SELECT StatBatchLogId = @iStatBatchLogId, PNCBatchId = @siPNCBatchId
		;
		SELECT * FROM [pnc].[PNCBatchStatBatchLogXref]
		;
		
	History:
		2019-07-22 - LSW - Created

*/
ALTER PROCEDURE [pnc].[uspPNCBatchStatBatchLogXrefEstablish]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@psiPNCBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1 = output feedback to Message tab.
	) 
AS 
BEGIN

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@siPNCBatchId INT = @psiPNCBatchId
	;
	SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000);
	
	IF @ptiDebug > 0 
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' )
		PRINT N''
	END

	SELECT
		@siPNCBatchId = PNCBatchId
	FROM [pnc].[PNCBatchStatBatchLogXref]
	WHERE StatBatchLogId = @iStatBatchLogId
	;

	IF @siPNCBatchId IS NULL
	BEGIN
	
		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId is NULL.'
		END

		SELECT
			@siPNCBatchId = MIN( mb.PNCBatchId )
		FROM DHayes.dbo.PNCBatch AS mb
		WHERE mb.PNCBatchTransferInitiatedDatetime IS NULL 
			AND mb.PNCBatchDeltaPromotionComplete = 0 
			AND NOT EXISTS( SELECT 'X' FROM [pnc].[PNCBatchStatBatchLogXref] AS x WHERE x.PNCBatchId = mb.PNCBatchId )  
--AND mb.PNCBatchId > 9999
		;
	
		IF @ptiDebug > 0 
			IF @siPNCBatchId IS NULL 
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId is _still_ NULL; no PNCBatchTransferInitiatedDatetime is NULL?, @siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' ) + N' exists in PNCBatchStatBatchLogXref?'
			ELSE
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' ) + N' and is no longer NULL.'
		;

	END

	IF @iStatBatchLogId IS NOT NULL 
		AND @siPNCBatchId IS NOT NULL
		AND @iStatBatchLogId <> @siPNCBatchId
		-- AtomicStat.stat.BatchLog.BatchStartDate must have a timestamp more recent than DHayes.dbo.PNCBatch.PNCBatchInitiatedDatetime.
		AND ( ISNULL( ( SELECT bl.BatchStartDate FROM AtomicStat.stat.BatchLog bl WHERE bl.BatchLogId = @iStatBatchLogId AND bl.OrgId = 100009 ), CONVERT( datetime2(0), '1901-01-01' ) ) > ISNULL( ( SELECT mb.PNCBatchInitiatedDatetime FROM DHayes.dbo.PNCBatch mb WHERE mb.PNCBatchId = @siPNCBatchId ), CONVERT( datetime2(0), '9999-12-31' ) ) )
		-- AtomicStat.stat.BatchLog.OrgId must be 100009 (PNC).
		AND ISNULL( ( SELECT bl.OrgId FROM AtomicStat.stat.BatchLog bl WHERE bl.BatchLogId = @iStatBatchLogId ), CONVERT( int, 0 ) ) = 100009
	BEGIN
	
		IF @ptiDebug > 0 
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' ) + N' and could be inserted into PNCBatchStatBatchLogXref.'
		;
			
		INSERT INTO [pnc].[PNCBatchStatBatchLogXref] 
			( 
				 PNCBatchId
				,StatBatchLogId 
			)
		SELECT
				@siPNCBatchId
			,@iStatBatchLogId
		WHERE NOT EXISTS( SELECT 'X' FROM [pnc].[PNCBatchStatBatchLogXref] AS x WHERE x.PNCBatchId = @siPNCBatchId ) -- if @siPNCBatchId already exists in any row, we can't add it again.
			AND NOT EXISTS( SELECT 'X' FROM [pnc].[PNCBatchStatBatchLogXref] AS x WHERE x.StatBatchLogId = @iStatBatchLogId ) -- if @iStatBatchLogId already exists in any row, we can't add it again, either.
		;

	END
	ELSE 
	BEGIN
		IF @ptiDebug > 0 
		BEGIN
			IF @iStatBatchLogId IS NULL OR @siPNCBatchId IS NULL
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'Cannot insert a NULL value into PNCBatchStatBatchLogXref.'
			IF @iStatBatchLogId = @siPNCBatchId
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'Batch values are suspect.'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'NULL' )
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' )
			SET @siPNCBatchId = NULL
			;
		END
	END

	SET @psiPNCBatchId = @siPNCBatchId
	;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END

GO
