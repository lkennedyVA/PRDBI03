USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspBatchStatBatchLogXrefEstablish

	Created By: VALIDRS\LWhiting
	
	Description: this stored procedure establishes a cross-ref 
		between a AtomicStat.stat.BatchLog.BatchLogId and a DHayes.dbo.RetailBatch.RetailBatchId,
		unless the BatchLogId already exists in AtomicStat.retail.BatchStatBatchLogXref.StatBatchLogId.

		In either case, the corresponding DHayes.dbo.RetailBatch.RetailBatchId value is returned 
		via the output parameter @piBatchId.
		If there are no available RetailBatchId's, then @piBatchId returns a NULL value.

	Example:

		DECLARE @iStatBatchLogId int = 30337
			,@iBatchId INT = NULL
		;
		EXEC retail.uspBatchStatBatchLogXrefEstablish @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId OUTPUT 
		;
		SELECT StatBatchLogId = @iStatBatchLogId, BatchId = @iBatchId
		;
		SELECT * FROM [retail].[BatchStatBatchLogXref] WHERE StatBatchLogId = @iStatBatchLogId
		;
		
	History:
		2019-10-07 - LSW - Created

*/
ALTER PROCEDURE [retail].[uspBatchStatBatchLogXrefEstablish]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of AtomicStat.stat.BatchLog.BatchLogId
		,@piBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1 = output feedback to Message tab.
	) 
AS 
BEGIN

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iBatchId INT = @piBatchId
		,@tiDebug tinyint = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
		,@nvThisSourceCode NVARCHAR(4000)
	;
	--SET @tiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;

	IF @tiDebug > 0 
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'StatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'       BatchId = ' + ISNULL( CONVERT( nvarchar(50), @piBatchId ), N'{null}' )
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

	SELECT
		@iBatchId = BatchId
	FROM [retail].[BatchStatBatchLogXref]
	WHERE @iStatBatchLogId IS NOT NULL
		AND StatBatchLogId = @iStatBatchLogId
	;

	IF @iBatchId IS NULL
	BEGIN -- Look in DHayes.dbo.RetailBatch for the latest BatchId to work with.
	
		SET @tiDebug = @tiDebug + 1
		;
		IF @tiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId is NULL.'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		SELECT
			@iBatchId = MIN( b.RetailBatchId )
		FROM DHayes.dbo.RetailBatch AS b
		WHERE b.RetailBatchTransferInitiatedDatetime IS NULL 
			AND b.RetailBatchDeltaPromotionComplete = 0 
			AND NOT EXISTS( SELECT 'X' FROM [retail].[BatchStatBatchLogXref] AS x WHERE x.BatchId = b.RetailBatchId )  
		;
	
		IF @tiDebug > 0 
			IF @iBatchId IS NULL 
				PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId is _still_ NULL; no BatchTransferInitiatedDatetime is NULL?; @iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' ) + N' exists in BatchStatBatchLogXref?'
			ELSE
				PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' ) + N' and is no longer NULL.'
		;
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		SET @tiDebug = @tiDebug - 1
		;

	END -- Look in DHayes.dbo.RetailBatch for the latest BatchId to work with.


	IF @iStatBatchLogId IS NOT NULL 
		AND @iBatchId IS NOT NULL
		AND @iStatBatchLogId <> @iBatchId
		-- AtomicStat.stat.BatchLog.BatchStartDate must have a timestamp more recent than DHayes.dbo.Batch.BatchInitiatedDatetime...
		AND ( ISNULL( ( SELECT bl.BatchStartDate FROM AtomicStat.stat.BatchLog bl WHERE bl.BatchLogId = @iStatBatchLogId AND bl.OrgId = 99999 ), CONVERT( datetime2(0), '1901-01-01' ) ) > ISNULL( ( SELECT b.RetailBatchInitiatedDatetime FROM DHayes.dbo.RetailBatch AS b WHERE b.RetailBatchId = @iBatchId ), CONVERT( datetime2(0), '9999-12-31' ) ) )
		-- AtomicStat.stat.BatchLog.OrgId must be 99999 (Retail)...
		AND ISNULL( ( SELECT bl.OrgId FROM AtomicStat.stat.BatchLog bl WHERE bl.BatchLogId = @iStatBatchLogId ), CONVERT( int, 0 ) ) = 99999
	BEGIN
	
		IF @tiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' ) + N' and could be inserted into BatchStatBatchLogXref.';
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END
			
		INSERT INTO [retail].[BatchStatBatchLogXref] 
			( 
				 BatchId
				,StatBatchLogId 
			)
		SELECT
			 @iBatchId AS BatchId
			,@iStatBatchLogId AS StatBatchLogId
		WHERE NOT EXISTS( SELECT 'X' FROM [retail].[BatchStatBatchLogXref] AS x WHERE x.BatchId = @iBatchId ) -- if @iBatchId already exists in any row, we can't add it again.
			AND NOT EXISTS( SELECT 'X' FROM [retail].[BatchStatBatchLogXref] AS x WHERE x.StatBatchLogId = @iStatBatchLogId ) -- if @iStatBatchLogId already exists in any row, we can't add it again, either.
		;

	END
	ELSE 
	BEGIN
		IF @tiDebug > 0 
		BEGIN
			SET @tiDebug = @tiDebug + 1
			;
			IF @iStatBatchLogId IS NULL OR @iBatchId IS NULL
				PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'Cannot insert a NULL value into BatchStatBatchLogXref.'
			IF @iStatBatchLogId = @iBatchId
				PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'Batch values are suspect.'
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'       @iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
			SET @tiDebug = @tiDebug - 1
			;
			SET @iBatchId = NULL -- Nullifying @iBatchId because a returned OUTPUT value of NULL is a sign of something went wrong.
			;
		END
	END

	SET @piBatchId = @iBatchId -- setting the OUTPUT value
	;

	IF @tiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'StatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'       BatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + @nvThisSourceCode
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END

GO
