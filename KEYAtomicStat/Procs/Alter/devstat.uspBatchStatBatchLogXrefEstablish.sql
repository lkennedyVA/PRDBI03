USE [KEYAtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspBatchStatBatchLogXrefEstablish

	Created By: VALIDRS\LWhiting
	
	Description: this stored procedure establishes a cross-ref 
		between a AtomicStat.stat.BatchLog.BatchLogId and a ???Risk.dbo.StatBatch.StatBatchId,
		unless the BatchLogId already exists in AtomicStat.???.BatchStatBatchLogXref.StatBatchLogId or dbo.BatchStatBatchLogXref.StatBatchLogId.

		In either case, the corresponding ???Risk.dbo.StatBatch.StatBatchId value is returned 
		via the output parameter @piBatchId.
		If there are no available StatBatchId's, then @piBatchId returns a NULL value.

	Example:

		DECLARE @iStatBatchLogId int = 30337
			,@iBatchId INT = NULL
		;
		EXEC stat.uspBatchStatBatchLogXrefEstablish @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId OUTPUT 
		;
		SELECT StatBatchLogId = @iStatBatchLogId, BatchId = @iBatchId
		;
		SELECT * FROM [stat].[BatchStatBatchLogXref] WHERE StatBatchLogId = @iStatBatchLogId
		;
		
	History:
		2019-10-07 - LSW - Created

*/
ALTER PROCEDURE [devstat].[uspBatchStatBatchLogXrefEstablish]
	( 
		 @piStatBatchLogId int -- by way of stat.BatchLog.BatchLogId
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
	FROM [stat].[BatchStatBatchLogXref]
	WHERE @iStatBatchLogId IS NOT NULL
		AND StatBatchLogId = @iStatBatchLogId
	;
	SET @iBatchId = ISNULL( @iBatchId, @piBatchId )
	;

	IF @iBatchId IS NULL -- if @iBatchId is _still_ NULL...
	BEGIN -- Look in ???Risk.dbo.StatBatch for the latest BatchId to work with.
	
		SET @tiDebug = @tiDebug + 1
		;
		IF @tiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId is NULL.'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END

		SELECT
			@iBatchId = MIN( b.StatBatchId )
		FROM [risk].[StatBatch] AS b
		WHERE b.StatBatchTransferInitiatedDatetime IS NULL 
			AND b.StatBatchDeltaPromotionComplete = 0 
			AND NOT EXISTS( SELECT 'X' FROM [stat].[BatchStatBatchLogXref] AS x WHERE x.BatchId = b.StatBatchId )  
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

	END -- Look in ???Risk.dbo.StatBatch for the latest BatchId to work with.


	IF @iStatBatchLogId IS NOT NULL 
		AND @iBatchId IS NOT NULL
		AND @iStatBatchLogId <> @iBatchId
		-- stat.BatchLog.BatchStartDate must have a timestamp more recent than ???Risk.dbo.Batch.BatchInitiatedDatetime...
		AND ( ISNULL( ( SELECT bl.BatchStartDate FROM /*[Atomic Stat].*/[stat].[BatchLog] bl WHERE bl.BatchLogId = @iStatBatchLogId AND bl.OrgId = ( SELECT c.[ClientOrgId] FROM [dbo].[Client] AS c ) ), CONVERT( datetime2(0), '1901-01-01' ) ) > ISNULL( ( SELECT b.StatBatchInitiatedDatetime FROM [risk].[StatBatch] AS b WHERE b.StatBatchId = @iBatchId ), CONVERT( datetime2(0), '9999-12-31' ) ) )
		-- stat.BatchLog.OrgId must be 1 0 0 0 1 0 (T D B)...
		AND ISNULL( ( SELECT bl.OrgId FROM /*[Atomic Stat].*/[stat].[BatchLog] bl WHERE bl.BatchLogId = @iStatBatchLogId ), CONVERT( int, 0 ) ) = ( SELECT c.[ClientOrgId] FROM [dbo].[Client] AS c )
	BEGIN
	
		IF @tiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' ) + N' and could be inserted into BatchStatBatchLogXref.';
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END
			
		INSERT INTO [stat].[BatchStatBatchLogXref] 
			( 
				 BatchId
				,StatBatchLogId 
			)
		SELECT
			 @iBatchId AS BatchId
			,@iStatBatchLogId AS StatBatchLogId
		WHERE NOT EXISTS( SELECT 'X' FROM [stat].[BatchStatBatchLogXref] AS x WHERE x.BatchId = @iBatchId ) -- if @iBatchId already exists in any row, we can't add it again.
			AND NOT EXISTS( SELECT 'X' FROM [stat].[BatchStatBatchLogXref] AS x WHERE x.StatBatchLogId = @iStatBatchLogId ) -- if @iStatBatchLogId already exists in any row, we can't add it again, either.
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
;

GO
