USE [CMBAtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*

	Name: uspBatchTransferInitiate

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30337
			,@iBatchId INT = NULL
		;
		EXEC ???.uspBatchTransferInitiate @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, BatchId = @iBatchId
		;
		SELECT * FROM [???Risk].[dbo].[StatBatch] WHERE BatchId = @iBatchId
		;
		
	History:
		2020-08-17 - VALIDRS\LWhiting - Created.  Based upon [m t b].[uspBatchTransferInitiate]

*/
ALTER PROCEDURE [stat].[uspBatchTransferInitiate]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@piBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	) 
AS 
BEGIN

	--SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iBatchId int = @piBatchId
		,@dt2BatchTransferInitiatedDatetime datetime2(0) = SYSDATETIME()
		,@tiDebug tinyint = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
		,@nvThisSourceCode nvarchar(4000)
	;
	IF @tiDebug > 0 
	BEGIN
		SET @nvThisSourceCode = [dbo].[ufnExecutingObjectString]( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'StatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'       BatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

	EXEC [stat].[uspBatchStatBatchLogXrefEstablish] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId OUTPUT, @ptiDebug = @tiDebug
	;
	IF @tiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'After Xref established @iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
	END

	SET @iBatchId = [stat].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
	;

	IF @tiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'After ufnBatchGetByStatBatchLogId @iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), @dt2BatchTransferInitiatedDatetime, 121 )
	END

	UPDATE [risk].[StatBatch] 
		SET [StatBatchTransferInitiatedDatetime] = @dt2BatchTransferInitiatedDatetime
		WHERE StatBatchId = @iBatchId 
			AND [StatBatchCompletedDatetime] IS NOT NULL -- can't initiate the batch for AtomicStat transfer if it is still being prepared by the ???Risk process.
			AND [StatBatchTransferInitiatedDatetime] IS NULL -- can't initiate the batch for AtomicStat transfer if it is already marked as initiated.
	;

	SELECT 
		@piBatchId = b.StatBatchId
	FROM [risk].[StatBatch] AS b
	WHERE b.StatBatchId = @iBatchId 
		AND b.[StatBatchTransferCompletedDatetime] IS NULL -- this better be NULL at this point, or we have a problem.
	;

	IF @tiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'@piBatchId = ' + ISNULL( CONVERT( nvarchar(50), @piBatchId ), N'{null}' )
	END

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
