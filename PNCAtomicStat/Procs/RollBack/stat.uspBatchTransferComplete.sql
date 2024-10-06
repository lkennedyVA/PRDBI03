USE [PNCAtomicStat]
GO

/****** Object:  StoredProcedure [stat].[uspBatchTransferComplete]    Script Date: 10/6/2024 9:58:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*

	Name: uspBatchTransferComplete

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30117
			,@iBatchId smallint = NULL
		;
		EXEC stat.uspBatchTransferComplete @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, BatchId = @iBatchId
		;
		SELECT * FROM [PNCRisk].[dbo].[StatBatch] WHERE StatBatchId = @iBatchId
		;
		
	History:
		2019-07-22 - LSW - Created

*/
ALTER PROCEDURE [stat].[uspBatchTransferComplete]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@piBatchId int OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	) 
AS 
BEGIN

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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piBatchId = ' + ISNULL( CONVERT( nvarchar(50), @piBatchId ), N'NULL' )
		PRINT N''
	END

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iBatchId int = @piBatchId
		,@dt2StatBatchTransferCompletedDatetime datetime2(0)
		,@tiRemainingCount tinyint
	;
	SET @iBatchId = stat.ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SELECT @tiRemainingCount = COUNT(1) FROM stat.ufnBatchDataSetRefreshLogUntransferredGetByBatchLogId( @iStatBatchLogId )
	;
	SET @dt2StatBatchTransferCompletedDatetime = SYSDATETIME()
	;
--	UPDATE [PNCRisk].[dbo].[StatBatch] 
	UPDATE [risk].[StatBatch] 
		SET [StatBatchTransferCompletedDatetime] = @dt2StatBatchTransferCompletedDatetime 
		WHERE StatBatchId = @iBatchId 
			AND @tiRemainingCount = 0 -- there is still work to do - can't mark as complete.
			AND [StatBatchTransferInitiatedDatetime] IS NOT NULL -- can't complete what hasn't been initiated.
			AND [StatBatchTransferCompletedDatetime] IS NULL -- can't complete what is already marked as complete.
	;
	SELECT 
		@piBatchId = mb.StatBatchId
	FROM [PNCRisk].[dbo].[StatBatch] AS mb
	WHERE mb.StatBatchId = @iBatchId 
		AND mb.[StatBatchTransferInitiatedDatetime] IS NOT NULL -- if this is NULL at this point, we return NULL via @piBatchId.
		AND mb.[StatBatchTransferCompletedDatetime] = @dt2StatBatchTransferCompletedDatetime
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@tiRemainingCount = ' + ISNULL( CONVERT( nvarchar(50), @tiRemainingCount ), N'NULL' ) + CASE WHEN @tiRemainingCount > 0 THEN N' Cannot mark the batch as complete.' ELSE N'' END
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piBatchId = ' + ISNULL( CONVERT( nvarchar(50), @piBatchId ), N'NULL' )
		PRINT N''
	END

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT; -- RAISERROR() is less prone to lag than PRINT in the Message tab of an SSMS session tab, and less prone to cutoff of job output logging.
	END

END
;
GO


