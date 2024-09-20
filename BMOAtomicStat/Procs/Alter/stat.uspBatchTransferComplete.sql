USE [BMOAtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspBatchTransferComplete

	Created By: VALIDRS\LWhiting
	
	Description: .

		StatusId	StatusDesc
				-1	Rolled Back
				 0	Initiated
				 1	Build Complete
				 2	Build Complete and Transfer Complete


	Example:

		DECLARE @iStatBatchLogId int = 30337
			,@iBatchId int = NULL
		;
		EXEC [???].[uspBatchTransferComplete] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, BatchId = @iBatchId
		;
		SELECT * FROM [???Risk].[dbo].[StatBatch] WHERE BatchId = @iBatchId
		;
		
	History:
		2020-08-17 - VALIDRS\LWhiting - Created.  Based upon [m t b].[uspBatchTransferComplete]

*/
ALTER PROCEDURE [stat].[uspBatchTransferComplete]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of AtomicStat.stat.BatchLog.BatchLogId
		,@piBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	) 
AS 
BEGIN

	--SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @tiDebug tinyint = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
		,@nvThisSourceCode nvarchar(4000);
	
	IF @tiDebug > 0 
	BEGIN
		SET @nvThisSourceCode = [dbo].[ufnExecutingObjectString]( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'StatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		PRINT REPLICATE( NCHAR(009), @tiDebug - 1 ) + N'       BatchId = ' + ISNULL( CONVERT( nvarchar(50), @piBatchId ), N'{null}' )
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iBatchId int 
	;
	SET @iBatchId = [stat].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
	;
	SELECT @piBatchId = @iBatchId 
	;

	UPDATE [risk].[StatBatch] 
		SET [StatBatchTransferCompletedDatetime] = SYSDATETIME()
			,StatBatchStatusId = 2 -- Transfer Complete
			,StatBatchRollBackComment = N'Build Complete and Transfer Complete' -- was known as "StatusDesc"
		WHERE CASE WHEN @iBatchId > 0 THEN @iBatchId ELSE NULL END IS NOT NULL
			AND [StatBatchId] = @iBatchId 
			AND [StatBatchTransferInitiatedDatetime] IS NOT NULL -- can't complete what hasn't been initiated.
			AND [StatBatchTransferCompletedDatetime] IS NULL -- can't complete what is already marked as complete.
			AND [StatBatchStatusId] = 1 -- Build Complete
	;

	UPDATE u
		SET TransferToHubAvailableDateTime = SYSDATETIME()
		FROM [stat].[BatchStatBatchLogXref] u
		WHERE u.StatBatchLogId = @iStatBatchLogId
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
