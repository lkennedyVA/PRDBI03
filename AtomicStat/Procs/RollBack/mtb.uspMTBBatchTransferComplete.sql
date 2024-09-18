USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspMTBBatchTransferComplete

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30117
			,@siMTBBatchId smallint = NULL
		;
		EXEC mtb.uspMTBBatchTransferComplete @piStatBatchLogId = @iStatBatchLogId, @psiMTBBatchId = @siMTBBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, MTBBatchId = @siMTBBatchId
		;
		SELECT * FROM [DHayes].[dbo].[MTBBatch] WHERE MTBBatchId = @siMTBBatchId
		;
		
	History:
		2019-03-11 - LSW - Created

*/
ALTER PROCEDURE [mtb].[uspMTBBatchTransferComplete]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@psiMTBBatchId smallint OUTPUT -- returned as verification; if NULL, then something went wrong.
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiMTBBatchId ), N'NULL' )
		PRINT N''
	END

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@siMTBBatchId smallint = @psiMTBBatchId
		,@dt2MTBBatchTransferCompletedDatetime datetime2(0)
		,@tiRemainingCount tinyint
	;
	SET @siMTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SELECT @tiRemainingCount = COUNT(1) FROM mtb.ufnMTBBatchDataSetRefreshLogUntransferredGetByBatchLogId( @iStatBatchLogId )
	;
	SET @dt2MTBBatchTransferCompletedDatetime = SYSDATETIME()
	;
	UPDATE [DHayes].[dbo].[MTBBatch] 
		SET [MTBBatchTransferCompletedDatetime] = @dt2MTBBatchTransferCompletedDatetime 
		WHERE MTBBatchId = @siMTBBatchId 
			AND @tiRemainingCount = 0 -- there is still work to do - can't mark as complete.
			AND [MTBBatchTransferInitiatedDatetime] IS NOT NULL -- can't complete what hasn't been initiated.
			AND [MTBBatchTransferCompletedDatetime] IS NULL -- can't complete what is already marked as complete.
	;
	SELECT 
		@psiMTBBatchId = mb.MTBBatchId
	FROM [DHayes].[dbo].[MTBBatch] AS mb
	WHERE mb.MTBBatchId = @siMTBBatchId 
		AND mb.[MTBBatchTransferInitiatedDatetime] IS NOT NULL -- if this is NULL at this point, we return NULL via @psiMTBBatchId.
		AND mb.[MTBBatchTransferCompletedDatetime] = @dt2MTBBatchTransferCompletedDatetime
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@tiRemainingCount = ' + ISNULL( CONVERT( nvarchar(50), @tiRemainingCount ), N'NULL' ) + CASE WHEN @tiRemainingCount > 0 THEN N' Cannot mark the batch as complete.' ELSE N'' END
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiMTBBatchId ), N'NULL' )
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

GO
