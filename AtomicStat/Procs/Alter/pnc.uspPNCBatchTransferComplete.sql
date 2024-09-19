USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspPNCBatchTransferComplete

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30117
			,@siPNCBatchId INT = NULL
		;
		EXEC pnc.uspPNCBatchTransferComplete @piStatBatchLogId = @iStatBatchLogId, @psiPNCBatchId = @siPNCBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, PNCBatchId = @siPNCBatchId
		;
		SELECT * FROM [DHayes].[dbo].[PNCBatch] WHERE PNCBatchId = @siPNCBatchId
		;
		
	History:
		2019-07-22 - LSW - Created

*/
ALTER PROCEDURE [pnc].[uspPNCBatchTransferComplete]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@psiPNCBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiPNCBatchId ), N'NULL' )
		PRINT N''
	END

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@siPNCBatchId INT = @psiPNCBatchId
		,@dt2PNCBatchTransferCompletedDatetime datetime2(0)
		,@tiRemainingCount tinyint
	;
	SET @siPNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SELECT @tiRemainingCount = COUNT(1) FROM pnc.ufnPNCBatchDataSetRefreshLogUntransferredGetByBatchLogId( @iStatBatchLogId )
	;
	SET @dt2PNCBatchTransferCompletedDatetime = SYSDATETIME()
	;
	UPDATE [DHayes].[dbo].[PNCBatch] 
		SET [PNCBatchTransferCompletedDatetime] = @dt2PNCBatchTransferCompletedDatetime 
		WHERE PNCBatchId = @siPNCBatchId 
			AND @tiRemainingCount = 0 -- there is still work to do - can't mark as complete.
			AND [PNCBatchTransferInitiatedDatetime] IS NOT NULL -- can't complete what hasn't been initiated.
			AND [PNCBatchTransferCompletedDatetime] IS NULL -- can't complete what is already marked as complete.
	;
	SELECT 
		@psiPNCBatchId = mb.PNCBatchId
	FROM [DHayes].[dbo].[PNCBatch] AS mb
	WHERE mb.PNCBatchId = @siPNCBatchId 
		AND mb.[PNCBatchTransferInitiatedDatetime] IS NOT NULL -- if this is NULL at this point, we return NULL via @psiPNCBatchId.
		AND mb.[PNCBatchTransferCompletedDatetime] = @dt2PNCBatchTransferCompletedDatetime
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@tiRemainingCount = ' + ISNULL( CONVERT( nvarchar(50), @tiRemainingCount ), N'NULL' ) + CASE WHEN @tiRemainingCount > 0 THEN N' Cannot mark the batch as complete.' ELSE N'' END
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiPNCBatchId ), N'NULL' )
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
