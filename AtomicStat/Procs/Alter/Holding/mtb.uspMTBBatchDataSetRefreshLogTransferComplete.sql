USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspMTBBatchDataSetRefreshLogTransferComplete

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30117
			,@siMTBBatchId INT = NULL
		;
		EXEC mtb.uspMTBBatchDataSetRefreshLogTransferComplete @piStatBatchLogId = @iStatBatchLogId, @psiMTBBatchId = @siMTBBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, MTBBatchId = @siMTBBatchId
		;
		SELECT * FROM [DHayes].[dbo].[MTBBatch] WHERE MTBBatchId = @siMTBBatchId
		;
		
	History:
		2019-03-11 - LSW - Created

*/
ALTER PROCEDURE [mtb].[uspMTBBatchDataSetRefreshLogTransferComplete]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@piMTBBatchDataSetRefreshLogId int
		,@psiMTBBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piDataSetRemainCount int OUTPUT
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
		,@siMTBBatchId INT 
		,@iDataSetRemainCount int = @piDataSetRemainCount
		,@iMTBBatchDataSetRefreshLogId int = @piMTBBatchDataSetRefreshLogId
		,@dt2MTBDataSetTransferCompletedDatetime datetime2(0)
	;
	SET @siMTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SET @dt2MTBDataSetTransferCompletedDatetime = SYSDATETIME()
	;
	UPDATE [DHayes].[dbo].[MTBBatchDataSetRefreshLog] 
		SET [MTBDataSetTransferCompletedDatetime] = @dt2MTBDataSetTransferCompletedDatetime
			,[MTBDataSetTransferredYN] = N'Y'
		WHERE [MTBBatchDataSetRefreshLogId] = @iMTBBatchDataSetRefreshLogId 
			AND [MTBDataSetTransferInitiatedDatetime] IS NOT NULL -- can't complete what hasn't been initiated.
			AND [MTBDataSetTransferCompletedDatetime] IS NULL -- can't complete what is already marked as complete.
			AND [MTBDataSetTransferredYN] = N'N'
	;
	SELECT 
		@psiMTBBatchId = mb.MTBBatchId
	FROM [DHayes].[dbo].[MTBBatchDataSetRefreshLog] AS mb
	WHERE mb.MTBBatchId = @siMTBBatchId 
		AND mb.[MTBDataSetTransferInitiatedDatetime] IS NOT NULL -- if this is NULL at this point, we return NULL via @psiMTBBatchId.
		AND mb.[MTBDataSetTransferCompletedDatetime] = @dt2MTBDataSetTransferCompletedDatetime
	;

	SELECT 
		@piDataSetRemainCount = COUNT(1) 
	FROM mtb.ufnMTBBatchDataSetRefreshLogGetByBatchLogId( @iStatBatchLogId ) 
	WHERE MTBDataSetReadyForTransferYN = N'Y' 
		AND MTBDataSetTransferredYN = N'N'
	;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT; -- RAISERROR() is less prone to lag than PRINT in the Message tab of an SSMS session tab, and less prone to cutoff of job output logging.
	END

END

GO
