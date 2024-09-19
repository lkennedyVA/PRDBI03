USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspPNCBatchDataSetRefreshLogTransferInitiate

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30113
			,@siPNCBatchId INT = NULL
			,@iPNCBatchDataSetRefreshLogId int = NULL
			,@iDataSetRemainCount int = NULL
		;
		EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate @piStatBatchLogId = @iStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT, @piPNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId OUTPUT, @piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		;
		SELECT StatBatchLogId = @iStatBatchLogId, PNCBatchId = @siPNCBatchId
		;
		SELECT * FROM [DHayes].[dbo].[PNCBatchDataSetRefreshLog] WHERE PNCBatchId = @siPNCBatchId
		;
		SELECT * FROM [DHayes].[dbo].[PNCBatch] WHERE PNCBatchId = @siPNCBatchId
		;
		
	History:
		2019-07-22 - LSW - Created

*/
ALTER PROCEDURE [pnc].[uspPNCBatchDataSetRefreshLogTransferInitiate]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@pnvPNCDataSetName nvarchar(255) = NULL
		,@psiPNCBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piPNCBatchDataSetRefreshLogId int OUTPUT -- returns the Id of a selected data set.
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiPNCBatchId ), N'NULL' )
		PRINT N''
	END

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@siPNCBatchId INT = @psiPNCBatchId
		,@iPNCBatchDataSetRefreshLogId int
		,@dt2PNCBatchDataSetRefreshLogTransferInitiate datetime2(0)
	;
	EXEC pnc.uspPNCBatchTransferInitiate @piStatBatchLogId = @iStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT
	;
	SET @siPNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SELECT 
		@iPNCBatchDataSetRefreshLogId = MIN( PNCBatchDataSetRefreshLogId ) 
	FROM pnc.ufnPNCBatchDataSetRefreshLogGetByBatchLogId( @iStatBatchLogId ) 
	WHERE PNCDataSetReadyForTransferYN = N'Y' 
		AND PNCDataSetTransferredYN = N'N' 
		AND PNCDataSetTransferInitiatedDatetime IS NULL
		AND ( @pnvPNCDataSetName IS NULL OR ( @pnvPNCDataSetName = PNCDataSetName ) ) 
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + CONVERT( nvarchar(50), @iStatBatchLogId )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' )
	END

	SET @dt2PNCBatchDataSetRefreshLogTransferInitiate = SYSDATETIME()
	;
	UPDATE [DHayes].[dbo].[PNCBatchDataSetRefreshLog] 
		SET [PNCDataSetTransferInitiatedDatetime] = @dt2PNCBatchDataSetRefreshLogTransferInitiate
		WHERE PNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId 
			AND [PNCDataSetReadyForTransferYN] = N'Y' -- can't initiate the batch data set for AtomicStat transfer if it is still being prepared by the DHayes PNC process.
			AND [PNCDataSetTransferredYN] = N'N'
			AND [PNCDataSetTransferInitiatedDatetime] IS NULL -- can't initiate the batch data set for AtomicStat transfer if it is already marked as initiated.
	;
	
	SELECT 
		@piPNCBatchDataSetRefreshLogId = mb.PNCBatchDataSetRefreshLogId
	FROM [DHayes].[dbo].[PNCBatchDataSetRefreshLog] AS mb
	WHERE mb.PNCBatchId = @siPNCBatchId 
		AND mb.[PNCDataSetTransferInitiatedDatetime] = @dt2PNCBatchDataSetRefreshLogTransferInitiate
		AND mb.[PNCDataSetTransferCompletedDatetime] IS NULL -- this better be NULL at this point, or we have a problem.
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @piPNCBatchDataSetRefreshLogId ), N'NULL' )
	END

	SELECT 
		@piDataSetRemainCount = COUNT(1) 
	FROM pnc.ufnPNCBatchDataSetRefreshLogGetByBatchLogId( @iStatBatchLogId ) 
	WHERE PNCDataSetReadyForTransferYN = N'Y' 
		AND PNCDataSetTransferredYN = N'N'
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piDataSetRemainCount = ' + ISNULL( CONVERT( nvarchar(50), @piDataSetRemainCount ), N'NULL' )
	END

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END

GO
