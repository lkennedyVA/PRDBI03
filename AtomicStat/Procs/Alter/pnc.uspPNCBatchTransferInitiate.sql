USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspPNCBatchTransferInitiate

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30117
			,@siPNCBatchId INT = NULL
		;
		EXEC pnc.uspPNCBatchTransferInitiate @piStatBatchLogId = @iStatBatchLogId, @psiPNCBatchId = @siPNCBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, PNCBatchId = @siPNCBatchId
		;
		SELECT * FROM [DHayes].[dbo].[PNCBatch] WHERE PNCBatchId = @siPNCBatchId
		;
		
	History:
		2019-07-22 - LSW - Created

*/
ALTER PROCEDURE [pnc].[uspPNCBatchTransferInitiate]
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
		,@dt2PNCBatchTransferInitiatedDatetime datetime2(0)
	;
	EXEC pnc.uspPNCBatchStatBatchLogXrefEstablish @piStatBatchLogId = @iStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT 
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'After Xref established @siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' )
	END
	SET @siPNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SET @dt2PNCBatchTransferInitiatedDatetime = SYSDATETIME()
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'After ufnPNCBatchGetByStatBatchLogId @siPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siPNCBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), @dt2PNCBatchTransferInitiatedDatetime, 121 )
	END

	UPDATE [DHayes].[dbo].[PNCBatch] 
		SET [PNCBatchTransferInitiatedDatetime] = @dt2PNCBatchTransferInitiatedDatetime 
		WHERE PNCBatchId = @siPNCBatchId 
			AND [PNCBatchCompletedDatetime] IS NOT NULL -- can't initiate the batch for AtomicStat transfer if it is still being prepared by the DHayes PNC process.
			AND [PNCBatchTransferInitiatedDatetime] IS NULL -- can't initiate the batch for AtomicStat transfer if it is already marked as initiated.
	;
	SELECT 
		@psiPNCBatchId = mb.PNCBatchId
	FROM [DHayes].[dbo].[PNCBatch] AS mb
	WHERE mb.PNCBatchId = @siPNCBatchId 
		--AND mb.[PNCBatchTransferInitiatedDatetime] = @dt2PNCBatchTransferInitiatedDatetime
		AND mb.[PNCBatchTransferCompletedDatetime] IS NULL -- this better be NULL at this point, or we have a problem.
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiPNCBatchId ), N'NULL' )
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
