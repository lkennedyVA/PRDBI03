USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspMTBBatchTransferInitiate

	Created By: VALIDRS\LWhiting
	
	Description: .

	Example:

		DECLARE @iStatBatchLogId int = 30117
			,@siMTBBatchId INT = NULL
		;
		EXEC mtb.uspMTBBatchTransferInitiate @piStatBatchLogId = @iStatBatchLogId, @psiMTBBatchId = @siMTBBatchId
		;
		SELECT StatBatchLogId = @iStatBatchLogId, MTBBatchId = @siMTBBatchId
		;
		SELECT * FROM [DHayes].[dbo].[MTBBatch] WHERE MTBBatchId = @siMTBBatchId
		;
		
	History:
		2019-03-11 - LSW - Created

*/
ALTER PROCEDURE [mtb].[uspMTBBatchTransferInitiate]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@psiMTBBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
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
		,@siMTBBatchId INT = @psiMTBBatchId
		,@dt2MTBBatchTransferInitiatedDatetime datetime2(0)
	;
	EXEC mtb.uspMTBBatchStatBatchLogXrefEstablish @piStatBatchLogId = @iStatBatchLogId, @psiMTBBatchId = @siMTBBatchId OUTPUT 
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'After Xref established @siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' )
	END
	SET @siMTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	SET @dt2MTBBatchTransferInitiatedDatetime = SYSDATETIME()
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'After ufnMTBBatchGetByStatBatchLogId @siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), @dt2MTBBatchTransferInitiatedDatetime, 121 )
	END

	UPDATE [DHayes].[dbo].[MTBBatch] 
		SET [MTBBatchTransferInitiatedDatetime] = @dt2MTBBatchTransferInitiatedDatetime 
		WHERE MTBBatchId = @siMTBBatchId 
			AND [MTBBatchCompletedDatetime] IS NOT NULL -- can't initiate the batch for AtomicStat transfer if it is still being prepared by the DHayes MTB process.
			AND [MTBBatchTransferInitiatedDatetime] IS NULL -- can't initiate the batch for AtomicStat transfer if it is already marked as initiated.
	;
	SELECT 
		@psiMTBBatchId = mb.MTBBatchId
	FROM [DHayes].[dbo].[MTBBatch] AS mb
	WHERE mb.MTBBatchId = @siMTBBatchId 
		--AND mb.[MTBBatchTransferInitiatedDatetime] = @dt2MTBBatchTransferInitiatedDatetime
		AND mb.[MTBBatchTransferCompletedDatetime] IS NULL -- this better be NULL at this point, or we have a problem.
	;
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @psiMTBBatchId ), N'NULL' )
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
