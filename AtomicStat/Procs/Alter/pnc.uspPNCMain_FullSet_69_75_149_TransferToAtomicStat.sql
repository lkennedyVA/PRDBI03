USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [pnc].[uspPNCMain_FullSet_69_75_149_TransferToAtomicStat]
	(
		@psiStatBatchLogId INT
		,@ptiDebug tinyint = 0
	)
AS
BEGIN

	SET NOCOUNT ON
	;

	DECLARE @siStatBatchLogId INT = @psiStatBatchLogId
		,@siPNCBatchId INT 
		,@nvMessage nvarchar(4000)
	;
	SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000)
	;
	SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
	;
	PRINT N''
	PRINT @nvThisSourceCode
	PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'

	IF @siStatBatchLogId IS NULL AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCBatch mb WHERE mb.PNCBatchCompletedDatetime IS NOT NULL AND NOT EXISTS( SELECT 'X' FROM pnc.PNCBatchStatBatchLogXref x WHERE mb.PNCBatchId = x.PNCBatchId ) )
	BEGIN
		IF ( SELECT COUNT(1) FROM DHayes.dbo.PNCBatch WHERE PNCBatchTransferInitiatedDatetime IS NULL ) > 0
		BEGIN
			PRINT N'@psiStatBatchLogId is NULL.'
			SET @siStatBatchLogId = -1; 
			EXEC [stat].[uspBatchLogUpsertOut] @psiBatchLogId = @siStatBatchLogId OUTPUT, @piOrgId = 100009; 
			PRINT N'New batch added to AtomicStat.stat.BatchLog.'
			PRINT N'New BatchLogId = ' + CONVERT( nvarchar(50), @siStatBatchLogId )
		END
	END
	ELSE IF @siStatBatchLogId IS NULL AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCBatch mb WHERE mb.PNCBatchCompletedDatetime IS NOT NULL AND EXISTS( SELECT 'X' FROM pnc.PNCBatchStatBatchLogXref x WHERE mb.PNCBatchId = x.PNCBatchId ) )
	BEGIN
		IF ( SELECT COUNT(1) FROM DHayes.dbo.PNCBatch WHERE PNCBatchTransferInitiatedDatetime IS NULL ) > 0
		BEGIN
			PRINT N'@psiStatBatchLogId is NULL.'
			SELECT TOP 1 @siStatBatchLogId = x.StatBatchLogId, @siPNCBatchId = x.PNCBatchId FROM DHayes.dbo.PNCBatch mb INNER JOIN pnc.PNCBatchStatBatchLogXref x ON mb.PNCBatchId = x.PNCBatchId WHERE mb.PNCBatchCompletedDatetime IS NOT NULL ORDER BY x.PNCBatchId; 
			PRINT N'Batch fetched from AtomicStat.stat.BatchLog.'
			PRINT N'BatchLogId = ' + CONVERT( nvarchar(50), @siStatBatchLogId )
		END
	END

	IF ISNULL( @siStatBatchLogId, 0 ) > 0
	BEGIN

		EXEC pnc.uspPNCBatchStatBatchLogXrefEstablish @piStatBatchLogId = @siStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT, @ptiDebug = @ptiDebug
		;
		EXEC [pnc].[uspPNCBatchTransferInitiate] @piStatBatchLogId = @siStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT, @ptiDebug = @ptiDebug



--/ *

		PRINT NCHAR(009) + REPLICATE( N'=', 80 )
		--IF EXISTS( SELECT 'X' FROM DHayes.dbo.PNCBatchDataSetRefreshLog WHERE PNCBatchId = @siPNCBatchId AND PNCDataSetName = N'PNCPayerTrxnShortTermStatsFullSet' AND PNCDataSetTransferCompletedDatetime IS NULL )
			EXEC [pnc].[uspPNCCustAtChannelLocationStatsFullSet_69_75_149_TransferToAtomicStat] @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		;

		PRINT NCHAR(009) + REPLICATE( N'=', 80 )
		--IF EXISTS( SELECT 'X' FROM DHayes.dbo.PNCBatchDataSetRefreshLog WHERE PNCBatchId = @siPNCBatchId AND PNCDataSetName = N'PNCPayerTrxnStatsFullSet' AND PNCDataSetTransferCompletedDatetime IS NULL )
			EXEC [pnc].[uspPNCCustAccountSummaryStatsFullSet_69_75_149_TransferToAtomicStat] @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		;

--* /

		EXEC [pnc].[uspPNCBatchTransferComplete] @piStatBatchLogId = @siStatBatchLogId, @psiPNCBatchId = @siPNCBatchId OUTPUT, @ptiDebug = @ptiDebug
		;

	END
	ELSE
	BEGIN
		PRINT N'No completed batches found in DHayes.dbo.PNCBatch that have not yet been transferred.'
		PRINT N'Nothing to process.'
		PRINT N'Exiting...'
	END

	PRINT N''
	PRINT N'@psiStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @psiStatBatchLogId ), N'NULL' )
	PRINT @nvThisSourceCode
	PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
	RAISERROR ( N'', 0, 1 ) WITH NOWAIT; -- RAISERROR() is less prone to lag than PRINT in the Message tab of an SSMS session tab, and less prone to cutoff of job output logging.

END

GO
