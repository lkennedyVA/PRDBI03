USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [mtb].[uspMTBMain_TransferToAtomicStat]
	(
		@psiStatBatchLogId smallint
		,@ptiDebug tinyint = 0
	)
AS
BEGIN

	SET NOCOUNT ON
	;

	DECLARE @siStatBatchLogId smallint = @psiStatBatchLogId
		,@siMTBBatchId smallint 
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

	IF @siStatBatchLogId IS NULL AND EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatch mb WHERE mb.MTBBatchCompletedDatetime IS NOT NULL AND NOT EXISTS( SELECT 'X' FROM mtb.MTBBatchStatBatchLogXref x WHERE mb.MTBBatchId = x.MTBBatchId ) )
	BEGIN
		IF ( SELECT COUNT(1) FROM DHayes.dbo.MTBBatch WHERE MTBBatchTransferInitiatedDatetime IS NULL ) > 0
		BEGIN
			PRINT N'@psiStatBatchLogId is NULL.'
			SET @siStatBatchLogId = -1; 
			EXEC [stat].[uspBatchLogUpsertOut] @psiBatchLogId = @siStatBatchLogId OUTPUT, @piOrgId = 100008; 
			PRINT N'New batch added to AtomicStat.stat.BatchLog.'
			PRINT N'New BatchLogId = ' + CONVERT( nvarchar(50), @siStatBatchLogId )
		END
	END
	ELSE IF @siStatBatchLogId IS NULL AND EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatch mb WHERE mb.MTBBatchCompletedDatetime IS NOT NULL AND EXISTS( SELECT 'X' FROM mtb.MTBBatchStatBatchLogXref x WHERE mb.MTBBatchId = x.MTBBatchId ) )
	BEGIN
		IF ( SELECT COUNT(1) FROM DHayes.dbo.MTBBatch WHERE MTBBatchTransferInitiatedDatetime IS NULL ) > 0
		BEGIN
			PRINT N'@psiStatBatchLogId is NULL.'
			SELECT TOP 1 @siStatBatchLogId = x.StatBatchLogId, @siMTBBatchId = x.MTBBatchId FROM DHayes.dbo.MTBBatch mb INNER JOIN mtb.MTBBatchStatBatchLogXref x ON mb.MTBBatchId = x.MTBBatchId WHERE mb.MTBBatchCompletedDatetime IS NOT NULL ORDER BY x.MTBBatchId; 
			PRINT N'Batch fetched from AtomicStat.stat.BatchLog.'
			PRINT N'BatchLogId = ' + CONVERT( nvarchar(50), @siStatBatchLogId )
		END
	END

	IF ISNULL( @siStatBatchLogId, 0 ) > 0
	BEGIN

		EXEC mtb.uspMTBBatchStatBatchLogXrefEstablish @piStatBatchLogId = @siStatBatchLogId, @psiMTBBatchId = @siMTBBatchId OUTPUT, @ptiDebug = @ptiDebug
		;
		EXEC [mtb].[uspMTBBatchTransferInitiate] @piStatBatchLogId = @siStatBatchLogId, @psiMTBBatchId = @siMTBBatchId OUTPUT, @ptiDebug = @ptiDebug


		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBABAAcctLengthStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBABAAcctLengthStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBABAAStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBABAStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBCustAcctSummaryStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBCustAcctSummaryStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBCustFreqStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBCustFreqStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBCustTrxnStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBCustTrxnStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug

		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBDailyCustNegBalDays30and90Delta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBDailyCustNegBalDays30and90Delta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBDollarStratStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBDollarStratStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug

		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBKCPFreqStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBKCPFreqStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBKCPTrxnStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBKCPTrxnStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug

		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBOnUsAccountStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBOnUsAccountStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug

		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBPayerGradeNegDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBPayerGradeNegDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug
		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBPayerTrxnStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBPayerTrxnStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug

		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBWeeklyBFCustAUStatsDelta' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBWeeklyBFCustAUStatsDelta_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug

		IF EXISTS( SELECT 'X' FROM DHayes.dbo.MTBBatchDataSetRefreshLog WHERE MTBBatchId = @siMTBBatchId AND MTBDataSetName = N'MTBOnUsPayerRoutingNumber' AND MTBDataSetTransferCompletedDatetime IS NULL )
			EXEC mtb.uspMTBOnUsPayerRoutingNumber_TransferToAtomicStat @psiStatBatchLogId = @siStatBatchLogId, @ptiDebug = @ptiDebug



		EXEC [mtb].[uspMTBBatchTransferComplete] @piStatBatchLogId = @siStatBatchLogId, @psiMTBBatchId = @siMTBBatchId OUTPUT, @ptiDebug = @ptiDebug
		;

	END
	ELSE
	BEGIN
		PRINT N'No completed batches found in DHayes.dbo.MTBBatch that have not yet been transferred.'
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
