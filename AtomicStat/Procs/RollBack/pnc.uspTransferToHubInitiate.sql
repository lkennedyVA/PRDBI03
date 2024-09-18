USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Signals that PrdBi02 is fetching data for a batch.

	@siStatBatchLogId (PrdBi03) must not be null.
	@psiHubBatchId is updated within the uspLoadStat (PrdBi02)
	@siStatBatchLogId must be in the result set of pnc.vwBatchTransferToHubAvailable.

	pnc.vwBatchTransferToHubAvailable.TransferToHubCompleteDateTime must be null.

*/
ALTER PROCEDURE [pnc].[uspTransferToHubInitiate]
	(
		@psiStatBatchLogId smallint
		--,@psiHubBatchId smallint
	)
AS
BEGIN

	DECLARE @siStatBatchLogId smallint = @psiStatBatchLogId
		--,@siHubBatchId smallint = @psiHubBatchId
		,@dt2TransferToHubAvailableDateTime datetime2(0)
	;

	IF @siStatBatchLogId IS NOT NULL --AND @siHubBatchId IS NOT NULL
	BEGIN
		SELECT @dt2TransferToHubAvailableDateTime = TransferToHubAvailableDateTime FROM pnc.vwBatchTransferToHubAvailable WHERE StatBatchLogId = @siStatBatchLogId
		;
		--IF EXISTS( SELECT 'X' FROM PNCAtomicStat.stat.vwBatchTransferToHubAvailable x WHERE x.StatBatchLogId = @siStatBatchLogId )
		IF @dt2TransferToHubAvailableDateTime IS NOT NULL
		BEGIN
			--SELECT @dt2TransferToHubAvailableDateTime = TransferToHubAvailableDateTime FROM pnc.vwBatchTransferToHubAvailable WHERE StatBatchLogId = @siStatBatchLogId
			--;
			UPDATE u
			SET TransferToHubInitiateDateTime = SYSDATETIME() 
				--,HubBatchId = @siHubBatchId
				,TransferToHubAvailableDateTime = @dt2TransferToHubAvailableDateTime
			FROM PNCAtomicStat.dbo.BatchStatBatchLogXref u
			WHERE StatBatchLogId = @siStatBatchLogId 
				AND TransferToHubCompleteDateTime IS NULL
			;
			RETURN 0 -- batch successfully initiated
		END
	END

	RETURN -1 -- batch not initiated

END
;

GO
