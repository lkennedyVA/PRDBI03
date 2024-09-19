USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Signals that PrdBi02 is done fetching data for a batch.

	@siStatBatchLogId (PrdBi03) and @psiHubBatchId (PrdBi02) must not be null.

	pnc.vwBatchTransferToHubAvailable.TransferToHubCompleteDateTime must be null.
We changed the synonym for the direct reference
*/
ALTER PROCEDURE [pnc].[uspTransferToHubComplete]
	(
		@psiStatBatchLogId INT
		,@psiHubBatchId INT
	)
AS
BEGIN

	DECLARE @siStatBatchLogId INT = @psiStatBatchLogId
		,@siHubBatchId INT = @psiHubBatchId
		,@iRecCount int
	;

	IF @siStatBatchLogId IS NOT NULL
	BEGIN
		UPDATE u
		SET TransferToHubCompleteDateTime = SYSDATETIME() 
			,HubBatchId = @siHubBatchId
		----FROM pnc.BatchStatBatchLogXref u  2022-08-12 LBD Keeping Lee from Dinner
		FROM PNCAtomicStat.stat.BatchStatBatchLogXref u		--2022-08-12 LBD
		WHERE StatBatchLogId = @siStatBatchLogId 
			AND TransferToHubCompleteDateTime IS NULL
		;
		SET @iRecCount = @@ROWCOUNT
		;
		IF @iRecCount > 0 RETURN 0 -- batch successfully completed
	END

	RETURN -1 -- batch not completed

END
;
GO
