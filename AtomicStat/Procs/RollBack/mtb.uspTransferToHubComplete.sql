USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ???.uspTransferToHubComplete
	Created By: VALIDRS\LWhiting
	
	Description: Signals that PrdBi02 is done fetching data for a batch.

		@iStatBatchLogId (PrdBi03) and @piHubBatchId (PrdBi02) must not be null.

		???.vwBatchTransferToHubAvailable.TransferToHubCompleteDateTime must be null.
		
	History:
		2019-04-05 - VALIDRS\LWhiting - Created.
		2020-07-02 - VALIDRS\LWhiting - Updated header block format.  Logic confirmed; unchanged.
		2020-07-20 - VALIDRS\LWhiting - CCF2105 New MTBRisk process deployment.

*****************************************************************************************/
ALTER PROCEDURE [mtb].[uspTransferToHubComplete]
	(
		@psiStatBatchLogId smallint
		,@psiHubBatchId smallint
	)
AS
BEGIN

	DECLARE @siStatBatchLogId smallint = @psiStatBatchLogId
		,@siHubBatchId smallint = @psiHubBatchId
		,@iRecCount int
	;

	IF @siStatBatchLogId IS NOT NULL
	BEGIN
		UPDATE u
		SET TransferToHubCompleteDateTime = SYSDATETIME() 
			,HubBatchId = @siHubBatchId
		FROM [mtb].[BatchStatBatchLogXref] u -- CCF2105
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
