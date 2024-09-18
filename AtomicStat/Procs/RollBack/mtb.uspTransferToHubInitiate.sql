USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ???.uspTransferToHubComplete
	Created By: VALIDRS\LWhiting
	
	Description: Signals that PrdBi02 is fetching data for a batch.

		@siStatBatchLogId (PrdBi03) must not be null.
		@psiHubBatchId is updated within the uspLoadStat (PrdBi02)
		@siStatBatchLogId must be in the result set of ???.vwBatchTransferToHubAvailable.

		???.vwBatchTransferToHubAvailable.TransferToHubCompleteDateTime must be null.
		
	History:
		2019-04-05 - VALIDRS\LWhiting - Created.
		2020-07-02 - VALIDRS\LWhiting - Updated header block format.  Logic confirmed; unchanged.
		2020-07-20 - VALIDRS\LWhiting - CCF2105 New MTBRisk process deployment.

*****************************************************************************************/
ALTER PROCEDURE [mtb].[uspTransferToHubInitiate]
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
		IF EXISTS( SELECT 'X' FROM [mtb].[vwBatchTransferToHubAvailable] x WHERE x.StatBatchLogId = @siStatBatchLogId )
		BEGIN
			SELECT @dt2TransferToHubAvailableDateTime = TransferToHubAvailableDateTime FROM [mtb].[vwBatchTransferToHubAvailable] WHERE StatBatchLogId = @siStatBatchLogId
			;
			UPDATE u
			SET TransferToHubInitiateDateTime = SYSDATETIME() 
				--,HubBatchId = @siHubBatchId
				,TransferToHubAvailableDateTime = @dt2TransferToHubAvailableDateTime
			FROM [mtb].[BatchStatBatchLogXref] u -- CCF2105
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
