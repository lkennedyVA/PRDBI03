USE [FNBAtomicStat]
GO

/****** Object:  StoredProcedure [stat].[uspTransferToHubComplete]    Script Date: 10/6/2024 9:15:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*

	Signals that PrdBi02 is done fetching data for a batch.

	@iStatBatchLogId (PrdBi03) and @piHubBatchId (PrdBi02) must not be null.

	[stat].[vwBatchTransferToHubAvailable].[TransferToHubCompleteDateTime] must be null.

*/
ALTER PROCEDURE [stat].[uspTransferToHubComplete]
	(
		@piStatBatchLogId int
		,@piHubBatchId int
	)
AS
BEGIN

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iHubBatchId smallint = @piHubBatchId
		,@iRecCount int
	;

	IF @iStatBatchLogId IS NOT NULL
	BEGIN
		UPDATE u
		SET TransferToHubCompleteDateTime = SYSDATETIME() 
			,HubBatchId = @iHubBatchId
		FROM [stat].[BatchStatBatchLogXref] u
		WHERE StatBatchLogId = @iStatBatchLogId 
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


