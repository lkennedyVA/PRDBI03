USE [AtomicStat]
GO

/****** Object:  StoredProcedure [stat].[uspTransferToHubComplete]    Script Date: 10/6/2024 12:53:10 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/****************************************************************************************
	Name: uspTransferToHubComplete
	CreatedBy: Larry Dugger
	Descr: Signals that PrdBi02 is fetching data for a batch.
		@piStatBatchLogId (PrdBi03) must not be null.
		@piStatBatchLogId must be in the result set of stat.vwBatchTransferToHubAvailable.
		@pnvAncestorStatGroupName must not be NULL
	Procedures: [tdb].[uspTransferToHubComplete]
		,[retail].[uspTransferToHubComplete]
		,[pnc].[uspTransferToHubComplete]
		,[mtb].[uspTransferToHubComplete]

	History:
		2019-11-27 - LBD - Created
		2019-12-04 - LSW - Adjusted to output feedback
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspTransferToHubComplete]
	(
		@piStatBatchLogId int
		,@piHubBatchId int
		,@pnvAncestorStatGroupName nvarchar(100)
	)
AS
BEGIN

	PRINT N''
	PRINT dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
	PRINT NCHAR(009) + N'        @piStatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
	PRINT NCHAR(009) + N'            @piHubBatchId = ' + CONVERT( nvarchar(50), @piHubBatchId )
	PRINT NCHAR(009) + N'@pnvAncestorStatGroupName = ' + CONVERT( nvarchar(50), @pnvAncestorStatGroupName )
	PRINT N''

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iHubBatchId int = @piHubBatchId
		,@nvAncestorStatGroupName nvarchar(100) = @pnvAncestorStatGroupName
		,@iReturnValue int
		,@iRecCount int;

	IF @iStatBatchLogId IS NOT NULL 
		AND @nvAncestorStatGroupName IS NOT NULL
	BEGIN
		IF @nvAncestorStatGroupName = N'Retail - Pt.Deux'
			BEGIN
				PRINT N'Executing [retail].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [retail].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'FTB'
			BEGIN
				PRINT N'Executing [ftb].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [ftb].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'TDB'
			BEGIN
				PRINT N'Executing [tdb].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [tdb].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName IN( N'Financial', N'PNC' )
			BEGIN
				PRINT N'Executing [pnc].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [pnc].[uspTransferToHubComplete] @psiStatBatchLogId = @iStatBatchLogId, @psiHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'MTB'
			BEGIN
				PRINT N'Executing [mtb].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [mtb].[uspTransferToHubComplete] @psiStatBatchLogId = @iStatBatchLogId, @psiHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'FNB'
			BEGIN
				PRINT N'Executing [fnb].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [fnb].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'KEY'
			BEGIN
				PRINT N'Executing [key].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [key].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'TFB'
			BEGIN
				PRINT N'Executing [tfb].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [tfb].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @nvAncestorStatGroupName = N'BMO'
			BEGIN
				PRINT N'Executing [bmo].[uspTransferToHubComplete]'
				EXECUTE @iReturnValue = [bmo].[uspTransferToHubComplete] @piStatBatchLogId = @iStatBatchLogId, @piHubBatchId = @iHubBatchId;
			END
		IF @iReturnValue = 0
			RETURN 0;
	END
	PRINT N'No execution of a [uspTransferToHubComplete] sproc occurred.'
	RETURN -1 -- batch not completed

END
;
GO


