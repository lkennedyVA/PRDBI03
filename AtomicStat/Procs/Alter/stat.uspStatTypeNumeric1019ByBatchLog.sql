USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatTypeNumeric1019ByBatchLog]
	Created By: Larry Dugger
	Descr: Retrieve the records associated with the parameter
			
	Tables: [stat].[StatTypeNumeric1019]

	History:
		2018-01-10 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspStatTypeNumeric1019ByBatchLog](
     @psiBatchLogId INT
)
AS 
BEGIN
	SET NOCOUNT ON;

	SELECT PartitionId, KeyElementId, StatId, StatValue, BatchLogId
	FROM [stat].[StatTypeNumeric1019]
	WHERE BatchLogId = @psiBatchLogId

END

GO
