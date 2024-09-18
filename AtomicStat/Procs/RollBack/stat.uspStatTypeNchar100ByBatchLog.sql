USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatTypeNchar100ByBatchLog]
	Created By: Larry Dugger
	Descr: Retrieve the records associated with the parameter
			
	Tables: [stat].[StatTypeNchar100]

	History:
		2017-10-10 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspStatTypeNchar100ByBatchLog](
     @psiBatchLogId SMALLINT
)
AS 
BEGIN
	SET NOCOUNT ON;

	SELECT PartitionId, KeyElementId, StatId, StatValue, BatchLogId
	FROM [stat].[StatTypeNchar100]
	WHERE BatchLogId = @psiBatchLogId

END

GO
