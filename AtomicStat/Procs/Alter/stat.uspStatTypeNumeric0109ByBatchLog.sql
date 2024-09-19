USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatTypeNumeric0109ByBatchLog]
	Created By: Larry Dugger
	Descr: Retrieve the records associated with the parameter
			
	Tables: [stat].[StatTypeNumeric0109]

	History:
		2018-01-23 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspStatTypeNumeric0109ByBatchLog](
     @psiBatchLogId INT
)
AS 
BEGIN
	SET NOCOUNT ON;

	SELECT PartitionId, KeyElementId, StatId, StatValue, BatchLogId
	FROM [stat].[StatTypeNumeric0109]
	WHERE BatchLogId = @psiBatchLogId

END

GO
