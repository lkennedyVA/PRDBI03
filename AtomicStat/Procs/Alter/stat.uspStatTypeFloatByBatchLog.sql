USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatTypeFloatByBatchLog]
	Created By: Larry Dugger
	Descr: Retrieve the records associated with the parameter
			
	Tables: [stat].[StatTypeFloat]

	History:
		2017-10-10 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspStatTypeFloatByBatchLog](
     @psiBatchLogId INT
)
AS 
BEGIN
	SET NOCOUNT ON;

	SELECT PartitionId, KeyElementId, StatId, StatValue, BatchLogId
	FROM [stat].[StatTypeFloat]
	WHERE BatchLogId = @psiBatchLogId

END

GO
