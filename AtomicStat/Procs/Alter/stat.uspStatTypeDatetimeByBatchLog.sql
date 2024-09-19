USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatTypeDatetimeByBatchLog]
	Created By: Larry Dugger
	Descr: Retrieve the records associated with the parameter
			
	Tables: [stat].[StatTypeDatetime]

	History:
		2017-10-10 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspStatTypeDatetimeByBatchLog](
     @psiBatchLogId INT
)
AS 
BEGIN
	SET NOCOUNT ON;

	SELECT PartitionId, KeyElementId, StatId, StatValue, BatchLogId
	FROM [stat].[StatTypeDatetime]
	WHERE BatchLogId = @psiBatchLogId

END

GO
