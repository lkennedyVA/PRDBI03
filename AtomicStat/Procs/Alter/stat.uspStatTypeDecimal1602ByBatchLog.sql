USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatTypeDecimal1602ByBatchLog]
	Created By: Chris Sharp 
	Descr: Retrieve the records associated with the parameter
			
	Tables: [stat].[StatTypeDecimal1602]

	History:
		2018-06-25 - CBS - Created
*****************************************************************************************/
ALTER   PROCEDURE [stat].[uspStatTypeDecimal1602ByBatchLog](
     @psiBatchLogId INT
)
AS 
BEGIN
	SET NOCOUNT ON;

	SELECT PartitionId, KeyElementId, StatId, StatValue, BatchLogId
	FROM [stat].[StatTypeDecimal1602]
	WHERE BatchLogId = @psiBatchLogId

END

GO
