USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[ufnBatchLog]
	Created By: Larry Dugger
	Description: This function retrieves all BatchLogIds greater then the parameter

	Tables: [stat].[BatchLog]

	History:
		2017-10-10 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [stat].[ufnBatchLog](
	@psiBatchLogdId INT
)
RETURNS @tblBatchLog TABLE (
	BatchLogId int
)
AS
BEGIN	
	INSERT INTO @tblBatchLog(BatchLogId)
	SELECT BatchLogId
	FROM [stat].[BatchLog]
	WHERE BatchLogId > @psiBatchLogdId;

	RETURN
END
