USE [TFBAtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[ufnBatchLogExist]
	Created By: Larry Dugger
	Description: This returns BatchLogId given the parameters, the 'guids' are
		treated as varchar for SSIS use

	Tables: [stat].[BatchLog]

	History:
		2018-01-21 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [stat].[ufnBatchLogExist](
	 @pvBatchUId VARCHAR(50)
	,@piBatchProcessId INT
	,@pvBatchProcessUId VARCHAR(50)
)
RETURNS INT
AS
BEGIN
	DECLARE @siBatchLogId INT;
		
	SELECT @siBatchLogId=BatchLogId
	FROM [stat].[BatchLog]  
	WHERE BatchUId = @pvBatchUId
		AND BatchProcessId = @piBatchProcessId
		AND BatchProcessUId = @pvBatchProcessUId;

	RETURN ISNULL(@siBatchLogId,-1);
END
