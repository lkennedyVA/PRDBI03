USE [AtomicStat]
GO

/****** Object:  UserDefinedFunction [stat].[ufnMaxBatchLog]    Script Date: 10/6/2024 1:26:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/****************************************************************************************
	Name: [stat].[ufnMaxBatchLog]
	Created By: Larry Dugger
	Description: This returns Max BatchLogId transfered to production StatGroup
		name provided

	Tables: [stat].[Stat]

	History:
		2018-01-20 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [stat].[ufnMaxBatchLog](
	@pnvStatGroupName NVARCHAR(50)
)
RETURNS SMALLINT
AS
BEGIN
	DECLARE @siBatchLogId smallint;
		
	SELECT @siBatchLogId = MaxBatchLogId
	FROM [stat].[StatGroupMaxBatch] sgmb
	INNER JOIN [Stat].[StatGroup] sg on sgmb.StatGroupId = sg.StatGroupId
	WHERE sg.[Name] = @pnvStatGroupName;

	RETURN ISNULL(@siBatchLogId,0);
END
GO


