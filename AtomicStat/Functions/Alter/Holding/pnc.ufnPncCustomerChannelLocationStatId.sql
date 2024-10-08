USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncCustomerChannelLocationStatId
	Created By: Larry Dugger
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2019-07-29 - LBD - Created
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPncCustomerChannelLocationStatId](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT ROW_NUMBER() OVER (ORDER BY HashId ASC) AS Id
		,HashId
		,ClientOrgId
		,ChannelOrgId
		,ProcessOrgId
		,CustomerNumber
	FROM [Pnc].[CustomerChannelLocationBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,ChannelOrgId,ProcessOrgId,CustomerNumber
	)
	SELECT Id
		,HashId
		,ClientOrgId
		,ChannelOrgId
		,ProcessOrgId
		,CustomerNumber
	FROM Cte;
