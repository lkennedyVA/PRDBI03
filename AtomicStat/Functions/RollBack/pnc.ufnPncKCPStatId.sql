USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncKCPStatId
	Created By: Larry Dugger
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncKCPStatId](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT ROW_NUMBER() OVER (ORDER BY HashId ASC) AS Id
		,HashId
		,ClientOrgId
		,CustomerNumber
		,RoutingNumber
		,AccountNumber
	FROM [Pnc].[KCPBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,CustomerNumber,RoutingNumber,AccountNumber
	)
	SELECT Id
		,HashId
		,ClientOrgId
		,CustomerNumber
		,RoutingNumber
		,AccountNumber
	FROM Cte;
