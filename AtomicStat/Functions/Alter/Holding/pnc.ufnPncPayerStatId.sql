USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncPayerStatId
	Created By: Larry Dugger
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncPayerStatId](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT ROW_NUMBER() OVER (ORDER BY HashId ASC) AS Id
		,HashId
		,ClientOrgId
		,RoutingNumber
		,AccountNumber
	FROM [Pnc].[PayerBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,RoutingNumber,AccountNumber
	)
	SELECT Id
		,HashId
		,ClientOrgId
		,RoutingNumber
		,AccountNumber
	FROM Cte;
