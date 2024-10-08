USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbKCPStatId
	Created By: Larry Dugger
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbKCPStatId](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT ROW_NUMBER() OVER (ORDER BY HashId ASC) AS Id
		,HashId
		,ClientOrgId
		,CustomerAccountNumber
		,RoutingNumber
		,AccountNumber
	FROM [mtb].[KCPBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,CustomerAccountNumber,RoutingNumber,AccountNumber
	)
	SELECT Id
		,HashId
		,ClientOrgId
		,CustomerAccountNumber
		,RoutingNumber
		,AccountNumber
	FROM Cte;
