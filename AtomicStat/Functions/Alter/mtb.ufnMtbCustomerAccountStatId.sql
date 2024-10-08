USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbCustomerAccountStatId
	Created By: Larry Dugger
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbCustomerAccountStatId](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT ROW_NUMBER() OVER (ORDER BY HashId ASC) AS Id
		,HashId
		,ClientOrgId
		,CustomerAccountNumber
	FROM [mtb].[CustomerAccountBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,CustomerAccountNumber
	)
	SELECT Id
		,HashId
		,ClientOrgId
		,CustomerAccountNumber
	FROM Cte;
