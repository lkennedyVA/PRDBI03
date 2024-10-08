USE [AtomicStat]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncCustomerAccountLevelStatId
	Created By: Lee Whiting
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2021-10-08 - LSW - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncCustomerAccountLevelStatId](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT 
		 Id = ROW_NUMBER() OVER (ORDER BY HashId ASC)
		,HashId
		,ClientOrgId
		,CustomerAccountNumber
	FROM [pnc].[CustomerAccountLevelBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,CustomerAccountNumber
	)
	SELECT 
		 Id
		,HashId
		,ClientOrgId
		,CustomerAccountNumber
	FROM Cte
;
