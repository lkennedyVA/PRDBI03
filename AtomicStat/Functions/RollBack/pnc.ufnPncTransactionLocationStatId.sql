USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncTransactionLocationStatId
	Created By: Lee Whiting
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2021-10-08 - LSW - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncTransactionLocationStatId](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT 
		 Id = ROW_NUMBER() OVER (ORDER BY HashId ASC)
		,HashId
		,ClientOrgId
		,LocationOrgId
	FROM [pnc].[TransactionLocationBulk]
	WHERE StatBatchLogId = @psiStatBatchLogId
	GROUP BY HashId,ClientOrgId,LocationOrgId
	)
	SELECT 
		 Id
		,HashId
		,ClientOrgId
		,LocationOrgId
	FROM Cte
;
