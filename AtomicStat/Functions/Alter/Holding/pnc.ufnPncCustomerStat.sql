USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncCustomerStat
	Created By: Larry Dugger
	Description: This designed to create Pnc Customer Stats for populating tables in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncCustomerStat](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT pcs.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.CustomerNumber
		,pb.StatId
		,pb.StatValue
	FROM [pnc].[ufnPncCustomerStatId](@psiStatBatchLogId) pcs 
	INNER JOIN [Pnc].[CustomerBulk] pb on pcs.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
