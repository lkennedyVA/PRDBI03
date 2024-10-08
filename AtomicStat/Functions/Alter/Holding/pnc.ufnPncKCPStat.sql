USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncKCPStat
	Created By: Larry Dugger
	Description: This designed to create Pnc KCP Stats for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncKCPStat](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.CustomerNumber
		,pb.RoutingNumber
		,pb.AccountNumber
		,pb.StatId
		,pb.StatValue
	FROM [Pnc].[ufnPncKCPStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [Pnc].[KCPBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
