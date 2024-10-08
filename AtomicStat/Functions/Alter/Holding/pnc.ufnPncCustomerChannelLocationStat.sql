USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncCustomerChannelLocationStat
	Created By: Larry Dugger
	Description: This designed to create Pnc CustomerChannelLocation Stats for populating tables in PRDBI02

	History:
		2019-07-29 - LBD - Created
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPncCustomerChannelLocationStat](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.ChannelOrgId
		,pb.ProcessOrgId
		,pb.CustomerNumber
		,pb.StatId
		,pb.StatValue
	FROM [Pnc].[ufnPncCustomerChannelLocationStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [Pnc].[CustomerChannelLocationBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
