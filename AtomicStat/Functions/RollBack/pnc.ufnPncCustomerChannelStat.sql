USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncCustomerChannelStat
	Created By: Larry Dugger
	Description: This designed to create Pnc CustomerChannel Stats for populating tables in PRDBI02

	History:
		2019-07-29 - LBD - Created
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPncCustomerChannelStat](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.ChannelOrgId
		,pb.CustomerNumber
		,pb.StatId
		,pb.StatValue
	FROM [Pnc].[ufnPncCustomerChannelStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [Pnc].[CustomerChannelBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
