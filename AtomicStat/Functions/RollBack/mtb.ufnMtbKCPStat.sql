USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbKCPStat
	Created By: Larry Dugger
	Description: This designed to create Mtb KCP Stats for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbKCPStat](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.CustomerAccountNumber
		,pb.RoutingNumber
		,pb.AccountNumber
		,pb.StatId
		,pb.StatValue
	FROM [mtb].[ufnMtbKCPStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [mtb].[KCPBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
