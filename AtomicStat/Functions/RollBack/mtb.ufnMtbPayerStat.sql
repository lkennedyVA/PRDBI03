USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbPayerStat
	Created By: Larry Dugger
	Description: This designed to create Mtb Payer Stats for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbPayerStat](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.RoutingNumber
		,pb.AccountNumber
		,pb.StatId
		,pb.StatValue
	FROM [mtb].[ufnMtbPayerStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [mtb].[PayerBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
