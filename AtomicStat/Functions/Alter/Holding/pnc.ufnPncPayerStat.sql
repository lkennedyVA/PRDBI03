USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncPayerStat
	Created By: Larry Dugger
	Description: This designed to create Pnc Payer Stats for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncPayerStat](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.RoutingNumber
		,pb.AccountNumber
		,pb.StatId
		,pb.StatValue
	FROM [Pnc].[ufnPncPayerStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [Pnc].[PayerBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
