USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncTransactionLocationStat
	Created By: Lee Whiting
	Description: This designed to create Pnc TransactionLocation Stats for populating tables in PRDBI02

	History:
		2021-10-08 - LSW - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncTransactionLocationStat](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT 
		 mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.LocationOrgId
		,pb.StatId
		,pb.StatValue
	FROM [pnc].[ufnPncTransactionLocationStatId]( @psiStatBatchLogId ) mpb 
		INNER JOIN [pnc].[TransactionLocationBulk] pb 
			ON mpb.HashId = pb.HashId
	WHERE pb.StatBatchLogId = @psiStatBatchLogId
;
