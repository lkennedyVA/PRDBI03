USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncCustomerAccountLevelStat
	Created By: Lee Whiting
	Description: This designed to create Pnc CustomerAccountLevel Stats for populating tables in PRDBI02

	History:
		2021-10-08 - LSW - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncCustomerAccountLevelStat](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT 
		 mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.CustomerAccountNumber
		,pb.StatId
		,pb.StatValue
	FROM [pnc].[ufnPncCustomerAccountLevelStatId]( @psiStatBatchLogId ) mpb 
		INNER JOIN [pnc].[CustomerAccountLevelBulk] pb 
			ON mpb.HashId = pb.HashId
	WHERE pb.StatBatchLogId = @psiStatBatchLogId
;
