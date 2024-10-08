USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbCustomerAccountStat
	Created By: Larry Dugger
	Description: This designed to create Mtb CustomerAccount Stats for populating tables in PRDBI02

	History:
		2019-03-15 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbCustomerAccountStat](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT mpb.Id 
		,pb.HashId
		,pb.ClientOrgId
		,pb.CustomerAccountNumber
		,pb.StatId
		,pb.StatValue
	FROM [mtb].[ufnMtbCustomerAccountStatId](@psiStatBatchLogId) mpb 
	INNER JOIN [mtb].[CustomerAccountBulk] pb on mpb.HashId = pb.HashId
	WHERE StatBatchLogId = @psiStatBatchLogId;
