USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbOnUsAccount
	Created By: Larry Dugger
	Description: This designed to retrieve OnUsRouting for populating table in PRDBI02

	History:
		2019-03-20 - LBD - Created
		2019-04-04 - LBD - Modified, returns all data
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMtbOnUsRouting](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT ClientOrgId AS OrgId
		--,TRY_CONVERT(NCHAR(9), [mtb].[ufnZeroFill](TRY_CONVERT(NVARCHAR(50),PayerRoutingNumber),9))  as RoutingNumber
		,TRY_CONVERT(NCHAR(9),PayerRoutingNumber) as RoutingNumber
		,StatBatchLogId as BatchProcessId
	FROM [mtb].[OnUsPayerRoutingNumberBulk];
	--WHERE StatBatchLogId = @psiStatBatchLogId;
