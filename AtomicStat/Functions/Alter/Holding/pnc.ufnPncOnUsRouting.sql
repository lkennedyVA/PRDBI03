USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncOnUsAccount
	Created By: Larry Dugger
	Description: This designed to retrieve OnUsRouting for populating table in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncOnUsRouting](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT ClientOrgId AS OrgId
		,TRY_CONVERT(NCHAR(9),PayerRoutingNumber) as RoutingNumber
		,StatBatchLogId as BatchProcessId
	FROM [pnc].[OnUsPayerRoutingNumberBulk];
