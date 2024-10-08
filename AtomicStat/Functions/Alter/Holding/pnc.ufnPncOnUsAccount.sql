USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncOnUsAccount
	Created By: Larry Dugger
	Description: This designed to retrieve OnUsAccount for populating table in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncOnUsAccount](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT AccountNumber
		,ClientOrgId AS OrgId
		,AccountCloseDate
		,AccountHoldCode
		,AccountRestrictionCode
		,AccountStatusDesc
		,AccountSubProductCode
		,StatBatchLogId as BatchProcessId
	FROM [pnc].[OnUsAccountStatsBulk];
