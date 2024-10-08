USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbOnUsAccount
	Created By: Larry Dugger
	Description: This designed to retrieve OnUsAccount for populating table in PRDBI02

	History:
		2019-03-20 - LBD - Created
		2019-04-04 - LBD - Modified, returns all data
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMtbOnUsAccount](
	@psiStatBatchLogId SMALLINT
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
	FROM [mtb].[OnUsAccountStatsBulk];
	--WHERE StatBatchLogId = @psiStatBatchLogId;
