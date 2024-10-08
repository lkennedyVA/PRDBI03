USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncPayerRtnVolume
	Created By: Larry Dugger
	Description: This designed to retrieve PayerRtnVolume for populating table in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncPayerRtnVolume](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT ClientOrgId AS OrgId
		,TRY_CONVERT(NCHAR(9),PayerRoutingNumber) as PayerRoutingNumber
		,PayerRtnNumberVolumeToDate as PayerRtnVolumeToDate
		,StatBatchLogId as BatchProcessId
	FROM [pnc].[ABAStatsBulk]
	WHERE PayerRoutingNumber not like '%[^0-9A-Za-z]%';
