USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncPayerRtnAcctNumLenVol
	Created By: Larry Dugger
	Description: This designed to retrieve PayerRtnAcctNumLenVol for populating table in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncPayerRtnAcctNumLenVol](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT ClientOrgId AS OrgId
		,TRY_CONVERT(NCHAR(9),PayerRoutingNumber) as PayerRoutingNumber
		,LengthPayerAcctNumber
		,PayerRtnAcctNumberLengthVolumeToDate
		,PayerRtnAcctNumLengthPercentOfTotalVolume
		,StatBatchLogId as BatchProcessId
	FROM [pnc].[ABAAcctLengthStatsBulk]
	WHERE PayerRoutingNumber not like '%[^0-9A-Za-z]%';
