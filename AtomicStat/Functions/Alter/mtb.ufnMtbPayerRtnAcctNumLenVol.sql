USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbPayerRtnAcctNumLenVol
	Created By: Larry Dugger
	Description: This designed to retrieve PayerRtnAcctNumLenVol for populating table in PRDBI02

	History:
		2019-03-20 - LBD - Created
		2019-04-04 - LBD - Modified, returns all data
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbPayerRtnAcctNumLenVol](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT ClientOrgId AS OrgId
		,TRY_CONVERT(NCHAR(9),PayerRoutingNumber) as PayerRoutingNumber
		,LengthPayerAcctNumber
		,PayerRtnAcctNumberLengthVolumeToDate
		,PayerRtnAcctNumLengthPercentOfTotalVolume
		,StatBatchLogId as BatchProcessId
	FROM [mtb].[ABAAcctLengthStatsBulk]
	WHERE PayerRoutingNumber not like '%[^0-9A-Za-z]%';
	--WHERE StatBatchLogId = @psiStatBatchLogId
	-- AND PayerRoutingNumber not like '%[^0-9A-Za-z]%';


