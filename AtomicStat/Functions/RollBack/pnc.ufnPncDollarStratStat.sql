USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncDollarStratStat
	Created By: Larry Dugger
	Description: This designed to retrieve DollarStratStat for populating table in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncDollarStratStat](
	@psiStatBatchLogId SMALLINT
)
RETURNS TABLE AS RETURN
	SELECT ClientOrgId AS OrgId
		,CONVERT(MONEY,DollarStratItemAmountFloor) as ItemAmountFloor
		,CONVERT(MONEY,DollarStratItemAmountCeiling) as ItemAmountCeiling
		,CONVERT(MONEY,0.0) as TotalAmount
		,CONVERT(MONEY,DollarStratFraudReturnedAmount) as FraudReturnedAmount
		,DollarStratTargetLossRateBPS as TargetLossRateBPS
		,DollarStratFraudLossRateBPS as FraudLossRateBPS
		,StatBatchLogId as BatchProcessId
	FROM [pnc].[DollarStratStatsBulk];
