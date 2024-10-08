USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMtbDollarStratStat
	Created By: Larry Dugger
	Description: This designed to retrieve DollarStratStat for populating table in PRDBI02

	History:
		2019-03-20 - LBD - Created
		2019-04-04 - LBD - Modified, returns all data
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMtbDollarStratStat](
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
	FROM [mtb].[DollarStratStatsBulk];
	--WHERE StatBatchLogId = @psiStatBatchLogId;
