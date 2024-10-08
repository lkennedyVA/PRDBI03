USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncDollarStratGeoId
	Created By: Larry Dugger
	Description: This designed to create a solid Id for populating tables in PRDBI02

	History:
		2019-07-27 - LBD - Created
		2019-07-28 - LBD - Modified, added the KeyTemplatePreprocessed
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPncDollarStratGeoId](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	WITH Cte AS (
	SELECT ROW_NUMBER() OVER (ORDER BY biscuit.HashId ASC) AS Id
		,biscuit.HashId
		,biscuit.OrgId
		,biscuit.DollarStratRangeId
		,biscuit.Geo
		,biscuit.ItemFloor
		,biscuit.ItemCeiling
	FROM (
		SELECT CONVERT(binary(64),HASHBYTES( N'SHA2_512',REPLACE(
			--2019-07-28 REPLACE('43|[{FinancialGeoOrgId}]|44|[{FinancialDollarStratRangeId}]','[{FinancialGeoOrgId}]', g.OrgId),
			REPLACE(kt.KeyTemplatePreprocessed,N'[{FinancialGeoOrgId}]', g.OrgId),
				N'[{FinancialDollarStratRangeId}]', dsr.DollarStratRangeId))
			) as HashId
			,g.OrgId
			,dsr.DollarStratRangeId
			,g.[Name] AS Geo
			,dsglsb.[DollarStratGeoLargeItemAmountFloor] AS ItemFloor
			,dsglsb.[DollarStratGeoLargeItemAmountCeiling] AS ItemCeiling
		FROM [pnc].[DollarStratGeoLargeStatsBulk] dsglsb
		INNER JOIN [stat].[DollarStratRange] dsr on dsglsb.[DollarStratGeoLargeItemAmountFloor] = dsr.RangeFloor
												and dsglsb.[DollarStratGeoLargeItemAmountCeiling] = dsr.RangeCeiling
		INNER JOIN [stat].[Geo] g on dsglsb.GeoLarge = g.[Name]
		CROSS APPLY [stat].[KeyType] kt 
		WHERE dsglsb.StatBatchLogId = @psiStatBatchLogId
			AND kt.KeyTypeCode = N'4F621A8F-DCED-4E24-A2DD-198EEDC1DF2F'
			and 0 = 1 -- 2021-08-23
		UNION
		SELECT CONVERT(binary(64),HASHBYTES( N'SHA2_512',REPLACE(
			--2019-07-28 REPLACE('43|[{FinancialGeoOrgId}]|44|[{FinancialDollarStratRangeId}]','[{FinancialGeoOrgId}]', g.OrgId),
			REPLACE(kt.KeyTemplatePreprocessed,N'[{FinancialGeoOrgId}]', g.OrgId),
				N'[{FinancialDollarStratRangeId}]', dsr.DollarStratRangeId))
		) as HashId
			,g.OrgId
			,dsr.DollarStratRangeId
			,g.[Name] AS Geo
			,dsglsb.[DollarStratGeoSmallItemAmountFloor] AS ItemFloor
			,dsglsb.[DollarStratGeoSmallItemAmountCeiling] AS ItemCeiling
		FROM [pnc].[DollarStratGeoSmallStatsBulk] dsglsb
		INNER JOIN [stat].[DollarStratRange] dsr on dsglsb.[DollarStratGeoSmallItemAmountFloor] = dsr.RangeFloor
												and dsglsb.[DollarStratGeoSmallItemAmountCeiling] = dsr.RangeCeiling
		INNER JOIN [stat].[Geo] g on dsglsb.GeoSmall = g.[Name]
		CROSS APPLY [stat].[KeyType] kt 
		WHERE dsglsb.StatBatchLogId = @psiStatBatchLogId
			AND kt.KeyTypeCode = N'4F621A8F-DCED-4E24-A2DD-198EEDC1DF2F'
			and 0 = 1 -- 2021-08-23
			)AS  biscuit
	GROUP BY biscuit.HashId,biscuit.OrgId,biscuit.DollarStratRangeId,biscuit.Geo,biscuit.ItemFloor,biscuit.ItemCeiling
	)
	SELECT Id
		,HashId
		,OrgId
		,DollarStratRangeId
		,Geo
		,ItemFloor
		,ItemCeiling
	FROM Cte
;
