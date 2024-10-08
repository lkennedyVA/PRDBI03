USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPncDollarStratGeo
	Created By: Larry Dugger
	Description: This designed to create Pnc Dollar Strat Geos for populating tables in PRDBI02

	History:
		2019-07-27 - LBD - Created
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPncDollarStratGeo](
	@psiStatBatchLogId INT
)
RETURNS TABLE AS RETURN
	SELECT biscuit.Id 
		,biscuit.HashId
		,biscuit.OrgId
		,biscuit.DollarStratRangeId
		,s.StatId
		--,biscuit.StatName
		,biscuit.StatValue
	FROM (SELECT pds.Id 
			,pds.HashId
			,unpvtlarge.OrgId
			,unpvtlarge.DollarStratRangeId
			,unpvtlarge.StatName
			,unpvtlarge.StatValue
		FROM [pnc].[ufnPncDollarStratGeoId](@psiStatBatchLogId) pds
		INNER JOIN (SELECT g.OrgId
				,dsr.DollarStratRangeId
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemsCleared180]),N'NULL') AS DollarStratGeoLargeItemsCleared180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountCleared180]),N'NULL') AS DollarStratGeoLargeItemAmountCleared180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmount180]),N'NULL') AS DollarStratGeoLargeItemAmount180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountReturned180]),N'NULL') AS DollarStratGeoLargeItemAmountReturned180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeLossRateBPS180]),N'NULL') AS DollarStratGeoLargeLossRateBPS180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemsCleared90]),N'NULL') AS DollarStratGeoLargeItemsCleared90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountCleared90]),N'NULL') AS DollarStratGeoLargeItemAmountCleared90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmount90]),N'NULL') AS DollarStratGeoLargeItemAmount90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountReturned90]),N'NULL') AS DollarStratGeoLargeItemAmountReturned90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeLossRateBPS90]),N'NULL') AS DollarStratGeoLargeLossRateBPS90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemsCleared60]),N'NULL') AS DollarStratGeoLargeItemsCleared60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountCleared60]),N'NULL') AS DollarStratGeoLargeItemAmountCleared60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmount60]),N'NULL') AS DollarStratGeoLargeItemAmount60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountReturned60]),N'NULL') AS DollarStratGeoLargeItemAmountReturned60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeLossRateBPS60]),N'NULL') AS DollarStratGeoLargeLossRateBPS60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemsCleared30]),N'NULL') AS DollarStratGeoLargeItemsCleared30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountCleared30]),N'NULL') AS DollarStratGeoLargeItemAmountCleared30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmount30]),N'NULL') AS DollarStratGeoLargeItemAmount30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeItemAmountReturned30]),N'NULL') AS DollarStratGeoLargeItemAmountReturned30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoLargeLossRateBPS30]),N'NULL') AS DollarStratGeoLargeLossRateBPS30
			FROM [pnc].[DollarStratGeoLargeStatsBulk] dsglsb
			INNER JOIN [stat].[DollarStratRange] dsr on dsglsb.[DollarStratGeoLargeItemAmountFloor] = dsr.RangeFloor
													and dsglsb.[DollarStratGeoLargeItemAmountCeiling] = dsr.RangeCeiling
			INNER JOIN [stat].[Geo] g on dsglsb.GeoLarge = g.[Name]
			WHERE dsglsb.StatBatchLogId = @psiStatBatchLogId
			and 0 = 1 -- 2021-08-23
			) stat
			UNPIVOT
			 (StatValue FOR StatName IN 
				--MUST CHANGE IF ADDITIONAL FIELDS ARE ADDED
				([DollarStratGeoLargeItemsCleared180], [DollarStratGeoLargeItemAmountCleared180], [DollarStratGeoLargeItemAmount180]
					,[DollarStratGeoLargeItemAmountReturned180], [DollarStratGeoLargeLossRateBPS180], [DollarStratGeoLargeItemsCleared90]
					,[DollarStratGeoLargeItemAmountCleared90], [DollarStratGeoLargeItemAmount90], [DollarStratGeoLargeItemAmountReturned90]
					,[DollarStratGeoLargeLossRateBPS90], [DollarStratGeoLargeItemsCleared60], [DollarStratGeoLargeItemAmountCleared60]
					,[DollarStratGeoLargeItemAmount60], [DollarStratGeoLargeItemAmountReturned60], [DollarStratGeoLargeLossRateBPS60]
					,[DollarStratGeoLargeItemsCleared30], [DollarStratGeoLargeItemAmountCleared30], [DollarStratGeoLargeItemAmount30]
					,[DollarStratGeoLargeItemAmountReturned30], [DollarStratGeoLargeLossRateBPS30]
				) 
		) AS unpvtlarge ON pds.OrgId = unpvtlarge.OrgId
				AND pds.DollarStratRangeId = unpvtlarge.DollarStratRangeId
		UNION
		SELECT pds.Id 
			,pds.HashId
			,unpvtsmall.OrgId
			,unpvtsmall.DollarStratRangeId
			,unpvtsmall.StatName
			,unpvtsmall.StatValue
		FROM [pnc].[ufnPncDollarStratGeoId](@psiStatBatchLogId) pds
		INNER JOIN (SELECT g.OrgId
				,dsr.DollarStratRangeId
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemsCleared180]),N'NULL') AS DollarStratGeoSmallItemsCleared180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountCleared180]),N'NULL') AS DollarStratGeoSmallItemAmountCleared180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmount180]),N'NULL') AS DollarStratGeoSmallItemAmount180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountReturned180]),N'NULL') AS DollarStratGeoSmallItemAmountReturned180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallLossRateBPS180]),N'NULL') AS DollarStratGeoSmallLossRateBPS180
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemsCleared90]),N'NULL') AS DollarStratGeoSmallItemsCleared90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountCleared90]),N'NULL') AS DollarStratGeoSmallItemAmountCleared90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmount90]),N'NULL') AS DollarStratGeoSmallItemAmount90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountReturned90]),N'NULL') AS DollarStratGeoSmallItemAmountReturned90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallLossRateBPS90]),N'NULL') AS DollarStratGeoSmallLossRateBPS90
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemsCleared60]),N'NULL') AS DollarStratGeoSmallItemsCleared60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountCleared60]),N'NULL') AS DollarStratGeoSmallItemAmountCleared60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmount60]),N'NULL') AS DollarStratGeoSmallItemAmount60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountReturned60]),N'NULL') AS DollarStratGeoSmallItemAmountReturned60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallLossRateBPS60]),N'NULL') AS DollarStratGeoSmallLossRateBPS60
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemsCleared30]),N'NULL') AS DollarStratGeoSmallItemsCleared30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountCleared30]),N'NULL') AS DollarStratGeoSmallItemAmountCleared30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmount30]),N'NULL') AS DollarStratGeoSmallItemAmount30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallItemAmountReturned30]),N'NULL') AS DollarStratGeoSmallItemAmountReturned30
				,ISNULL(TRY_CONVERT(NVARCHAR(100),dsglsb.[DollarStratGeoSmallLossRateBPS30]),N'NULL') AS DollarStratGeoSmallLossRateBPS30
			FROM [pnc].[DollarStratGeoSmallStatsBulk] dsglsb
			INNER JOIN [stat].[DollarStratRange] dsr on dsglsb.[DollarStratGeoSmallItemAmountFloor] = dsr.RangeFloor
													and dsglsb.[DollarStratGeoSmallItemAmountCeiling] = dsr.RangeCeiling
			INNER JOIN [stat].[Geo] g on dsglsb.GeoSmall = g.[Name]
			where 0 = 1 -- 2021-08-23
			) stat
			UNPIVOT
			 (StatValue FOR StatName IN 
				--MUST CHANGE IF ADDITIONAL FIELDS ARE ADDED
				([DollarStratGeoSmallItemsCleared180], [DollarStratGeoSmallItemAmountCleared180], [DollarStratGeoSmallItemAmount180]
					,[DollarStratGeoSmallItemAmountReturned180], [DollarStratGeoSmallLossRateBPS180], [DollarStratGeoSmallItemsCleared90]
					,[DollarStratGeoSmallItemAmountCleared90], [DollarStratGeoSmallItemAmount90], [DollarStratGeoSmallItemAmountReturned90]
					,[DollarStratGeoSmallLossRateBPS90], [DollarStratGeoSmallItemsCleared60], [DollarStratGeoSmallItemAmountCleared60]
					,[DollarStratGeoSmallItemAmount60], [DollarStratGeoSmallItemAmountReturned60], [DollarStratGeoSmallLossRateBPS60]
					,[DollarStratGeoSmallItemsCleared30], [DollarStratGeoSmallItemAmountCleared30], [DollarStratGeoSmallItemAmount30]
					,[DollarStratGeoSmallItemAmountReturned30], [DollarStratGeoSmallLossRateBPS30]
				) 
		) AS unpvtsmall ON pds.OrgId = unpvtsmall.OrgId
				AND pds.DollarStratRangeId = unpvtsmall.DollarStratRangeId
	) as biscuit
	INNER JOIN [AtomicStat].[stat].[Stat] s on biscuit.StatName = s.[Name];
