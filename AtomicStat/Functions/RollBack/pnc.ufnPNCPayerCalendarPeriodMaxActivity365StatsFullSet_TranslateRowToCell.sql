USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 5FABD8D6-9F33-4F18-B984-F949D6A2AEC2
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCPayerCalendarPeriodMaxActivity365StatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100009 AS ClientOrgId
	-- CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	 unpvt.StatIdTransportRef AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[PayerRoutingNumber],unpvt.[PayerAccountNumber]
FROM (
			SELECT
				 [PayerRoutingNumber],[PayerAccountNumber]

				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerDayClearedItemAmount365 ), N'0.00' ) AS [0173]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerMonthClearedItemAmount365 ), N'0.00' ) AS [0174]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerMonthClearedItemCount365 ), N'0' ) AS [0175]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerWeekClearedItemAmount365 ), N'0.00' ) AS [0176]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerWeekClearedItemCount365 ), N'0' ) AS [0177]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerDayClearedMobileItemCount365 ), N'0' ) AS [0444]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerWeekClearedMobileItemCount365 ), N'0' ) AS [0445]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerMonthClearedMobileItemCount365 ), N'0' ) AS [0446]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerMonthClearedMobileItemAmount365 ), N'0.00' ) AS [0447]
			FROM DHayes.dbo.PNCPayerCalendarPeriodMaxActivity365StatsFullSet
			
			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0173],[0174],[0175],[0176],[0177],[0444],[0445],[0446],[0447]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
