USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_447_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: B3F79C21-C24B-4C12-981A-F48CEB04279B
	Generation set version: 7BB785E4-FEE9-4BF2-99A6-CF9B04E60CB5
	Description: Unpivot the relevant stat columns from PNCPayerCalendarPeriodMaxActivity365StatsFullSet.
		
	History:
		2019-08-21 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_447_TranslateRowToCell]( @psiStatBatchLogId int )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), PayerMaxPerMonthClearedMobileItemAmount365 ), N'0.00' ) AS [0447]
			FROM DHayes.dbo.PNCPayerCalendarPeriodMaxActivity365StatsFullSet
			
			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0447]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
