USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerCalendarPeriodMaxActivity365StatsDelta_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 85C4BFEA-6E25-4B09-BB7A-E8EB78DC0840
	Generation set version: 705899EC-2492-46DE-9BF1-CA33817E415E
	Description: Unpivot the relevant stat columns from PNCPayerCalendarPeriodMaxActivity365StatsDelta.
		
	History:
		2019-08-21 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsDelta_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
			FROM DHayes.dbo.PNCPayerCalendarPeriodMaxActivity365StatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0173],[0174],[0175],[0176],[0177],[0444],[0445],[0446],[0447]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
