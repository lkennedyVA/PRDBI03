USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerCalendarPeriodMaxActivity365StatsDelta_447_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 94D73BB9-62E6-4965-B1AF-453D542E22A1
	Generation set version: 9FA419FD-D4EA-484A-989C-30EFBEA98149
	Description: Unpivot the relevant stat columns from PNCPayerCalendarPeriodMaxActivity365StatsDelta.
		
	History:
		2019-08-21 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsDelta_447_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
			FROM DHayes.dbo.PNCPayerCalendarPeriodMaxActivity365StatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0447]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
