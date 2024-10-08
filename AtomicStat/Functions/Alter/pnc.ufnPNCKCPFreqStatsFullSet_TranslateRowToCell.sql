USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCKCPFreqStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 62479FB5-1573-4933-A55A-A979589A867C
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCKCPFreqStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCKCPFreqStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100009 AS ClientOrgId
	-- CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	 unpvt.StatIdTransportRef AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber],unpvt.[PayerRoutingNumber],unpvt.[PayerAccountNumber]
FROM (
			SELECT
				 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]

				,NULLIF( TRY_CONVERT( nvarchar(100), KCPMaxPerWeekClearedItemTotalAmount ), N'0.00' ) AS [0160]
			FROM DHayes.dbo.PNCKCPFreqStatsFullSet
			
			--GROUP BY 
			--	 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0160]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCKCPFreqStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
