USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustTrxnStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 138B47BF-ADC1-4FD0-8C9F-3321062CF639
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustTrxnStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustTrxnStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100009 AS ClientOrgId
	-- CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	 unpvt.StatIdTransportRef AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber]
FROM (
			SELECT
				 [CustomerNumber]

				,NULLIF( TRY_CONVERT( nvarchar(100), CustClearedItemMaxAmount ), N'0.00' ) AS [0134]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustSinceLastReturnClearedItemCount ), N'0' ) AS [0141]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustSinceLastReturnClearedItemTotalAmount ), N'0.00' ) AS [0142]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustClearedItemDistinctPayerCount ), N'0' ) AS [0238]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustCycleDateCount ), N'0' ) AS [0239]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustMobileItemAmount ), N'0.00' ) AS [0240]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustMobileItemCount ), N'0' ) AS [0241]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustClearedItemDistinctPayerCount180 ), N'0' ) AS [0345]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustCycleWeekCount ), N'0' ) AS [0367]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustMobileCycleDateCount ), N'0' ) AS [0368]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustOver1KClearedItemAmount ), N'0.00' ) AS [0453]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustOver1KClearedItemCount ), N'0' ) AS [0454]
			FROM DHayes.dbo.PNCCustTrxnStatsFullSet
			
			--GROUP BY 
			--	 [CustomerNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0134],[0141],[0142],[0238],[0239],[0240],[0241],[0345],[0367],[0368],[0453],[0454]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustTrxnStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
