USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustTrxnStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: AEE8FED2-85FA-411C-8FBC-D52708A9E752
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPNCCustTrxnStatsDelta_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
			FROM DHayes.dbo.PNCCustTrxnStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [CustomerNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0134],[0141],[0142],[0238],[0239],[0240],[0241],[0345],[0367],[0368],[0454],[0453]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustTrxnStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;

