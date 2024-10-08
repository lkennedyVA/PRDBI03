USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAccountSummaryStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: E973F22E-35BD-48A7-8673-8AD2F955E16D
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustAccountSummaryStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAccountSummaryStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId int )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), NSFItemsCycle3 ), N'0' ) AS [0069]
				,NULLIF( TRY_CONVERT( nvarchar(100), OverdraftItemsCycle3 ), N'0' ) AS [0075]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustNumberOfOpenAccounts ), N'0' ) AS [0381]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustNumberOfOpenSavingsAccounts ), N'0' ) AS [0382]
			FROM DHayes.dbo.PNCCustAccountSummaryStatsFullSet
			
			--GROUP BY 
			--	 [CustomerNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0069],[0075],[0381],[0382]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAccountSummaryStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
