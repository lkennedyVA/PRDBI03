USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAccountSummaryStatsFullSet_69_75_149_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 8ED78E6E-2408-4D18-B23C-DFC67DBBBE4D
	Generation set version: 2C3C9DD2-4C2A-49D8-9BF0-076EA8C34488
	Description: Unpivot the relevant stat columns from PNCCustAccountSummaryStatsFullSet.
		
	History:
		2019-08-05 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAccountSummaryStatsFullSet_69_75_149_TranslateRowToCell]( @psiStatBatchLogId int )
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
			FROM DHayes.dbo.PNCCustAccountSummaryStatsFullSet
			
			--GROUP BY 
			--	 [CustomerNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0069],[0075]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAccountSummaryStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
