USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustMonthlyMaxStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: ACC8A286-2DE9-4DBE-9A0C-DD63B8F86802
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustMonthlyMaxStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustMonthlyMaxStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), CustMaxPerMonthClearedItemAmount180 ), N'0.00' ) AS [0337]
			FROM DHayes.dbo.PNCCustMonthlyMaxStatsFullSet
			
			--GROUP BY 
			--	 [CustomerNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0337]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustMonthlyMaxStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
