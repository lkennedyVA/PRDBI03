USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayer95thAmountStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 11FF4D97-88F4-40B3-B70D-23B99710987E
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCPayer95thAmountStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayer95thAmountStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId int )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), Payer95thClearedCheckAmount ), N'0.00' ) AS [0165]
			FROM DHayes.dbo.PNCPayer95thAmountStatsFullSet
			
			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0165]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayer95thAmountStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
