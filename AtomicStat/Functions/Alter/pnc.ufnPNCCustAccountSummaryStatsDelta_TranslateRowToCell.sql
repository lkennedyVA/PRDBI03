USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAccountSummaryStatsDelta_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: FAE26E5F-8B08-4689-A2F1-04E6B5F0023B
	Generation set version: 018DE30F-4EE9-45CE-B8AD-8ACCE07401AC
	Description: Unpivot the relevant stat columns from PNCCustAccountSummaryStatsDelta.
		
	History:
		2019-08-06 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAccountSummaryStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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
			FROM DHayes.dbo.PNCCustAccountSummaryStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [CustomerNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0069],[0075],[0381],[0382]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAccountSummaryStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
