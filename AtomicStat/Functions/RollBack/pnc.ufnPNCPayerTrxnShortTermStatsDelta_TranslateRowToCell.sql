USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerTrxnShortTermStatsDelta_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 6F0FCA30-B172-413D-8C95-59DB0535AC17
	Generation set version: 8F0BC5EE-CD07-46A6-9595-20610FD538DD
	Description: Unpivot the relevant stat columns from PNCPayerTrxnShortTermStatsDelta.
		
	History:
		2019-07-30 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerTrxnShortTermStatsDelta_TranslateRowToCell]( @psiStatBatchLogId smallint )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), PayerThisMonthItemCount ), N'0' ) AS [0449]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerItemCount30 ), N'0' ) AS [0450]
				,NULLIF( TRY_CONVERT( nvarchar(100), PayerItemAmount30 ), N'0' ) AS [0451]
			FROM DHayes.dbo.PNCPayerTrxnShortTermStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0449],[0450],[0451]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayerTrxnShortTermStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
