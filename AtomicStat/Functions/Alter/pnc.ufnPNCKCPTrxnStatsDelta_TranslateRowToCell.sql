USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCKCPTrxnStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 201177A4-62CD-4FDA-9DD0-D86C6FF0960D
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCKCPTrxnStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), KCPDistinctDepositMonths ), N'0' ) AS [0154]
				,NULLIF( TRY_CONVERT( nvarchar(100), KCPMaxClearedItemAmount ), N'0.00' ) AS [0158]
				,NULLIF( TRY_CONVERT( nvarchar(100), KCPSinceLastReturnClearedDepositCount ), N'0' ) AS [0164]
				,NULLIF( TRY_CONVERT( nvarchar(100), KCPReturnedAmount ), N'0.00' ) AS [0254]
				,NULLIF( TRY_CONVERT( nvarchar(100), KCPDistinctDepositWeeks ), N'0' ) AS [0435]
			FROM DHayes.dbo.PNCKCPTrxnStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0154],[0158],[0164],[0254],[0435]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCKCPTrxnStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
