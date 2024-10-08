USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerReturnedTrxnStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 92040FF9-AB45-41C6-B129-16FD9EAC8F8B
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerReturnedTrxnStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), PayerAllReturnedAmount ), N'0.00' ) AS [0081]
			FROM DHayes.dbo.PNCPayerReturnedTrxnStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0081]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayerReturnedTrxnStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
