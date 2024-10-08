USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayer95thAmountStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: A98B92B9-4BA8-4A2D-888D-3DBB256CD274
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayer95thAmountStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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
			FROM DHayes.dbo.PNCPayer95thAmountStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0165]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCPayer95thAmountStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
