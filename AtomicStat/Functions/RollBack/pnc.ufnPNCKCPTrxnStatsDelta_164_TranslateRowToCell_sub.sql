USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCKCPTrxnStatsDelta_164_TranslateRowToCell_sub()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 43FE29C2-2E82-40F1-8445-8633C55B08C5
	Generation set version: 44F1427E-C439-4C84-8880-74EBFF6FC70A
	Description: Unpivot the relevant stat columns from PNCKCPTrxnStatsDelta.
		
	History:
		2019-08-19 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER FUNCTION [pnc].[ufnPNCKCPTrxnStatsDelta_164_TranslateRowToCell_sub]( @psiStatBatchLogId smallint, @pnSubdivideBy numeric(36,18) )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100009 AS ClientOrgId
	-- CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	 unpvt.StatIdTransportRef AS StatId
	--,( ROW_NUMBER() OVER ( ORDER BY unpvt.StatIdTransportRef, unpvt.[CustomerNumber],unpvt.[PayerRoutingNumber],unpvt.[PayerAccountNumber] ) ) * ( CASE WHEN ISNULL( unpvt.StatValue, N'0' ) = N'0' THEN 0 ELSE 1 END ) AS StatBatchLogStatHashKeySeq
	,CEILING( ( ( ROW_NUMBER() OVER ( ORDER BY unpvt.StatIdTransportRef, unpvt.[CustomerNumber],unpvt.[PayerRoutingNumber],unpvt.[PayerAccountNumber] ) ) * 1.0 ) / @pnSubdivideBy ) AS Subdivision
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber]
	,unpvt.[PayerRoutingNumber]
	,unpvt.[PayerAccountNumber]
FROM (
			SELECT
				 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]

				,NULLIF( TRY_CONVERT( nvarchar(100), KCPSinceLastReturnClearedDepositCount ), N'0' ) AS [0164]
			FROM DHayes.dbo.PNCKCPTrxnStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0164]
			) 
	) AS unpvt
	--INNER JOIN [pnc].[ufnPNCKCPTrxnStatsDelta_HashId]( @psiStatBatchLogId ) h 
	--	ON unpvt.[CustomerNumber] = h.[CustomerNumber] 
	--		AND unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] 
	--		AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
	INNER JOIN [pncwork].[KCPHash] h
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
			AND unpvt.[PayerRoutingNumber] = h.[RoutingNumber]
			AND unpvt.[PayerAccountNumber] = h.[AccountNumber]
;
