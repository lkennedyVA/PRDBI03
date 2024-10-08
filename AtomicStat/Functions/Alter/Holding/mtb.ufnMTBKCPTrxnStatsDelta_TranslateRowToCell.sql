USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBKCPFreqStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 8DE5E7AC-067D-4211-9C8F-70543BA035CD
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBKCPFreqStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100008 AS ClientOrgId
	 CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber],unpvt.[PayerRoutingNumber],unpvt.[PayerAccountNumber]
FROM (SELECT 
[CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]

			,ISNULL( TRY_CONVERT( nvarchar(100), KCPMaxFreqPerDayCount ), N'☠' ) AS _0109
			,ISNULL( TRY_CONVERT( nvarchar(100), KCPMaxPerDayTotalAmount ), N'☠' ) AS _0110
			,ISNULL( TRY_CONVERT( nvarchar(100), KCPMaxFreqPerWeekClearedItemCount ), N'☠' ) AS _0159
			,ISNULL( TRY_CONVERT( nvarchar(100), KCPMaxPerWeekClearedItemTotalAmount ), N'☠' ) AS _0160
	FROM DHayes.dbo.MTBKCPFreqStatsDelta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = mtb.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )
	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0109,_0110,_0159,_0160
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBKCPFreqStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
