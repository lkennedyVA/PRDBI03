USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBPayerTrxnStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: C6AD6E39-B758-41E1-96B7-F804773B1728
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBPayerTrxnStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100008 AS ClientOrgId
	 CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[PayerRoutingNumber],unpvt.[PayerAccountNumber]
FROM (SELECT 
[PayerRoutingNumber],[PayerAccountNumber]

			,ISNULL( TRY_CONVERT( nvarchar(100), Payer0to6WksBackClearedItemCount ), N'☠' ) AS _0079
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerAllItemCount ), N'☠' ) AS _0080
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerAllReturnedAmount ), N'☠' ) AS _0081
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerAllReturnedItemCount ), N'☠' ) AS _0082
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerClearedItemCount ), N'☠' ) AS _0083
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerFirstTranDate ), N'☠' ) AS _0084
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerLastClearedCycleDate ), N'☠' ) AS _0087
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerLastFraudReturnDate ), N'☠' ) AS _0089
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerLastTranDate ), N'☠' ) AS _0090
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxCheckNum0to6WksBack ), N'☠' ) AS _0091
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxCheckNum0WkBack ), N'☠' ) AS _0092
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxCheckNum1stWkBack ), N'☠' ) AS _0093
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxCheckNum2ndWkBack ), N'☠' ) AS _0094
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxClearedCheckAmount ), N'☠' ) AS _0095
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMinCheckNum0to6WksBack ), N'☠' ) AS _0096
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMinCheckNum0WkBack ), N'☠' ) AS _0097
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMinCheckNum1stWkBack ), N'☠' ) AS _0098
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMinCheckNum2ndWkBack ), N'☠' ) AS _0099
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerNSFReturnedItemCount ), N'☠' ) AS _0100
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerDistinctClearedCustomerCount180 ), N'☠' ) AS _0167
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerFraudReturnedAmount ), N'☠' ) AS _0168
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerFraudReturnedItemCount ), N'☠' ) AS _0169
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerLastAllReturnDate ), N'☠' ) AS _0170
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerLastNSFReturnDate ), N'☠' ) AS _0171
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerClearedItemAmount ), N'☠' ) AS _0244
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerClearedItemCountMobile180 ), N'☠' ) AS _0245
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxClearedCheckNum365 ), N'☠' ) AS _0246
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMaxClearedCheckNum42 ), N'☠' ) AS _0247
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMinClearedCheckNum365 ), N'☠' ) AS _0248
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerMinClearedCheckNum42 ), N'☠' ) AS _0249
	FROM DHayes.dbo.MTBPayerTrxnStatsDelta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = mtb.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )
	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0079,_0080,_0081,_0082,_0083,_0084,_0087,_0089,_0090,_0091,_0092,_0093,_0094,_0095,_0096,_0097,_0098,_0099,_0100,_0167,_0168,_0169,_0170,_0171,_0244,_0245,_0246,_0247,_0248,_0249
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBPayerTrxnStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
