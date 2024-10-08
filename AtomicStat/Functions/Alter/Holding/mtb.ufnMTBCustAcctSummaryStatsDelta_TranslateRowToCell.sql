USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBCustAcctSummaryStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 128F8777-44BD-40B7-9071-BB5469746903
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBCustAcctSummaryStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100008 AS ClientOrgId
	 CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber]
FROM (SELECT 
[CustomerNumber]

			,ISNULL( TRY_CONVERT( nvarchar(100), CustAverageLedgerBalanceCycle1 ), N'☠' ) AS _0039
			,ISNULL( TRY_CONVERT( nvarchar(100), CustCurrentBalance ), N'☠' ) AS _0041
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemCount ), N'☠' ) AS _0050
			,ISNULL( TRY_CONVERT( nvarchar(100), CustomerTypeDesc ), N'☠' ) AS _0053
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOpenDate ), N'☠' ) AS _0054
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftCount ), N'☠' ) AS _0055
			,ISNULL( TRY_CONVERT( nvarchar(100), CustFirstAnyAcctOpenDate ), N'☠' ) AS _0064
			,ISNULL( TRY_CONVERT( nvarchar(100), CustHardHoldFlag ), N'☠' ) AS _0065
			,ISNULL( TRY_CONVERT( nvarchar(100), CustLastDepositDate ), N'☠' ) AS _0066
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemsCycle1 ), N'☠' ) AS _0067
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemsCycle2 ), N'☠' ) AS _0068
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemsCycle3 ), N'☠' ) AS _0069
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemsCycle4 ), N'☠' ) AS _0070
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemsCycle5 ), N'☠' ) AS _0071
			,ISNULL( TRY_CONVERT( nvarchar(100), CustNSFItemsCycle6 ), N'☠' ) AS _0072
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftItemsCycle1 ), N'☠' ) AS _0073
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftItemsCycle2 ), N'☠' ) AS _0074
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftItemsCycle3 ), N'☠' ) AS _0075
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftItemsCycle4 ), N'☠' ) AS _0076
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftItemsCycle5 ), N'☠' ) AS _0077
			,ISNULL( TRY_CONVERT( nvarchar(100), CustOverdraftItemsCycle6 ), N'☠' ) AS _0078
	FROM DHayes.dbo.MTBCustAcctSummaryStatsDelta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = mtb.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )
	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0039,_0041,_0050,_0053,_0054,_0055,_0064,_0065,_0066,_0067,_0068,_0069,_0070,_0071,_0072,_0073,_0074,_0075,_0076,_0077,_0078
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBCustAcctSummaryStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
