USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBCustTrxnStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 835B2EF7-AB15-40FE-A252-7EA5064D6467
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBCustTrxnStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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

			,ISNULL( TRY_CONVERT( nvarchar(100), CustAcctClearedItemAvgAmount ), N'☠' ) AS _0038
			,ISNULL( TRY_CONVERT( nvarchar(100), CustAllItemAmount ), N'☠' ) AS _0042
			,ISNULL( TRY_CONVERT( nvarchar(100), CustAllItemCount ), N'☠' ) AS _0043
			,ISNULL( TRY_CONVERT( nvarchar(100), CustAllRetAmt ), N'☠' ) AS _0044
			,ISNULL( TRY_CONVERT( nvarchar(100), CustAllRets ), N'☠' ) AS _0045
			,ISNULL( TRY_CONVERT( nvarchar(100), CustClearedItemCount ), N'☠' ) AS _0046
			,ISNULL( TRY_CONVERT( nvarchar(100), CustClearedItemTotalAmount ), N'☠' ) AS _0047
			,ISNULL( TRY_CONVERT( nvarchar(100), CustMobileCycleWeekCount ), N'☠' ) AS _0048
			,ISNULL( TRY_CONVERT( nvarchar(100), CustMobileLastCycleDate ), N'☠' ) AS _0049
			,ISNULL( TRY_CONVERT( nvarchar(100), CustGradeNegativeFlag ), N'☠' ) AS _0051
			,ISNULL( TRY_CONVERT( nvarchar(100), CustGradeNegativeMsg ), N'☠' ) AS _0052
			,ISNULL( TRY_CONVERT( nvarchar(100), CustPriorMonthItemAmount ), N'☠' ) AS _0056
			,ISNULL( TRY_CONVERT( nvarchar(100), CustPriorMonthItemCount ), N'☠' ) AS _0057
			,ISNULL( TRY_CONVERT( nvarchar(100), CustPriorWeekItemAmount ), N'☠' ) AS _0058
			,ISNULL( TRY_CONVERT( nvarchar(100), CustPriorWeekItemCount ), N'☠' ) AS _0059
			,ISNULL( TRY_CONVERT( nvarchar(100), CustThisMonthPriorItemAmount ), N'☠' ) AS _0060
			,ISNULL( TRY_CONVERT( nvarchar(100), CustThisMonthPriorItemCount ), N'☠' ) AS _0061
			,ISNULL( TRY_CONVERT( nvarchar(100), CustThisWeekPriorItemAmount ), N'☠' ) AS _0062
			,ISNULL( TRY_CONVERT( nvarchar(100), CustThisWeekPriorItemCount ), N'☠' ) AS _0063
			,ISNULL( TRY_CONVERT( nvarchar(100), CustClearedItemMaxAmount ), N'☠' ) AS _0134
			,ISNULL( TRY_CONVERT( nvarchar(100), CustSinceLastReturnClearedItemCount ), N'☠' ) AS _0141
			,ISNULL( TRY_CONVERT( nvarchar(100), CustSinceLastReturnClearedItemTotalAmount ), N'☠' ) AS _0142
			,ISNULL( TRY_CONVERT( nvarchar(100), CustClearedItemDistinctPayerCount ), N'☠' ) AS _0238
			,ISNULL( TRY_CONVERT( nvarchar(100), CustCycleDateCount ), N'☠' ) AS _0239
			,ISNULL( TRY_CONVERT( nvarchar(100), CustMobileItemAmount ), N'☠' ) AS _0240
			,ISNULL( TRY_CONVERT( nvarchar(100), CustMobileItemCount ), N'☠' ) AS _0241
	FROM DHayes.dbo.MTBCustTrxnStatsDelta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = mtb.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )
	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0038,_0042,_0043,_0044,_0045,_0046,_0047,_0048,_0049,_0051,_0052,_0056,_0057,_0058,_0059,_0060,_0061,_0062,_0063,_0134,_0141,_0142,_0238,_0239,_0240,_0241
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBCustTrxnStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
