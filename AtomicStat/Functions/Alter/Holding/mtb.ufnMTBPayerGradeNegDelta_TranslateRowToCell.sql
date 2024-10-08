USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBPayerGradeNegDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 0D9E95A1-E923-4995-B0C0-BFDCFB288E5C
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBPayerGradeNegDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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

			,ISNULL( TRY_CONVERT( nvarchar(100), PayerGradeNegativeFlag ), N'☠' ) AS _0085
			,ISNULL( TRY_CONVERT( nvarchar(100), PayerGradeNegativeMsg ), N'☠' ) AS _0086
	FROM DHayes.dbo.MTBPayerGradeNegDelta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = mtb.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )
	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0085,_0086
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBPayerGradeNegDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[PayerRoutingNumber] = h.[PayerRoutingNumber] AND unpvt.[PayerAccountNumber] = h.[PayerAccountNumber]
;
