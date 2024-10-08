USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBDailyCustNegBalDays30and90Delta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: A633E946-E1E6-4BE2-9B01-A14B8695B78D
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBDailyCustNegBalDays30and90Delta_TranslateRowToCell]( @psiStatBatchLogId int )
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

			,ISNULL( TRY_CONVERT( nvarchar(100), CustCurrentBalanceNegativeCount30 ), N'☠' ) AS _0135
			,ISNULL( TRY_CONVERT( nvarchar(100), CustCurrentBalanceNegativeCount90 ), N'☠' ) AS _0136
	FROM DHayes.dbo.MTBDailyCustNegBalDays30and90Delta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = mtb.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )
	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0135,_0136
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBDailyCustNegBalDays30and90Delta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
