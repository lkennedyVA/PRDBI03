USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBWeeklyBFCustAUStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 7C365D5A-379F-4ACC-A980-D9A6F7E79890
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBWeeklyBFCustAUStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
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

			,ISNULL( TRY_CONVERT( nvarchar(100), AccountUtilizationBalance ), N'☠' ) AS _0113
			,ISNULL( TRY_CONVERT( nvarchar(100), AccountUtilizationDeposits ), N'☠' ) AS _0115
			,ISNULL( TRY_CONVERT( nvarchar(100), AccountUtilizationSpend ), N'☠' ) AS _0116
			,ISNULL( TRY_CONVERT( nvarchar(100), AccountUtilizationOverall ), N'☠' ) AS _0117
	FROM DHayes.dbo.MTBWeeklyBFCustAUStatsDelta d
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				_0113,_0115,_0116,_0117
			) 
	) AS unpvt
	INNER JOIN [mtb].[ufnMTBWeeklyBFCustAUStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber]
;
