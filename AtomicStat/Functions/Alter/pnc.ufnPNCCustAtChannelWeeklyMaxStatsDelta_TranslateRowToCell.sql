USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelWeeklyMaxStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 094F4FA1-1D8C-40D9-8E84-113B3AC8D460
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelWeeklyMaxStatsDelta_TranslateRowToCell]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100009 AS ClientOrgId
	-- CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	 unpvt.StatIdTransportRef AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber],unpvt.[ChannelOrgId]
FROM (
			SELECT
				 [CustomerNumber],luc.[OrgId] AS ChannelOrgId

				,NULLIF( TRY_CONVERT( nvarchar(100), CustMaxPerWeekAtChannelClearedItemAmount180 ), N'0.00' ) AS [0151]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustMaxPerWeekAtChannelClearedItemCount180 ), N'0' ) AS [0152]
			FROM DHayes.dbo.PNCCustAtChannelWeeklyMaxStatsDelta d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )


	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0151],[0152]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelWeeklyMaxStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId]
;
