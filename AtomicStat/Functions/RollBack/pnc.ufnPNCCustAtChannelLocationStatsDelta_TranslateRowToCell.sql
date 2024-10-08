USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelLocationStatsDelta_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 1B03256B-26F3-4F00-928D-90A58B1D3191
	Generation set version: 018DE30F-4EE9-45CE-B8AD-8ACCE07401AC
	Description: Unpivot the relevant stat columns from PNCCustAtChannelLocationStatsDelta.
		
	History:
		2019-08-06 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelLocationStatsDelta_TranslateRowToCell]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

SELECT 
	--@psiStatBatchLogId AS StatBatchLogId
	--,100009 AS ClientOrgId
	-- CONVERT( smallint, SUBSTRING( unpvt.StatIdTransportRef, 2, 4 ) ) AS StatId
	 unpvt.StatIdTransportRef AS StatId
	,h.[HashId]
	,unpvt.StatValue AS StatValue
	,unpvt.[CustomerNumber],unpvt.[ChannelOrgId],unpvt.[ProcessOrgId]
FROM (
			SELECT
				 [CustomerNumber],luc.[OrgId] AS ChannelOrgId,lul.[OrgId] AS ProcessOrgId

				,NULLIF( TRY_CONVERT( nvarchar(100), CustAtChannelLocationClearedItemAmount180 ), N'0.00' ) AS [0147]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustAtChannelLocationClearedItemCount180 ), N'0' ) AS [0149]
			FROM DHayes.dbo.PNCCustAtChannelLocationStatsDelta d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				INNER JOIN stat.ClientLocation lul
					ON d.LocationIdentifier = lul.ClientLocationCode
						AND lul.ClientOrgId = 100009
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )


	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0147],[0149]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelLocationStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId] AND unpvt.[ProcessOrgId] = h.[ProcessOrgId]
;
