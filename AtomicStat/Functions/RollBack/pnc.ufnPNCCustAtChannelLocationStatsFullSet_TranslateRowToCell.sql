USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelLocationStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: FE851959-AA2F-4EC2-8676-CC419A0EF2C2
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustAtChannelLocationStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelLocationStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
			FROM DHayes.dbo.PNCCustAtChannelLocationStatsFullSet d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				INNER JOIN stat.ClientLocation lul
					ON d.LocationIdentifier = lul.ClientLocationCode
						AND lul.ClientOrgId = 100009

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0147],[0149]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelLocationStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId] AND unpvt.[ProcessOrgId] = h.[ProcessOrgId]
;
