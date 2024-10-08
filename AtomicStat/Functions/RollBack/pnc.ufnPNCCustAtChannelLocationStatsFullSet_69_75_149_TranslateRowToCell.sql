USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelLocationStatsFullSet_69_75_149_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: CEBCEA91-7675-4B61-A419-460BA36A998B
	Generation set version: 2C3C9DD2-4C2A-49D8-9BF0-076EA8C34488
	Description: Unpivot the relevant stat columns from PNCCustAtChannelLocationStatsFullSet.
		
	History:
		2019-08-05 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelLocationStatsFullSet_69_75_149_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
				[0149]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelLocationStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId] AND unpvt.[ProcessOrgId] = h.[ProcessOrgId]
;
