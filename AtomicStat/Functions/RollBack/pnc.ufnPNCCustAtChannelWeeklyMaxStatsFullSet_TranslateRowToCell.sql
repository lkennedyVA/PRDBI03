USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelWeeklyMaxStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 96902453-6E47-4A13-9EEB-D2559F5FAE43
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustAtChannelWeeklyMaxStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelWeeklyMaxStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
			FROM DHayes.dbo.PNCCustAtChannelWeeklyMaxStatsFullSet d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
			

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0151],[0152]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelWeeklyMaxStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId]
;
