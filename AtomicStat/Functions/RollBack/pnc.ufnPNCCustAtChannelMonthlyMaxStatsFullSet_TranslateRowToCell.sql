USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelMonthlyMaxStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 9003104A-41F4-4553-AB87-5822CBB73BFC
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustAtChannelMonthlyMaxStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelMonthlyMaxStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), CustMaxPerMonthAtChannelClearedItemAmount180 ), N'0.00' ) AS [0315]
			FROM DHayes.dbo.PNCCustAtChannelMonthlyMaxStatsFullSet d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
			

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0315]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelMonthlyMaxStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId]
;
