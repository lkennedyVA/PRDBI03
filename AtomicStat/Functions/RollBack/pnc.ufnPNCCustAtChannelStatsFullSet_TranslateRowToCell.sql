USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelStatsFullSet_TranslateRowToCell()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 9D67DBEF-85F4-4DB7-ABA7-A83820882ECF
	Generation set version: 57EFA6EC-61F2-4840-B66C-54777B1D34D8
	Description: Unpivot the relevant stat columns from PNCCustAtChannelStatsFullSet.
		
	History:
		2019-08-16 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelStatsFullSet_TranslateRowToCell]( @psiStatBatchLogId smallint )
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

				,NULLIF( TRY_CONVERT( nvarchar(100), CustAtChannelClearedItemAmount180 ), N'0.00' ) AS [0318]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustAtChannelClearedItemCount180 ), N'0' ) AS [0319]
				,NULLIF( TRY_CONVERT( nvarchar(100), CustAtChannelThisMonthItemCount ), N'0' ) AS [0328]
			FROM DHayes.dbo.PNCCustAtChannelStatsFullSet d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
			

	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0318],[0319],[0328]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelStatsFullSet_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId]
;
