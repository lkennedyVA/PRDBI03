USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelStatsDelta_TranslateRowToCell()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: C4EAAB93-311D-4371-970C-F979079BF301
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelStatsDelta_TranslateRowToCell]( @psiStatBatchLogId smallint )
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
			FROM DHayes.dbo.PNCCustAtChannelStatsDelta d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )


	) stat
	UNPIVOT
	(StatValue FOR StatIdTransportRef IN 
			(
				[0318],[0319],[0328]
			) 
	) AS unpvt
	INNER JOIN [pnc].[ufnPNCCustAtChannelStatsDelta_HashId]( @psiStatBatchLogId ) h 
		ON unpvt.[CustomerNumber] = h.[CustomerNumber] AND unpvt.[ChannelOrgId] = h.[ChannelOrgId]
;
