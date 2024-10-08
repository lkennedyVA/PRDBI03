USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelLocationStatsFullSet_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 55EB7EE3-18B5-47C9-9390-4F4A1DCE7AB3
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
		2019-12-10 - VALIDRS\LWhiting - Changed from stat.Location to stat.ClientLocation
													and changed from lul.LocationCode to lul.ClientLocationCode.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelLocationStatsFullSet_HashId]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber],luc.[OrgId] AS ChannelOrgId,lul.[OrgId] AS ProcessOrgId

			FROM DHayes.dbo.PNCCustAtChannelLocationStatsFullSet d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				INNER JOIN stat.ClientLocation lul
					ON d.LocationIdentifier = lul.ClientLocationCode
						AND lul.ClientOrgId = 100009

		)
	SELECT 
		-- 100009 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.PNCPayerGradeNeg tables.
		 [CustomerNumber],[ChannelOrgId],[ProcessOrgId]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
						
						,N'20|100009|18|25|19|' + CONVERT( nvarchar(50), CustomerNumber ) + N'|21|' + CONVERT( nvarchar(50), ChannelOrgId ) + N'|22|' + CONVERT( nvarchar(50), ProcessOrgId )

				) 
			, 1 ) AS HashId
	FROM cteIdentity
	;
