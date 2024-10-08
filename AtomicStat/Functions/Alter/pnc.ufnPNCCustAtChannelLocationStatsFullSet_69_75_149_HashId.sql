USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelLocationStatsFullSet_69_75_149_HashId()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 1A234F25-C0DF-44B8-842E-C77E53060AD2
	Generation set version: 2C3C9DD2-4C2A-49D8-9BF0-076EA8C34488
	Description: Generate the relevant HashId values from PNCCustAtChannelLocationStatsFullSet.
		
	History:
		2019-08-05 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelLocationStatsFullSet_69_75_149_HashId]( @psiStatBatchLogId int )
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

