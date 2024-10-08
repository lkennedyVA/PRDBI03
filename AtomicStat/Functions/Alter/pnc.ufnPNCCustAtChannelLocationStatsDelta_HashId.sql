USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelLocationStatsDelta_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 0147DF24-EBCE-4ACD-8BEC-9F8767AE5871
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
		2019-12-10 - VALIDRS\LWhiting - Changed from stat.Location to stat.ClientLocation
													and changed from lul.LocationCode to lul.ClientLocationCode.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelLocationStatsDelta_HashId]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber],luc.[OrgId] AS ChannelOrgId,lul.[OrgId] AS ProcessOrgId

			FROM DHayes.dbo.PNCCustAtChannelLocationStatsDelta d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				INNER JOIN stat.ClientLocation lul
					ON d.LocationIdentifier = lul.ClientLocationCode
						AND lul.ClientOrgId = 100009
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )


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
