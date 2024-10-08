USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelWeeklyMaxStatsDelta_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 71C37433-40D9-48CB-AE46-D4792B349BC0
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelWeeklyMaxStatsDelta_HashId]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber],luc.[OrgId] AS ChannelOrgId

			FROM DHayes.dbo.PNCCustAtChannelWeeklyMaxStatsDelta d
				INNER JOIN stat.Channel luc
					ON d.ChannelId = luc.ChannelCode
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )


		)
	SELECT 
		-- 100009 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.PNCPayerGradeNeg tables.
		 [CustomerNumber],[ChannelOrgId]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
						
						,N'20|100009|18|25|19|' + CONVERT( nvarchar(50), CustomerNumber ) + N'|21|' + CONVERT( nvarchar(50), ChannelOrgId )

				) 
			, 1 ) AS HashId
	FROM cteIdentity

