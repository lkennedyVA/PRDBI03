USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAtChannelMonthlyMaxStatsDelta_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 114585CF-AB85-424E-9F12-9E3A7C83D29B
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAtChannelMonthlyMaxStatsDelta_HashId]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber],luc.[OrgId] AS ChannelOrgId

			FROM DHayes.dbo.PNCCustAtChannelMonthlyMaxStatsDelta d
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

