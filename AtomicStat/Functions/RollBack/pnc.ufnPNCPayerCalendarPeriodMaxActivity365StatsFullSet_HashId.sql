USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: AF326AA4-18DD-45B1-9110-BDA8C156353F
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerCalendarPeriodMaxActivity365StatsFullSet_HashId]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [PayerRoutingNumber],[PayerAccountNumber]

			FROM DHayes.dbo.PNCPayerCalendarPeriodMaxActivity365StatsFullSet
			
			--GROUP BY 
			--	 [PayerRoutingNumber],[PayerAccountNumber]

		)
	SELECT 
		-- 100009 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.PNCPayerGradeNeg tables.
		 [PayerRoutingNumber],[PayerAccountNumber]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
						
						,N'20|100009|14|' + CONVERT( nvarchar(50), PayerRoutingNumber ) + N'|15|' + CONVERT( nvarchar(50), PayerAccountNumber )

				) 
			, 1 ) AS HashId
	FROM cteIdentity

