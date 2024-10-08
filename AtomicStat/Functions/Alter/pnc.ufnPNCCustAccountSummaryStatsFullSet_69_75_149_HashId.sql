USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAccountSummaryStatsFullSet_69_75_149_HashId()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: E73A4816-FDE5-406E-B424-1E54319FF7E8
	Generation set version: 2C3C9DD2-4C2A-49D8-9BF0-076EA8C34488
	Description: Generate the relevant HashId values from PNCCustAccountSummaryStatsFullSet.
		
	History:
		2019-08-05 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAccountSummaryStatsFullSet_69_75_149_HashId]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber]

			FROM DHayes.dbo.PNCCustAccountSummaryStatsFullSet
			
			--GROUP BY 
			--	 [CustomerNumber]

		)
	SELECT 
		-- 100009 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.PNCPayerGradeNeg tables.
		 [CustomerNumber]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
						
						,N'20|100009|18|25|19|' + CONVERT( nvarchar(50), CustomerNumber ) 

				) 
			, 1 ) AS HashId
	FROM cteIdentity

