USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCCustAccountSummaryStatsFullSet_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 66C67F8B-8864-476B-8447-FD474DB12C01
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCCustAccountSummaryStatsFullSet_HashId]( @psiStatBatchLogId int )
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

