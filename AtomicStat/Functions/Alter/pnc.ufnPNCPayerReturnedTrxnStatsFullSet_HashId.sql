USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerReturnedTrxnStatsFullSet_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: CDDDACF4-EF05-45B4-ACCA-1E45A74A55F6
	Description: Generated user-defined function.
		
	History:
		2019-07-29 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerReturnedTrxnStatsFullSet_HashId]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [PayerRoutingNumber],[PayerAccountNumber]

			FROM DHayes.dbo.PNCPayerReturnedTrxnStatsFullSet
			
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

