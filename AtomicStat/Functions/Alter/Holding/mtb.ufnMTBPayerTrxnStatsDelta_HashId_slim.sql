USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBPayerTrxnStatsDelta_HashId_slim()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 46E6FFC2-2373-49FE-9DAF-7737601CC3A3
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
		2019-06-26 - VALIDRS\LWhiting - Removed the CONVERT()'s to varbinary(512) and nvarchar(512).
*****************************************************************************************/
ALTER FUNCTION [mtb].[ufnMTBPayerTrxnStatsDelta_HashId_slim]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

	--WITH cteIdentity AS 
	--	(
	--		SELECT
	--			 [PayerRoutingNumber],[PayerAccountNumber]
	--		FROM DHayes.dbo.MTBPayerTrxnStatsDelta
	--		WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
	--		GROUP BY 
	--			 [PayerRoutingNumber],[PayerAccountNumber]
	--	)
	SELECT 
		-- 100008 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.MTBPayerGradeNeg tables.
		 [PayerRoutingNumber],[PayerAccountNumber]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
						
						,REPLACE( 
							REPLACE( 
									N'36|100008|40|[{MTBRoutingNumber}]|41|[{MTBAccountNumber}]'
							, N'[{MTBRoutingNumber}]', CONVERT( nvarchar(50), PayerRoutingNumber ) )
						, N'[{MTBAccountNumber}]', CONVERT( nvarchar(50), PayerAccountNumber ) )

				) 
			, 1 ) AS HashId
	--FROM cteIdentity
	FROM DHayes.dbo.MTBPayerTrxnStatsDelta
	WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
