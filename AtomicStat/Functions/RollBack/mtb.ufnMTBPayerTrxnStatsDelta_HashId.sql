USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBPayerTrxnStatsDelta_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 46E6FFC2-2373-49FE-9DAF-7737601CC3A3
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBPayerTrxnStatsDelta_HashId]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [PayerRoutingNumber],[PayerAccountNumber]
			FROM DHayes.dbo.MTBPayerTrxnStatsDelta
			WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
			GROUP BY 
				 [PayerRoutingNumber],[PayerAccountNumber]
		)
	SELECT 
		-- 100008 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.MTBPayerGradeNeg tables.
		 [PayerRoutingNumber],[PayerAccountNumber]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
				,CONVERT(varbinary(512)
					,CONVERT(nvarchar(512)
						
						,REPLACE( 
							REPLACE( 
									N'36|100008|40|[{MTBRoutingNumber}]|41|[{MTBAccountNumber}]'
							, N'[{MTBRoutingNumber}]', CONVERT( nvarchar(50), PayerRoutingNumber ) )
						, N'[{MTBAccountNumber}]', CONVERT( nvarchar(50), PayerAccountNumber ) )

						) 
					) 
				) 
			, 1 ) AS HashId
	FROM cteIdentity

