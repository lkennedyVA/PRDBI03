USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBKCPFreqStatsDelta_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: DFFFADC9-78E3-4DAD-A905-7DBC200A8B61
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBKCPFreqStatsDelta_HashId]( @psiStatBatchLogId int )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]
			FROM DHayes.dbo.MTBKCPFreqStatsDelta
			WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
			GROUP BY 
				 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]
		)
	SELECT 
		-- 100008 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.MTBPayerGradeNeg tables.
		 [CustomerNumber],[PayerRoutingNumber],[PayerAccountNumber]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
				,CONVERT(varbinary(512)
					,CONVERT(nvarchar(512)
						
						,REPLACE( 
							REPLACE( 
								REPLACE( 
									N'36|100008|38|[{MTBCustomerAccountNumber}]|40|[{MTBRoutingNumber}]|41|[{MTBAccountNumber}]'
								, N'[{MTBCustomerAccountNumber}]', CONVERT( nvarchar(50), CustomerNumber ) )
							, N'[{MTBRoutingNumber}]', CONVERT( nvarchar(50), PayerRoutingNumber ) )
						, N'[{MTBAccountNumber}]', CONVERT( nvarchar(50), PayerAccountNumber ) )

						) 
					) 
				) 
			, 1 ) AS HashId
	FROM cteIdentity

