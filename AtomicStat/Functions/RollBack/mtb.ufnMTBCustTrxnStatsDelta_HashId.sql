USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnMTBCustTrxnStatsDelta_HashId()
	Generator Executed By: VALIDRS\LWhiting
	Generated Version: 2C0E4511-BD5D-4071-85BC-AABC9C40F29A
	Description: Generated user-defined function.
		
	History:
		2019-04-23 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [mtb].[ufnMTBCustTrxnStatsDelta_HashId]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [CustomerNumber]
			FROM DHayes.dbo.MTBCustTrxnStatsDelta
			WHERE MTBBatchId = mtb.ufnMTBBatchGetByStatBatchLogId( @psiStatBatchLogId )
			GROUP BY 
				 [CustomerNumber]
		)
	SELECT 
		-- 100008 AS ClientOrgId -- ClientOrgId is not in the DHayes.dbo.MTBPayerGradeNeg tables.
		 [CustomerNumber]
		,CONVERT(binary(64)
			,HASHBYTES( N'SHA2_512'
				,CONVERT(varbinary(512)
					,CONVERT(nvarchar(512)
						
						,REPLACE( 
									N'36|100008|38|[{MTBCustomerAccountNumber}]'
						, N'[{MTBCustomerAccountNumber}]', CONVERT( nvarchar(50), CustomerNumber ) )

						) 
					) 
				) 
			, 1 ) AS HashId
	FROM cteIdentity

