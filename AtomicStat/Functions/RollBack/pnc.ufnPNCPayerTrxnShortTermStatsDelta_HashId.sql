USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: ufnPNCPayerTrxnShortTermStatsDelta_HashId()
	Generator executed by: VALIDRS\LWhiting
	Generated code version: 135164C0-AA15-41E7-BB7D-1D87C451860B
	Generation set version: 8F0BC5EE-CD07-46A6-9595-20610FD538DD
	Description: Generate the relevant HashId values from PNCPayerTrxnShortTermStatsDelta.
		
	History:
		2019-07-30 - VALIDRS\LWhiting - Created via generator.
*****************************************************************************************/
ALTER   FUNCTION [pnc].[ufnPNCPayerTrxnShortTermStatsDelta_HashId]( @psiStatBatchLogId smallint )
RETURNS TABLE AS RETURN

	WITH cteIdentity AS 
		(
			SELECT
				 [PayerRoutingNumber],[PayerAccountNumber]

			FROM DHayes.dbo.PNCPayerTrxnShortTermStatsDelta
				WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @psiStatBatchLogId )
		AND CycleDate = pnc.ufnCycleDateGetByStatBatchLogId( @psiStatBatchLogId )

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

