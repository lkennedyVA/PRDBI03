USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [stat].[uspFinancialKCPClearedCheckNumberDHayesReview]
	(
		@psiBatchLogId smallint = NULL
	)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @nvMessageText nvarchar(4000)
	;

	SET @nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Begin...'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT;

	DECLARE @siBatchLogId smallint = @psiBatchLogId
	;
	SET @siBatchLogId = ISNULL( @siBatchLogId, ( SELECT MAX( BatchLogId ) FROM [stat].[BatchLog] ) )
	;

	SET @nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(4) + N'BatchLogId = ' + CONVERT( nvarchar(10), @siBatchLogId ); 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT;

	SET @nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(4) + N'Truncating [DHayes].[dbo].[AS_FIPNC_KCP_CheckNumberCleared_Incremental]'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT;

	TRUNCATE TABLE [DHayes].[dbo].[AS_FIPNC_KCP_CheckNumberCleared_Incremental]
	;

	SET @nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(4) + N'Populating [DHayes].[dbo].[AS_FIPNC_KCP_CheckNumberCleared_Incremental]'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT;

	INSERT INTO [DHayes].[dbo].[AS_FIPNC_KCP_CheckNumberCleared_Incremental]
		(
			 CustomerId
			,PayerId

			,KCPMinCheckNumberCleared
			,KCPMaxCheckNumberCleared

			,KCPMinCheckNumberClearedBatchDate
			,KCPMaxCheckNumberClearedBatchDate
		)
	SELECT
		 ISNULL(CustomerId,-1) AS CustomerId
		,ISNULL(PayerId,-1) AS PayerId

		,KCPMinCheckNumberCleared
		,KCPMaxCheckNumberCleared

		,KCPMinCheckNumberClearedBatchDate
		,KCPMaxCheckNumberClearedBatchDate

	FROM [precalc].[vwStat_FIPNC_KCP]
	WHERE @siBatchLogId IN( KCPMinCheckNumberClearedBatchLogId, KCPMaxCheckNumberClearedBatchLogId )
	ORDER BY 
		 CustomerId
		,PayerId
	;

	SET @nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'...end.'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT;

END

GO
