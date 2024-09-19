USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	Name: uspMTBBatchStatBatchLogXrefEstablish

	Created By: VALIDRS\LWhiting
	
	Description: this stored procedure establishes a cross-ref 
		between a Stat.stat.BatchLog.BatchLogId 
		and an AtomicStat.mtb.MTBBatch.MTBBatchId,
		unless the BatchLogId already exists in 
		AtomicStat.mtb.MTBBatchStatBatchLogXref.StatBatchLogId.
		In either case, the corresponding AtomicStat.mtb.MTBBatch.MTBBatchId 
		value is returned via the output parameter @psiMTBBatchId.
		If there are no available MTBBatchId's, then @psiMTBBatchId 
		returns a NULL value.

	Example:

		DECLARE @iStatBatchLogId int = 135
			,@siMTBBatchId INT = NULL
		;
		EXEC mtb.uspMTBBatchStatBatchLogXrefEstablish @piStatBatchLogId = @iStatBatchLogId, @psiMTBBatchId = @siMTBBatchId OUTPUT 
		;
		SELECT @siMTBBatchId AS MTBBatchId
		SELECT StatBatchLogId = @iStatBatchLogId, MTBBatchId = @siMTBBatchId
		;
		SELECT * FROM [mtb].[MTBBatchStatBatchLogXref]
		;
		
	History:
		2019-03-11 - LSW - Created

*/
ALTER PROCEDURE [mtb].[uspMTBBatchStatBatchLogXrefEstablish]
	( 
		 @piStatBatchLogId int -- AtomicStat works by way of Stat.stat.BatchLog.BatchLogId
		,@psiMTBBatchId INT OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1 = output feedback to Message tab.
	) 
AS 
BEGIN

	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@siMTBBatchId INT = @psiMTBBatchId
	;
	SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000);
	
	IF @ptiDebug > 0 
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' )
		PRINT N''
	END

	SELECT
		@siMTBBatchId = MTBBatchId
	FROM [mtb].[MTBBatchStatBatchLogXref]
	WHERE StatBatchLogId = @iStatBatchLogId
	;

	IF @siMTBBatchId IS NULL
	BEGIN
	
		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siMTBBatchId is NULL.'
		END

		SELECT
			@siMTBBatchId = MIN( mb.MTBBatchId )
		FROM DHayes.dbo.MTBBatch AS mb
		WHERE mb.MTBBatchTransferInitiatedDatetime IS NULL 
			AND NOT EXISTS( SELECT 'X' FROM [mtb].[MTBBatchStatBatchLogXref] AS x WHERE x.MTBBatchId = mb.MTBBatchId )  
--AND mb.MTBBatchId > 9999
		;
	
		IF @ptiDebug > 0 
			IF @siMTBBatchId IS NULL 
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siMTBBatchId is _still_ NULL; no MTBBatchTransferInitiatedDatetime is NULL?, @siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' ) + N' exists in MTBBatchStatBatchLogXref?'
			ELSE
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' ) + N' and is no longer NULL.'
		;

	END

	IF @iStatBatchLogId IS NOT NULL 
		AND @siMTBBatchId IS NOT NULL
		AND @iStatBatchLogId <> @siMTBBatchId
		-- AtomicStat.stat.BatchLog.BatchStartDate must have a timestamp more recent than DHayes.dbo.MTBBatch.MTBBatchInitiatedDatetime.
		AND ( ISNULL( ( SELECT bl.BatchStartDate FROM AtomicStat.stat.BatchLog bl WHERE bl.BatchLogId = @iStatBatchLogId AND bl.OrgId = 100008 ), CONVERT( datetime2(0), '1901-01-01' ) ) > ISNULL( ( SELECT mb.MTBBatchInitiatedDatetime FROM DHayes.dbo.MTBBatch mb WHERE mb.MTBBatchId = @siMTBBatchId ), CONVERT( datetime2(0), '9999-12-31' ) ) )
		-- AtomicStat.stat.BatchLog.OrgId must be 100008 (MTB).
		AND ISNULL( ( SELECT bl.OrgId FROM AtomicStat.stat.BatchLog bl WHERE bl.BatchLogId = @iStatBatchLogId ), CONVERT( int, 0 ) ) = 100008
	BEGIN
	
		IF @ptiDebug > 0 
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' ) + N' and could be inserted into MTBBatchStatBatchLogXref.'
		;
			
		INSERT INTO [mtb].[MTBBatchStatBatchLogXref] 
			( 
				 MTBBatchId
				,StatBatchLogId 
			)
		SELECT
				@siMTBBatchId
			,@iStatBatchLogId
		WHERE NOT EXISTS( SELECT 'X' FROM [mtb].[MTBBatchStatBatchLogXref] AS x WHERE x.MTBBatchId = @siMTBBatchId ) -- if @siMTBBatchId already exists in any row, we can't add it again.
			AND NOT EXISTS( SELECT 'X' FROM [mtb].[MTBBatchStatBatchLogXref] AS x WHERE x.StatBatchLogId = @iStatBatchLogId ) -- if @iStatBatchLogId already exists in any row, we can't add it again, either.
		;

	END
	ELSE 
	BEGIN
		IF @ptiDebug > 0 
		BEGIN
			IF @iStatBatchLogId IS NULL OR @siMTBBatchId IS NULL
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'Cannot insert a NULL value into MTBBatchStatBatchLogXref.'
			IF @iStatBatchLogId = @siMTBBatchId
				PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'Batch values are suspect.'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'NULL' )
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@siMTBBatchId = ' + ISNULL( CONVERT( nvarchar(50), @siMTBBatchId ), N'NULL' )
			SET @siMTBBatchId = NULL
			;
		END
	END

	SET @psiMTBBatchId = @siMTBBatchId
	;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
GO
