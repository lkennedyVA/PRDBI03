USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************

	Name: uspABAAcctLengthStatsDelta_TransferToAtomicStat

	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from ABAAcctLengthStatsDelta to ABAAcctLengthStatsBulk.
		This does not take the horizontal rows of ABAAcctLengthStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-12-06 - VALIDRS\LWhiting - Created based upon [m t b].[uspABAAcctLengthStatsDelta_TransferToAtomicStat].

*****************************************************************************************/
ALTER PROCEDURE [tdb].[uspABAAcctLengthStatsDelta_TransferToAtomicStat]
	(
		 @piStatBatchLogId smallint
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	)
AS
BEGIN

	SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000)
		,@biRowCount bigint
		,@nvMessage nvarchar(4000)
	;
	
	IF @ptiDebug > 0
	BEGIN
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' Begin...'
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + CONVERT( nvarchar(50), @piStatBatchLogId )
		PRINT N''
	END

	DECLARE @iStatBatchLogId smallint = @piStatBatchLogId
		,@iBatchId int
		,@iBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iBatchDataSetRefreshLogId = [tdb].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( @iStatBatchLogId, N'ABAAcctLengthStatsDelta' )
	;
	SELECT @iBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' ) + N' via [tdb].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( ' + CONVERT( nvarchar(50), @iStatBatchLogId ) + N'}, N''ABAAcctLengthStatsDelta'' )'
	END

	--EXEC [tdb].uspBatchDataSetRefreshLogTransferInitiate
	--	 @piStatBatchLogId = @iStatBatchLogId
	--	,@pnvDataSetName = N'ABAAcctLengthStatsDelta'
	--	,@psiBatchId = @iBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
	--	,@piBatchDataSetRefreshLogId = @iBatchDataSetRefreshLogId OUTPUT
	--	,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
	--	,@ptiDebug = @ptiDebug
	--;
	
	IF @ptiDebug > 0 
	BEGIN
		IF @iBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
	END

	
	--IF ( OBJECT_ID('tempdb..##BatchStatBatchLogDescendant', 'U') IS NULL ) EXEC [tdb].uspBatchStatBatchLogDescendantListPopulate @piBatchId = @iBatchId;


	IF @iBatchDataSetRefreshLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM [TDBRisk].[dbo].[ABAAcctLengthStatsDelta] x WHERE x.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) )
		--AND @iStatBatchLogId > ( SELECT MAX( StatBatchLogId ) FROM [tdb].[ABAAcctLengthStatsBulk] )
		--AND NOT EXISTS( SELECT 'X' FROM ##BatchStatBatchLogDescendant x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
			,@numeric16_12DeepestNegativeValue numeric(16,12) = -9999.999999999999
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [tdb].[StatAtomicStatXref]
		--WHERE [ObjectName] = N'ABAAcctLengthStatsDelta'
		;
		--SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		--FROM [TDBRisk].dbo.BatchDataSetRefreshLog l
		--WHERE l.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) AND DataSetName = N'ABAAcctLengthStatsDelta'
		--;
		SELECT @biDataSetRowCount = COUNT(1)
		FROM [TDBRisk].[dbo].[ABAAcctLengthStatsDelta] d
		WHERE d.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
		;


		TRUNCATE TABLE [tdbprior].[ABAAcctLengthStatsBulk]
		;
		INSERT INTO [tdbprior].[ABAAcctLengthStatsBulk]
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,LengthPayerAcctNumber
				,ClientOrgId
				,CycleDate
				,PayerRtnAcctNumberLengthVolumeToDate
				,PayerRtnAcctNumLengthPercentOfTotalVolume
			)
		SELECT
				 StatBatchLogId
				,PayerRoutingNumber
				,LengthPayerAcctNumber
				,ClientOrgId
				,CycleDate
				,PayerRtnAcctNumberLengthVolumeToDate
				,PayerRtnAcctNumLengthPercentOfTotalVolume
		FROM [tdb].[ABAAcctLengthStatsBulk]
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Upsert into [tdb].[ABAAcctLengthStatsBulk]'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' raw source rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END



		UPDATE u
		SET StatBatchLogId = @iStatBatchLogId
			,CycleDate = t.CycleDate
			,PayerRtnAcctNumberLengthVolumeToDate = t.PayerRtnAcctNumberLengthVolumeToDate
			,PayerRtnAcctNumLengthPercentOfTotalVolume = t.PayerRtnAcctNumLengthPercentOfTotalVolume
		FROM [TDBRisk].[dbo].[ABAAcctLengthStatsDelta] t
		--FROM [TDBRisk].[dbo].[ABAAcctLengthStatsFullSet] t
			INNER JOIN [tdb].[ABAAcctLengthStatsBulk] u
				ON t.PayerRoutingNumber = u.PayerRoutingNumber
					--AND t.HashId = u.HashId
		WHERE	( ISNULL( t.PayerRtnAcctNumberLengthVolumeToDate, @biDeepestNegativeValue ) <> ISNULL( u.PayerRtnAcctNumberLengthVolumeToDate, @biDeepestNegativeValue )
				OR ISNULL( t.PayerRtnAcctNumLengthPercentOfTotalVolume, @numeric16_12DeepestNegativeValue ) <> ISNULL( u.PayerRtnAcctNumLengthPercentOfTotalVolume, @numeric16_12DeepestNegativeValue ) )
			AND t.LengthPayerAcctNumber = u.LengthPayerAcctNumber -- 2019-05-21 - LSW - condition was missing LengthPayerAcctNumber comparison.
--			AND t.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			--AND u.StatBatchLogId < @iStatBatchLogId
			--AND NOT EXISTS( SELECT 'X' FROM ##BatchStatBatchLogDescendant x WHERE x.StatBatchLogId = u.StatBatchLogId )
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in [tdb].[ABAAcctLengthStatsBulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END



		INSERT INTO [tdb].[ABAAcctLengthStatsBulk]
			(
				 StatBatchLogId
				,PayerRoutingNumber
				,LengthPayerAcctNumber
				,ClientOrgId
				,CycleDate
				,PayerRtnAcctNumberLengthVolumeToDate
				,PayerRtnAcctNumLengthPercentOfTotalVolume
			)
		SELECT
			 @iStatBatchLogId AS StatBatchLogId
			,PayerRoutingNumber
			,LengthPayerAcctNumber
			,100010 AS ClientOrgId
			,CycleDate
			,PayerRtnAcctNumberLengthVolumeToDate
			,PayerRtnAcctNumLengthPercentOfTotalVolume
		FROM [TDBRisk].[dbo].[ABAAcctLengthStatsDelta] t
		--FROM [TDBRisk].[dbo].[ABAAcctLengthStatsFullSet] t
		WHERE ISNULL( @iBatchDataSetRefreshLogId, 0 ) > 0
			AND t.StatBatchId = [tdb].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM [tdb].[ABAAcctLengthStatsBulk] x
					WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
						AND x.LengthPayerAcctNumber = t.LengthPayerAcctNumber -- 2019-05-21 - LSW - condition was missing LengthPayerAcctNumber comparison.
						--AND x.StatBatchLogId = @iStatBatchLogId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [tdb].[ABAAcctLengthStatsBulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

	END -- are there rows to process?


	--EXEC [tdb].[uspBatchDataSetRefreshLogTransferComplete]
	--	 @piStatBatchLogId = @iStatBatchLogId
	--	,@piBatchDataSetRefreshLogId = @iBatchDataSetRefreshLogId 
	--	,@psiBatchId = @iBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
	--	,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
	--	,@ptiDebug = @ptiDebug
	--;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@piStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @piStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;

GO
