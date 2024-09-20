USE [FTBAtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************

	Name: uspABAAcctLengthStatsDelta_TransferToAtomicStat

	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from ???Risk.dbo.ABAAcctLengthStatsDelta to AtomicStat.???.ABAAcctLengthStatsBulk.
		This does not take the horizontal rows of ABAAcctLengthStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2020-08-17 - VALIDRS\LWhiting - Created.  Based upon [m t b].[uspABAAcctLengthStatsDelta_TransferToAtomicStat]
		2024-04-30 - VALIDRS\LWhiting - VALID-1816: Adjusted to accomodate changes made in FTBRisk.

*****************************************************************************************/
ALTER PROCEDURE [stat].[uspABAAcctLengthStatsDelta_TransferToAtomicStat]
	(
		 @piStatBatchLogId INT
		,@ptiDebug tinyint = 0 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	)
AS
BEGIN

	SET @ptiDebug = CASE WHEN @ptiDebug > 0 THEN @ptiDebug + 1 ELSE 0 END
	;
	DECLARE @nvThisSourceCode NVARCHAR(4000)
		,@biRowCount bigint
		,@nvMessage nvarchar(4000)
		,@iClientOrgId int = ( SELECT sg.OrgId FROM stat.StatGroup AS sg WHERE sg.[Name] = N'FTB' AND sg.[StatGroupId] = sg.[AncestorStatGroupId] )
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

	DECLARE @iStatBatchLogId INT = @piStatBatchLogId
		,@iBatchId int
		,@iBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iBatchDataSetRefreshLogId = [stat].[ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName]( @iStatBatchLogId, N'ABAAcctLengthStatsDelta' )
	;
	IF @iBatchDataSetRefreshLogId IS NULL SELECT @iBatchDataSetRefreshLogId = [stat].ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName( @iStatBatchLogId, N'CustAccountStatsDelta' ) 
	;
	SELECT @iBatchId = [stat].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' ) + N' via [stat].[ufnBatchDataSetRefreshLogIdGetByBatchLogIdDataSetName]( ' + CONVERT( nvarchar(50), @iStatBatchLogId ) + N'}, N''ABAAcctLengthStatsDelta'' )'
	END

	--EXEC [stat].[uspBatchDataSetRefreshLogTransferInitiate]
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

	
	--IF ( OBJECT_ID('tempdb..##BatchStatBatchLogDescendant', 'U') IS NULL ) EXEC [stat].[uspBatchStatBatchLogDescendantListPopulate] @piBatchId = @iBatchId;


	IF @iBatchDataSetRefreshLogId IS NOT NULL 
		--AND EXISTS( SELECT 'X' FROM [risk].[ABAAcctLengthStatsDelta] x WHERE x.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) )
		--AND @iStatBatchLogId > ( SELECT MAX( StatBatchLogId ) FROM [stat].[ABAAcctLengthStatsBulk] )
		--AND NOT EXISTS( SELECT 'X' FROM ##BatchStatBatchLogDescendant x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
			,@numeric16_12DeepestNegativeValue numeric(16,12) = -9999.999999999999
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [stat].[StatAtomicStatXref]
		--WHERE [ObjectName] = N'ABAAcctLengthStatsDelta'
		;
		--SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		--FROM [FTBRisk].dbo.BatchDataSetRefreshLog l
		--WHERE l.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId ) AND DataSetName = N'ABAAcctLengthStatsDelta'
		--;
		SELECT @biDataSetRowCount = COUNT(1)
		FROM [risk].[ABAAcctLengthStatsDelta] d
		WHERE d.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
		;


		TRUNCATE TABLE [prior].[ABAAcctLengthStatsBulk]
		;
		INSERT INTO [prior].[ABAAcctLengthStatsBulk]
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
		FROM [dbo].[ABAAcctLengthStatsBulk]
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Upsert into [stat].[ABAAcctLengthStatsBulk]'
			--PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' raw source rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END



		UPDATE u
		SET StatBatchLogId = @iStatBatchLogId
			,CycleDate = t.CycleDate
			,PayerRtnAcctNumberLengthVolumeToDate = t.PayerRtnAcctNumberLengthVolumeToDate
			,PayerRtnAcctNumLengthPercentOfTotalVolume = t.PayerRtnAcctNumLengthPercentOfTotalVolume
		FROM [risk].[ABAAcctLengthStatsDelta] t
		--FROM [risk].[ABAAcctLengthStatsFullSet] t
			INNER JOIN [dbo].[ABAAcctLengthStatsBulk] u
				ON t.PayerRoutingNumber = u.PayerRoutingNumber
					--AND t.HashId = u.HashId
		WHERE	( ISNULL( t.PayerRtnAcctNumberLengthVolumeToDate, @biDeepestNegativeValue ) <> ISNULL( u.PayerRtnAcctNumberLengthVolumeToDate, @biDeepestNegativeValue )
				OR ISNULL( t.PayerRtnAcctNumLengthPercentOfTotalVolume, @numeric16_12DeepestNegativeValue ) <> ISNULL( u.PayerRtnAcctNumLengthPercentOfTotalVolume, @numeric16_12DeepestNegativeValue ) )
			AND t.LengthPayerAcctNumber = u.LengthPayerAcctNumber -- 2019-05-21 - LSW - condition was missing LengthPayerAcctNumber comparison.
--			AND t.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			--AND u.StatBatchLogId < @iStatBatchLogId
			--AND NOT EXISTS( SELECT 'X' FROM ##BatchStatBatchLogDescendant x WHERE x.StatBatchLogId = u.StatBatchLogId )
		;
		SET @biRowCount = @@ROWCOUNT
		;
		SET @biDataSetRowCount = @biRowCount
		;
		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in [stat].[ABAAcctLengthStatsBulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END



		INSERT INTO [dbo].[ABAAcctLengthStatsBulk]
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
			,@iClientOrgId AS ClientOrgId
			,CycleDate
			,PayerRtnAcctNumberLengthVolumeToDate
			,PayerRtnAcctNumLengthPercentOfTotalVolume
		FROM [risk].[ABAAcctLengthStatsDelta] t
		--FROM [risk].[ABAAcctLengthStatsFullSet] t
		WHERE ISNULL( @iBatchDataSetRefreshLogId, 0 ) > 0
			--AND t.StatBatchId = [stat].ufnBatchGetByStatBatchLogId( @iStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM [dbo].[ABAAcctLengthStatsBulk] x
					WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
						AND x.LengthPayerAcctNumber = t.LengthPayerAcctNumber -- 2019-05-21 - LSW - condition was missing LengthPayerAcctNumber comparison.
						--AND x.StatBatchLogId = @iStatBatchLogId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;
		SET @biDataSetRowCount += @biRowCount
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into [stat].[ABAAcctLengthStatsBulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' raw source rows...'
		END

	END -- are there rows to process?


	--EXEC [stat].[uspBatchDataSetRefreshLogTransferComplete]
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
