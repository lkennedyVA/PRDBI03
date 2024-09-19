USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: uspPNCABAAcctLengthStatsDelta_TransferToAtomicStat
	Created By: VALIDRS\LWhiting
	
	Description: Transfer the approved columns from PNCABAAcctLengthStatsDelta to ABAAcctLengthStatsBulk.
		This does not take the horizontal rows of PNCABAAcctLengthStatsDelta and make them verticle.
		It only moves the approved columns.
		
	History:
		2019-07-22 - VALIDRS\LWhiting - Created.
*****************************************************************************************/
ALTER PROCEDURE [pnc].[uspPNCABAAcctLengthStatsDelta_TransferToAtomicStat]
	(
		 @psiStatBatchLogId INT
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
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + CONVERT( nvarchar(50), @psiStatBatchLogId )
		PRINT N''
	END

	DECLARE @siStatBatchLogId INT = @psiStatBatchLogId
		,@iPNCBatchId int
		,@iPNCBatchDataSetRefreshLogId int
		,@iDataSetRemainCount int
	;
	SELECT @iPNCBatchDataSetRefreshLogId = pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( @siStatBatchLogId, N'PNCABAAcctLengthStatsDelta' )
	;
	
	IF @ptiDebug > 0 
	BEGIN
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' ) + N' via pnc.ufnPNCBatchDataSetRefreshLogIdGetByBatchLogIdPNCDataSetName( ' + CONVERT( nvarchar(50), @siStatBatchLogId ) + N'}, N''PNCABAAcctLengthStatsDelta'' )'
	END

	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferInitiate
		 @piStatBatchLogId = @siStatBatchLogId
		,@pnvPNCDataSetName = N'PNCABAAcctLengthStatsDelta'
		,@psiPNCBatchId = @iPNCBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piPNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId OUTPUT
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;
	
	IF @ptiDebug > 0 
	BEGIN
		IF @iPNCBatchDataSetRefreshLogId IS NULL PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId is NULL.  No rows will be transfered.'
	END

	
	IF ( OBJECT_ID('tempdb..##PNCBatchStatBatchLogDescendant', 'U') IS NULL ) EXEC pnc.uspPNCBatchStatBatchLogDescendantListPopulate @psiPNCBatchId = @iPNCBatchId;


	IF @iPNCBatchDataSetRefreshLogId IS NOT NULL 
		AND EXISTS( SELECT 'X' FROM DHayes.dbo.PNCABAAcctLengthStatsDelta x WHERE x.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) )
		--AND @siStatBatchLogId > ( SELECT MAX( StatBatchLogId ) FROM pnc.ABAAcctLengthStatsBulk )
		--AND NOT EXISTS( SELECT 'X' FROM ##PNCBatchStatBatchLogDescendant x )
	BEGIN -- are there rows to process?

		DECLARE @siStatCount smallint
			,@biDataSetRowCount bigint
			,@biDeepestNegativeValue bigint = -9223372036854775808
			,@numeric16_12DeepestNegativeValue numeric(16,12) = -9999.999999999999
		;
		SELECT @siStatCount = 1
		--SELECT @siStatCount = COUNT(1)
		--FROM [pnc].[PNCStatAtomicStatXref]
		--WHERE [ObjectName] = N'PNCABAAcctLengthStatsDelta'
		;
		SELECT @biDataSetRowCount = NumberOfRecordsInDataSetRefresh 
		FROM DHayes.dbo.PNCBatchDataSetRefreshLog 
		WHERE PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId ) AND PNCDataSetName = N'PNCABAAcctLengthStatsDelta'
		;


		TRUNCATE TABLE PNCprior.ABAAcctLengthStatsBulk
		;
		INSERT INTO PNCprior.ABAAcctLengthStatsBulk
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
		FROM pnc.ABAAcctLengthStatsBulk
		;

		IF @ptiDebug > 0 
		BEGIN
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Insert into pnc.ABAAcctLengthStatsBulk'
			PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + NCHAR(009) + N'...' + CONVERT( nvarchar(50), @siStatCount * @biDataSetRowCount ) + N' stat rows...'
			RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
		END


		UPDATE u
		SET StatBatchLogId = @siStatBatchLogId
			,CycleDate = t.CycleDate
			,PayerRtnAcctNumberLengthVolumeToDate = t.PayerRtnAcctNumberLengthVolumeToDate
			,PayerRtnAcctNumLengthPercentOfTotalVolume = t.PayerRtnAcctNumLengthPercentOfTotalVolume
		FROM DHayes.dbo.PNCABAAcctLengthStatsDelta t
			INNER JOIN pnc.ABAAcctLengthStatsBulk u
				ON t.PayerRoutingNumber = u.PayerRoutingNumber
					--AND t.HashId = u.HashId
		WHERE	( ISNULL( t.PayerRtnAcctNumberLengthVolumeToDate, @biDeepestNegativeValue ) <> ISNULL( u.PayerRtnAcctNumberLengthVolumeToDate, @biDeepestNegativeValue )
				OR ISNULL( t.PayerRtnAcctNumLengthPercentOfTotalVolume, @numeric16_12DeepestNegativeValue ) <> ISNULL( u.PayerRtnAcctNumLengthPercentOfTotalVolume, @numeric16_12DeepestNegativeValue ) )
			AND t.LengthPayerAcctNumber = u.LengthPayerAcctNumber -- 2019-05-21 - LSW - condition was missing LengthPayerAcctNumber comparison.
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			--AND u.StatBatchLogId < @siStatBatchLogId
			AND NOT EXISTS( SELECT 'X' FROM ##PNCBatchStatBatchLogDescendant x WHERE x.StatBatchLogId = u.StatBatchLogId )
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows updated in pnc.ABAAcctLengthStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END



		INSERT INTO pnc.ABAAcctLengthStatsBulk
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
			  @siStatBatchLogId AS StatBatchLogId
			 ,PayerRoutingNumber
			 ,LengthPayerAcctNumber
			 ,100009 AS ClientOrgId
			 ,CycleDate
			 ,PayerRtnAcctNumberLengthVolumeToDate
			 ,PayerRtnAcctNumLengthPercentOfTotalVolume 
		FROM DHayes.dbo.PNCABAAcctLengthStatsDelta t
		WHERE ISNULL( @iPNCBatchDataSetRefreshLogId, 0 ) > 0
			AND t.PNCBatchId = pnc.ufnPNCBatchGetByStatBatchLogId( @siStatBatchLogId )
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM pnc.ABAAcctLengthStatsBulk x
					WHERE x.PayerRoutingNumber = t.PayerRoutingNumber
						AND x.LengthPayerAcctNumber = t.LengthPayerAcctNumber -- 2019-05-21 - LSW - condition was missing LengthPayerAcctNumber comparison.
						--AND x.StatBatchLogId = @siStatBatchLogId
				)
		;
		SET @biRowCount = @@ROWCOUNT
		;

		IF @ptiDebug > 0 
		BEGIN
			SET @nvMessage = REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT( nvarchar(50), @biRowCount ) + N' rows inserted into pnc.ABAAcctLengthStatsBulk.'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

	END -- are there rows to process?


	EXEC pnc.uspPNCBatchDataSetRefreshLogTransferComplete
		 @piStatBatchLogId = @siStatBatchLogId
		,@piPNCBatchDataSetRefreshLogId = @iPNCBatchDataSetRefreshLogId 
		,@psiPNCBatchId = @iPNCBatchId OUTPUT -- returned as verification; if NULL, then something went wrong.
		,@piDataSetRemainCount = @iDataSetRemainCount OUTPUT
		,@ptiDebug = @ptiDebug
	;

	IF @ptiDebug > 0 
	BEGIN
		PRINT N''
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@psiStatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @psiStatBatchLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + N'@iPNCBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iPNCBatchDataSetRefreshLogId ), N'NULL' )
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + @nvThisSourceCode
		PRINT REPLICATE( NCHAR(009), @ptiDebug - 1 ) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N' ...end.'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END

END
;


GO
