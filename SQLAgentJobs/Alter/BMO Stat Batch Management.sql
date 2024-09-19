USE [msdb]
GO

/****** Object:  Job [BMO Stat Batch Management]    Script Date: 9/19/2024 8:13:12 AM ******/
EXEC msdb.dbo.sp_delete_job @job_id=N'eb64edaa-b079-435d-9f5c-e8e5a60ff4c6', @delete_unused_schedule=1
GO

/****** Object:  Job [BMO Stat Batch Management]    Script Date: 9/19/2024 8:13:12 AM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 9/19/2024 8:13:12 AM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'BMO Stat Batch Management', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'
https://validsystems.sharepoint.com/:w:/s/DataAssets/BMO

2024-03-20 - VALIDRS\LWhiting - VALID-1723

', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'VALIDRS\PRDBI03Sqljob', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Process Monitor: Initialize]    Script Date: 9/19/2024 8:13:13 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Process Monitor: Initialize', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvPriorOccurrenceKeyValue nvarchar(50)
	,@nvCurrentOccurrenceKeyValue nvarchar(50)
	,@nvMessageText nvarchar(4000)
;

SET @nvPriorOccurrenceKeyValue = [processmonitor].[StatBatchManagementJob_GetCurrentOccurrenceKey]()
;
EXEC [processmonitor].[usp_StatBatchManagementJob_NewOccurrenceKey] @pbitForceNew = 1 -- job is being executed from the first job step, therefore force a new Occurrence Key value.
;
SET @nvCurrentOccurrenceKeyValue = [processmonitor].[StatBatchManagementJob_GetCurrentOccurrenceKey]()
;

/*
	
	If Current OccurrenceKey is null
	_or_
	if the Prior OccurrenceKey is not null and _is_equal_ to the Current OccurrenceKey which is also not null (boy howdy do we have a problem if this is ever the case!)
	...
	then we bail out of the job, because something is fundamentally wrong in the quantum foam.

*/
IF ( @nvCurrentOccurrenceKeyValue IS NULL ) OR ( ISNULL( @nvPriorOccurrenceKeyValue, N''☠'' ) = ISNULL( @nvCurrentOccurrenceKeyValue, NCHAR(255) ) )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvCurrentOccurrenceKeyValue + N''"''
END
;
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Promote Deltas]    Script Date: 9/19/2024 8:13:13 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Promote Deltas', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@nvMessageText nvarchar(4000)
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



EXECUTE [dbo].[sp_ScheduledPromoteDeltaStatsToFullSet]
;



SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 10 -- Deltas Promoted
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Returns and deposits - Watch for an available FileSet]    Script Date: 9/19/2024 8:13:13 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Returns and deposits - Watch for an available FileSet', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @siWhileStatus smallint = -1
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@nvMessageText nvarchar(4000)
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [risk].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;

IF SUSER_SNAME() = N''VALIDRS\PRDBI03SqlAgnt'' -- if SQL Agent is running the job, send the notification.  Otherwise, this is a manual launch and we do not want a notification sent.
BEGIN
	SELECT @nvSubject = N''Info: '' + @nvJobName + N'' - Beginning watch for an available FileSet''
		,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' - watching for an available FileSet in Hadoop before generating stats.'' + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
	;

	PRINT REPLICATE( N''-'', 80 )
	PRINT @nvSubject
	PRINT @nvBody
	PRINT REPLICATE( N''-'', 80 )
	;

	EXEC [dbo].[uspSendEmail]
		 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
		-- @pnvRecipients = ''lwhiting@validadvantage.com;''
		,@pnvSubject = @nvSubject
		,@pnvBodyText = @nvBody
	;
END


EXEC [stat].[uspWatchForAggregateFileSet]
	@pnvDelayDuration = ''00:05:00'' 
	,@pnvWatchEndTime = ''17:00:00''
	,@psiWhileStatus = @siWhileStatus OUTPUT
	,@pnvStatGenerator = N''Daily Stats - Trxn and Return''
	,@piJobStepId = @iStepId
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;

--set @siWhileStatus = 1 -- for testing purposes
PRINT N''@siWhileStatus = '' + CONVERT( nvarchar(50), @siWhileStatus )
;
IF @siWhileStatus <> 1 
BEGIN


	IF @siWhileStatus = 0 SET @iPMStatusCode = 7 -- Files Not Ready - Timeout
	;
	IF @siWhileStatus = -1 SET @iPMStatusCode = 18 -- No Available CycleDates To Process
	;
	IF @siWhileStatus IN( 0, -1 )
	BEGIN

		EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
			 @processInstanceId = @iPMProcessInstanceId
			,@processInstanceGuid = @uPMProcessInstanceGuid
			,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
			,@stage = @iPMStage
			,@statusCode = @iPMStatusCode
			,@statusMessage = @nvPMStatusMessage
		;

	END
	;


	SET @nvSubject = N''Warning: '' + @nvJobRef + N'' - Watch for an available FileSet timed out or encountered a problem.''
	;
	SET @nvBody = CASE WHEN @siWhileStatus = 0
				THEN ( N''Warning notification.  Attention may be required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' timed out.'' )
			WHEN @siWhileStatus = -1
				THEN ( N''Warning notification.  Attention may be required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' did not find any available CycleDates to process.'' )
			WHEN @siWhileStatus = -2
				THEN ( N''Warning notification.  Attention may be required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' missing value for parameter "@pnvStatGenerator".'' )
			ELSE ( N''Warning notification.  Attention required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' received unrecognized value for @siWhileStatus.'' + NCHAR(013) + NCHAR(010) + N''The watch procedure exited with @siWhileStatus = '' + CONVERT( nvarchar(50), @siWhileStatus ) )
		END 
	;
	SET @nvBody = @nvBody + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
	;

	PRINT REPLICATE( N''-'', 80 )
	PRINT @nvSubject
	PRINT @nvBody
	PRINT REPLICATE( N''-'', 80 )
	;

	EXEC [dbo].[uspSendEmail]
		 @pnvRecipients = ''dwsupport@validsystems.net;dbmonitoralerts@validsystems.net;''
		-- @pnvRecipients = ''lwhiting@validadvantage.com;''
		,@pnvSubject = @nvSubject
		,@pnvBodyText = @nvBody
	;

	--RAISERROR( @nvBody, 16, 1 ) 
	RAISERROR( @nvBody, 0, 1 ) WITH LOG;
	;
END

IF @siWhileStatus = 1 
BEGIN
	SET @iPMStatusCode = 8 -- Files Ready
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;
END


PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''
', 
		@database_name=N'BMOAtomicStat', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Returns and deposits - Stat generation]    Script Date: 9/19/2024 8:13:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Returns and deposits - Stat generation', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50) 
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@nvStatGenerator nvarchar(128) = N''Daily Stats - Trxn and Return''
	,@nvClientAbbr nvarchar(5)
	,@nvMessageText nvarchar(4000)
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;
SELECT @nvClientAbbr = [FileProcessingClientRef] FROM [dbo].[Client]
;

--PRINT N''Step: Run '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' Stat generation processes'';

DECLARE @dCycleDate date
;
SELECT @dCycleDate = MAX( [BuildCycleDate] ) FROM [BMOAtomicStat].[dbo].[vwProcessDate] pd WHERE pd.[BuildCycleDate] < CONVERT( date, SYSDATETIME() )

IF EXISTS
	( 
		SELECT ''X'' FROM [AtomicStat].[hadoop].[ufnClientFileCycleDateRangeReadiness]( @dCycleDate, @nvClientAbbr, N''deposits'', 6 ) AS x 
		WHERE x.AggregateControl_ResultIsDownstreamSafe = 1 
			AND NOT EXISTS( select ''x'' from [dbo].[StatBatch] as b where x.CycleDate = b.CycleDate AND b.StatBatchDataSetName = @nvStatGenerator )
	)
BEGIN

	SELECT @nvSubject = N''Info: '' + @nvJobName + N'' - '' + @nvStatGenerator + N'' stat generation''
		,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' - initiating '' + @nvStatGenerator + N'' stat generation.'' + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
	;

	PRINT REPLICATE( N''-'', 80 )
	PRINT @nvSubject
	PRINT @nvBody
	PRINT REPLICATE( N''-'', 80 )
	;

	EXEC [BMOAtomicStat].[dbo].[uspSendEmail]
		 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
		-- @pnvRecipients = ''lwhiting@validadvantage.com;''
		,@pnvSubject = @nvSubject
		,@pnvBodyText = @nvBody
	;

--print N''Simulating execution of [dbo].[uspDataBuildGate] for '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' Stat generation'';

	PRINT N''Running the gate for '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' stat generation''
	;
	EXEC [dbo].[uspDataBuildGate] @pnvStatGenerator = @nvStatGenerator --@pdCycleDate = ''2019-07-16''
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 9 -- Stats Calculated
	;
	EXEC [processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;


END
ELSE
BEGIN
	PRINT N''Nothing found to process.  Ignoring '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' stat generation''
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Nothing available to process.''
	;
	SET @iPMStatusCode = 3 -- Warning
	;
	EXEC [processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 19 -- "Skipped" (but not really skipped) because there was nothing available to process
	;
	EXEC [processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;



	IF DATENAME( weekday, SYSDATETIME() ) NOT IN( ''Sunday'', ''Monday'' )
	BEGIN
		SELECT @nvSubject = N''Warning: '' + @nvJobRef + N'' - nothing to process for '' + @nvStatGenerator
			,@nvBody = N''Warning notification.  Attention may be required.  Nothing found for '' + @nvJobRef + N'' to process.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
		;

		PRINT REPLICATE( N''-'', 80 )
		PRINT @nvSubject
		PRINT @nvBody
		PRINT REPLICATE( N''-'', 80 )
		;

		/* Uncomment this, once ready for live operation...*/
		EXEC [BMOAtomicStat].[dbo].[uspSendEmail]
			 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
			-- @pnvRecipients = ''lwhiting@validadvantage.com;''
			,@pnvSubject = @nvSubject
			,@pnvBodyText = @nvBody
		;
	END

END


PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Invoke job on [RptDb3\PrdBi02]: "PrdTrx01 FiServ BMO EligibleReturnsDaily"]    Script Date: 9/19/2024 8:13:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Invoke job on [RptDb3\PrdBi02]: "PrdTrx01 FiServ BMO EligibleReturnsDaily"', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=4, 
		@on_success_step_id=7, 
		@on_fail_action=4, 
		@on_fail_step_id=6, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvMessageText nvarchar(4000)
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;


SET @nvMessageText = N''Disabled invocation of job "PrdTrx01 FiServ BMO EligibleReturnsDaily", on PrdBi02, until we know we are ready to invoke this job.''
;
PRINT @nvMessageText
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + @nvMessageText
;
SET @iPMStatusCode = 2 -- Info
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



EXECUTE PRDBI02.msdb.dbo.sp_start_job @job_name = N''PrdTrx01 FiServ BMO EligibleReturnsDaily'', @server_name = N''930237-RPTDB3\PRDBI02'', @step_name = N''BMO FiServEligibleReturnExtract'';




SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 14 -- Job Spawned
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;




SET @iStepId = @iStepId + 1
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;
SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Prior Stage job spawn completed w/o issue.''
;
SET @iPMStatusCode = 19 -- Skipped
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send "FiServ BMO EligibleReturnsDaily" Problem Notification]    Script Date: 9/19/2024 8:13:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send "FiServ BMO EligibleReturnsDaily" Problem Notification', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50) = N''???''
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@uJobId uniqueidentifier = $(ESCAPE_NONE(JOBID))
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [risk].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
--SET @nvJobRef = @nvServerName + SPACE(1) + @nvJobName;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;
DECLARE @nvSubject nvarchar(256) = N''''
	,@nvBody nvarchar(4000) = N''''
;

PRINT N''Send Notification that the FiServ '' + @nvAbbrv + N'' ElligibleReturnsDaily job failed to start.''
;
PRINT N''''

SELECT @nvSubject = N''Alert: '' + @nvJobRef + N'' - FiServ '' + @nvAbbrv + N'' ElligibleReturnsDaily job failed to start.''
	,@nvBody = N''Alert!  Investigate why the recent job execution of '' + @nvJobRef + N'' could not launch the FiServ '' + @nvAbbrv + N'' ElligibleReturnsDaily job.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
;

EXEC [dbo].[uspSendEmail]
	 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
	-- @pnvRecipients = ''lwhiting@validadvantage.com;''
	,@pnvSubject = @nvSubject
	,@pnvBodyText = @nvBody
;
PRINT @nvBody
;



SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 15 -- Notification Sent
;
EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;


PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''

', 
		@database_name=N'BMOAtomicStat', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Returns and deposits - update statistics for BMORisk.dbo.StatExportBulk]    Script Date: 9/19/2024 8:13:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Returns and deposits - update statistics for BMORisk.dbo.StatExportBulk', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;





UPDATE STATISTICS dbo.StatExportBulk
;






SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 13 -- Table Statistics Updated
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Returns and deposits - Transfer to [AtomicStat]]    Script Date: 9/19/2024 8:13:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Returns and deposits - Transfer to [AtomicStat]', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50) = N''???''
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [risk].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;
--PRINT N''Step: Run AtomicStat '' + @nvAbbrv + N'' Transfer process'';


DECLARE 
	 @iBatchId        int            =    -1 -- shall be determined via the following query
	,@iStatBatchLogId int            =    -1 -- supplying -1 for StatBatchLogId causes a new row to be inserted into stat.BatchLog, which is what we want to do.
	,@iBatchCount     int            =     0 -- tally for how many batches are processed
	,@nvDataSetName   nvarchar(128)
	,@nvMessage       nvarchar(4000)
;

WHILE @iBatchId IS NOT NULL -- value is set above to -1 in order for the loop to be entered
	AND @iBatchCount < 11
BEGIN

	SELECT @iBatchId = NULL -- value is set to NULL in case the following query fails to retrieve a BatchId, ensuring the loop will exit
		,@iStatBatchLogId = -1 -- reset so that a new StatBatchLogId can be fetched
	;
	SELECT 
		 @iBatchId = x.BatchId 
		,@nvDataSetName = x.BatchDataSetName
	FROM (
			SELECT
				 ROW_NUMBER() OVER( ORDER BY CycleDate ASC, DataSetSeq ASC, BatchId ASC ) AS RowSeq
				,CycleDate
				,DataSetSeq
				,BatchDataSetName
				,BatchId
				,BatchCompletedDatetime
				,StatBatchLogId
			FROM [stat].[vwBatchStatBatchLogXref]
			WHERE 1 = 1
				AND BatchDataSetName IN( N''Daily Stats - Trxn and Return'', N''Daily Stats'' )
--				AND StatBatchLogId IS NULL
				AND ( StatBatchLogId IS NULL OR ( StatBatchLogId IS NOT NULL AND BatchTransferCompletedDatetime IS NULL ) )
				AND CycleDate > DATEADD( dd, -7, SYSDATETIME() )
		) x
	WHERE x.RowSeq = 1
		AND x.BatchCompletedDatetime IS NOT NULL -- if BatchCompletedDatetime is null, then the batch is still being generated by BMORisk
--		AND x.StatBatchLogId IS NULL
	;

	IF @iBatchId IS NOT NULL
	BEGIN

		SELECT @nvSubject = N''Info: '' + @nvJobName + N'' - stat transfer '' + @nvAbbrv + N''Risk batch '' + CONVERT( nvarchar(10), @iBatchId )
			,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' - initiating stat transfer of '' + @nvAbbrv + N''Risk batch: '' + CONVERT( nvarchar(10), @iBatchId ) + N''.'' + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
		;
		EXEC [dbo].[uspSendEmail]
			 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
			-- @pnvRecipients = ''lwhiting@validadvantage.com;''
			,@pnvSubject = @nvSubject
			,@pnvBodyText = @nvBody
		;


		PRINT REPLICATE( N''='', 80 )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''    BatchId = '' + CONVERT( nvarchar(50), @iBatchId )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''DataSetName = '' + @nvDataSetName
		SET @nvMessage = N''''
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;


		SET @nvMessageText = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''RiskBatchId='' + ISNULL( TRY_CONVERT( nvarchar(10), @iBatchId ), N''{null}'' ) + N'',AtomicStatBatchId='' + ISNULL( TRY_CONVERT( nvarchar(10), @iStatBatchLogId ), N''{null}'' )
		;
		SET @nvPMStatusMessage = @nvMessageText + SPACE(1) + N''Begin...''
		;
		SET @iPMStatusCode = 2 -- Info
		;
		EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
			 @processInstanceId = @iPMProcessInstanceId
			,@processInstanceGuid = @uPMProcessInstanceGuid
			,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
			,@stage = @iPMStage
			,@statusCode = @iPMStatusCode
			,@statusMessage = @nvPMStatusMessage
		;


-- -- print ''Execute [stat].[uspStatExportBulk_TransferToAtomicStat] simulated.''
		EXEC [stat].[uspStatExportBulk_TransferToAtomicStat] @piBatchId = @iBatchId, @piStatBatchLogId = @iStatBatchLogId OUTPUT, @ptiDebug = 1, @piJobStepId = @iStepId
		;

		PRINT REPLICATE( N''-'', 40 )
		RAISERROR ( N'''', 0, 1 ) WITH NOWAIT;

-- -- print N''Execution of [stat].[uspQueueStatTransferToHub] simulated.'';
		EXEC [stat].[uspQueueStatTransferToHub] @piStatBatchLogId = @iStatBatchLogId, @piJobStepId = @iStepId -- without a parameter, it grabs the earliest available batch (which should be the one that was just processed by AtomicStat).


		SET @nvPMStatusMessage = @nvMessageText + SPACE(1) + N''...End''
		;
		SET @iPMStatusCode = 2 -- Info
		;
		EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
			 @processInstanceId = @iPMProcessInstanceId
			,@processInstanceGuid = @uPMProcessInstanceGuid
			,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
			,@stage = @iPMStage
			,@statusCode = @iPMStatusCode
			,@statusMessage = @nvPMStatusMessage
		;


		PRINT REPLICATE( N''-'', 40 )
		RAISERROR ( N'''', 0, 1 ) WITH NOWAIT;

		SET @iBatchCount = @iBatchCount + 1
		;

-- -- set @iBatchId = null -- forcing a single pass through the loop for testing purposes -- TO-DO: remove this when done testing
	END
	ELSE
	BEGIN
		IF @iBatchCount > 0
		BEGIN
			PRINT REPLICATE( N''='', 80 )
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''    BatchId = {null}'' 
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''DataSetName = {null}''
			PRINT N''''
			PRINT N''Nothing else to process.''
			PRINT N''''
			SET @nvMessage = N''''
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
	END
	;

	WAITFOR DELAY ''00:00:01'' -- slight pause before looping

END
;


IF @iBatchCount = 0
BEGIN
	PRINT N''Not executing [stat].[uspStatExportBulk_TransferToAtomicStat].  No available batch found.''
	PRINT N''''

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Nothing available to process.''
	;
	SET @iPMStatusCode = 3 -- Warning
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 19 -- "Skipped" (but not really skipped) because there was nothing available to process
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;


	IF DATENAME( weekday, SYSDATETIME() ) NOT IN( ''Sunday'', ''Monday'' )
	BEGIN

		SELECT @nvSubject = N''Warning: "'' + @nvJobRef + N''" - No available batch found.''
			,@nvBody = N''Warning notification.  Attention may be required.  No batch available to '' + @nvJobRef + N'' for processing.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
		;

		EXEC [dbo].[uspSendEmail]
			 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
			-- @pnvRecipients = ''lwhiting@validadvantage.com;''
			,@pnvSubject = @nvSubject
			,@pnvBodyText = @nvBody
		;

		PRINT @nvBody
		;
	END

END
ELSE
BEGIN

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 12 -- Queued In Hub
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

END
;


PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''
', 
		@database_name=N'BMOAtomicStat', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Promote Deltas - Returns and deposits - no table truncates]    Script Date: 9/19/2024 8:13:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Promote Deltas - Returns and deposits - no table truncates', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@nvMessageText nvarchar(4000)
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;




PRINT N''Promote Deltas without truncating tables''
;
EXEC [dbo].[sp_PromoteDeltaStatsTrxnAndReturnOnly]
;




SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 10 -- Deltas Promoted
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Multi Customer Account file]    Script Date: 9/19/2024 8:13:15 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Multi Customer Account file', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
/*

   [dbo].[sp_ScheduledMultiCustomerAccountBuild]

   Processes the Multi Customer Account file in arears.

   All logic is contained within the sproc.

   Does not require any job step logic to check for file availability.
   "The data is available or it is not.
      If available, it processes the data and progresses forward.
      If not available, it does nothing and the job still progresses forward."

*/

DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvMessageText nvarchar(4000)
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;




--print N''Simulated execution of [dbo].[sp_ScheduledMultiCustomerAccountBuild]''
;
EXEC [dbo].[sp_ScheduledMultiCustomerAccountBuild]
;




SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 11 -- Multi Customer Processed
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Accounts - Watch for an available FileSet]    Script Date: 9/19/2024 8:13:15 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Accounts - Watch for an available FileSet', 
		@step_id=11, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @siWhileStatus smallint = -1
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@nvMessageText nvarchar(4000)
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [risk].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;

IF SUSER_SNAME() = N''VALIDRS\PRDBI03SqlAgnt'' -- if SQL Agent is running the job, send the notification.  Otherwise, this is a manual launch and we do not want a notification sent.
BEGIN
	SELECT @nvSubject = N''Info: '' + @nvJobName + N'' - Beginning watch for an available FileSet''
		,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' - watching for an available FileSet in Hadoop before generating stats.'' + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
	;

	PRINT REPLICATE( N''-'', 80 )
	PRINT @nvSubject
	PRINT @nvBody
	PRINT REPLICATE( N''-'', 80 )
	;

	EXEC [dbo].[uspSendEmail]
		 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
		-- @pnvRecipients = ''lwhiting@validadvantage.com;''
		,@pnvSubject = @nvSubject
		,@pnvBodyText = @nvBody
	;
END



EXEC [stat].[uspWatchForAggregateFileSet]
	@pnvDelayDuration = ''00:15:00'' 
	,@pnvWatchEndTime = ''20:00:00''
	,@psiWhileStatus = @siWhileStatus OUTPUT
	,@pnvStatGenerator = N''Daily Stats - Account''
	,@piJobStepId = @iStepId
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;

--set @siWhileStatus = 1 -- for testing purposes
PRINT N''@siWhileStatus = '' + CONVERT( nvarchar(50), @siWhileStatus )
;
IF @siWhileStatus <> 1 
BEGIN


	IF @siWhileStatus = 0 SET @iPMStatusCode = 7 -- Files Not Ready - Timeout
	;
	IF @siWhileStatus = -1 SET @iPMStatusCode = 18 -- No Available CycleDates To Process
	;
	IF @siWhileStatus IN( 0, -1 )
	BEGIN

		EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
			 @processInstanceId = @iPMProcessInstanceId
			,@processInstanceGuid = @uPMProcessInstanceGuid
			,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
			,@stage = @iPMStage
			,@statusCode = @iPMStatusCode
			,@statusMessage = @nvPMStatusMessage
		;

	END
	;


	SET @nvSubject = N''Warning: '' + @nvJobRef + N'' - Watch for an available FileSet timed out or encountered a problem.''
	;
	SET @nvBody = CASE WHEN @siWhileStatus = 0
				THEN ( N''Warning notification.  Attention may be required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' timed out.'' )
			WHEN @siWhileStatus = -1
				THEN ( N''Warning notification.  Attention may be required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' did not find any available CycleDates to process.'' )
			WHEN @siWhileStatus = -2
				THEN ( N''Warning notification.  Attention may be required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' missing value for parameter "@pnvStatGenerator".'' )
			ELSE ( N''Warning notification.  Attention required.'' +  NCHAR(013) + NCHAR(010) + N''File watch by job '' + @nvJobRef + N'' received unrecognized value for @siWhileStatus.'' + NCHAR(013) + NCHAR(010) + N''The watch procedure exited with @siWhileStatus = '' + CONVERT( nvarchar(50), @siWhileStatus ) )
		END 
	;
	SET @nvBody = @nvBody + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
	;

	PRINT REPLICATE( N''-'', 80 )
	PRINT @nvSubject
	PRINT @nvBody
	PRINT REPLICATE( N''-'', 80 )
	;

	EXEC [dbo].[uspSendEmail]
		 @pnvRecipients = ''dwsupport@validsystems.net;dbmonitoralerts@validsystems.net;''
		-- @pnvRecipients = ''lwhiting@validadvantage.com;''
		,@pnvSubject = @nvSubject
		,@pnvBodyText = @nvBody
	;

--	PRINT @nvBody;

	--RAISERROR( @nvBody, 16, 1 ) -- for when the job is live
	RAISERROR( @nvBody, 0, 1 ) -- for dev-test
	;
END

IF @siWhileStatus = 1 
BEGIN
	SET @iPMStatusCode = 8 -- Files Ready
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;
END


PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''
', 
		@database_name=N'BMOAtomicStat', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Accounts - Stat generation]    Script Date: 9/19/2024 8:13:15 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Accounts - Stat generation', 
		@step_id=12, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50)
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@nvStatGenerator nvarchar(128) = N''Daily Stats - Account''
	,@nvClientAbbr nvarchar(5)
	,@nvMessageText nvarchar(4000)
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;




SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;
SELECT @nvClientAbbr = [FileProcessingClientRef] FROM [dbo].[Client]
;

--PRINT N''Step: Run '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' Stat generation processes'';

DECLARE @dCycleDate date
;
SELECT @dCycleDate = MAX( [BuildCycleDate] ) FROM [BMOAtomicStat].[dbo].[vwProcessDate] pd WHERE pd.[BuildCycleDate] < CONVERT( date, SYSDATETIME() )

--IF EXISTS( SELECT ''X'' FROM [AtomicStat].[{ClientAbbr}].[ufnImportSetVerify]( default ) WHERE [IsAvailableToProcess] = 1 AND FileSet > DATEADD( day, -7, SYSDATETIME() ) ) 
IF EXISTS
	( 
		SELECT ''X'' FROM [AtomicStat].[hadoop].[ufnClientFileCycleDateRangeReadiness]( @dCycleDate, @nvClientAbbr, N''accounts'', 6 ) AS x 
		WHERE x.AggregateControl_ResultIsDownstreamSafe = 1 
			AND NOT EXISTS( select ''x'' from [dbo].[StatBatch] as b where x.CycleDate = b.CycleDate AND b.StatBatchDataSetName = @nvStatGenerator )
	)
BEGIN

	SELECT @nvSubject = N''Info: '' + @nvJobName + N'' - '' + @nvStatGenerator + N'' stat generation''
		,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' - initiating '' + @nvStatGenerator + N'' stat generation.'' + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
	;

	PRINT REPLICATE( N''-'', 80 )
	PRINT @nvSubject
	PRINT @nvBody
	PRINT REPLICATE( N''-'', 80 )
	;

	EXEC [BMOAtomicStat].[dbo].[uspSendEmail]
		 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
		-- @pnvRecipients = ''lwhiting@validadvantage.com;''
		,@pnvSubject = @nvSubject
		,@pnvBodyText = @nvBody
	;

--print N''Simulating execution of [dbo].[uspDataBuildGate] for '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' Stat generation'';
	PRINT N''Running the gate for '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' stat generation''
	;
	EXEC [dbo].[uspDataBuildGate] @pnvStatGenerator = @nvStatGenerator --@pdCycleDate = ''2020-07-10''
	;


	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 9 -- Stats Calculated
	;
	EXEC [processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

END
ELSE
BEGIN
	PRINT N''Nothing found to process.  Ignoring '' + @nvAbbrv + N''Risk '' + @nvAbbrv + N'' stat generation''
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Nothing available to process.''
	;
	SET @iPMStatusCode = 3 -- Warning
	;
	EXEC [processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 19 -- "Skipped" (but not really skipped) because there was nothing available to process
	;
	EXEC [processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;



	IF DATENAME( weekday, SYSDATETIME() ) NOT IN( ''Sunday'', ''Monday'' )
	BEGIN
		SELECT @nvSubject = N''Warning: '' + @nvJobRef + N'' - nothing to process for '' + @nvStatGenerator
			,@nvBody = N''Warning notification.  Attention may be required.  Nothing found for '' + @nvJobRef + N'' to process.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
		;

		PRINT REPLICATE( N''-'', 80 )
		PRINT @nvSubject
		PRINT @nvBody
		PRINT REPLICATE( N''-'', 80 )
		;

		/* Uncomment this, once ready for live operation...*/
		EXEC [BMOAtomicStat].[dbo].[uspSendEmail]
			 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
			-- @pnvRecipients = ''lwhiting@validadvantage.com;''
			,@pnvSubject = @nvSubject
			,@pnvBodyText = @nvBody
		;
	END

END

PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Accounts - update statistics for BMORisk.dbo.StatExportBulk]    Script Date: 9/19/2024 8:13:15 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Accounts - update statistics for BMORisk.dbo.StatExportBulk', 
		@step_id=13, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;





UPDATE STATISTICS dbo.StatExportBulk
;





SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 13 -- Table Statistics Updated
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Accounts - Transfer to [AtomicStat]]    Script Date: 9/19/2024 8:13:16 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Accounts - Transfer to [AtomicStat]', 
		@step_id=14, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50) = N''???''
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [risk].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;




SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;
--PRINT N''Step: Run AtomicStat '' + @nvAbbrv + N'' Transfer process'';


DECLARE 
	 @iBatchId        int            =    -1 -- shall be determined via the following query
	,@iStatBatchLogId int            =    -1 -- supplying -1 for StatBatchLogId causes a new row to be inserted into stat.BatchLog, which is what we want to do.
	,@iBatchCount     int            =     0 -- tally for how many batches are processed
	,@nvDataSetName   nvarchar(128)
	,@nvMessage       nvarchar(4000)
;

WHILE @iBatchId IS NOT NULL -- value is set above to -1 in order for the loop to be entered
	AND @iBatchCount < 11
BEGIN

	SELECT @iBatchId = NULL -- value is set to NULL in case the following query fails to retrieve a BatchId, ensuring the loop will exit
		,@iStatBatchLogId = -1 -- reset so that a new StatBatchLogId can be fetched
	;
	SELECT 
		 @iBatchId = x.BatchId 
		,@nvDataSetName = x.BatchDataSetName
	FROM (
			SELECT
				 ROW_NUMBER() OVER( ORDER BY CycleDate ASC, DataSetSeq ASC, BatchId ASC ) AS RowSeq
				,CycleDate
				,DataSetSeq
				,BatchDataSetName
				,BatchId
				,BatchCompletedDatetime
				,StatBatchLogId
			FROM [stat].[vwBatchStatBatchLogXref]
			WHERE 1 = 1
				AND BatchDataSetName IN( N''Daily Stats - Account'', N''Weekly AU Stats'', @nvAbbrv + SPACE(1) + N''Weekly AU Stats'' )
--				AND StatBatchLogId IS NULL
				AND ( StatBatchLogId IS NULL OR ( StatBatchLogId IS NOT NULL AND BatchTransferCompletedDatetime IS NULL ) )
				AND CycleDate > DATEADD( dd, -7, SYSDATETIME() )
		) x
	WHERE x.RowSeq = 1
		AND x.BatchCompletedDatetime IS NOT NULL -- if BatchCompletedDatetime is null, then the batch is still being generated by BMORisk
--		AND x.StatBatchLogId IS NULL
	;

	IF @iBatchId IS NOT NULL
	BEGIN

		SELECT @nvSubject = N''Info: '' + @nvJobName + N'' - stat transfer '' + @nvAbbrv + N''Risk batch '' + CONVERT( nvarchar(10), @iBatchId )
			,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' - initiating stat transfer of '' + @nvAbbrv + N''Risk batch: '' + CONVERT( nvarchar(10), @iBatchId ) + N''.'' + NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
		;
		EXEC [dbo].[uspSendEmail]
			 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
			-- @pnvRecipients = ''lwhiting@validadvantage.com;''
			,@pnvSubject = @nvSubject
			,@pnvBodyText = @nvBody
		;


		PRINT REPLICATE( N''='', 80 )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''    BatchId = '' + CONVERT( nvarchar(50), @iBatchId )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''DataSetName = '' + @nvDataSetName
		SET @nvMessage = N''''
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;


		SET @nvMessageText = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''RiskBatchId='' + ISNULL( TRY_CONVERT( nvarchar(10), @iBatchId ), N''{null}'' ) + N'',AtomicStatBatchId='' + ISNULL( TRY_CONVERT( nvarchar(10), @iStatBatchLogId ), N''{null}'' )
		;
		SET @nvPMStatusMessage = @nvMessageText + SPACE(1) + N''Begin...''
		;
		SET @iPMStatusCode = 2 -- Info
		;
		EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
			 @processInstanceId = @iPMProcessInstanceId
			,@processInstanceGuid = @uPMProcessInstanceGuid
			,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
			,@stage = @iPMStage
			,@statusCode = @iPMStatusCode
			,@statusMessage = @nvPMStatusMessage
		;


-- -- print ''Simulating execution of [stat].[uspStatExportBulk_TransferToAtomicStat].''
		EXEC [stat].[uspStatExportBulk_TransferToAtomicStat] @piBatchId = @iBatchId, @piStatBatchLogId = @iStatBatchLogId OUTPUT, @ptiDebug = 1, @piJobStepId = @iStepId
		;

		PRINT REPLICATE( N''-'', 40 )
		RAISERROR ( N'''', 0, 1 ) WITH NOWAIT;

-- -- print N''Simulating execution of [stat].[uspQueueStatTransferToHub].'';
		EXEC [stat].[uspQueueStatTransferToHub] @piStatBatchLogId = @iStatBatchLogId, @piJobStepId = @iStepId -- without a parameter, it grabs the earliest available batch (which should be the one that was just processed by AtomicStat).


		SET @nvPMStatusMessage = @nvMessageText + SPACE(1) + N''...End''
		;
		SET @iPMStatusCode = 2 -- Info
		;
		EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
			 @processInstanceId = @iPMProcessInstanceId
			,@processInstanceGuid = @uPMProcessInstanceGuid
			,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
			,@stage = @iPMStage
			,@statusCode = @iPMStatusCode
			,@statusMessage = @nvPMStatusMessage
		;


		PRINT REPLICATE( N''-'', 40 )
		RAISERROR ( N'''', 0, 1 ) WITH NOWAIT;

		SET @iBatchCount = @iBatchCount + 1
		;

-- --set @iBatchId = null -- forcing a single pass through the loop for testing purposes -- TO-DO: remove this when done testing
	END
	ELSE
	BEGIN
		IF @iBatchCount > 0
		BEGIN
			PRINT REPLICATE( N''='', 80 )
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''    BatchId = {null}'' 
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + NCHAR(009) + N''DataSetName = {null}''
			PRINT N''''
			PRINT N''Nothing else to process.''
			PRINT N''''
			SET @nvMessage = N''''
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
	END
	;

	WAITFOR DELAY ''00:00:01'' -- slight pause before looping

END
;


IF @iBatchCount = 0 
BEGIN
	PRINT N''Not executing [stat].[uspStatExportBulk_TransferToAtomicStat].  No available batch found.''
	PRINT N''''

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Nothing available to process.''
	;
	SET @iPMStatusCode = 3 -- Warning
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 19 -- "Skipped" (but not really skipped) because there was nothing available to process
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;


	IF DATENAME( weekday, SYSDATETIME() ) NOT IN( ''Sunday'', ''Monday'' )
	BEGIN

		SELECT @nvSubject = N''Warning: '' + @nvJobRef + N'' - No available batch found.''
			,@nvBody = N''Warning notification.  Attention may be required.  No batch available to '' + @nvJobRef + N'' for processing.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
		;

		EXEC [dbo].[uspSendEmail]
			 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
			-- @pnvRecipients = ''lwhiting@validadvantage.com;''
			,@pnvSubject = @nvSubject
			,@pnvBodyText = @nvBody
		;

		PRINT @nvBody
		;
	END

END
ELSE
BEGIN

	SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
	;
	SET @iPMStatusCode = 12 -- Queued In Hub
	;
	EXEC [BMORisk].[processmonitor].[usp_UpdateStatus] 
		 @processInstanceId = @iPMProcessInstanceId
		,@processInstanceGuid = @uPMProcessInstanceGuid
		,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
		,@stage = @iPMStage
		,@statusCode = @iPMStatusCode
		,@statusMessage = @nvPMStatusMessage
	;

END
;

PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''
', 
		@database_name=N'BMOAtomicStat', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Promote Deltas - post Accounts]    Script Date: 9/19/2024 8:13:16 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Promote Deltas - post Accounts', 
		@step_id=15, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@nvMessageText nvarchar(4000)
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



	PRINT N''Promote Deltas''
	;
	EXECUTE [dbo].[sp_ScheduledPromoteDeltaStatsToFullSet]
	;





SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 10 -- Deltas Promoted
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Invoke Job 'DatabaseBackup - BMO - FULL']    Script Date: 9/19/2024 8:13:16 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Invoke Job ''DatabaseBackup - BMO - FULL''', 
		@step_id=16, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;





EXECUTE msdb.dbo.sp_start_job @job_name = N''DatabaseBackup - BMO - FULL''
;






SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 14 -- Job Spawned
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [End of Core Job Steps]    Script Date: 9/19/2024 8:13:16 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'End of Core Job Steps', 
		@step_id=17, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=4, 
		@on_fail_step_id=18, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50) = N''???''
	,@nvSubject nvarchar(256)
	,@nvBody nvarchar(4000)
	,@iAvailableForHubCount int
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@uJobId uniqueidentifier = $(ESCAPE_NONE(JOBID))
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;

SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 1 -- In Progress
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;


SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;

PRINT N''Sending end of job notification.''
;
PRINT N''''

SELECT @iAvailableForHubCount = COUNT(1) FROM [BMOAtomicStat].[stat].[vwBatchTransferToHubAvailable] WHERE CycleDate > DATEADD( day, -7, SYSDATETIME() )
;
--SET @nvBody = CONVERT( nvarchar(50), @iAvailableForHubCount ) + N'' batches queued for Hub Transfer.'';
SELECT @nvSubject = N''Info: "'' + @nvJobRef + N''" - Completed.''
	,@nvBody = N''Informational only.  No action required.  '' + @nvJobRef + N'' completed.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
;

EXEC [BMOAtomicStat].[dbo].[uspSendEmail]
	 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
	-- @pnvRecipients = ''lwhiting@validadvantage.com;''
	,@pnvSubject = @nvSubject
	,@pnvBodyText = @nvBody
;

PRINT @nvBody
;

PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''



SET @nvMessageText = N''Copying log data to [DBA].[dbo].[JobStepLogHistory]''
;
SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + @nvMessageText
;
SET @iPMStatusCode = 2 -- Info
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;


PRINT @nvMessageText
;
EXEC [DBA].[dbo].[uspJobStepLogHistory] @puJobId = @uJobId
;


SET @nvMessageText = N''Copied log data to [DBA].[dbo].[JobStepLogHistory]''
;
SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + @nvMessageText
;
SET @iPMStatusCode = 2 -- Info
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;





SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString
;
SET @iPMStatusCode = 16 -- Job Completed Successfully
;
EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



/* ===================================================================================================

	End a successful job execution by clearing out the Occurrence Key in the persisted occurence table for this job

=================================================================================================== */
EXEC [processmonitor].[usp_StatBatchManagementJob_UpdateOccurrenceStatus] @piStatusCode = NULL, @piStage = NULL -- resets columns CurrentOccurrenceKeyValue, CurrentOccurrenceDatetime, StatusCode to NULL values
;



/* ===================================================================================================

	We are "skipping" the last Job Step / PM Stage with a Pass StatusCode to conclude the current job execution.

	The last job step for all "{client} Stat Batch Management" jobs is a catchall for any job-halt worthy errors.

=================================================================================================== */
SET @iStepId = @iStepId + 1
;
SET @nvStepIdAsString = ISNULL( TRY_CONVERT( nvarchar(50), @iStepId ), N''{null}'' )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed in ProcessMonitor as a Stage.
;
SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Prior Stage end of core job completed w/o issue.''
;
SET @iPMStatusCode = 19 -- Skipped
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;

', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send Problem Notification]    Script Date: 9/19/2024 8:13:16 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send Problem Notification', 
		@step_id=18, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DECLARE @nvAbbrv nvarchar(50) = N''???''
	,@nvServerName nvarchar(128) = N''['' + @@ServerName + N'']''
	,@nvJobName nvarchar(128) = N''"$(ESCAPE_SQUOTE(JOBNAME))"''
	,@nvStepName nvarchar(128) = N''"$(ESCAPE_SQUOTE(STEPNAME))"''
	,@nvJobRef nvarchar(256) = N''''
	,@uJobId uniqueidentifier = $(ESCAPE_NONE(JOBID))
	,@nvStepIdAsString nvarchar(128) = N''$(ESCAPE_SQUOTE(STEPID))''
	,@iStepId int
	,@iPMProcessInstanceId int 
	,@uPMProcessInstanceGuid uniqueidentifier
	,@nvPMCurrentOccurrenceKeyValue nvarchar(50)
	,@iPMStage int
	,@iPMStatusCode int
	,@nvPMStatusMessage nvarchar(250)
	,@nvMessageText nvarchar(4000)
	,@iPMStageFailed int
;

SET @iStepId = ISNULL( TRY_CONVERT( int, @nvStepIdAsString ), 0 )
;
SET @iPMStage = @iStepId - 1 -- subtracting (1) because the first job step is not listed ProcessMonitor as a Stage.
;

SELECT TOP 1
	 @iPMProcessInstanceId = ProcessInstanceId
	,@uPMProcessInstanceGuid = ProcessInstanceGuid
	,@nvPMCurrentOccurrenceKeyValue = CurrentOccurrenceKeyValue
	,@iPMStageFailed = Stage -- The stored value should be the Stage in/around which a failure occurred.
FROM [processmonitor].[vw_StatBatchManagementJob]
;

IF ( @nvPMCurrentOccurrenceKeyValue IS NULL )
BEGIN
	SET @nvMessageText = N''Job "'' + @nvJobName + N''" does not have an Occurrence Key.  Cancelling job execution.''
	RAISERROR( @nvMessageText, 16, 1 ) WITH LOG;
	;
END
ELSE
BEGIN
	PRINT N''Occurrence Key = "'' + @nvPMCurrentOccurrenceKeyValue + N''"''
END
;



SET @nvPMStatusMessage = N''Prior Job Step or SQL Agent''
;
SET @iPMStatusCode = 5 -- Fail
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStageFailed -- Attaching this fail to the failed stage.
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + N''Error or failure encountered.''
;
SET @iPMStatusCode = 3 -- Warning
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



/*

	 Leave values alone in columns CurrentOccurrenceKeyValue, CurrentOccurrenceDatetime, set Stage to current stage and set StatusCode to -1 indicating the job is no longer In Process and was interrupted before successful completion.

*/
EXEC [processmonitor].[usp_StatBatchManagementJob_UpdateOccurrenceStatus] 
	 @piStatusCode = -1
	,@piStage = @iPMStage
;


SET @nvAbbrv = SUBSTRING( @nvJobName, 1, 3 )
;
--SET @nvJobRef = @nvServerName + SPACE(1) + @nvJobName;
SET @nvJobRef = @nvJobName + N'', '' + @nvStepName
;
DECLARE @nvSubject nvarchar(256) = N''''
	,@nvBody nvarchar(4000) = N''''
;

PRINT N''Send Problem Notification''
;
PRINT N''''

SELECT @nvSubject = N''Alert: '' + @nvJobRef + N'' - Problem encountered.''
	,@nvBody = N''Alert!  Investigate recent job execution of '' + @nvJobRef + N''.'' +  NCHAR(013) + NCHAR(010) + N''{MT:'' + CONVERT( nvarchar(50), NEWID() ) + N''}'' -- "MT:" means "Message Tag"
;

EXEC [BMOAtomicStat].[dbo].[uspSendEmail]
	 @pnvRecipients = ''dbmonitoralerts@validsystems.net;''
	-- @pnvRecipients = ''lwhiting@validadvantage.com;''
	,@pnvSubject = @nvSubject
	,@pnvBodyText = @nvBody
;
PRINT @nvBody
;

PRINT N''''
PRINT REPLICATE( N''='', 80 )
PRINT REPLICATE( N''='', 80 )
PRINT N''''



SET @nvMessageText = N''Copying job log data to [DBA].[dbo].[JobStepLogHistory]''
;
SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + @nvMessageText
;
SET @iPMStatusCode = 2 -- Info
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;


PRINT @nvMessageText
;

EXEC [DBA].[dbo].[uspJobStepLogHistory] @puJobId = @uJobId
;


SET @nvMessageText = N''Copied job log data to [DBA].[dbo].[JobStepLogHistory]''
;
SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + N'':'' + SPACE(1) + @nvMessageText
;
SET @iPMStatusCode = 2 -- Info
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;



SET @nvPMStatusMessage = N''Job Step'' + SPACE(1) + @nvStepIdAsString + SPACE(1) + @nvStepName
;
SET @iPMStatusCode = 17 -- Job Abend -- because if we made it to this job step, we definitely have an abnormal end to the job.
;
PRINT @nvPMStatusMessage
;

EXEC [processmonitor].[usp_UpdateStatus] 
	 @processInstanceId = @iPMProcessInstanceId
	,@processInstanceGuid = @uPMProcessInstanceGuid
	,@occurrenceKey = @nvPMCurrentOccurrenceKeyValue
	,@stage = @iPMStage
	,@statusCode = @iPMStatusCode
	,@statusMessage = @nvPMStatusMessage
;


IF @nvJobName NOT LIKE N''%not live%''
BEGIN
	RAISERROR( @nvBody, 16, 1 ) WITH LOG -- Actually raise an error and write it into the SQL Server log.
	;
END
ELSE
BEGIN
	RAISERROR( N''Dev-Test Monitor'', 0, 1 ) WITH NOWAIT -- Not actually raising an error, since people tend to get their freak on even when we are just performing dev-test.
	;
END
', 
		@database_name=N'BMORisk', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'BMO Stat Batch Management Schedule', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=124, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230829, 
		@active_end_date=99991231, 
		@active_start_time=3013, 
		@active_end_time=235959, 
		@schedule_uid=N'f50f4c72-3ba3-4d3b-9bd7-a36e9b4802c5'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


