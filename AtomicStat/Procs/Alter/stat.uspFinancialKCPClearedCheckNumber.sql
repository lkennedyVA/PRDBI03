USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspFinancialKCPClearedCheckNumber]
	Created By: Chris Sharp 
	Description: This procedure uses the pre-populated precalc tables and calculates
		the following stats into their respective Stat Type storage tables.

		Prior to executing this, dont forget to build all the KeyElements
		We Need to Return Solidified Process For Aging Out Stat Values

	Stat Value(s):	KCPMinCheckNumberCleared
					KCPMaxCheckNumberCleared

	Table(s): 
		,[ValidFI].[dbo].[Transaction] 

	Function(s): [stat].[ufnStatIdByName]
		,[precalc].[ufnParameterDate]

	Procedure(s): [error].[uspLogErrorDetailInsertOut]

	History:
		2018-10-11 - CBS - Created	
		2018-10-12 - LSW - Modified, adjusted WAY LATE night 
		2018-10-16 - CBS - Modified, dammit
		2018-10-25 - LSW - code rage - gutted a lot of code and replaced with Diana's 
								full history query to cut down on the complaints/excuses 
								(even though the original core query came straight from Diana).
		2018-10-31 - LSW - Diana changed the data gathering mechanism, again.
								New date calculations, and new candidate key collecting.
								Candidate key collecting is similar to original requirements/specs.
		2018-11-20 - LBD - Added added a distinct
		2018-12-04 - LSW - Added BatchLogId to list of columns inserted into stat.KeyElement.
		2019-01-14 - LSW - Adjusted the template string tags ("[{tagReference}]") to the latest tag references in stat.KeyType.KeyTemplatePreprocessed.
		2019-01-15 - LSW - Added upkeep of kegen.HashKCP.
		2019-01-17 - LBD - Just a note, it is called within 
				'\SSISDB\DataLoad\Load_Stats\LoadStatsMaster.dtsx'
		2019-01-18 - LBD - Definition of Hashstring was miss set to to the full hash, 
			not the string, added kegen.HashKCP update
		2021-08-20 - LSW - This sproc has been disabled per CCF2645

*****************************************************************************************/
ALTER PROCEDURE [stat].[uspFinancialKCPClearedCheckNumber](
 @psiBatchLogId INT = NULL OUTPUT
--,@pbiSetSize BIGINT = NULL
)
AS 
BEGIN
	SET NOCOUNT ON;
	DECLARE @nvThisSProcName sysname = ISNULL( ( ISNULL( QUOTENAME( OBJECT_SCHEMA_NAME( @@PROCID ) ) + N'.', N'' ) + QUOTENAME( OBJECT_NAME( @@PROCID ) ) ), N'running as script (' + CONVERT( nvarchar(50), @@PROCID ) + N')' )
	;


print N'This sproc has been disabled: ' + @nvThisSProcName
;
RETURN -- This sproc has been disabled per CCF2645.
/*
	DECLARE @biKCPCount bigint = ( SELECT COUNT(1) FROM [precalc].[KCPCustomerPayer] AS k )
		,@biSetSize bigint = 2000000 -- ISNULL(@pbiSetSize, 2000000)
		,@dtDateActivated datetime2(7) = SYSDATETIME()
		,@dtClearedDay5Parameter date
		,@iErrorDetailId int
		,@iPageNumber int
		,@iPageCount bigint 
		,@iRowCount int = 0
		,@iCustomerNumberIdTypeId int = 25 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
		,@iOrgId int = 100009 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
		,@nvCustomerNumberIdTypeId nvarchar(100) 
		,@nvKeyTemplate nvarchar(1024)
		,@nvOrgId nvarchar(100)  
		,@nvMessageText nvarchar(256)
		,@nvWorker nvarchar(1024) 
		,@siClientOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial Client Organization')
		,@siIdTypeKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial Customer Credential Type')
		,@siRoutingNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial Routing Number')
		,@siAccountNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial Account Number')
		,@siCustomerNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial Customer Identifier')
		,@siFinancialKCPKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial KCP')
		,@siKCPMinCheckNumberClearedStatId smallint = [stat].[ufnStatIdByName]('KCPMinCheckNumberCleared')	
		,@siKCPMaxCheckNumberClearedStatId smallint = [stat].ufnStatIdByName('KCPMaxCheckNumberCleared')
		,@sSchemaName sysname = 'stat'
		,@dt2TimeStamp datetime2(7)
		,@dtTimerDate datetime2(7)
		,@iEllapsedTimeInSeconds int
		,@siBatchLogId INT = @psiBatchLogId -- btw: Not this in production!-->> = ISNULL( @psiBatchLogId, ( SELECT MAX( BatchLogId ) FROM [stat].[BatchLog] ) )
		,@biMissingKeyElementCount bigint
		,@dtClearedDay5Back90 date
	;


	DECLARE 
		 @dtToday date = SYSDATETIME() -- used for Calander driven dates vs Cycle Date driven dates
		,@dtNow datetime2(0) = SYSDATETIME()
		,@dtIncrementalBeginCycleDate date = NULL -- ISNULL( @pdtIncrementalLowCycleDate, ISNULL( ( SELECT MAX( CycleDate ) FROM [ValidFI].[PNC].[Cycle] ) , GETDATE()-1 ) ) -- '2018-10-12' -- '2001-01-01' -- '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
		,@dtIncrementalEndCycleDate date
		,@dtPriorCycleDate date
		,@dtClearedCycleDate date
		,@dtGetTranChangesBegin date
		,@dtGetTranChangesEnd date
		,@dtGetRetChangesBegin date
		,@dtGetRetChangesEnd date
		,@dtGetAcctCustChangesBegin date
		,@dtGetAcctCustChangesEnd date
		,@dtLastSuccessfulCycleDateRefresh date
	;


	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	;
	SET @nvMessageText = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + @nvThisSProcName + SPACE(1) + N'Begin...'; 
	PRINT N''
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.
	PRINT N''


	IF @siBatchLogId IS NULL -- stat.uspFinancialKCPClearedCheckNumber is running in default mode ("give me a new BatchLogId")
		BEGIN
			SET @siBatchLogId = -99 -- a negative value tells precalc.uspFinancialKCPBatchRunValuePrepare_recentDHayesMods to generate a new BatchLogId and calculate respective date values
			;
			EXEC precalc.uspFinancialKCPBatchRunValuePrepare_recentDHayesMods @psiBatchLogId = @siBatchLogId OUTPUT
			;
		END


	DECLARE @MinCustomerId BIGINT;
	DECLARE @MaxCustomerId BIGINT;
	DECLARE @WorkingCustomerIdBegin BIGINT;
	DECLARE @WorkingCustomerIdEnd BIGINT;

	SELECT 
		 @MinCustomerId = 54770316 -- actual oldest CustomerId for PNC *****************************************************
		,@MaxCustomerId = MAX( c.customerId ) 
	FROM ValidFI.dbo.Customer c 
	WHERE c.BankId = 100003
	;
	SELECT 
		 @WorkingCustomerIdBegin = @MinCustomerId
		,@WorkingCustomerIdEnd = @MaxCustomerId
	;

	--Grab the KeyTemplate for Financial KCP: 
	--'[{ClientOrgIdKeyTypeId}]|[{ClientOrgId}]|[{IdTypeKeyTypeId}]|[{IdTypeId}]|[{CustomerNumberKeyTypeId}]|[{CustomerNumber}]|[{RoutingNumberKeyTypeId}]|[{RoutingNumber}]|[{AccountNumberKeyTypeId}]|[{AccountNumber}]';
	-- [{FinancialClientOrgIdKeyTypeId}]|[{FinancialClientOrgId}]|[{FinancialCustomerIdTypeIdKeyTypeId}]|[{FinancialCustomerIdTypeId}]|[{FinancialCustomerNumberKeyTypeId}]|[{FinancialCustomerNumber}]|[{FinancialRoutingNumberKeyTypeId}]|[{FinancialRoutingNumber}]|[{FinancialAccountNumberKeyTypeId}]|[{FinancialAccountNumber}]
	-- 20|[{FinancialClientOrgId}]|18|25|19|[{FinancialCustomerNumber}]|14|[{FinancialRoutingNumber}]|15|[{FinancialAccountNumber}]
	SELECT @nvKeyTemplate = KeyTemplate 
	FROM [stat].[KeyType]
	WHERE KeyTypeId = @siFinancialKCPKeyTypeId;

	SET @nvWorker = REPLACE( @nvKeyTemplate, '[{FinancialClientOrgIdKeyTypeId}]', CONVERT(nvarchar(6),@siClientOrgIdKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{FinancialClientOrgId}]', CONVERT(nvarchar(50),@iOrgId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{FinancialCustomerIdTypeIdKeyTypeId}]', CONVERT(nvarchar(6),@siIdTypeKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{FinancialCustomerIdTypeId}]', CONVERT(nvarchar(50), @iCustomerNumberIdTypeId ) );
	SET @nvWorker = REPLACE( @nvWorker, '[{FinancialCustomerNumberKeyTypeId}]', CONVERT(nvarchar(6),@siCustomerNumberKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{FinancialRoutingNumberKeyTypeId}]', CONVERT(nvarchar(6),@siRoutingNumberKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{FinancialAccountNumberKeyTypeId}]', CONVERT(nvarchar(6),@siAccountNumberKeyTypeId) );


	SELECT @dtToday = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Today', @siBatchLogId ) AS p;
	SELECT @dtClearedCycleDate = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'ClearedCycleDate', @siBatchLogId ) AS p;
	SELECT @dtLastSuccessfulCycleDateRefresh = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'LastSuccessfulCycleDateRefresh', @siBatchLogId ) AS p;
	SELECT @dtIncrementalEndCycleDate = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'IncrementalEndCycleDate', @siBatchLogId ) AS p;
	SELECT @dtPriorCycleDate = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'PriorCycleDate', @siBatchLogId ) AS p;
	SELECT @dtGetTranChangesBegin = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetTranChangesBegin', @siBatchLogId ) AS p;
	SELECT @dtGetTranChangesEnd = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetTranChangesEnd', @siBatchLogId ) AS p;
	SELECT @dtGetRetChangesBegin = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetRetChangesBegin', @siBatchLogId ) AS p;
	SELECT @dtGetRetChangesEnd = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetRetChangesEnd', @siBatchLogId ) AS p;
	SELECT @dtGetAcctCustChangesBegin = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetAcctCustChangesBegin', @siBatchLogId ) AS p;
	SELECT @dtGetAcctCustChangesEnd = p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetAcctCustChangesEnd', @siBatchLogId ) AS p;

	----Setting the ClearedDay5 value
	--SELECT @dtClearedDay5Parameter = ParameterValue 
	--FROM [precalc].[ufnParameterDate]( 'ClearedDay5', default )
	--;
	--SET @dtClearedDay5Back90 = DATEADD( dd, -90, @dtClearedDay5Parameter )
	--;

	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
		,@nvMessageText = N''
	;
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Running with           @siBatchLogId = ' + CONVERT( nvarchar(10), @siBatchLogId )
	PRINT N''
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                            @dtToday = ' + CONVERT( nvarchar(10), @dtToday )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'          @dtIncrementalEndCycleDate = ' + CONVERT( nvarchar(10), @dtIncrementalEndCycleDate ) + N' (aka CurrentCycleDate)'
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                   @dtPriorCycleDate = ' + CONVERT( nvarchar(10), @dtPriorCycleDate )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'   @dtLastSuccessfulCycleDateRefresh = ' + CONVERT( nvarchar(10), @dtLastSuccessfulCycleDateRefresh )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                 @dtClearedCycleDate = ' + CONVERT( nvarchar(10), @dtClearedCycleDate )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'              @dtGetTranChangesBegin = ' + CONVERT( nvarchar(10), @dtGetTranChangesBegin )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                @dtGetTranChangesEnd = ' + CONVERT( nvarchar(10), @dtGetTranChangesEnd )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'               @dtGetRetChangesBegin = ' + CONVERT( nvarchar(10), @dtGetRetChangesBegin )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                 @dtGetRetChangesEnd = ' + CONVERT( nvarchar(10), @dtGetRetChangesEnd )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'          @dtGetAcctCustChangesBegin = ' + CONVERT( nvarchar(10), @dtGetAcctCustChangesBegin )
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'            @dtGetAcctCustChangesEnd = ' + CONVERT( nvarchar(10), @dtGetAcctCustChangesEnd )
	PRINT N''
	PRINT NCHAR(009) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'        Preprocessed template string = ' + @nvWorker
	RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 

	
	BEGIN

		BEGIN TRY

			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Grab Customer/Account list...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'CustTrxnRetList...'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();


				--IF ( OBJECT_ID('tempdb..#CustTrxnRetList', 'U') IS NOT NULL ) DROP TABLE #CustTrxnRetList;
				--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList;
				TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList;
				SET NOCOUNT OFF;
				INSERT INTO precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList ( CustomerId, ObtainedFrom0Tran1Ret2Both )
				SELECT
					 x.CustomerId
					,MAX( x.ObtainedFrom0Tran1Ret2Both ) AS ObtainedFrom0Tran1Ret2Both
				FROM
					(
					SELECT 
						 a.CustomerId
						,CASE 
							WHEN ( t.CycleDate BETWEEN @dtGetTranChangesBegin AND @dtGetTranChangesEnd AND t.TransactionTypeId = 2 AND a.CustomerId > @MinCustomerId ) AND ( t.ReturnDate >= @dtGetRetChangesBegin AND t.ReturnStatusId <> 0 AND t.TransactionTypeId = 2 AND a.CustomerId > @MinCustomerId ) 
								THEN CONVERT( tinyint, 2 ) -- both
							WHEN ( t.CycleDate BETWEEN @dtGetTranChangesBegin AND @dtGetTranChangesEnd AND t.TransactionTypeId = 2 AND a.CustomerId > @MinCustomerId ) 
								THEN CONVERT( tinyint, 0 ) -- TranChanges
							WHEN ( t.ReturnDate >= @dtGetRetChangesBegin AND t.ReturnStatusId <> 0 AND t.TransactionTypeId = 2 AND a.CustomerId > @MinCustomerId ) 
								THEN CONVERT( tinyint, 1 ) -- RetChanges
							ELSE CONVERT( tinyint, 9 ) -- if we hit an else, then something went seriously wrong
						 END AS ObtainedFrom0Tran1Ret2Both
					--INTO #CustTrxnRetList
					--INTO precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList
					FROM ValidFI.dbo.[Transaction] t
						INNER JOIN ValidFI.dbo.Account a 
							ON t.AccountId = a.AccountId
					WHERE t.BankId = 100003
						AND
						(
							( 
								t.CycleDate BETWEEN @dtGetTranChangesBegin AND @dtGetTranChangesEnd
								AND t.TransactionTypeId = 2 
								AND a.CustomerId > @MinCustomerId
							)
							OR
							(
								t.ReturnDate >= @dtGetRetChangesBegin 
								AND t.ReturnStatusId <> 0
								AND t.TransactionTypeId = 2 
								AND a.CustomerId > @MinCustomerId
							)
						)
					) AS x
				GROUP BY
					x.CustomerId
				;
				SET NOCOUNT ON;


				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...CustTrxnRetList'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();





				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'CustListChanges...'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();


				--IF ( OBJECT_ID('tempdb..#CustListChanges', 'U') IS NOT NULL ) DROP TABLE #CustListChanges;
				--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_2_CustListChanges', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_2_CustListChanges;
				TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_2_CustListChanges;
				ALTER INDEX ALL ON precalc.FinancialKCPClearedCheckNumber_2_CustListChanges REBUILD; -- 2019-05-30 LSW
				SET NOCOUNT OFF;
				INSERT INTO precalc.FinancialKCPClearedCheckNumber_2_CustListChanges ( CustomerId, ObtainedFrom0AccountDetailHistory1Account )
					SELECT 
					DISTINCT 
						ah.CustomerId
						,CONVERT( tinyint, 0 ) AS ObtainedFrom0AccountDetailHistory1Account
					FROM ValidFI.dbo.Account a
					INNER JOIN ValidFI.dbo.AccountDetailHistory ah
						ON a.AccountId = ah.AccountId
							AND ah.CycleDate >= @dtGetAcctCustChangesBegin
					WHERE a.BankId = 100003
						AND ah.BankId = 100003
						AND ah.CustomerId <> a.CustomerId
						AND ah.CustomerId > @MinCustomerId
				;
				INSERT INTO precalc.FinancialKCPClearedCheckNumber_2_CustListChanges ( CustomerId, ObtainedFrom0AccountDetailHistory1Account )
					SELECT 
					DISTINCT 
						a.CustomerId
						,CONVERT( tinyint, 1 ) AS ObtainedFrom0AccountDetailHistory1Account
					FROM ValidFI.dbo.Account a
					INNER JOIN ValidFI.dbo.AccountDetailHistory ah
						ON a.AccountId = ah.AccountId
							AND ah.CycleDate >= @dtGetAcctCustChangesBegin
					WHERE a.BankId = 100003
						AND ah.BankId = 100003
						AND ah.CustomerId <> a.CustomerId
						AND ah.CustomerId > @MinCustomerId
						AND NOT EXISTS
							(
								SELECT 'X'
								FROM precalc.FinancialKCPClearedCheckNumber_2_CustListChanges AS x
								WHERE x.CustomerId = a.CustomerId
							)
				;
				/*
				SELECT DISTINCT 
					CustLists.CustomerId
				--INTO #CustListChanges
				--INTO precalc.FinancialKCPClearedCheckNumber_2_CustListChanges
				FROM (
					SELECT 
					DISTINCT 
						ah.CustomerId
						,CONVERT( tinyint, 0 ) AS ObtainedFrom0AccountDetailHistory1Account
					FROM ValidFI.dbo.Account a
					INNER JOIN ValidFI.dbo.AccountDetailHistory ah
						ON a.AccountId = ah.AccountId
							AND ah.CycleDate >= @dtGetAcctCustChangesBegin
					WHERE a.BankId = 100003
						AND ah.BankId = 100003
						AND ah.CustomerId <> a.CustomerId
						AND ah.CustomerId > @MinCustomerId
	
					UNION
	
					SELECT 
					DISTINCT 
						a.CustomerId
					FROM ValidFI.dbo.Account a
					INNER JOIN ValidFI.dbo.AccountDetailHistory ah
						ON a.AccountId = ah.AccountId
							AND ah.CycleDate >= @dtGetAcctCustChangesBegin
					WHERE a.BankId = 100003
						AND ah.BankId = 100003
						AND ah.CustomerId <> a.CustomerId
						AND ah.CustomerId > @MinCustomerId
					) CustLists
				;
				*/
				SET NOCOUNT ON;


				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...CustListChanges'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();




				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'CustFullList...'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();

				--IF ( OBJECT_ID('tempdb..#CustFullList', 'U') IS NOT NULL ) DROP TABLE #CustFullList;
				--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_3_CustFullList', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_3_CustFullList;
				TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_3_CustFullList;
				ALTER INDEX ALL ON precalc.FinancialKCPClearedCheckNumber_3_CustFullList REBUILD; -- 2019-05-30 LSW
				SET NOCOUNT OFF;
				INSERT INTO  precalc.FinancialKCPClearedCheckNumber_3_CustFullList ( CustomerId, ObtainedFrom0CustListChanges1CustTrxnRetList )
					SELECT c.CustomerId
						,CONVERT( tinyint, 0 ) AS ObtainedFrom0CustListChanges1CustTrxnRetList
					--FROM #CustListChanges c
					FROM precalc.FinancialKCPClearedCheckNumber_2_CustListChanges c
				;
				INSERT INTO  precalc.FinancialKCPClearedCheckNumber_3_CustFullList ( CustomerId, ObtainedFrom0CustListChanges1CustTrxnRetList )
					SELECT DISTINCT c2.CustomerId --2018-11-20
						,CONVERT( tinyint, 1 ) AS ObtainedFrom0CustListChanges1CustTrxnRetList
					--FROM #CustTrxnRetList c2
					FROM precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList c2
					WHERE NOT EXISTS
						(
							SELECT 'X' 
							FROM precalc.FinancialKCPClearedCheckNumber_3_CustFullList AS x
							WHERE x.CustomerId = c2.CustomerId
						)
				;	
				/*
				SELECT 
				DISTINCT 
					 u.CustomerId
					,u.ObtainedFrom0CustListChanges1CustTrxnRetList
				--INTO #CustFullList
				--INTO precalc.FinancialKCPClearedCheckNumber_3_CustFullList
				FROM (
					SELECT c.CustomerId
						,CONVERT( tinyint, 0 ) AS ObtainedFrom0CustListChanges1CustTrxnRetList
					--FROM #CustListChanges c
					FROM precalc.FinancialKCPClearedCheckNumber_2_CustListChanges c
	
					UNION
	
					SELECT c2.CustomerId
						,CONVERT( tinyint, 1 ) AS ObtainedFrom0CustListChanges1CustTrxnRetList
					--FROM #CustTrxnRetList c2
					FROM precalc.FinancialKCPClearedCheckNumber_1_CustTrxnRetList c2
					) u
				; -- WHERE u.CustomerId > 0;
				*/
				SET NOCOUNT ON;


				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...CustFullList'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();




				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'AcctCustFullList...'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();


				--IF ( OBJECT_ID('tempdb..#AcctCustFullList', 'U') IS NOT NULL ) DROP TABLE #AcctCustFullList;
				--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_4_AcctCustFullList', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_4_AcctCustFullList;
				TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_4_AcctCustFullList;
				SET NOCOUNT OFF;
				INSERT INTO precalc.FinancialKCPClearedCheckNumber_4_AcctCustFullList ( AccountId, CustomerId, ObtainedFrom0CustListChanges1CustTrxnRetList )
				SELECT acc.AccountId
					,acc.CustomerId
					,t.ObtainedFrom0CustListChanges1CustTrxnRetList
				--INTO #AcctCustFullList
				--INTO precalc.FinancialKCPClearedCheckNumber_4_AcctCustFullList
				--FROM #CustFullList t
				FROM precalc.FinancialKCPClearedCheckNumber_3_CustFullList t
				INNER JOIN ValidFI.dbo.Account acc
					ON t.CustomerId = acc.CustomerId
				;
				SET NOCOUNT ON;


				SELECT
					 @dt2TimeStamp = SYSDATETIME()
					,@nvMessageText = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...AcctCustFullList'
				;
				RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @dtTimerDate = SYSDATETIME();


			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'...Customer/Account list grabbed.'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 




			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Calculate Min/Max...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 

			--IF ( OBJECT_ID('tempdb..#KCPMinMaxCheckNumberCleared', 'U') IS NOT NULL ) DROP TABLE #KCPMinMaxCheckNumberCleared;
			--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_5_KCPMinMaxCheckNumberCleared', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_5_KCPMinMaxCheckNumberCleared;
			TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_5_KCPMinMaxCheckNumberCleared;
			SET NOCOUNT OFF;
			INSERT INTO precalc.FinancialKCPClearedCheckNumber_5_KCPMinMaxCheckNumberCleared ( CustomerId, PayerId, KCPMinCheckNumberCleared, KCPMaxCheckNumberCleared )
			SELECT
				 ca.CustomerId
				,t.PayerId

				--,MIN(t.CheckNumber) AS [KCPMinCheckNumberCleared] -- 2018/11/06 LSW Old and busted
				--,MAX(t.CheckNumber) AS [KCPMaxCheckNumberCleared] -- 2018/11/06 LSW Old and busted
				,CONVERT( bigint, MIN( CASE WHEN t.ReturnStatusId = 0 THEN t.CheckNumber ELSE NULL END ) ) AS [KCPMinCheckNumberCleared] -- 2018/11/06 LSW New hotness
				,CONVERT( bigint, MAX( CASE WHEN t.ReturnStatusId = 0 THEN t.CheckNumber ELSE NULL END ) ) AS [KCPMaxCheckNumberCleared] -- 2018/11/06 LSW New hotness

			--INTO #KCPMinMaxCheckNumberCleared
			--INTO precalc.FinancialKCPClearedCheckNumber_5_KCPMinMaxCheckNumberCleared
			--FROM #AcctCustFullList ca
			FROM precalc.FinancialKCPClearedCheckNumber_4_AcctCustFullList ca
			INNER JOIN ValidFI.dbo.[Transaction] t
				ON ca.AccountId = t.AccountId
			WHERE t.CycleDate <= @dtClearedCycleDate
				AND t.transactionTypeId = 2
				AND t.CheckNumber > 10
				--AND t.ReturnStatusId = 0 -- 2018/11/06 LSW Old n busted.  Moved this filter into a CASE inside the 2 aggregates, per Diana. 
			GROUP BY ca.CustomerId
				,t.PayerId
			ORDER BY
				 ca.CustomerId
				,t.PayerId
			;
			SET NOCOUNT ON;
			--ALTER TABLE #KCPMinMaxCheckNumberCleared ADD PRIMARY KEY CLUSTERED ( PartitionId, KeyElementId )
			;

		
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'...Min/Max calculated.'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'KeyElement management begin...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...preparing #KCP...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			--IF ( OBJECT_ID('tempdb..#KCP', 'U') IS NOT NULL ) DROP TABLE #KCP;
			--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_6_KCP', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_6_KCP;
			TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_6_KCP;
			SET NOCOUNT OFF;
			INSERT INTO precalc.FinancialKCPClearedCheckNumber_6_KCP ( CustomerId, PayerId, KCPMinCheckNumberCleared, KCPMaxCheckNumberCleared, CustomerNumber, RoutingNumber, AccountNumber, IsExistingKeyElement, PartitionId, KeyElementId, HashId )
			SELECT
				 kcp.CustomerId
				,kcp.PayerId
				,kcp.KCPMinCheckNumberCleared
				,kcp.KCPMaxCheckNumberCleared
				,c.CustomerNumber
				,p.RoutingNumber
				,p.AccountNumber
				,CONVERT( bit, CASE WHEN ke.PartitionId IS NULL THEN 0 ELSE 1 END ) AS IsExistingKeyElement -- 0 = KeyElement does not exist at this moment, 1 = KeyElement already exists.
				,ke.PartitionId
				,ke.KeyElementId
				,ke.HashId
			--INTO #KCP
			--INTO precalc.FinancialKCPClearedCheckNumber_6_KCP
			--FROM #KCPMinMaxCheckNumberCleared AS kcp
			FROM precalc.FinancialKCPClearedCheckNumber_5_KCPMinMaxCheckNumberCleared AS kcp
				INNER JOIN ValidFI.dbo.Customer c
					ON kcp.CustomerId = c.CustomerId
				INNER JOIN ValidFI.dbo.Payer p
					ON kcp.PayerId = p.PayerId
				LEFT JOIN [stat].[KeyElement] ke
					ON CONVERT(binary(64)
						,HASHBYTES( N'SHA2_512'
							,CONVERT(varbinary(512)
							,CONVERT(nvarchar(512)
								,REPLACE( REPLACE( REPLACE( @nvWorker, N'[{FinancialCustomerNumber}]', CONVERT( nvarchar(50), c.CustomerNumber ) ) 
									,N'[{FinancialRoutingNumber}]', CONVERT( nvarchar(50), p.RoutingNumber ) )
										,N'[{FinancialAccountNumber}]', CONVERT( nvarchar(50), p.AccountNumber ) )						
								))),1) = ke.HashId
			;
			SET NOCOUNT ON;

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...preparing #NewKeyElement...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			--IF ( OBJECT_ID('tempdb..#NewKeyElement', 'U') IS NOT NULL ) DROP TABLE #NewKeyElement;
			--IF ( OBJECT_ID('precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement', 'U') IS NOT NULL ) DROP TABLE precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement;
			TRUNCATE TABLE precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement;
			SET NOCOUNT OFF;
			INSERT INTO precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement ( PartitionId, KeyElementId, HashId, CustomerId, PayerId, IsExistingKeyElement )
			SELECT
				 --CONVERT( bigint, NULL ) AS PartitionId
				 kcp.PartitionId
				,NEXT VALUE FOR [stat].[seqKeyElement] OVER ( ORDER BY kcp.CustomerId, kcp.PayerId )AS KeyElementId 
				,CONVERT(binary(64)
						,HASHBYTES( N'SHA2_512'
							,CONVERT(varbinary(512)
							,CONVERT(nvarchar(512)
								,REPLACE( REPLACE( REPLACE( @nvWorker, N'[{FinancialCustomerNumber}]', CONVERT( nvarchar(50), kcp.CustomerNumber ) ) 
									,N'[{FinancialRoutingNumber}]', CONVERT( nvarchar(50), kcp.RoutingNumber ) )
										,N'[{FinancialAccountNumber}]', CONVERT( nvarchar(50), kcp.AccountNumber ) )						
								))),1) AS HashId
				,kcp.CustomerId
				,kcp.PayerId
				,kcp.IsExistingKeyElement
			--INTO #NewKeyElement
			--INTO precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement
			--FROM #KCP AS kcp
			FROM precalc.FinancialKCPClearedCheckNumber_6_KCP AS kcp
			WHERE kcp.IsExistingKeyElement = 0 -- 0 = KeyElement does not exist at this moment, 1 = KeyElement already exists.
			;
			INSERT INTO precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement ( PartitionId, KeyElementId, HashId, CustomerId, PayerId, IsExistingKeyElement )
			SELECT
				 --CONVERT( bigint, NULL ) AS PartitionId
				 kcp.PartitionId
				,kcp.KeyElementId 
				,kcp.HashId
				,kcp.CustomerId
				,kcp.PayerId
				,kcp.IsExistingKeyElement
			--INTO #NewKeyElement
			--INTO precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement
			--FROM #KCP AS kcp
			FROM precalc.FinancialKCPClearedCheckNumber_6_KCP AS kcp
			WHERE kcp.IsExistingKeyElement = 1 AND NOT EXISTS
				(
					SELECT 'X'
					FROM kegen.KCP_PNC_SurrogateKeyXref AS x
					WHERE kcp.CustomerId = x.CustomerId
						AND kcp.PayerId = x.PayerId
				)
			;
			SET NOCOUNT ON;

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...adjusting #NewKeyElement...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			SET NOCOUNT OFF;
			--UPDATE #NewKeyElement SET PartitionId = [stat].[ufnKeyElementId256Modulus]( KeyElementId ) WHERE PartitionId IS NULL
			UPDATE precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement SET PartitionId = [stat].[ufnKeyElementId256Modulus]( KeyElementId ) WHERE PartitionId IS NULL
			;
			SET NOCOUNT ON;

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...inserting stat.KeyElement from #NewKeyElement...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			SET NOCOUNT OFF;
			--Insert into stat.KeyElement
			INSERT INTO [stat].[KeyElement] ( 
				 PartitionId
				,KeyElementId
				,HashId
				,BatchLogId
			)
			SELECT 
				 nke.PartitionId
				,nke.KeyElementId
				,nke.HashId
				,@siBatchLogId AS BatchLogId
			--FROM #NewKeyElement nke
			FROM precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement nke
			--WHERE NOT EXISTS ( SELECT 'X' FROM [stat].[KeyElement] AS x WHERE nke.HashId = x.HashId )
			WHERE NOT EXISTS ( SELECT 'X' FROM [stat].[KeyElement] AS x WHERE nke.KeyElementId = x.KeyElementId )
			ORDER BY nke.KeyElementId
			;
			SET NOCOUNT ON;

			
			INSERT INTO kegen.KCP_PNC_SurrogateKeyXref
				(
					 CustomerId
					,PayerId
					,KeyElementId
					,PartitionId
					,BatchLogId
				)
			SELECT 
				 k.CustomerId
				,k.PayerId
				,k.KeyElementId
				,k.PartitionId
				,@siBatchLogId AS BatchLogId
			--FROM #NewKeyElement AS k
			FROM precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement AS k
			WHERE k.CustomerId > 0
				AND k.PayerId > 0
				AND NOT EXISTS
					(
						SELECT 'X'
						FROM kegen.KCP_PNC_SurrogateKeyXref AS x
						WHERE k.CustomerId = x.CustomerId
							AND k.PayerId = x.PayerId
					)
			ORDER BY
				 k.CustomerId
				,k.PayerId
			;

			
			INSERT INTO kegen.HashKCP
				(
					 ValidFI_CustomerId
					,ValidFI_PayerId
					,PartitionId
					,KeyElementId
					,BatchLogId
					,FinancialCustomerNumber
					,FinancialRoutingNumber
					,FinancialAccountNumber
					,HashId
					,HashString
				)
			SELECT 
				 k.CustomerId AS ValidFI_CustomerId
				,k.PayerId AS ValidFI_PayerId
				,k.PartitionId
				,k.KeyElementId
				,@siBatchLogId AS BatchLogId
				,k6.CustomerNumber AS FinancialCustomerNumber
				,k6.RoutingNumber AS FinancialRoutingNumber
				,k6.AccountNumber AS FinancialAccountNumber
				,k.HashId
				--2019-01-18 
				--,CONVERT( binary(64) -- new Hash method
				--	,HASHBYTES( N'SHA2_512'
				--		,CONVERT( varbinary(512)
				--			,CONVERT( nvarchar(512)
				--				,REPLACE( REPLACE( REPLACE( @nvWorker, N'[{FinancialCustomerNumber}]', CONVERT( nvarchar(50), k6.CustomerNumber ) )
				--					,N'[{FinancialRoutingNumber}]', CONVERT( nvarchar(50), k6.RoutingNumber ) )
				--						,N'[{FinancialAccountNumber}]', CONVERT( nvarchar(50), k6.AccountNumber ) )
				--		))),1) AS HashString
				,CONVERT( nvarchar(512)
							,REPLACE( REPLACE( REPLACE( @nvWorker, N'[{FinancialCustomerNumber}]', CONVERT( nvarchar(50), k6.CustomerNumber ) )
								,N'[{FinancialRoutingNumber}]', CONVERT( nvarchar(50), k6.RoutingNumber ) )
									,N'[{FinancialAccountNumber}]', CONVERT( nvarchar(50), k6.AccountNumber ) )
					) AS HashString
				--2019-01-18 
			--FROM #NewKeyElement AS k
			FROM precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement AS k
				INNER JOIN [AtomicStat].[precalc].[FinancialKCPClearedCheckNumber_6_KCP] k6 
					ON k.KeyElementId = k6.KeyElementId
			WHERE k.CustomerId > 0
				AND k.PayerId > 0
				AND NOT EXISTS
					(
						SELECT 'X'
						FROM kegen.HashKCP AS x
						WHERE k.CustomerId = x.ValidFI_CustomerId
							AND k.PayerId = x.ValidFI_PayerId
					)
			ORDER BY
				 k.CustomerId
				,k.PayerId
			;

/*
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...inserting precalc.KCPKeyElement from #NewKeyElement... (not 100% necessary - adding BatchLogId will remove any need for this here)'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			--Insert into precalc.KCPKeyElement
			INSERT INTO [precalc].[KCPKeyElement] ( 
				 CustomerId
				,PayerId
				,KeyElementId
				,PartitionId
				,IsNewKeyElement
			)
			SELECT 
				 nke.CustomerId
				,nke.PayerId
				,nke.KeyElementId
				,nke.PartitionId
				,1 AS IsNewKeyElement
			FROM #NewKeyElement nke
			WHERE NOT EXISTS ( SELECT 'X' FROM [precalc].[KCPKeyElement] AS x WHERE nke.KeyElementId = x.KeyElementId )
			ORDER BY nke.KeyElementId
			;
*/

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(4) + N'...adjusting #KCP with new KeyElementId values from #NewKeyElement...'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			SET NOCOUNT OFF;
			UPDATE kcp -- update the KCP temp table with the now recently added KeyElements
				SET 
					 kcp.PartitionId = nke.PartitionId
					,kcp.KeyElementId = nke.KeyElementId
					,kcp.HashId = nke.HashId
			--FROM #KCP AS kcp 
			FROM precalc.FinancialKCPClearedCheckNumber_6_KCP AS kcp
				--INNER JOIN #NewKeyElement AS nke 
				INNER JOIN precalc.FinancialKCPClearedCheckNumber_7_NewKeyElement AS nke
					ON kcp.CustomerId = nke.CustomerId 
						AND kcp.PayerId = nke.PayerId
			WHERE kcp.IsExistingKeyElement = 0 -- 0 = KeyElement did not exist at the moment, 1 = KeyElement already existed.
			;
			SET NOCOUNT ON;

			
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'...end KeyElement management.'
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			--SELECT @biMissingKeyElementCount = COUNT_BIG(1) FROM #KCP WHERE KeyElementId IS NULL
			SELECT @biMissingKeyElementCount = COUNT_BIG(1) FROM precalc.FinancialKCPClearedCheckNumber_6_KCP WHERE KeyElementId IS NULL
			;
			IF @biMissingKeyElementCount > 0
				BEGIN
					SELECT
						 @dt2TimeStamp = SYSDATETIME()
						,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
						,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Missing ' + CONVERT( nvarchar(10), @biMissingKeyElementCount ) + N' KeyElementId entries in stat.KeyElement. <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
					;
					RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
				END


			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Updating Min/Max for customers'  -- + CONVERT( nvarchar(10), @WorkingCustomerIdBegin ) + N' -> ' + CONVERT( nvarchar(10), @WorkingCustomerIdEnd )
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			SET NOCOUNT OFF;
			-- making one pass for both the Min and the Max...
			UPDATE s
				SET s.StatValue = TRY_CONVERT( nchar(50), CASE s.StatId WHEN 161 THEN c.KCPMinCheckNumberCleared WHEN 157 THEN c.KCPMaxCheckNumberCleared END )
					,s.BatchLogId = @siBatchLogId
				--FROM #KCP c
				FROM precalc.FinancialKCPClearedCheckNumber_6_KCP c
					INNER JOIN [stat].[StatTypeNchar50] s
						ON s.PartitionId = c.PartitionId
						AND s.KeyElementId = c.KeyElementId
						AND s.StatId IN( 161, 157 )
				WHERE ISNULL( s.StatValue, -1 ) <> ISNULL( CASE s.StatId WHEN 161 THEN c.KCPMinCheckNumberCleared WHEN 157 THEN c.KCPMaxCheckNumberCleared END, -1 )
				--WHERE ( s.StatId = 161 AND s.StatValue > c.KCPMinCheckNumberCleared )
				--	OR ( s.StatId = 157 AND s.StatValue < c.KCPMaxCheckNumberCleared )
			;
			SET NOCOUNT ON;

		--/ *
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Inserting Min for customers'  -- + CONVERT( nvarchar(10), @WorkingCustomerIdBegin ) + N' -> ' + CONVERT( nvarchar(10), @WorkingCustomerIdEnd )
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			SET NOCOUNT OFF;
			INSERT INTO [stat].[StatTypeNchar50]
				(
					 PartitionId
					,KeyElementId
					,StatId
					,StatValue
					,BatchLogId
				)
			SELECT
				 c.PartitionId
				,c.KeyElementId
				,161 AS StatId
				,c.KCPMinCheckNumberCleared AS StatValue
				,@siBatchLogId AS BatchLogId
			--FROM #KCP c
			FROM precalc.FinancialKCPClearedCheckNumber_6_KCP c
			WHERE c.KCPMinCheckNumberCleared IS NOT NULL
				AND c.KeyElementId IS NOT NULL
				AND NOT EXISTS
					(
						SELECT 'X'
						FROM [stat].[StatTypeNchar50] x
						WHERE x.PartitionId = c.PartitionId
							AND x.KeyElementId = c.KeyElementId
							AND x.StatId = 161
					)
			;
			SET NOCOUNT ON;


			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Inserting Max for customers'  -- + CONVERT( nvarchar(10), @WorkingCustomerIdBegin ) + N' -> ' + CONVERT( nvarchar(10), @WorkingCustomerIdEnd )
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			SET NOCOUNT OFF;
			INSERT INTO [stat].[StatTypeNchar50]
				(
					 PartitionId
					,KeyElementId
					,StatId
					,StatValue
					,BatchLogId
				)
			SELECT
				 c.PartitionId
				,c.KeyElementId
				,157 AS StatId
				,c.KCPMaxCheckNumberCleared AS StatValue
				,@siBatchLogId AS BatchLogId
			--FROM #KCP c
			FROM precalc.FinancialKCPClearedCheckNumber_6_KCP c
			WHERE c.KCPMaxCheckNumberCleared IS NOT NULL
				AND c.KeyElementId IS NOT NULL
				AND NOT EXISTS
					(
						SELECT 'X'
						FROM [stat].[StatTypeNchar50] x
						WHERE x.PartitionId = c.PartitionId
							AND x.KeyElementId = c.KeyElementId
							AND x.StatId = 157
					)
			;
			SET NOCOUNT OFF;

	
			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Updating kegen.HashKCP'  -- + CONVERT( nvarchar(10), @WorkingCustomerIdBegin ) + N' -> ' + CONVERT( nvarchar(10), @WorkingCustomerIdEnd )
			;
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 


			--2019-01-18 
			-- Update  the ones that already exist kegen.HashKCP, lazy but it works
			UPDATE hkcp
				SET BatchLogId = @siBatchLogId
			FROM [kegen].[HashKCP] hkcp
			INNER JOIN  [stat].[StatTypeNchar50] s on hkcp.partitionId = s.PartitionId
													AND hkcp.KeyElementId = s.KeyElementId
			WHERE s.BatchLogId = @siBatchLogId
				AND hkcp.BatchLogId <>  @siBatchLogId;

			SELECT
				 @dt2TimeStamp = SYSDATETIME()
				,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
				,@nvMessageText = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Updated kegen.HashKCP'
			RAISERROR (	@nvMessageText, 0, 1 ) WITH NOWAIT; 
			--2019-01-18 

		END TRY
		BEGIN CATCH
			EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId = @iErrorDetailId OUTPUT;
			SET @iErrorDetailId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
			THROW;
		END CATCH	
			
		SET @iPageNumber += 1;
	
		WAITFOR DELAY '00:00:00.1';

		CHECKPOINT;			
	END

	SET @nvMessageText = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + @nvThisSProcName + SPACE(1) + N'...end.'; 
	PRINT N''
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.
	PRINT N''
	
	WAITFOR DELAY '00:00:03';
*/

END
;

GO
