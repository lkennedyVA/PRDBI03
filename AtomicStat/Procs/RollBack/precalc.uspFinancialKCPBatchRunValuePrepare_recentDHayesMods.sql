USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [precalc].[uspFinancialKCPBatchRunValuePrepare_recentDHayesMods]
	(
		 @psiBatchLogId smallint = NULL OUTPUT -- if NULL, then grab the last BatchLogId from precalc.BatchRunValue
													 -- ; if greater than 0, then check the specified BatchLogId associated rows
													 -- ; if less than 0, calculate the dates and insert them and retrieve the assigned BatchLogId.
	)
AS
BEGIN
SET NOCOUNT ON;

DECLARE 
	 @nvThisSProcName sysname = ( ISNULL( QUOTENAME( OBJECT_SCHEMA_NAME( @@PROCID ) ) + N'.', N'' ) + ISNULL( QUOTENAME( OBJECT_NAME( @@PROCID ) ), N'' ) )
	,@siBatchLogId smallint = @psiBatchLogId
;
IF ISNULL( OBJECT_NAME( @@PROCID ), N'' ) > N'' -- if OBJECT_NAME() is null, we are running a script, so ignore this block of batch Id logic
	BEGIN
		IF @siBatchLogId IS NOT NULL 
			AND @siBatchLogId < 1
			BEGIN 
				SET @siBatchLogId = NULL
				;
				EXEC [stat].[uspBatchLogUpsertOut] @psiBatchLogId = @siBatchLogId OUTPUT, @piOrgId = 100009
				;
			END
		ELSE IF @siBatchLogId IS NULL 
			SET @siBatchLogId = ISNULL( @siBatchLogId, ( ISNULL( ( SELECT MAX( x.BatchLogId ) FROM precalc.BatchRunValue AS x WHERE x.BatchLogId > 0 ), 0 ) ) )
	END
;

---- parameters for when this is a sproc
--DECLARE 
--	 @pdtIncrementalLowCycleDate date = '2018/10/15' -- '2016/01/01' -- GETDATE()-366 -- NULL
--	,@pdtIncrementalHighCycleDate date = '2018/10/15' -- '2018/10/13' -- GETDATE()-1 -- NULL


DECLARE
	 @dtTimerDate datetime2(7) = SYSDATETIME()
	,@dtInitial datetime2(7) = SYSDATETIME()
	,@nvMessage nvarchar(4000)
	,@nvSection nvarchar(4000)
	,@dt2TimeStamp datetime2(7)
	,@iEllapsedTimeInSeconds int
;
DECLARE @tblTiming table 
	(
		 RowId int IDENTITY(1, 1)
		,Section nvarchar(100)
		,[TimeStamp] datetime2(7)
		,EllapsedTimeInSeconds int
	)
;


SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + @nvThisSProcName + N' Begin...'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME();

--PRINT 'initial parameter values'
;

DECLARE 
	 @dtToday date = GETDATE() -- used for Calander driven dates vs Cycle Date driven dates
	--,@dtIncrementalStartCycleDate date = ISNULL( @pdtIncrementalLowCycleDate, ISNULL( ( SELECT MAX( CycleDate ) FROM [ValidFI].[PNC].[Cycle] ) , GETDATE()-1 ) ) -- '2018-10-12' -- '2001-01-01' -- '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
	-- Remove the @siBatchLogId cheat assignment before going live!
	-- This needs to be driven, in post development, by the mechanism Chris already has in place.
	--,@siBatchLogId smallint = ISNULL( @psiBatchLogId, ( ISNULL( ( /*cheating for the time being*/ SELECT MAX( x.BatchLogId ) FROM precalc.Parameter AS x WHERE x.BatchLogId > 0 ), 0 ) + 1 ) )
--DECLARE 
--	 @dtIncrementalEndCycleDate date = ISNULL( @pdtIncrementalHighCycleDate, @dtIncrementalStartCycleDate ) -- '2018-10-12' -- '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
;	
--SELECT
--	 @dtToday AS Today
--	,@dtIncrementalStartCycleDate AS IncrementalStartCycleDate
--	,@dtIncrementalEndCycleDate AS IncrementalEndCycleDate
--	,@siBatchLogId AS BatchLogId
--;	
--PRINT N'precalc.uspFinancialKCPBatchRunValuePrepare:'
;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                         @dtToday = ' + FORMAT( @dtToday, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
--SELECT
--	 @dt2TimeStamp = SYSDATETIME()
--	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'@dtIncrementalStartCycleDate = ' + FORMAT( @dtIncrementalStartCycleDate, N'yyyy-MM-dd' )
--;
--RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
--SELECT
--	 @dt2TimeStamp = SYSDATETIME()
--	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'  @dtIncrementalEndCycleDate = ' + FORMAT( @dtIncrementalEndCycleDate, N'yyyy-MM-dd' )
--;
--RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                    @siBatchLogId = ' + ISNULL( CONVERT( nvarchar(10), @siBatchLogId ), N'{not supplied}' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 


/*
IF ( OBJECT_ID('tempdb..#tblOrderedCalendarDays', 'U') IS NOT NULL ) DROP TABLE #tblOrderedCalendarDays;
SELECT ROW_NUMBER() OVER (ORDER BY DayByDate) AS RowNumVal
	,DayByDate
INTO #tblOrderedCalendarDays
FROM [DHayes].[dbo].[DimCalendar]
WHERE DayByDate BETWEEN '2016-01-01' AND ( GETDATE() + 1 ) -- '2018-12-31'
	AND DATEPART(weekday, DayByDate) BETWEEN 2 AND 6
	AND DayByDate NOT IN ('2018-01-01','2018-01-15','2018-02-19','2018-05-28'
		,'2018-07-04','2018-09-03','2018-10-08','2018-11-12','2018-11-22','2018-12-25'
		,'2016-05-30','2016-07-04','2016-09-05','2016-10-10','2016-11-11','2016-11-24'
		,'2016-12-26','2017-01-02','2017-01-16','2017-02-20','2017-05-29','2017-07-04'
		,'2017-09-04','2017-10-09','2017-11-23','2017-12-25')
;


IF ( OBJECT_ID('tempdb..#tblCycleDates', 'U') IS NOT NULL ) DROP TABLE #tblCycleDates;
SELECT cal.DayByDate AS CycleDate
	,cal1.DayByDate AS CycleDateBack1
INTO #tblCycleDates
FROM #tblOrderedCalendarDays cal
INNER JOIN #tblOrderedCalendarDays cal1 
	ON cal.RowNumVal = cal1.RowNumVal + 1;
*/



IF ( OBJECT_ID('tempdb..#tblCycleDates', 'U') IS NOT NULL ) DROP TABLE #tblCycleDates;
SELECT 
	 ROW_NUMBER() OVER ( ORDER BY cd.CycleDate DESC ) AS RowSeq
	,cd.CycleDate AS CycleDate
INTO #tblCycleDates
FROM [ValidFI].[PNC].[Cycle] cd
WHERE cd.CycleDate BETWEEN '2015-12-29' AND SYSDATETIME() -- '2018-12-31'
ORDER BY cd.CycleDate
;
--SELECT * FROM #tblCycleDates
;


DECLARE 
	 --@dtToday date = SYSDATETIME() -- used for Calander driven dates vs Cycle Date driven dates
	 @dtNow datetime2(0) = SYSDATETIME()
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
	,@iSeqLastSuccessfulCycleDateRefresh int
;

SELECT @dtIncrementalEndCycleDate = MAX(cd.CycleDate)
FROM #tblCycleDates cd
WHERE cd.CycleDate < SYSDATETIME()
;
SELECT @dtPriorCycleDate = MAX(cd.CycleDate)
FROM #tblCycleDates cd
WHERE cd.CycleDate < @dtIncrementalEndCycleDate
;

SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'       @dtIncrementalEndCycleDate = ' + FORMAT( @dtIncrementalEndCycleDate, N'yyyy-MM-dd' ) + N' (aka "current cycle date")'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 

SET @dtLastSuccessfulCycleDateRefresh = ISNULL( ( SELECT TRY_CONVERT( date, p.ParameterValue ) FROM precalc.ufnParameterDate( 'LastSuccessfulCycleDateRefresh', -1 ) AS p ), @dtPriorCycleDate )
--SELECT @dtLastSuccessfulCycleDateRefresh AS LastSuccessfulCycleDateRefresh, @dtPriorCycleDate AS PriorCycleDate
--;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'@dtLastSuccessfulCycleDateRefresh = ' + FORMAT( @dtLastSuccessfulCycleDateRefresh, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT
; 
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                @dtPriorCycleDate = ' + FORMAT( @dtPriorCycleDate, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT
; 

SELECT @iSeqLastSuccessfulCycleDateRefresh = RowSeq FROM #tblCycleDates cd WHERE cd.CycleDate = @dtLastSuccessfulCycleDateRefresh
;

--No.No.No!No!NO--SELECT @dtClearedCycleDate = c.CycleDate FROM #tblCycleDates c WHERE c.RowSeq = 5 -- CycleDateBack4
SET @dtClearedCycleDate = DATEADD( dd, -4, @dtIncrementalEndCycleDate ) -- "four days back from the latest Cycle Date"
--_if_ we were calculating based on the run-date (ie: now, today, this moment, ...), then it would be DATEADD( dd, -5, SYSDATETIME() )
;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'              @dtClearedCycleDate = ' + FORMAT( @dtClearedCycleDate, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT
; 

SELECT
	-- @dtGetTranChangesBegin = DATEADD( dd, 1, c.CycleDate )
	--,@dtGetTranChangesEnd = @dtClearedCycleDate
	-- @dtGetTranChangesBegin = @dtClearedCycleDate
	--,@dtGetTranChangesEnd = DATEADD( dd, 1, c.CycleDate )
	-- @dtGetTranChangesBegin = c.CycleDate -- DATEADD( dd, -3, c.CycleDate )
	--,@dtGetTranChangesEnd = @dtClearedCycleDate
	 @dtGetTranChangesBegin = DATEADD( dd, -3, @dtPriorCycleDate )
	,@dtGetTranChangesEnd = DATEADD( dd, -4, @dtIncrementalEndCycleDate )
--FROM #tblCycleDates c WHERE c.RowSeq = ( CASE WHEN @dtLastSuccessfulCycleDateRefresh < @dtPriorCycleDate THEN @iSeqLastSuccessfulCycleDateRefresh ELSE 2 END + 3 ) -- CycleDateBack3
;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'             @dtGetTranChangesEnd = ' + FORMAT( @dtGetTranChangesEnd, N'yyyy-MM-dd' )
;
PRINT CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'           @dtGetTranChangesBegin = ' + FORMAT( @dtGetTranChangesBegin, N'yyyy-MM-dd' )
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT
; 

SELECT
	 @dtGetRetChangesBegin = DATEADD( dd, 1, c.CycleDate )
	,@dtGetRetChangesEnd = @dtIncrementalEndCycleDate -- @MostRecentCycleDate
FROM #tblCycleDates c WHERE c.RowSeq = CASE WHEN @dtLastSuccessfulCycleDateRefresh < @dtPriorCycleDate THEN @iSeqLastSuccessfulCycleDateRefresh ELSE 2 END -- @PriorCycleDate (ie: RowSeq = 2)
;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'              @dtGetRetChangesEnd = ' + FORMAT( @dtGetRetChangesEnd, N'yyyy-MM-dd' )
;
PRINT CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'            @dtGetRetChangesBegin = ' + FORMAT( @dtGetRetChangesBegin, N'yyyy-MM-dd' )
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT
; 

SELECT
	 @dtGetAcctCustChangesBegin = c.CycleDate
	,@dtGetAcctCustChangesEnd = @dtIncrementalEndCycleDate -- @MostRecentCycleDate
FROM #tblCycleDates c WHERE c.RowSeq = CASE WHEN @dtLastSuccessfulCycleDateRefresh < @dtPriorCycleDate THEN @iSeqLastSuccessfulCycleDateRefresh ELSE 2 END -- @PriorCycleDate (ie: RowSeq = 2)
;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'         @dtGetAcctCustChangesEnd = ' + FORMAT( @dtGetAcctCustChangesEnd, N'yyyy-MM-dd' )
;
PRINT CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'       @dtGetAcctCustChangesBegin = ' + FORMAT( @dtGetAcctCustChangesBegin, N'yyyy-MM-dd' )
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT
; 


DECLARE @tbParameter table ( RowId int IDENTITY(1,1) NOT NULL, BatchLogId smallint NOT NULL, ParameterMessage varchar(128) NOT NULL, ParameterValue nvarchar(128) NULL )
;

--DELETE FROM precalc.BatchRunValue WHERE BatchLogId = @siBatchLogId
IF NOT EXISTS( SELECT 'X' FROM precalc.BatchRunValue AS x WHERE x.BatchLogId = @siBatchLogId )
	BEGIN
		INSERT INTO precalc.BatchRunValue ( BatchLogId, RunValueKeyReference, RunValueAsString, DateActivated )
		SELECT BatchLogId, RunValueKeyReference, RunValueAsString, DateActivated
		FROM
			(

				SELECT @siBatchLogId AS BatchLogId, N'Today' RunValueKeyReference, FORMAT( @dtToday, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtToday (TodaysDate)
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'ClearedCycleDate' RunValueKeyReference, FORMAT( @dtClearedCycleDate, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtClearedCycleDate (ClearedCycleDate)
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'LastSuccessfulCycleDateRefresh' RunValueKeyReference, FORMAT( @dtLastSuccessfulCycleDateRefresh, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtLastSuccessfulCycleDateRefresh (Diana's)
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'Now' RunValueKeyReference, FORMAT( @dtNow, N'yyyy-MM-dd HH:mm:ss' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtNow (RefreshDate)
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'IncrementalEndCycleDate' RunValueKeyReference, FORMAT( @dtIncrementalEndCycleDate, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtIncrementalEndCycleDate (MostRecentCycleDate)
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'PriorCycleDate' RunValueKeyReference, FORMAT( @dtPriorCycleDate, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtPriorCycleDate (PriorCycleDate)
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'GetTranChangesBegin' RunValueKeyReference, FORMAT( @dtGetTranChangesBegin, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtGetTranChangesBegin
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'GetTranChangesEnd' RunValueKeyReference, FORMAT( @dtGetTranChangesEnd, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtGetTranChangesEnd
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'GetRetChangesBegin' RunValueKeyReference, FORMAT( @dtGetRetChangesBegin, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtGetRetChangesBegin
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'GetRetChangesEnd' RunValueKeyReference, FORMAT( @dtGetRetChangesEnd, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtGetRetChangesEnd
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'GetAcctCustChangesBegin' RunValueKeyReference, FORMAT( @dtGetAcctCustChangesBegin, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtGetAcctCustChangesBegin
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, N'GetAcctCustChangesEnd' RunValueKeyReference, FORMAT( @dtGetAcctCustChangesEnd, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated -- dtGetAcctCustChangesEnd

			) x
		WHERE NOT EXISTS( SELECT 'X' FROM precalc.BatchRunValue AS x WHERE x.BatchLogId = @siBatchLogId )
		;
	END
ELSE
	BEGIN
		INSERT INTO @tbParameter ( BatchLogId, ParameterMessage, ParameterValue )
		SELECT 
			 @siBatchLogId AS BatchLogId
			,N'A parameter value has not been set for ParameterReference ' + QUOTENAME( x.ParameterReference ) + N' of BatchLogId ' + QUOTENAME( CONVERT( nvarchar(10), @siBatchLogId ) ) AS ParameterMessage
			,x.ParameterValue
		FROM	
			(
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Today', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'ClearedCycleDate', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'LastSuccessfulCycleDateRefresh', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Now', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'IncrementalEndCycleDate', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'PriorCycleDate', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetTranChangesBegin', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetTranChangesEnd', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetRetChangesBegin', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetRetChangesEnd', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetAcctCustChangesBegin', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'GetAcctCustChangesEnd', @siBatchLogId ) AS p


			) x
			WHERE x.ParameterValue IS NULL
			;
	END
;

--IF ( SELECT COUNT(1) FROM @tbParameter ) > 0 SELECT * FROM @tbParameter
--;

--SELECT * FROM precalc.BatchRunValue WHERE BatchLogId = @siBatchLogId
--;

SET @psiBatchLogId = @siBatchLogId
;

SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + @nvThisSProcName + N' ...end.'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 

END

GO
