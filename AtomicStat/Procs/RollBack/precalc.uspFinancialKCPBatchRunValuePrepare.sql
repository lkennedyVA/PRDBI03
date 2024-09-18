USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [precalc].[uspFinancialKCPBatchRunValuePrepare]
	(
		 @psiBatchLogId smallint = NULL OUTPUT -- if NULL, then grab the last BatchLogId from precalc.BatchRunValue
													 -- ; if greater than 0, then check the specified BatchLogId associated rows
													 -- ; if less than 0, calculate the dates and insert them and retrieve the assigned BatchLogId.
		,@pdtIncrementalLowCycleDate date = NULL -- passed in value is used if @psiBatchLogId is less than 1; if value is NULL, then latest valid Cycle Date shall be used.
		,@pdtIncrementalHighCycleDate date = NULL -- passed in value is used if @psiBatchLogId is less than 1; if value is NULL, then latest valid Cycle Date shall be used.
	)
AS
BEGIN
SET NOCOUNT ON;

DECLARE @siBatchLogId smallint = @psiBatchLogId
;
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
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'Begin...'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME();

PRINT 'initial parameter values'
;

DECLARE 
	 @dtToday date = GETDATE() -- used for Calander driven dates vs Cycle Date driven dates
	,@dtIncrementalStartCycleDate date = ISNULL( @pdtIncrementalLowCycleDate, ISNULL( ( SELECT MAX( CycleDate ) FROM [ValidFI].[PNC].[Cycle] ) , GETDATE()-1 ) ) -- '2018-10-12' -- '2001-01-01' -- '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
	-- Remove the @siBatchLogId cheat assignment before going live!
	-- This needs to be driven, in post development, by the mechanism Chris already has in place.
	--,@siBatchLogId smallint = ISNULL( @psiBatchLogId, ( ISNULL( ( /*cheating for the time being*/ SELECT MAX( x.BatchLogId ) FROM precalc.Parameter AS x WHERE x.BatchLogId > 0 ), 0 ) + 1 ) )
DECLARE 
	 @dtIncrementalEndCycleDate date = ISNULL( @pdtIncrementalHighCycleDate, @dtIncrementalStartCycleDate ) -- '2018-10-12' -- '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
;	
--SELECT
--	 @dtToday AS Today
--	,@dtIncrementalStartCycleDate AS IncrementalStartCycleDate
--	,@dtIncrementalEndCycleDate AS IncrementalEndCycleDate
--	,@siBatchLogId AS BatchLogId
--;	
PRINT 'precalc.uspFinancialKCPBatchRunValuePrepare:'
;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'                    @dtToday = ' + FORMAT( @dtToday, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'@dtIncrementalStartCycleDate = ' + FORMAT( @dtIncrementalStartCycleDate, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'  @dtIncrementalEndCycleDate = ' + FORMAT( @dtIncrementalEndCycleDate, N'yyyy-MM-dd' )
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'               @siBatchLogId = ' + CONVERT( nvarchar(10), @siBatchLogId )
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



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'CycleDates'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME();

IF ( OBJECT_ID('tempdb..#tblCycleDates', 'U') IS NOT NULL ) DROP TABLE #tblCycleDates;
SELECT cd.CycleDate AS CycleDate
	,p.CycleDate AS CycleDatePrior
INTO #tblCycleDates
FROM [ValidFI].[PNC].[Cycle] cd
INNER JOIN [ValidFI].[PNC].[Cycle] p
	ON cd.CycleId = p.CycleId + 1
WHERE cd.CycleDate BETWEEN '2016-01-01' AND ( GETDATE() + 1 ) -- '2018-12-31'
ORDER BY cd.CycleDate
;



--SELECT 'calculate parameter values' AS Event, GETDATE() AS EventDateTime
--;

--DECLARE @dtIncrementalStartCycleDate date = '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
--	,@dtIncrementalEndCycleDate date = '2018-09-07' -- DATEADD(DAY, -3, convert(date, GETDATE()))
--	,@dtPriorCycleDate date;	
DECLARE
	@dtPriorCycleDate date
;
SELECT @dtPriorCycleDate = MAX(cd.CycleDatePrior)
FROM #tblCycleDates cd
WHERE cd.CycleDate <= @dtIncrementalEndCycleDate
;
/*
SELECT * FROM #tblCycleDates cd
SELECT cd.CycleDatePrior
FROM #tblCycleDates cd
WHERE cd.CycleDate = @dtIncrementalStartCycleDate

WITH cteCycleDate AS
	(
		SELECT cd.CycleDate AS CycleDate
			,p.CycleDate AS CycleDatePrior
		FROM [ValidFI].[PNC].[Cycle] cd
		INNER JOIN [ValidFI].[PNC].[Cycle] p
			ON cd.CycleId = p.CycleId + 1
		--WHERE cd.CycleDate BETWEEN '2018-01-01' AND '2018-12-31'
		--WHERE cd.CycleDate BETWEEN ( GETDATE() - 366 ) AND ( GETDATE() + 1 )
		WHERE cd.CycleDate BETWEEN ( CONVERT( date, '2016/01/01' ) ) AND ( GETDATE() + 1 )
		ORDER BY cd.CycleDate
	)
SELECT @dtPriorCycleDate = cd.CycleDatePrior
FROM cteCycleDate cd
WHERE cd.CycleDate = @dtIncrementalStartCycleDate
;
*/
--SELECT @dtPriorCycleDate AS PriorCycleDate
--;
--SELECT DATEADD( year, DATEDIFF( year, 0, getdate() ), 0 )

DECLARE @dtDropOff180Start date = DATEADD(DAY, -179, @dtPriorCycleDate) --<------+-- for IncrementalStartCycleDate = '2018/09/07', the DropOff180 dates calculated as 2018/03/11 (Sunday)
	,@dtDropOff180End date = DATEADD(DAY, -180, @dtIncrementalEndCycleDate) --<--+

	,@dtDropOff365Start date = DATEADD(DAY, -364, @dtPriorCycleDate)
	,@dtDropOff365End date = DATEADD(DAY, -365, @dtIncrementalEndCycleDate)

	,@dtInclude10Start date = DATEADD(DAY, -8, @dtPriorCycleDate)
	,@dtInclude10End date = DATEADD(DAY, -9, @dtIncrementalEndCycleDate)

	,@dtInclude5Start date = DATEADD(DAY, -3, @dtPriorCycleDate) --<-------------+-- for IncrementalStartCycleDate = '2018/09/07', the Include5 dates calculated as 2018/09/03 (Monday - Labor Day Holiday), and then PayerClearedDay5ActivityRefreshList went into a tailspin
	,@dtInclude5End date = DATEADD(DAY, -4, @dtIncrementalEndCycleDate) --<------+

	,@dtAccChangeBack1Start date
	,@dtAccChangeBack1End date;

DECLARE @dtClearedDay5 date = DATEADD(DAY, -4, @dtIncrementalEndCycleDate)
	,@dtClearedDay10 date = DATEADD(DAY, -9, @dtIncrementalEndCycleDate)
	,@dtCycleDate180 date = DATEADD(DAY, -179, @dtIncrementalEndCycleDate)
	,@dtCycleDate365 date = DATEADD(DAY, -365, @dtIncrementalEndCycleDate);

SELECT @dtAccChangeBack1Start = MIN( CycleDatePrior ) -- CycleDateBack1 
FROM #tblCycleDates 
WHERE CycleDate >= @dtIncrementalStartCycleDate
;
SELECT @dtAccChangeBack1End = MAX( CycleDatePrior ) -- CycleDateBack1 
FROM #tblCycleDates 
WHERE CycleDate <= @dtIncrementalEndCycleDate
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
				SELECT @siBatchLogId AS BatchLogId, 'Today' AS RunValueKeyReference, FORMAT( @dtToday, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'IncrementalStartCycleDate' AS RunValueKeyReference, FORMAT( @dtIncrementalStartCycleDate, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'IncrementalEndCycleDate' AS RunValueKeyReference, FORMAT( @dtIncrementalEndCycleDate, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'PriorCycleDate' AS RunValueKeyReference, FORMAT( @dtPriorCycleDate, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'DropOff180Start' AS RunValueKeyReference, FORMAT( @dtDropOff180Start, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'DropOff180End' AS RunValueKeyReference, FORMAT( @dtDropOff180End, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'DropOff365Start' AS RunValueKeyReference, FORMAT( @dtDropOff365Start, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'DropOff365End' AS RunValueKeyReference, FORMAT( @dtDropOff365End, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'Include10Start' AS RunValueKeyReference, FORMAT( @dtInclude10Start, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'Include10End' AS RunValueKeyReference, FORMAT( @dtInclude10End, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'Include5Start' AS RunValueKeyReference, FORMAT( @dtInclude5Start, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'Include5End' AS RunValueKeyReference, FORMAT( @dtInclude5End, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'AccChangeBack1Start' AS RunValueKeyReference, FORMAT( @dtAccChangeBack1Start, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'AccChangeBack1End' AS RunValueKeyReference, FORMAT( @dtAccChangeBack1End, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'ClearedDay5' AS RunValueKeyReference, FORMAT( @dtClearedDay5, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'ClearedDay10' AS RunValueKeyReference, FORMAT( @dtClearedDay10, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'CycleDate180' AS RunValueKeyReference, FORMAT( @dtCycleDate180, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
				UNION ALL
				SELECT @siBatchLogId AS BatchLogId, 'CycleDate365' AS RunValueKeyReference, FORMAT( @dtCycleDate365, N'yyyy-MM-dd' ) AS RunValueAsString, GETDATE() AS DateActivated
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
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'IncrementalStartCycleDate', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'IncrementalEndCycleDate', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'PriorCycleDate', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'DropOff180Start', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'DropOff180End', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'DropOff365Start', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'DropOff365End', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Include10Start', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Include10End', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Include5Start', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'Include5End', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'AccChangeBack1Start', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'AccChangeBack1End', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'ClearedDay5', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'ClearedDay10', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'CycleDate180', @siBatchLogId ) AS p
				UNION ALL
				SELECT p.ParameterReference, p.ParameterValue FROM [precalc].[ufnParameterDate]( N'CycleDate365', @siBatchLogId ) AS p
			) x
			WHERE x.ParameterValue IS NULL
			;
	END
;

IF ( SELECT COUNT(1) FROM @tbParameter ) > 0 SELECT * FROM @tbParameter
;

SELECT * FROM precalc.BatchRunValue WHERE BatchLogId = @siBatchLogId
;

SET @psiBatchLogId = @siBatchLogId
;

END

GO
