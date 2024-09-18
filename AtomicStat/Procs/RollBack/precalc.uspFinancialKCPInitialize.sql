USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
	
	Reloads the precalc tables before stats are calculated.

	Calls precalc.uspFinancialKCPBatchRunValuePrepare before reloading the precalc tables.

*/
ALTER   PROCEDURE [precalc].[uspFinancialKCPInitialize]
	(
		-- IF a positive value is supplied, the parameter values for that BatchLogId will be used for the run.
		-- IF a negative value is supplied, the parameter values will be determined by formula, saved with a new generated BatchLogId, and then used for the run.
		-- IF a NULL value is supplied, the parameter values of the most recently added BatchLogId will be used for the run.
		 @psiBatchLogId smallint = NULL

		-- IF running for history, set @pdtIncrementalLowCycleDate to the furthest back date required for obtaining the history.
		,@pdtIncrementalLowCycleDate date = NULL -- if value is NULL, then default is to operate incrementally and assigns the same date to both @pdtIncrementalLowCycleDate and @pdtIncrementalHighCycleDate

		-- IF running for history, set @pdtIncrementalHighCycleDate to the most recent date for capping the history.
		,@pdtIncrementalHighCycleDate date = NULL -- if value is NULL, then default is to operate incrementally and assigns the same date to both @pdtIncrementalLowCycleDate and @pdtIncrementalHighCycleDate
	)
AS
/*
	
	Reloads the precalc tables before stats are calculated.

	Calls precalc.uspFinancialKCPBatchRunValuePrepare before reloading the precalc tables.

*/
BEGIN
SET NOCOUNT ON;
-- parameters for when this is a sproc
DECLARE 
	 @siBatchLogId smallint = @psiBatchLogId -- NULL -- 5 is the History run.
	,@dtIncrementalLowCycleDate date = @pdtIncrementalLowCycleDate -- '2018/10/15' -- '2016/01/01' -- GETDATE()-366 -- NULL
	,@dtIncrementalHighCycleDate date = @pdtIncrementalHighCycleDate -- '2018/10/15' -- '2018/10/13' -- GETDATE()-1 -- NULL


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

PRINT 'Initialize parameter values'
;

EXEC precalc.uspFinancialKCPBatchRunValuePrepare @psiBatchLogId = @siBatchLogId, @pdtIncrementalLowCycleDate = @dtIncrementalLowCycleDate, @pdtIncrementalHighCycleDate = @dtIncrementalHighCycleDate
;

SET NOCOUNT OFF;

PRINT ''
;
PRINT ''
;

SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerNewActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--PayerNewActivityRefreshList
TRUNCATE TABLE precalc.PayerNewActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS IncrementalStartCycleDate, b.ParameterValue AS IncrementalEndCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalStartCycleDate', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'IncrementalEndCycleDate', default ) b )
INSERT INTO precalc.PayerNewActivityRefreshList ( PayerId )
SELECT DISTINCT PayerId
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtIncrementalStartCycleDate AND @dtIncrementalEndCycleDate
WHERE CycleDate BETWEEN p.IncrementalStartCycleDate AND p.IncrementalEndCycleDate
	AND TransactionTypeId = 2
ORDER BY PayerId
;
WITH cteParameter AS ( SELECT a.ParameterValue AS IncrementalStartCycleDate, b.ParameterValue AS IncrementalEndCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalStartCycleDate', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'IncrementalEndCycleDate', default ) b )
INSERT INTO precalc.PayerNewActivityRefreshList ( PayerId )
SELECT DISTINCT t.PayerId 
FROM [ValidFI].[dbo].[ReturnConsolidation] rc 
	CROSS APPLY cteParameter p
	INNER JOIN [ValidFI].[dbo].[Transaction] t 
		ON rc.MatchedTransactionId = t.TransactionId
			--AND rc.BusinessDate BETWEEN @dtIncrementalStartCycleDate AND @dtIncrementalEndCycleDate
			AND rc.BusinessDate BETWEEN p.IncrementalStartCycleDate AND p.IncrementalEndCycleDate
			AND t.TransactionTypeId = 2
WHERE NOT EXISTS( SELECT 'X' FROM precalc.PayerNewActivityRefreshList AS x WHERE x.PayerId = t.PayerId )
ORDER BY t.PayerId
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerDay180ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;

--PayerDay180ActivityRefreshList
TRUNCATE TABLE precalc.PayerDay180ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS DropOff180Start, b.ParameterValue AS DropOff180End FROM [precalc].[ufnParameterDate]( 'DropOff180Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'DropOff180End', default ) b )
INSERT INTO precalc.PayerDay180ActivityRefreshList ( PayerId )
SELECT DISTINCT PayerId 
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtDropOff180Start AND @dtDropOff180End
WHERE CycleDate BETWEEN p.DropOff180Start AND p.DropOff180End
	AND TransactionTypeId = 2
ORDER BY PayerId
;


SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerDay365ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--PayerDay365ActivityRefreshList
TRUNCATE TABLE precalc.PayerDay365ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS DropOff365Start, b.ParameterValue AS DropOff365End FROM [precalc].[ufnParameterDate]( 'DropOff365Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'DropOff365End', default ) b )
INSERT INTO precalc.PayerDay365ActivityRefreshList ( PayerId )
SELECT DISTINCT PayerId 
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtDropOff365Start AND @dtDropOff365End
WHERE CycleDate BETWEEN p.DropOff365Start AND p.DropOff365End
	AND TransactionTypeId = 2
ORDER BY PayerId
;


SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerClearedDay5ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--PayerClearedDay5ActivityRefreshList
TRUNCATE TABLE precalc.PayerClearedDay5ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS Include5Start, b.ParameterValue AS Include5End FROM [precalc].[ufnParameterDate]( 'Include5Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'Include5End', default ) b )
INSERT INTO precalc.PayerClearedDay5ActivityRefreshList ( PayerId )
SELECT DISTINCT PayerId 
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtInclude5Start AND @dtInclude5End
WHERE CycleDate BETWEEN p.Include5Start AND p.Include5End
	AND TransactionTypeId = 2
ORDER BY PayerId
;


SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'AccountCustomerChangeRefreshList: precalc.CustListChanged'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--AccountCustomerChangeRefreshList
--WITH cteCustListChanged 
--AS 
TRUNCATE TABLE precalc.CustListChanged
;
--SELECT a.ParameterValue AS AccChangeBack1Start, b.ParameterValue AS AccChangeBack1End FROM [precalc].[ufnParameterDate]( 'AccChangeBack1Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'AccChangeBack1End', default ) b
--;
WITH cteParameter AS ( SELECT a.ParameterValue AS AccChangeBack1Start, b.ParameterValue AS AccChangeBack1End FROM [precalc].[ufnParameterDate]( 'AccChangeBack1Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'AccChangeBack1End', default ) b )
INSERT INTO precalc.CustListChanged ( AfterCustomerId, BeforeCustomerId )
SELECT DISTINCT acc.CustomerId AS AfterCustomerId
	,ah.CustomerId AS BeforeCustomerId
FROM [ValidFI].[dbo].[Account] acc 
	CROSS APPLY cteParameter p
	INNER JOIN [ValidFI].[dbo].[AccountDetailHistory] ah
		--ON ah.CycleDate BETWEEN @dtAccChangeBack1Start and @dtAccChangeBack1End
		ON acc.AccountId = ah.AccountId
			AND ah.CycleDate BETWEEN p.AccChangeBack1Start and p.AccChangeBack1End
WHERE acc.CustomerId <> ah.CustomerId
	AND ah.CustomerId > 0
ORDER BY AfterCustomerId, BeforeCustomerId
;

SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'AccountCustomerChangeRefreshList: precalc.AccountCustomerChangeRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
TRUNCATE TABLE precalc.AccountCustomerChangeRefreshList
;
INSERT INTO precalc.AccountCustomerChangeRefreshList ( AccountId )
SELECT DISTINCT acc.AccountId
FROM precalc.CustListChanged chg
INNER JOIN [ValidFI].[dbo].[Account] acc 
	ON chg.BeforeCustomerId = acc.CustomerId
ORDER BY acc.AccountId
;
INSERT INTO precalc.AccountCustomerChangeRefreshList ( AccountId )
SELECT DISTINCT acc.AccountId
FROM precalc.CustListChanged chg
INNER JOIN [ValidFI].[dbo].[Account] acc 
	ON chg.AfterCustomerId = acc.CustomerId
WHERE NOT EXISTS( SELECT 'X' FROM precalc.AccountCustomerChangeRefreshList AS x WHERE x.AccountId = acc.AccountId )
ORDER BY acc.AccountId
;


SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.AccountNewActivityRefreshList 1'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--AccountNewActivityRefreshList
TRUNCATE TABLE precalc.AccountNewActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS IncrementalStartCycleDate, b.ParameterValue AS IncrementalEndCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalStartCycleDate', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'IncrementalEndCycleDate', default ) b )
INSERT INTO precalc.AccountNewActivityRefreshList ( AccountId )
SELECT DISTINCT AccountId
FROM [ValidFI].[dbo].[Transaction] 
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtIncrementalStartCycleDate AND @dtIncrementalEndCycleDate
WHERE CycleDate BETWEEN p.IncrementalStartCycleDate and p.IncrementalEndCycleDate
	AND TransactionTypeId = 2
	AND AccountId > 0
ORDER BY AccountId
;

SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.AccountNewActivityRefreshList 2'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
WITH cteParameter AS ( SELECT a.ParameterValue AS IncrementalStartCycleDate, b.ParameterValue AS IncrementalEndCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalStartCycleDate', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'IncrementalEndCycleDate', default ) b )
INSERT INTO precalc.AccountNewActivityRefreshList ( AccountId )
SELECT DISTINCT t.AccountId
FROM [ValidFI].[dbo].[ReturnConsolidation] rc 
	CROSS APPLY cteParameter p
INNER JOIN [ValidFI].[dbo].[Transaction] t 
	ON rc.MatchedTransactionId = t.TransactionId
		--AND rc.BusinessDate BETWEEN @dtIncrementalStartCycleDate AND @dtIncrementalEndCycleDate
		AND rc.BusinessDate BETWEEN p.IncrementalStartCycleDate and p.IncrementalEndCycleDate
		AND t.TransactionTypeId = 2 
		AND t.AccountId > 0
WHERE NOT EXISTS( SELECT 'X' FROM precalc.AccountNewActivityRefreshList AS x WHERE x.AccountId = t.AccountId )
ORDER BY t.AccountId
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.AccountDay180ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--AccountDay180ActivityRefreshList
TRUNCATE TABLE precalc.AccountDay180ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS DropOff180Start, b.ParameterValue AS DropOff180End FROM [precalc].[ufnParameterDate]( 'DropOff180Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'DropOff180End', default ) b )
INSERT INTO precalc.AccountDay180ActivityRefreshList ( AccountId )
SELECT DISTINCT AccountId 
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtDropOff180Start AND @dtDropOff180End
WHERE CycleDate BETWEEN p.DropOff180Start and p.DropOff180End
	AND TransactionTypeId = 2 
	AND AccountId > 0
ORDER BY AccountId
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.AccountDay365ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--AccountDay365ActivityRefreshList
TRUNCATE TABLE precalc.AccountDay365ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS DropOff365Start, b.ParameterValue AS DropOff365End FROM [precalc].[ufnParameterDate]( 'DropOff365Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'DropOff365End', default ) b )
INSERT INTO precalc.AccountDay365ActivityRefreshList ( AccountId )
SELECT DISTINCT AccountId 
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtDropOff365Start AND @dtDropOff365End
WHERE CycleDate BETWEEN p.DropOff365Start and p.DropOff365End
	AND TransactionTypeId = 2 
	AND AccountId > 0
ORDER BY AccountId
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.AccountClearedDay10ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--AccountClearedDay10ActivityRefreshList
TRUNCATE TABLE precalc.AccountClearedDay10ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS Include10Start, b.ParameterValue AS Include10End FROM [precalc].[ufnParameterDate]( 'Include10Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'Include10End', default ) b )
INSERT INTO precalc.AccountClearedDay10ActivityRefreshList ( AccountId )
SELECT DISTINCT AccountId
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtInclude10Start AND @dtInclude10End
WHERE CycleDate BETWEEN p.Include10Start and p.Include10End
	AND TransactionTypeId = 2 
	AND AccountId > 0
ORDER BY AccountId
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.AccountClearedDay5ActivityRefreshList'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--AccountClearedDay5ActivityRefreshList
TRUNCATE TABLE precalc.AccountClearedDay5ActivityRefreshList
;
WITH cteParameter AS ( SELECT a.ParameterValue AS Include5Start, b.ParameterValue AS Include5End FROM [precalc].[ufnParameterDate]( 'Include5Start', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'Include5End', default ) b )
INSERT INTO precalc.AccountClearedDay5ActivityRefreshList ( AccountId )
SELECT DISTINCT AccountId
FROM [ValidFI].[dbo].[Transaction]
	CROSS APPLY cteParameter p
--WHERE CycleDate BETWEEN @dtInclude5Start AND @dtInclude5End
--WHERE CycleDate BETWEEN [precalc].[ufnParameterDate]( 'Include5Start', default ) aND [precalc].[ufnParameterDate]( 'Include5End' )
WHERE CycleDate BETWEEN p.Include5Start and p.Include5End
	AND TransactionTypeId = 2 
	AND AccountId > 0
ORDER BY AccountId
;





SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.KCPAcctCustList (aka AcctListKCP) 1'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
--KCPAcctCustList aka AcctListKCP
TRUNCATE TABLE precalc.KCPAcctCustList
;
INSERT INTO precalc.KCPAcctCustList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a1.AccountId
		,acc.CustomerId
	FROM precalc.AccountCustomerChangeRefreshList a1
	INNER JOIN ValidFI.dbo.Account acc
		ON a1.AccountId = acc.AccountId
;
--	UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.KCPAcctCustList (aka AcctListKCP) 2'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.KCPAcctCustList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a2.AccountId
		,acc.CustomerId
	FROM precalc.AccountNewActivityRefreshList a2
	INNER JOIN ValidFI.dbo.Account acc
		ON a2.AccountId = acc.AccountId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.KCPAcctCustList AS x 
			WHERE x.AccountId = a2.AccountId
				AND x.CustomerId = acc.CustomerId
		)
;	
--	UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.KCPAcctCustList (aka AcctListKCP) 3'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.KCPAcctCustList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a3.AccountId
		,acc.CustomerId
	FROM precalc.AccountClearedDay5ActivityRefreshList a3
	INNER JOIN ValidFI.dbo.Account acc
		ON a3.AccountId = acc.AccountId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.KCPAcctCustList AS x 
			WHERE x.AccountId = a3.AccountId 
				AND x.CustomerId = acc.CustomerId
		)
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.Cust'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
TRUNCATE TABLE precalc.Cust
;
INSERT INTO precalc.Cust ( CustomerId, CustomerNumber )
SELECT 
	 c.CustomerId
	,c.CustomerNumber
FROM ValidFI.dbo.Customer c
WHERE c.CustomerId > 0
	AND c.BankId = 100003 -- PNC
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared10AcctList (aka AcctListCustCleared10) 1'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
-- CustCleared10AcctList (aka AcctListCustCleared10)
TRUNCATE TABLE precalc.CustCleared10AcctList
;
INSERT INTO precalc.CustCleared10AcctList ( AccountId, CustomerId, CustomerNumber )
	SELECT 
	DISTINCT
		 a1.AccountId
		,acc.CustomerId
		,c.CustomerNumber
	FROM precalc.AccountCustomerChangeRefreshList a1
	INNER JOIN ValidFI.dbo.Account acc
		ON a1.AccountId = acc.AccountId
	INNER JOIN precalc.Cust c
		ON acc.CustomerId = c.CustomerId
;
--	UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared10AcctList (aka AcctListCustCleared10) 2'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.CustCleared10AcctList ( AccountId, CustomerId, CustomerNumber )
	SELECT 
	DISTINCT
		 a2.AccountId
		,acc.CustomerId
		,c.CustomerNumber
	FROM precalc.AccountNewActivityRefreshList a2
	INNER JOIN ValidFI.dbo.Account acc
		ON a2.AccountId = acc.AccountId
	INNER JOIN precalc.Cust c
		ON acc.CustomerId = c.CustomerId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.CustCleared10AcctList AS x 
			WHERE x.AccountId = a2.AccountId 
				AND x.CustomerId = acc.CustomerId
		)
;	
--	UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared10AcctList (aka AcctListCustCleared10) 3'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.CustCleared10AcctList ( AccountId, CustomerId, CustomerNumber )
	SELECT 
	DISTINCT
		a3.AccountId
		,acc.CustomerId
		,c.CustomerNumber
	FROM precalc.AccountClearedDay10ActivityRefreshList a3
	INNER JOIN ValidFI.dbo.Account acc
		ON a3.AccountId = acc.AccountId
	INNER JOIN precalc.Cust c
		ON acc.CustomerId = c.CustomerId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.CustCleared10AcctList AS x 
			WHERE x.AccountId = a3.AccountId 
				AND x.CustomerId = acc.CustomerId
		)
;	





SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared180to10AcctList (aka AcctListCustCleared180to10) 1'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
-- CustCleared180to10AcctList (aka AcctListCustCleared180to10)
TRUNCATE TABLE precalc.CustCleared180to10AcctList
;
INSERT INTO precalc.CustCleared180to10AcctList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a1.AccountId
		,acc.CustomerId
	FROM precalc.AccountCustomerChangeRefreshList a1
	INNER JOIN ValidFI.dbo.Account acc
		ON a1.AccountId = acc.AccountId
;
--UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared180to10AcctList (aka AcctListCustCleared180to10) 2'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.CustCleared180to10AcctList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a2.AccountId
		,acc.CustomerId
	FROM precalc.AccountNewActivityRefreshList a2
	INNER JOIN ValidFI.dbo.Account acc
		ON a2.AccountId = acc.AccountId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.CustCleared180to10AcctList AS x 
			WHERE x.AccountId = a2.AccountId 
				AND x.CustomerId = acc.CustomerId
		)
;
--UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared180to10AcctList (aka AcctListCustCleared180to10) 3'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.CustCleared180to10AcctList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a3.AccountId
		,acc.CustomerId
	FROM precalc.AccountClearedDay10ActivityRefreshList a3
	INNER JOIN ValidFI.dbo.Account acc
		ON a3.AccountId = acc.AccountId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.CustCleared180to10AcctList AS x 
			WHERE x.AccountId = a3.AccountId 
				AND x.CustomerId = acc.CustomerId
		)
;
--UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.CustCleared180to10AcctList (aka AcctListCustCleared180to10) 4'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.CustCleared180to10AcctList ( AccountId, CustomerId )
	SELECT 
	DISTINCT
		a4.AccountId
		,acc.CustomerId
	FROM precalc.AccountDay180ActivityRefreshList a4
	INNER JOIN ValidFI.dbo.Account acc
		ON a4.AccountId = acc.AccountId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.CustCleared180to10AcctList AS x 
			WHERE x.AccountId = a4.AccountId 
				AND x.CustomerId = acc.CustomerId
		)
;





SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerActivity365 1'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
-- PayerActivity365
TRUNCATE TABLE precalc.PayerActivity365
;
INSERT INTO precalc.PayerActivity365 ( PayerId, RoutingNumber, AccountNumber )
	SELECT 
	DISTINCT
		 p1.PayerId
		,vp.RoutingNumber
		,vp.AccountNumber
	FROM precalc.PayerNewActivityRefreshList p1
		INNER JOIN ValidFI.dbo.Payer vp
			ON p1.PayerId = vp.PayerId
;
--UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerActivity365 2'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.PayerActivity365 ( PayerId, RoutingNumber, AccountNumber )
	SELECT 
	DISTINCT
		 p2.PayerId
		,vp.RoutingNumber
		,vp.AccountNumber
	FROM precalc.PayerDay365ActivityRefreshList p2
		INNER JOIN ValidFI.dbo.Payer vp
			ON p2.PayerId = vp.PayerId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.PayerActivity365 AS x 
			WHERE x.PayerId = p2.PayerId
		)
;
--UNION
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.PayerActivity365 3'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
INSERT INTO precalc.PayerActivity365 ( PayerId, RoutingNumber, AccountNumber )
	SELECT 
	DISTINCT
		 p3.PayerId
		,vp.RoutingNumber
		,vp.AccountNumber
	FROM precalc.PayerClearedDay5ActivityRefreshList p3
		INNER JOIN ValidFI.dbo.Payer vp
			ON p3.PayerId = vp.PayerId
	WHERE NOT EXISTS
		(
			SELECT 'X' 
			FROM precalc.PayerActivity365 AS x 
			WHERE x.PayerId = p3.PayerId
		)
;





SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'precalc.KCPCustomerPayer'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;

DECLARE
		 @nvKeyTemplate nvarchar(1024)
		,@nvWorker nvarchar(1024)
		,@iCustomerNumberIdTypeId int = 25 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
		,@iOrgId int = 100009 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
		,@nvCustomerNumberIdTypeId nvarchar(100) 
		,@nvOrgId nvarchar(100)  
		,@siClientOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('ClientOrgId')
		,@siIdTypeKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('IdType')
		,@siRoutingNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Routing Number')
		,@siAccountNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Account Number')
		,@siCustomerNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Customer Identifier')
		,@siFinancialKCPKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial KCP')
;
	SELECT
		 @nvKeyTemplate = 
			( 
				SELECT KeyTemplate 
				FROM [stat].[KeyType] 
				WHERE KeyTypeId = @siFinancialKCPKeyTypeId 
			)
		,@nvWorker = 
			REPLACE( -- [{ClientOrgId}]
				REPLACE( -- [{IdTypeKeyTypeId}]
					REPLACE( -- [{IdTypeId}]
						REPLACE( -- [{CustomerNumberKeyTypeId}]
							REPLACE( -- [{RoutingNumberKeyTypeId}]
								REPLACE( -- [{AccountNumberKeyTypeId}]
									REPLACE( @nvKeyTemplate, '[{ClientOrgIdKeyTypeId}]', CONVERT(nvarchar(6),@siClientOrgIdKeyTypeId) )
										, '[{AccountNumberKeyTypeId}]', CONVERT(nvarchar(6),@siAccountNumberKeyTypeId) )
									, '[{RoutingNumberKeyTypeId}]', CONVERT(nvarchar(6),@siRoutingNumberKeyTypeId) )
								, '[{CustomerNumberKeyTypeId}]', CONVERT(nvarchar(6),@siCustomerNumberKeyTypeId) )
							, '[{IdTypeId}]', CONVERT(nvarchar(50), @iCustomerNumberIdTypeId ) )
						, '[{IdTypeKeyTypeId}]', CONVERT(nvarchar(6),@siIdTypeKeyTypeId) )
					, '[{ClientOrgId}]', CONVERT(nvarchar(50),@iOrgId) )
	;
SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'KeyTemplate: "' + @nvKeyTemplate + N'"'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;

-- KCPCustomerPayer
TRUNCATE TABLE precalc.KCPCustomerPayer
;
WITH cteParameter AS ( SELECT a.ParameterValue AS CycleDate365, b.ParameterValue AS IncrementalEndCycleDate FROM [precalc].[ufnParameterDate]( 'CycleDate365', default ) a CROSS APPLY [precalc].[ufnParameterDate]( 'IncrementalEndCycleDate', default ) b )
INSERT INTO precalc.KCPCustomerPayer ( CustomerNumber, RoutingNumber, AccountNumber, CustomerId, PayerId, HashId )
SELECT 
DISTINCT 
--TOP 100  --<<== let's not overexert the hamsters while dev-testing
	  ca.CustomerNumber
	 ,p.RoutingNumber
	 ,p.AccountNumber
	 ,ca.CustomerId
	 ,t.PayerId
	 ,HashId = CONVERT(binary(64) -- let's include the generated hash value to speed up [stat].[uspFinancialKCPKeyElementInsert]
					,HASHBYTES( N'SHA2_512'
						,CONVERT(varbinary(512)
						,CONVERT(nvarchar(512)
							,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT( nvarchar(50), ca.CustomerNumber ) )
								,'[{RoutingNumber}]', CONVERT( nvarchar(50), p.RoutingNumber ) )
									,'[{AccountNumber}]', CONVERT( nvarchar(50), p.AccountNumber ) )
							))),1)
FROM precalc.CustCleared10AcctList ca
	CROSS APPLY cteParameter prm
	INNER JOIN ValidFI.dbo.[Transaction] t
		ON ca.AccountId = t.AccountId
	INNER JOIN precalc.PayerActivity365 p
		ON t.PayerId = p.PayerId
			AND t.ReturnStatusId = 0
			AND t.TransactionTypeId = 2 
	WHERE t.CycleDate >= prm.CycleDate365
		AND t.CycleDate <= prm.IncrementalEndCycleDate
;



SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'KCPKeyElement (truncate only)'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME()
;
SELECT 'KCPKeyElement' AS Event, GETDATE() AS EventDateTime
;
-- KCPKeyElement
TRUNCATE TABLE precalc.KCPKeyElement -- because the other sprocs fill in this table, it should be empty when the rest of the stat sprocs begin their work.
;


SELECT
	 @dt2TimeStamp = SYSDATETIME()
	,@iEllapsedTimeInSeconds = DATEDIFF( second, @dtTimerDate, @dt2TimeStamp )
	,@nvMessage = CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) + N'...End.'
;
RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 
SET @dtTimerDate = SYSDATETIME();

END

GO
