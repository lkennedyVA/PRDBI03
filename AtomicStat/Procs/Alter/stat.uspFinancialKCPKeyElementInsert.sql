USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspFinancialKCPKeyElementInsert]
	Created By: Chris Sharp 
	Description: This procedure insert entities into [stat].[KeyElement],

	Table(s): [stat].[KeyType]
		,[precalc].[KCPCustomerPayer]
		,[stat].[KeyElement]

	Function(s): [stat].[ufnKeyTypeIdByName]
		,[stat].[ufnKeyElementId256Modulus]

	Procedure(s): [error].[uspLogErrorDetailInsertOut]

	Sequence(s): [stat].[seqTokenSet]
		,[stat].[seqKeyElement]

	History:
		2018-10-10 - CBS - Created	
		2018-12-04 - LSW - Added @psiBatchLogId as an optional parameter, 
								and BatchLogId to list of columns inserted into stat.KeyElement.
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspFinancialKCPKeyElementInsert]
	(
		 @pbTiming bit = NULL -- 0 = Off, 1 = On (default).
		,@psiBatchLogId INT = NULL OUTPUT
	)
AS
BEGIN	
	SET NOCOUNT ON;
	IF OBJECT_ID('tempdb..#tblSubject', 'U') IS NOT NULL DROP TABLE #tblSubject;
	CREATE TABLE #tblSubject ( 
		 PageId int
		,CustomerNumber varchar(50)
		,RoutingNumber nchar(9) 
		,AccountNumber nvarchar(50)
		,PRIMARY KEY CLUSTERED 
		( 
			 PageId ASC
			,CustomerNumber ASC
			,RoutingNumber ASC
			,AccountNumber ASC
		) WITH ( FILLFACTOR = 100 )
	);	
	IF OBJECT_ID('tempdb..#tblSubjectPageSize', 'U') IS NOT NULL DROP TABLE #tblSubjectPageSize;
	CREATE TABLE #tblSubjectPageSize ( 
		 PageId int 
		,CustomerNumber varchar(50)
		,RoutingNumber nchar(9) 
		,AccountNumber nvarchar(50)
		,HashId binary(64) not null default(0x0)
		,TokenSetId bigint not null default(0)
		,PRIMARY KEY CLUSTERED 
		( 
			 CustomerNumber ASC
			,RoutingNumber ASC
			,AccountNumber ASC
		) WITH ( FILLFACTOR = 100 )
		, UNIQUE (HashId) 
		  WITH ( FILLFACTOR = 100 ) 
	);		
	IF OBJECT_ID('tempdb..#tblKeyElementTokenSet', 'U') IS NOT NULL DROP TABLE #tblKeyElementTokenSet;
	CREATE TABLE #tblKeyElementTokenSet (
		 KeyElementId bigint not null default(0)
		,TokenSetId bigint
		,HashId binary(64) 
		,PRIMARY KEY CLUSTERED 
		(	
			 KeyElementId ASC
			,TokenSetId ASC
		) WITH ( FILLFACTOR = 100 )
	);
	DECLARE @tblTiming table (
		 RowId int identity(1, 1)
		,Section nvarchar(100)
		,[TimeStamp] datetime2(7)
		,EllapsedTimeInSeconds int
	);
	DECLARE @dtTimerDate datetime2(7) = SYSDATETIME()
		,@dtInitial datetime2(7) = SYSDATETIME()
		,@iCustomerNumberIdTypeId int = 25 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
		,@iErrorDetailId int
		,@iOrgId int = 100009 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
		,@iPageNumber int = 1
		,@iPageSize int = 100000
		,@iPageCount int = 1
		,@iRowCount int = 0
		,@nvCustomerNumberIdTypeId nvarchar(100) 
		,@nvKeyTemplate nvarchar(1024)
		,@nvMessageText nvarchar(256)
		,@nvOrgId nvarchar(100)  
		,@nvWorker nvarchar(1024) 
		,@siClientOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('ClientOrgId')
		,@siIdTypeKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('IdType')
		,@siRoutingNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Routing Number')
		,@siAccountNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Account Number')
		,@siCustomerNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Customer Identifier')
		,@siFinancialKCPKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial KCP')
		,@sSchemaName sysname = 'stat'
		,@biRowCount bigint
		,@siBatchLogId INT = ISNULL( @psiBatchLogId, ( SELECT MAX( BatchLogId ) FROM [stat].[BatchLog] ) ) -- assumption is that if we are running without an explicit @psiBatchLogId value, then we want the last BatchLogId added in stat.BatchLog.
		;

	DECLARE @bTiming bit = ISNULL( @pbTiming, 1 ); -- default to 1 ("On")

	IF @bTiming <> 0
	BEGIN
		INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
		SELECT N'Prior to Assigning KeyTemplate', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
		SET @dtTimerDate  = SYSDATETIME();
	END
	
	--Grab the KeyTemplate for Financial KCP: 
	--'[{ClientOrgIdKeyTypeId}]|[{ClientOrgId}]|[{IdTypeKeyTypeId}]|[{IdTypeId}]|[{CustomerNumberKeyTypeId}]|[{CustomerNumber}]|[{RoutingNumberKeyTypeId}]|[{RoutingNumber}]|[{AccountNumberKeyTypeId}]|[{AccountNumber}]';
	SELECT @nvKeyTemplate = KeyTemplate 
	FROM [stat].[KeyType]
	WHERE KeyTypeId = @siFinancialKCPKeyTypeId;

	IF @bTiming <> 0
	BEGIN
		INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
		SELECT N'After to Replacing vKeyTemplate using vWorker', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
		SET @dtTimerDate  = SYSDATETIME();
	END

	SET @nvWorker = REPLACE( @nvKeyTemplate, '[{ClientOrgIdKeyTypeId}]', CONVERT(nvarchar(6),@siClientOrgIdKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{ClientOrgId}]', CONVERT(nvarchar(50),@iOrgId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{IdTypeKeyTypeId}]', CONVERT(nvarchar(6),@siIdTypeKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{IdTypeId}]', CONVERT(nvarchar(50), @iCustomerNumberIdTypeId ) );
	SET @nvWorker = REPLACE( @nvWorker, '[{CustomerNumberKeyTypeId}]', CONVERT(nvarchar(6),@siCustomerNumberKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{RoutingNumberKeyTypeId}]', CONVERT(nvarchar(6),@siRoutingNumberKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{AccountNumberKeyTypeId}]', CONVERT(nvarchar(6),@siAccountNumberKeyTypeId) );

	--In preparation for NTILE below
	SELECT @iPageCount = (COUNT(1)/@iPageSize) + 1
		,@iPageNumber = 1
	FROM [precalc].[KCPCustomerPayer];

	IF @bTiming <> 0
	BEGIN		 
		INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
		SELECT N'Entering tblSubject with Full Data Set', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
		SET @dtTimerDate  = SYSDATETIME();	
	END;	

	--Insert full list into #tblSubject...  
	INSERT INTO #tblSubject (
		 PageId
		,CustomerNumber
		,RoutingNumber 
		,AccountNumber
	)
	SELECT NTILE(@iPageCount) OVER(	ORDER BY kcp.CustomerNumber ASC,kcp.RoutingNumber ASC,kcp.AccountNumber ASC ) AS PageId
		,kcp.CustomerNumber
		,kcp.RoutingNumber
		,kcp.AccountNumber	
	FROM [precalc].[KCPCustomerPayer] kcp
	ORDER BY kcp.CustomerNumber ASC
		,kcp.RoutingNumber ASC
		,kcp.AccountNumber ASC;

	SET @biRowCount = @@ROWCOUNT;
	SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Base KCP Count: '+ CONVERT( nvarchar(50), @biRowCount ); 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.
	SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'    Page Count: ' + CONVERT( nvarchar(50), @iPageCount ); 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.
	PRINT NCHAR(013) + NCHAR(010) + NCHAR(013) + NCHAR(010)

	IF @bTiming <> 0
	BEGIN		 
		INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
		SELECT N'Exiting tblSubject with Full Data Set', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
		SET @dtTimerDate  = SYSDATETIME();	
	END;	
	
	--Grabbing a Page worth of records, here we being the looping
	WHILE @iPageNumber <= @iPageCount
	BEGIN	
		SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Beginning Iteration '+ CONVERT( nvarchar(50), @iPageNumber ) +' Of ' +CONVERT( nvarchar(50), @iPageCount ); 
		RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.

		IF @bTiming <> 0
		BEGIN		 
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Entering tblSubjectPageSize Section', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END;	
		
		BEGIN TRY
		
		INSERT INTO #tblSubjectPageSize (
			 PageId
			,CustomerNumber
			,RoutingNumber 
			,AccountNumber
			,HashId
			,TokenSetId
		)
		SELECT PageId
			,CustomerNumber
			,RoutingNumber
			,AccountNumber	
			,HashId = CONVERT(binary(64)
					,HASHBYTES( N'SHA2_512'
						,CONVERT(varbinary(512)
						,CONVERT(nvarchar(512)
							,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT(nvarchar(50),CustomerNumber ) ) 
								,'[{RoutingNumber}]', CONVERT(nvarchar(50),RoutingNumber ) )
									,'[{AccountNumber}]', CONVERT(nvarchar(50),AccountNumber ))						
							))),1) 
			,TokenSetId = NEXT VALUE FOR [stat].[seqTokenSet] OVER ( ORDER BY CustomerNumber ASC,RoutingNumber ASC,AccountNumber ASC)
		FROM #tblSubject kcp  
		WHERE PageId = @iPageNumber
		ORDER BY CustomerNumber ASC 
			,RoutingNumber ASC		 
			,AccountNumber ASC;

		SET @biRowCount = @@ROWCOUNT;
		SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Page KCP Count: '+ CONVERT( nvarchar(50), @biRowCount ); 
		RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.

		IF @bTiming <> 0
		BEGIN		 
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Exiting tblSubjectPageSize Section', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END;	

		IF @bTiming <> 0
		BEGIN
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Entering Removing HashIds From tblSubjectPageSize', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END		
		
		-- 2018-10-09 Populating our pre-calc "Hand Off" table 
		INSERT INTO [precalc].[KCPKeyElement](
			 CustomerId
			,PayerId
			,KeyElementId
			,PartitionId
			,IsNewKeyElement
		)
		SELECT kcp.CustomerId
			,kcp.PayerId
			,ke.KeyElementId
			,ke.PartitionId
			,0 AS IsNewKeyElement
		FROM #tblSubjectPageSize sps
		INNER JOIN [precalc].[KCPCustomerPayer] kcp
			ON sps.CustomerNumber = kcp.CustomerNumber
				AND sps.RoutingNumber = kcp.RoutingNumber
				AND sps.AccountNumber = kcp.AccountNumber
		INNER JOIN [stat].[KeyElement] ke
			ON sps.HashId = ke.HashId
		WHERE NOT EXISTS (SELECT 'X' 
						FROM [precalc].[KCPKeyElement] kcpke
						WHERE ke.KeyElementId = kcpke.KeyElementId);	

		IF @bTiming <> 0
		BEGIN
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Exiting Removing HashIds From tblSubjectPageSize', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END	
		
		IF @bTiming <> 0
		BEGIN
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Entering tblKeyElementTokenSet Insert', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END	
		
		--We want to assign a KeyElementId per TokenSetId 
		INSERT INTO #tblKeyElementTokenSet ( 
			 KeyElementId
			,TokenSetId
			,HashId
		)
		SELECT NEXT VALUE FOR [stat].[seqKeyElement] OVER ( ORDER BY TokenSetId) AS KeyElementId 
			,TokenSetId
			,HashId
		FROM #tblSubjectPageSize sps
		WHERE NOT EXISTS (SELECT 'X'
						FROM [stat].[KeyElement] ke
						WHERE sps.HashId = ke.HashId)
		ORDER BY TokenSetId ASC;

		IF @bTiming <> 0
		BEGIN
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Exiting tblKeyElementTokenSet Insert', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END	

		IF @bTiming <> 0
		BEGIN
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Entering [stat].[KeyElement] Insert', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END	

		--Insert into stat.KeyElement
		INSERT INTO [stat].[KeyElement] 
			( 
				 HashId
				,PartitionId
				,KeyElementId
				,BatchLogId
			)
		SELECT 
			 HashId
			,[stat].[ufnKeyElementId256Modulus](kel.KeyElementId) AS PartitionId
			,KeyElementId
			,@siBatchLogId AS BatchLogId
		FROM #tblKeyElementTokenSet kel
		WHERE NOT EXISTS (SELECT 'X' FROM [stat].[KeyElement] WHERE kel.HashId = HashId)
		GROUP BY HashId, KeyElementId
		ORDER BY kel.KeyElementId;
		
		INSERT INTO [precalc].[KCPKeyElement](
			 CustomerId
			,PayerId
			,KeyElementId
			,PartitionId
			,IsNewKeyElement
		)
		SELECT kcp.CustomerId
			,kcp.PayerId
			,ke.KeyElementId
			,[stat].[ufnKeyElementId256Modulus](ke.KeyElementId) AS PartitionId
			,1 AS IsNewKeyElement
		FROM #tblSubjectPageSize sps
		INNER JOIN [precalc].[KCPCustomerPayer] kcp
			ON sps.CustomerNumber = kcp.CustomerNumber
				AND sps.RoutingNumber = kcp.RoutingNumber
				AND sps.AccountNumber = kcp.AccountNumber
		INNER JOIN [stat].[KeyElement] ke
			ON sps.HashId = ke.HashId
		WHERE NOT EXISTS (SELECT 'X' 
						FROM [precalc].[KCPKeyElement] kcpke
						WHERE ke.KeyElementId = kcpke.KeyElementId);		

		IF @bTiming <> 0
		BEGIN
			INSERT INTO @tblTiming(Section, [TimeStamp], EllapsedTimeInSeconds)
			SELECT N'Exiting [stat].[KeyElement] Insert', SYSDATETIME(), DATEDIFF(second,@dtTimerDate,SYSDATETIME());
			SET @dtTimerDate  = SYSDATETIME();	
		END	
			
		--Clean the PageSize tables
		TRUNCATE TABLE #tblKeyElementTokenSet;
		TRUNCATE TABLE #tblSubjectPageSize;
			
		SET @iPageNumber += 1;
	
		WAITFOR DELAY '00:00:00.1';

		END TRY
		BEGIN CATCH
			EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
			SET @iErrorDetailId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
			THROW;
		END CATCH				
	END
END
GO
