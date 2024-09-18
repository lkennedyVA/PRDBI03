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

	Table(s): [precalc].[KCPKeyElement]  
		,[precalc].[KCPCustomerPayer] 
		,[precalc].[KCPAcctCustList]
		,[ValidFI].[dbo].[Transaction] 

	Function(s): [stat].[ufnStatIdByName]
		,[precalc].[ufnParameterDate]

	Procedure(s): [error].[uspLogErrorDetailInsertOut]

	History:
		2018-10-11 - CBS - Created	
		2018-10-12 - LSW - Modified, adjusted WAY LATE night 
		2018-10-16 - CBS - Modified, dammit
		2018-10-25 - LSW - shortcircuited/disabled

*****************************************************************************************/
ALTER PROCEDURE [stat].[zzzuspFinancialKCPClearedCheckNumber_wrongDriverSet](
	 @psiBatchLogId SMALLINT
	,@pbiSetSize BIGINT = NULL
)
AS 
IF 0 = 1 -- shortcircuiting this badboy - leaving this sproc behind temporarily as a reference 
BEGIN
	SET NOCOUNT ON;
	IF OBJECT_ID('tempdb..#tblSubject', 'U') IS NOT NULL DROP TABLE #tblSubject;
	CREATE TABLE #tblSubject ( 
		 SetId int 
		,CustomerNumber varchar(50)
		,RoutingNumber nchar(9) 
		,AccountNumber nvarchar(50)
		,CustomerId int
		,PayerId int
		,HashId binary(64) not null default(0x0) --Adding hashing earlier
		,PRIMARY KEY CLUSTERED 
		( 
			 SetId ASC 
			,CustomerNumber ASC
			,RoutingNumber ASC
			,AccountNumber ASC
		) WITH ( FILLFACTOR = 100 )
	);		
	IF OBJECT_ID('tempdb..#tblSubjectPageSize', 'U') IS NOT NULL DROP TABLE #tblSubjectPageSize;
	CREATE TABLE #tblSubjectPageSize ( 
		 SetId int 
		,CustomerNumber varchar(50)
		,RoutingNumber nchar(9) 
		,AccountNumber nvarchar(50)
		,CustomerId int
		,PayerId int
		,HashId binary(64) not null default(0x0)
		,KeyElementId bigint not null default(0)
		,PartitionId tinyint not null default(0)
		,PRIMARY KEY CLUSTERED 
		( 
			 CustomerNumber ASC
			,RoutingNumber ASC
			,AccountNumber ASC
		) WITH ( FILLFACTOR = 100 )
	);	
	IF OBJECT_ID('tempdb..#tblStatPageSize', 'U') IS NOT NULL DROP TABLE #tblStatPageSize;
	CREATE TABLE #tblStatPageSize ( 
		 KeyElementId bigint
		,PartitionId tinyint
		,StatId smallint
		,StatValue nchar(50)
		,PRIMARY KEY CLUSTERED 
		( 
			 KeyElementId ASC
			,StatId ASC
		) WITH ( FILLFACTOR = 100 )
	);		
	DECLARE @biKCPCount bigint = ( SELECT COUNT(1) FROM [precalc].[KCPCustomerPayer] AS k )
		,@biSetSize bigint = ISNULL(@pbiSetSize, 2000000)
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
		,@siClientOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('ClientOrgId')
		,@siIdTypeKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('IdType')
		,@siRoutingNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Routing Number')
		,@siAccountNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Account Number')
		,@siCustomerNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Customer Identifier')
		,@siFinancialKCPKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial KCP')
		,@siKCPMinCheckNumberClearedStatId smallint = [stat].[ufnStatIdByName]('KCPMinCheckNumberCleared')	
		,@siKCPMaxCheckNumberClearedStatId smallint = [stat].ufnStatIdByName('KCPMaxCheckNumberCleared')
		,@sSchemaName sysname = 'stat';

	--Setting the ClearedDay5 value
	SELECT @dtClearedDay5Parameter = ParameterValue 
	FROM [precalc].[ufnParameterDate]( 'ClearedDay5', default );

	--Grab the KeyTemplate for Financial KCP: 
	--'[{ClientOrgIdKeyTypeId}]|[{ClientOrgId}]|[{IdTypeKeyTypeId}]|[{IdTypeId}]|[{CustomerNumberKeyTypeId}]|[{CustomerNumber}]|[{RoutingNumberKeyTypeId}]|[{RoutingNumber}]|[{AccountNumberKeyTypeId}]|[{AccountNumber}]';
	SELECT @nvKeyTemplate = KeyTemplate 
	FROM [stat].[KeyType]
	WHERE KeyTypeId = @siFinancialKCPKeyTypeId;

	SET @nvWorker = REPLACE( @nvKeyTemplate, '[{ClientOrgIdKeyTypeId}]', CONVERT(nvarchar(6),@siClientOrgIdKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{ClientOrgId}]', CONVERT(nvarchar(50),@iOrgId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{IdTypeKeyTypeId}]', CONVERT(nvarchar(6),@siIdTypeKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{IdTypeId}]', CONVERT(nvarchar(50), @iCustomerNumberIdTypeId ) );
	SET @nvWorker = REPLACE( @nvWorker, '[{CustomerNumberKeyTypeId}]', CONVERT(nvarchar(6),@siCustomerNumberKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{RoutingNumberKeyTypeId}]', CONVERT(nvarchar(6),@siRoutingNumberKeyTypeId) );
	SET @nvWorker = REPLACE( @nvWorker, '[{AccountNumberKeyTypeId}]', CONVERT(nvarchar(6),@siAccountNumberKeyTypeId) );

	SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Beginning Execution'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.
	
	INSERT INTO #tblSubject (
		 SetId 
		,CustomerNumber 
		,RoutingNumber 
		,AccountNumber 
		,CustomerId 
		,PayerId	
		,HashId
	)
	SELECT SetId = NTILE( ( @biKCPCount / @biSetSize ) + 1 ) OVER( ORDER BY CustomerNumber, RoutingNumber, AccountNumber ) 
		,CustomerNumber
		,RoutingNumber
		,AccountNumber
		,CustomerId
		,PayerId
		-- Determining the HashId at this point allows us to avoid an extra UPDATE pass at the data later on. LSW
		,HashId = CONVERT(binary(64)
					,HASHBYTES( N'SHA2_512'
						,CONVERT(varbinary(512)
						,CONVERT(nvarchar(512)
							,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT(nvarchar(50),CustomerNumber ) ) 
								,'[{RoutingNumber}]', CONVERT(nvarchar(50),RoutingNumber ) )
									,'[{AccountNumber}]', CONVERT(nvarchar(50),AccountNumber ))						
							))),1)
	FROM [precalc].[KCPCustomerPayer] -- this identifies the Customer-Payer pairs (KCPs) we are working with now.
	ORDER BY SetId ASC
		,CustomerNumber ASC
		,RoutingNumber ASC
		,AccountNumber ASC;

	SELECT @iPageCount = MAX(SetId) 
	FROM #tblSubject;
	
	SET @iPageNumber = 1;
	
	WHILE @iPageNumber <= @iPageCount
	BEGIN	
		SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Beginning Iteration '+ CONVERT( nvarchar(50), @iPageNumber ) +' Of ' +CONVERT( nvarchar(50), @iPageCount ); 
		RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.

		INSERT INTO #tblSubjectPageSize ( 
			 SetId 
			,CustomerNumber 
			,RoutingNumber 
			,AccountNumber 
			,CustomerId 
			,PayerId
			,HashId
			,KeyElementId
			,PartitionId
		)
		SELECT s.SetId
			,s.CustomerNumber
			,s.RoutingNumber
			,s.AccountNumber
			,s.CustomerId
			,s.PayerId
			,s.HashId
			,ke.KeyElementId
			,ke.PartitionId
		FROM #tblSubject s
		INNER JOIN [stat].[KeyElement] ke -- grabbing KeyElementId and PartitionId at this point (by virtue of moving the HashId determination to the #tblSubject INSERT prior to this loop ) allows us to avoid the now commented out UPDATE immediately following this INSERT. LSW
			ON s.HashId = ke.HashId
		WHERE SetId = @iPageNumber;

		INSERT INTO #tblStatPageSize ( 
			 KeyElementId 
			,PartitionId
			,StatId 
			,StatValue 
		)
		SELECT sps.KeyElementId
			,sps.PartitionId
			,@siKCPMinCheckNumberClearedStatId AS StatId
			,TRY_CONVERT(nchar(50), MIN(t.CheckNumber)) AS KCPMinCheckNumberCleared
		FROM #tblSubjectPageSize sps 
		INNER JOIN [ValidFI].[dbo].[Transaction] t
			ON sps.PayerId = t.PayerId 
		INNER JOIN [ValidFI].[dbo].[Account] a
			ON t.AccountId = a.AccountId	-- 2018-10-16
				AND sps.CustomerId = a.CustomerId 
		WHERE t.CycleDate <= @dtClearedDay5Parameter
			AND t.ReturnStatusId = 0
			AND t.TransactionTypeId = 2
			AND t.CheckNumber > 10
		GROUP BY sps.KeyElementId
			,sps.PartitionId;

		INSERT INTO #tblStatPageSize ( 
			 KeyElementId 
			,PartitionId
			,StatId 
			,StatValue 
		)
		SELECT sps.KeyElementId
			,sps.PartitionId
			,@siKCPMaxCheckNumberClearedStatId
			,TRY_CONVERT(nchar(50), MAX(t.CheckNumber)) AS KCPMaxCheckNumberCleared
		FROM #tblSubjectPageSize sps 
		INNER JOIN [ValidFI].[dbo].[Transaction] t
			ON sps.PayerId = t.PayerId 
		INNER JOIN [ValidFI].[dbo].[Account] a
			ON t.AccountId = a.AccountId	-- 2018-10-16
				AND sps.CustomerId = a.CustomerId 
		WHERE t.CycleDate <= @dtClearedDay5Parameter
			AND t.ReturnStatusId = 0
			AND t.TransactionTypeId = 2
			AND t.CheckNumber > 10
		GROUP BY sps.KeyElementId
			,sps.PartitionId;

		BEGIN TRY 
			
		--If we find a matching KeyMemberId and StatId, update those records with our calculated values
		UPDATE st
		SET StatValue = s.StatValue
			,BatchLogId = @psiBatchLogId 
		FROM [stat].[StatTypeNchar50] st
		INNER JOIN #tblStatPageSize s
			ON st.PartitionId = s.PartitionId
				AND st.KeyElementId = s.KeyElementId
				AND st.StatId = s.StatId 
		WHERE s.StatId IN (@siKCPMinCheckNumberClearedStatId, @siKCPMaxCheckNumberClearedStatId)
			AND ( ISNULL( st.StatValue, N'-{null}+' ) <> ISNULL( s.StatValue, N'-{null}+' ) )
			--AND st.BatchLogId <> @psiBatchLogId)-- 
		;
			
		--If we dont find a matching KeyElementId and StatId in the typed table, insert a new record
		INSERT INTO [stat].[StatTypeNchar50](
			  PartitionId
			 ,KeyElementId
			 ,StatId
			 ,StatValue
			 ,BatchLogId
		)
		SELECT s.PartitionId
			,s.KeyElementId
			,s.StatId
			,s.StatValue 
			,@psiBatchLogId
		FROM #tblStatPageSize s
		WHERE s.StatValue IS NOT NULL
			AND s.StatId IN (@siKCPMinCheckNumberClearedStatId, @siKCPMaxCheckNumberClearedStatId)
			AND NOT EXISTS (SELECT 'X'
					FROM [stat].[StatTypeNchar50] st
					WHERE s.KeyElementId = st.KeyElementId
						AND s.StatId = st.StatId
					)
		;
						
		--Clean the PageSize tables
		TRUNCATE TABLE #tblSubjectPageSize;
		TRUNCATE TABLE #tblStatPageSize;

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

	SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Completed Execution'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; --Soft error that forces a refresh of the Messages tab.
END

GO
