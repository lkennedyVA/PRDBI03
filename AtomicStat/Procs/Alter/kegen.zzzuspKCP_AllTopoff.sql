USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [kegen].[zzzuspKCP_AllTopoff]
	(
		@psiBatchLogId INT = NULL
	)
AS
/*
	Adds any new KCPs that have appeared since the last time kegen.KCP_All was refreshed or updated.
	
	Keep in mind, kegen.KCP_All only has relevance while dealing with the full history load and is not part of the regular process.
	Therefore, DO NOT build kegen.KCP_All into anything that is intended to be part of standard use.

*/
BEGIN
	SET NOCOUNT ON;

	DECLARE 
		 --@biCustomerIdMin bigint = ( SELECT MIN( x.CustomerId ) FROM ValidFI.dbo.Customer AS x WHERE x.BankId = 100003 AND x.CustomerId > 0 ) -- 54770316
		 @biCustomerIdMin bigint = ISNULL( ( SELECT MAX( x.CustomerId ) FROM kegen.KCP_All AS x ), 54770316 ) -- 72283902
		,@biCustomerIdMax bigint = ( SELECT MAX( x.CustomerId ) FROM ValidFI.dbo.Customer AS x WHERE x.BankId = 100003 )
		--,@biKCPCount bigint = ( SELECT COUNT(1) FROM Stat.stat.AllKCP AS k INNER JOIN ValidFI.dbo.Customer AS c ON k.ValidFICustomerId = c.CustomerId WHERE c.BankId = 100003 AND c.CustomerId > 0 ) -- Counted 199,664,572 in 00:01:18.
		,@biKCPCount bigint
		,@biSetSize bigint = 1000000
		,@siNTile smallint
		,@siBatchLogId INT = @psiBatchLogId
		,@biRowIdWatermark bigint
		,@dtFloor date = DATEADD( dd, -91, SYSDATETIME() )
	;
	IF @siBatchLogId IS NULL SELECT @siBatchLogId = MAX( x.BatchLogId ) FROM precalc.BatchRunValue AS x
	;
	SELECT @biKCPCount = COUNT(1) FROM ( SELECT DISTINCT a.CustomerId, t.PayerId FROM [ValidFI].[dbo].[Transaction] AS t INNER JOIN [ValidFI].[dbo].[Account] AS a ON t.AccountId = a.AccountId WHERE t.BankId = 100003 AND a.CustomerId > 0 ) AS x
	;


		DECLARE
			 @dtTimerDate datetime2(7) = SYSDATETIME()
			,@dtInitial datetime2(7) = SYSDATETIME()
			,@iCustomerNumberIdTypeId int = 25 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
			,@iErrorDetailId int
			,@iOrgId int = 100009 --NT AUTHORITY\ANONYMOUS LOGON error when trying to access PRDBI02 linked server
			,@iPageNumber int = 1
			,@iPageSize int = @biSetSize -- 100000
			,@iPageCount int = 1
			,@iRowCount int = 0
			,@nvCustomerNumberIdTypeId nvarchar(100) 
			,@nvMessageText nvarchar(256)
			,@nvOrgId nvarchar(100)  
			,@siClientOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('ClientOrgId')
			,@siIdTypeKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('IdType')
			,@siRoutingNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Routing Number')
			,@siAccountNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Account Number')
			,@siCustomerNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Customer Identifier')
			,@siFinancialKCPKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial KCP')
			,@nvWorker nvarchar(1024)
			,@nvKeyTemplate nvarchar(1024)
			,@nvMessage nvarchar(4000)
			,@nvSection nvarchar(4000)
			,@dt2TimeStamp datetime2(7)
			,@iEllapsedTimeInSeconds int
			,@siSetId smallint
		;

	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@nvMessage = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) 
			+ N'...kegen.uspKCP_AllTopoff Begin...'
	;
	RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 

	SELECT @biRowIdWatermark = MAX( RowId ) FROM [kegen].[KCP_All]
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
			,@iPageNumber = 0 -- 200
			,@siSetId = ( SELECT MAX( x.SetId ) FROM AtomicStat.kegen.KCP_All AS x ) + 1
			,@siNTile = ( CONVERT( numeric, @biKCPCount ) / CONVERT( numeric, @biSetSize ) ) + 1.0
		;
PRINT N'@siNTile = ' + CONVERT( nvarchar(50), @siNTile )

	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@nvMessage = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) 
			+ NCHAR(009) + N'Updating missing KeyElementId values'
	;
	RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 


	SET NOCOUNT OFF;
	UPDATE k
		SET k.HashId = 
			 CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
									,CONVERT(varbinary(512)
									,CONVERT(nvarchar(512)
										,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT( nvarchar(50), k.CustomerNumber ) )
											,'[{RoutingNumber}]', CONVERT( nvarchar(50), k.RoutingNumber ) )
												,'[{AccountNumber}]', CONVERT( nvarchar(50), k.AccountNumber ) )
										))),1)
			,k.KeyElementId = ke.KeyElementId
	FROM [kegen].[KCP_All] AS k
		INNER JOIN [AtomicStat].[stat].[KeyElement] AS ke
			ON CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
									,CONVERT(varbinary(512)
									,CONVERT(nvarchar(512)
										,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT( nvarchar(50), k.CustomerNumber ) )
											,'[{RoutingNumber}]', CONVERT( nvarchar(50), k.RoutingNumber ) )
												,'[{AccountNumber}]', CONVERT( nvarchar(50), k.AccountNumber ) )
							))),1) = ke.HashId
	WHERE ( k.KeyElementId IS NULL OR k.HashId IS NULL )
	;
	SET NOCOUNT ON;


--/ *
	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@nvMessage = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) 
			+ NCHAR(009) + N'Inserting new KCP rows : kegen.KCP_All'
	;
	RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 

	SET NOCOUNT OFF;
	INSERT INTO kegen.KCP_All ( SetId, CustomerNumber, RoutingNumber, AccountNumber, CustomerId, PayerId, HashId )
	SELECT 
	DISTINCT
			-- CASE WHEN @siSetId IS NULL THEN CONVERT( smallint, ( NTILE( @siNTile ) OVER ( ORDER BY c.[CustomerNumber], p.[RoutingNumber], p.[AccountNumber] ) ) ) ELSE @siSetId END AS SetId
			 @siSetId AS SetId
			,c.[CustomerNumber]
			,p.[RoutingNumber]
			,p.[AccountNumber]
			,a.[CustomerId]
			,t.[PayerId]
			--,CONVERT(binary(64)
			--					,HASHBYTES( N'SHA2_512'
			--						,CONVERT(varbinary(512)
			--						,CONVERT(nvarchar(512)
			--							,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT( nvarchar(50), c.CustomerNumber ) )
			--								,'[{RoutingNumber}]', CONVERT( nvarchar(50), p.RoutingNumber ) )
			--									,'[{AccountNumber}]', CONVERT( nvarchar(50), p.AccountNumber ) )
			--							))),1) AS HashId
			,ke.[HashId]
	
	FROM [ValidFI].[dbo].[Transaction] AS t
		INNER JOIN [ValidFI].[dbo].[Account] AS a
			ON t.AccountId = a.AccountId
		INNER JOIN [ValidFI].[dbo].[Customer] AS c
			ON a.CustomerId = c.CustomerId
		INNER JOIN [ValidFI].[dbo].[Payer] AS p
			ON t.PayerId = p.PayerId
		INNER JOIN [stat].[KeyElement] AS ke
			ON CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
									,CONVERT(varbinary(512)
									,CONVERT(nvarchar(512)
										,REPLACE( REPLACE( REPLACE( @nvWorker, '[{CustomerNumber}]', CONVERT( nvarchar(50), c.CustomerNumber ) )
											,'[{RoutingNumber}]', CONVERT( nvarchar(50), p.RoutingNumber ) )
												,'[{AccountNumber}]', CONVERT( nvarchar(50), p.AccountNumber ) )
										))),1) = ke.HashId
	WHERE t.BankId = 100003 -- PNC
		--AND t.CycleDate > @dtFloor
		AND a.CustomerId > 0
		AND t.PayerId > 0
		AND ( a.CustomerId BETWEEN @biCustomerIdMin AND @biCustomerIdMax )
		AND NOT EXISTS
			(
				SELECT 'X'
				FROM AtomicStat.kegen.KCP_All AS x
				WHERE x.[CustomerNumber] = c.[CustomerNumber]
					AND x.[RoutingNumber] = p.[RoutingNumber]
					AND x.[AccountNumber] = p.[AccountNumber]
			)
	ORDER BY c.[CustomerNumber], p.[RoutingNumber], p.[AccountNumber]
	;
	SET NOCOUNT ON;
--* /


	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@nvMessage = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) 
			+ NCHAR(009) + N'Inserting new KCP rows : kegen.KCP_PNC_SurrogateKeyXref'
	;
	RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 


	SET NOCOUNT OFF;
	INSERT INTO kegen.KCP_PNC_SurrogateKeyXref
		(
			 [CustomerId]
			,[PayerId]
			,[KeyElementId]
			,[PartitionId]
			,[BatchLogId]
		)
	SELECT
			 ka.[CustomerId]
			,ka.[PayerId]
			,ka.[KeyElementId]
			,ke.[PartitionId]
			,@siBatchLogId AS [BatchLogId]
	FROM kegen.KCP_All AS ka
		INNER JOIN stat.KeyElement AS ke
			ON ka.KeyElementId = ke.KeyElementId
	WHERE ka.RowId > @biRowIdWatermark
		AND NOT EXISTS
			(
				SELECT 'X'
				FROM kegen.KCP_PNC_SurrogateKeyXref AS x
				WHERE x.CustomerId = ka.CustomerId
					AND x.PayerId = ka.PayerId
			)
	;
	SET NOCOUNT ON;



	SELECT
		 @dt2TimeStamp = SYSDATETIME()
		,@nvMessage = NCHAR(013) + NCHAR(010) + CONVERT( nvarchar(50), @dt2TimeStamp, 121 ) + SPACE(1) 
			+ N'...kegen.uspKCP_AllTopoff ...End.'
	;
	RAISERROR (	@nvMessage, 0, 1 ) WITH NOWAIT; 

END

GO
