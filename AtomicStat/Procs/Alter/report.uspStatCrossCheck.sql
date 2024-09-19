USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [report].[uspStatCrossCheck]
(
	@pnvClientCode nvarchar(5) = NULL -- if an incorrect value or no value is supplied, the sproc will return a list of valid ClientCode values to use
)
AS
/*

	This sproc pulls together data elements from...

		* the source code of the Client's relative [AtomicStat] "uspStatExportBulk_TransferToAtomicStat" sproc
		* [???Risk].[dbo].[StatExportBulk]
		* [AtomicStat].[report].[vwStatGroupStatKeyType]
		* [AtomicStat].[stat].[Stat]

	...allowing a comparison matrix to be built for the sake of more quickly examining the stat config data for discrepencies.

	Example:

		EXEC [report].[uspStatCrossCheck] @pnvClientCode = N'MTB'
		EXEC [report].[uspStatCrossCheck] @pnvClientCode = N'TDB'
		EXEC [report].[uspStatCrossCheck] @pnvClientCode = N'FTB'
		EXEC [report].[uspStatCrossCheck] @pnvClientCode = N'PNC'
		EXEC [report].[uspStatCrossCheck] @pnvClientCode = N'FNB'

*/
BEGIN -- [report].[uspStatCrossCheck]

SET NOCOUNT ON
;
/*
	
	Set @nvClientCode to FTB, TDB, or MTB.

	@nvClientCode drives the rest of the script.

*/
IF NOT EXISTS
	(
		SELECT 'X'
		FROM
			(	
				VALUES
					 ( N'MTB' )
					,( N'TDB' )
					,( N'FTB' )
					,( N'PNC' )
					,( N'FNB' )
			) AS x ( [ClientCode] )
		WHERE x.ClientCode = ISNULL( @pnvClientCode, N'' )
	)
	BEGIN
		SELECT [@pnvClientCode must be one of the following] = x.[ClientCode]
		FROM
			(	
				VALUES
					 ( N'MTB' )
					,( N'TDB' )
					,( N'FTB' )
					,( N'PNC' )
					,( N'FNB' )
			) AS x ( [ClientCode] )
		;
		RETURN (-1)
	END
;

DECLARE @nvClientCode nvarchar(5) = @pnvClientCode -- N'MTB'
;

DECLARE 
	  @nvSearchForObjectRef nvarchar(256) = CASE 
																WHEN @nvClientCode = N'PNC' THEN N'stat.uspStatExportBulk_TransferToAtomicStat'
																WHEN @nvClientCode = N'FTB' THEN N'ftb.uspStatExportBulk_TransferToAtomicStat'
																WHEN @nvClientCode = N'TDB' THEN N'tdb.uspStatExportBulk_TransferToAtomicStat_manualesque_2'
																WHEN @nvClientCode = N'MTB' THEN N'mtb.uspStatExportBulk_TransferToAtomicStat'
																WHEN @nvClientCode = N'FNB' THEN N'fnb.uspStatExportBulk_TransferToAtomicStat'
																ELSE N'???'
															END
	, @nvObjectSourceCode nvarchar(max)
	, @nvStatEvaluationSourceCode nvarchar(max)
	, @iResultCount int
	, @nvSearchStringBegin nvarchar(100) = N',CASE -- StatId is evaluated in order of' -- search string to find the top of StatId evaluation 
	, @nvSearchStringEnd nvarchar(100) = N'END AS StatValue' -- search string to find the end of StatId evaluation
	, @iPosStringBegin int = 0
	, @iPosStringEnd int = 0
	, @nvWHENBegin nvarchar(100) = N'WHEN seb.StatId IN' -- search string to find the beginning of a "WHEN" evaluator
	, @iWHENBegin int = 0
	, @nvStatList nvarchar(max) = N''
	, @nvSourceColumn nvarchar(max) = N''
	, @nvFetchAllSampleRowsSql nvarchar(max) = N''
	, @siStatIdMax smallint = 0
	, @nvSqlObjectSource nvarchar(max)
	, @nvSourceDatabase nvarchar(128) = DB_NAME()
;
--IF @nvClientCode = N'PNC' SET @nvSourceDatabase = ( @nvClientCode + N'AtomicStat' )
SET @nvSourceDatabase = CASE WHEN @nvClientCode IN( N'PNC', N'FNB' ) THEN ( @nvClientCode + N'AtomicStat' ) ELSE @nvSourceDatabase END
;


DROP TABLE IF EXISTS #ObjectSourceCode
;
DROP TABLE IF EXISTS #StatBase0
;
DROP TABLE IF EXISTS #StatBase
;
DROP TABLE IF EXISTS #SampleRow
;
DROP TABLE IF EXISTS #StatList
;
CREATE TABLE #StatList 
( 
	[SourceColumn] nvarchar(128),
	[StatId] smallint 
)
;
CREATE TABLE #SampleRow
(
	[ClientCode] nvarchar(10) NOT NULL,
	[StatName] nvarchar(100) NOT NULL,
	[SourceColumnContainingData] nvarchar(128) NOT NULL,

	[StatExportBulkId] [bigint] NOT NULL,
	[StatBuildName] [nvarchar](20) NOT NULL,
	[StatBuildBatchId] [int] NOT NULL,
	[StatBuildCycleDate] [date] NOT NULL,
	[InsertDatetime] [datetime2](7) NULL,
	[StatId] [int] NULL,
	[KeyTypeId] [int] NULL,
	[ParentOrgId] [int] NULL,
	[CustomerIdIFA] [bigint] NULL,
	[IdTypeId] [int] NULL,
	[IdOrgId] [int] NULL,
	[IdStateId] [int] NULL,
	[IdMac] [varbinary](64) NULL,
	[CustomerNumberStringFromClient] [nvarchar](256) NULL,
	[CustomerAccountNumber] [nvarchar](30) NULL,
	[ClientOrgId] [int] NULL,
	[PayerClientOrgId] [int] NULL,
	[PayerRoutingNumber] [nvarchar](30) NULL,
	[PayerAccountNumber] [nvarchar](30) NULL,
	[LengthPayerAccountNumber] [int] NULL,
	[ChannelId] [int] NULL,
	[LocationOrgId] [int] NULL,
	[LocationOrgCode] [nvarchar](25) NULL,
	[GeoLarge] [nvarchar](20) NULL,
	[GeoSmall] [nvarchar](20) NULL,
	[GeoZip4] [nvarchar](20) NULL,
	[RetailDollarStrat] [decimal](16, 2) NULL,
	[DollarStrat] [decimal](16, 2) NULL,
	[StatValueInt] [int] NULL,
	[StatValueBigInt] [bigint] NULL,
	[StatValueDecimal1602] [decimal](16, 2) NULL,
	[StatValueDecimal1604] [decimal](16, 4) NULL,
	[StatValueNChar100] [nvarchar](100) NULL,
	[StatValueDate] [date] NULL,
	[StatValueBit] [bit] NULL,
	[StatValueDatetime] [datetime] NULL
)
;


DROP TABLE IF EXISTS #ObjectSourceCode
;

CREATE TABLE #ObjectSourceCode (
		 DatabaseName nvarchar(128)
		,SchemaName nvarchar(128)
		,ObjectType nvarchar(128)
		,ObjectName nvarchar(128)
		,CreateDate datetime
		,ModifyDate datetime
		,ObjectSourceCode  nvarchar(max)
	)
;

-- Fetch the sproc's source code...
SET @nvSqlObjectSource = N'
SELECT 
DISTINCT 
    DB_Name() AS DatabaseName
   ,s.name AS SchemaName
   ,o.type_desc AS ObjectType
   ,o.name AS ObjectName
   ,o.create_date AS CreateDate
   ,o.modify_date AS ModifyDate
   ,m.definition /*+ CHAR(013) + CHAR(010) + N''GO'' + CHAR(013) + CHAR(010)*/ AS ObjectSourceCode
--INTO #ObjectSourceCode
FROM [{SourceDatabase}].sys.schemas s 
   FULL OUTER JOIN [{SourceDatabase}].sys.objects o 
		ON s.schema_id = o.schema_id
	FULL OUTER JOIN [{SourceDatabase}].sys.sql_modules m 
		ON o.object_id = m.object_id
WHERE 1 = 1
   --AND m.definition LIKE ss.SearchString
	AND ( s.name + N''.'' + o.name ) = N''[{SearchForObjectRef}]''
ORDER BY
	 s.name -- SchemaName
	,o.type_desc
	,o.name -- ObjectName
;'
;
SET @nvSqlObjectSource = REPLACE( REPLACE( @nvSqlObjectSource, N'[{SourceDatabase}]', @nvSourceDatabase ), N'[{SearchForObjectRef}]', @nvSearchForObjectRef )
;

INSERT INTO #ObjectSourceCode ( DatabaseName, SchemaName, ObjectType, ObjectName, CreateDate, ModifyDate, ObjectSourceCode )
EXEC (@nvSqlObjectSource)
;


SELECT @iResultCount = COUNT(1) FROM #ObjectSourceCode
;
IF ( @iResultCount ) = 1
	BEGIN
		--SELECT MessageText = N'Found: ' + @nvSearchForObjectRef;
		--PRINT N'Found: ' + @nvSearchForObjectRef;

		SELECT @nvObjectSourceCode = osc.ObjectSourceCode FROM #ObjectSourceCode AS osc
		;
		SET @iPosStringBegin = CHARINDEX( @nvSearchStringBegin, @nvObjectSourceCode, 1 )
		;

		IF @iPosStringBegin > 0
		BEGIN -- We found the beginning of the StatId evaluation CASE statement

			SET @iPosStringEnd = CHARINDEX( @nvSearchStringEnd, @nvObjectSourceCode, @iPosStringBegin )
			;

			IF @iPosStringEnd > 0
			BEGIN -- We have the StatId evaluation CASE statement to work with

				SET @nvStatEvaluationSourceCode = SUBSTRING( @nvObjectSourceCode, @iPosStringBegin, ( @iPosStringEnd - @iPosStringBegin ) )
				;
				SET @nvStatEvaluationSourceCode = REPLACE( REPLACE( @nvStatEvaluationSourceCode, NCHAR(013) + NCHAR(010), NCHAR(013) ), NCHAR(010), NCHAR(013) )
				;
				--PRINT @nvStatEvaluationSourceCode
				SET @nvSearchStringBegin = NCHAR(013)
				;
				SET @iPosStringBegin = CHARINDEX( @nvSearchStringBegin, @nvStatEvaluationSourceCode, 1 )
				SET @nvStatEvaluationSourceCode = SUBSTRING( @nvStatEvaluationSourceCode, @iPosStringBegin, POWER(2,30) )
				--PRINT @nvStatEvaluationSourceCode
				--PRINT N''
				--PRINT N'================================================================================================================================================'
				--PRINT N'================================================================================================================================================'
				;
			

				/*

					Remove all in-line comments from the StatId evaluation CASE statement.

					If block comments get added within the full CASE statement, then code will need to be added to remove any comment blocks as well.

				*/
				WHILE CHARINDEX( N'--', @nvStatEvaluationSourceCode, 1 ) > 0
				BEGIN
					SET @iPosStringBegin = CHARINDEX( N'--', @nvStatEvaluationSourceCode, 1 )
					;
					SET @iPosStringEnd = CHARINDEX( NCHAR(013), @nvStatEvaluationSourceCode, @iPosStringBegin )
					;
					SET @nvStatEvaluationSourceCode = SUBSTRING( @nvStatEvaluationSourceCode, 1, ( @iPosStringBegin - 1 ) ) + SUBSTRING( @nvStatEvaluationSourceCode, @iPosStringEnd, POWER(2,30) )
					;				
				END

			
				/*
				
					Obtain the lists of StatId values by iterating through the list of WHEN/THEN statements of the StatId evaluation CASE statement.

				*/
				WHILE CHARINDEX( @nvWHENBegin, @nvStatEvaluationSourceCode, 1 ) > 0
				BEGIN
					--PRINT N''
					;
					-- Find the start of the next available WHEN...
					SET @iWHENBegin = CHARINDEX( @nvWHENBegin, @nvStatEvaluationSourceCode, 1 )
					;
					SET @nvStatEvaluationSourceCode = SUBSTRING( @nvStatEvaluationSourceCode, @iWHENBegin, POWER(2,30) )
					;
				
					-- Find the start of the list of StatId values...
					SET @iPosStringBegin = CHARINDEX( N'(', @nvStatEvaluationSourceCode, 1 )
					;
					SET @nvStatEvaluationSourceCode = SUBSTRING( @nvStatEvaluationSourceCode, ( @iPosStringBegin + 1 ), POWER(2,30) )
					;
				
					-- Find the end of the list of StatId values...
					SET @iPosStringEnd = CHARINDEX( N')', @nvStatEvaluationSourceCode, 1 )
					;
					SET @nvStatList = SUBSTRING( @nvStatEvaluationSourceCode, 1, ( @iPosStringEnd - 1 ) )
					;
					--PRINT @nvStatList
					;
					SET @nvStatEvaluationSourceCode = SUBSTRING( @nvStatEvaluationSourceCode, ( @iPosStringEnd + 1 ), POWER(2,30) )
					;

					-- Find the [???Risk].[dbo].[StatExportBulk].{column} containing the calculated stat values related to the current list of StatId values...
					SET @iPosStringBegin = CHARINDEX( N'seb.', @nvStatEvaluationSourceCode, 1 )
					;
					SET @iPosStringEnd = CHARINDEX( N',', @nvStatEvaluationSourceCode, @iPosStringBegin )
					;
					SET @nvSourceColumn = SUBSTRING( @nvStatEvaluationSourceCode, ( @iPosStringBegin + 4 ), ( @iPosStringEnd - @iPosStringBegin - 4 ) )
					;
					--PRINT @nvSourceColumn
					;
				
					-- Add the list of StatId values to #StatList...
					INSERT INTO #StatList ( SourceColumn, StatId )
					SELECT [SourceColumn] = @nvSourceColumn, [StatId] = LTRIM( RTRIM( t.[ValueText] ) ) 
					FROM [DBA].[dbo].[fnStringToTableWithIDN]( @nvStatList, N',' ) AS t -- turn the StatId list string into a table

				END -- Obtain the lists of StatId values

	--			SELECT * FROM #StatList
				;

			END -- We have the StatId evaluation CASE statement to work with -- @iPosStringEnd > 0

		END -- We found the beginning of the StatId evaluation CASE statement
		--, @nvSearchStringEnd nvarchar(100) = N'END AS StatValue' -- end of StatId evaluation
		--, @iPosStringEnd int = 0
	
	END
ELSE
	BEGIN

		-- "There can be only one."
		SELECT MessageText = CASE WHEN @iResultCount = 0 THEN N'No' ELSE N'Too many' END + N' candidate rows found for ' + @nvSearchForObjectRef
		;
		SELECT * FROM #ObjectSourceCode
		;

	END
;


;with cteClientCode as
	(
		select [ClientCode] = @nvClientCode
		--select [ClientCode] = N'???' -- for test/debug
	)
select 
	  cc.ClientCode
	, r.StatId
	, r.StatName
	, [Risk_DataTypeDesc] = r.DataTypeDesc
	, [AtomicStat_DataType] = s.DataType
	, s.TargetTable
	, [AtomicStat_sproc_expectedSourceColumn] = p.SourceColumn
	, [HasStatGroupXref] = CASE WHEN EXISTS( select 'X' from [report].[vwStatGroupStatKeyType] as x where x.AncestorStatGroupName = cc.ClientCode and x.StatId = r.StatId ) THEN 1 ELSE 0 END
	, [IsFlaggedForExportToHub] = CASE WHEN EXISTS( select 'X' from [report].[vwStatGroupStatKeyType] as x where x.AncestorStatGroupName = cc.ClientCode and x.StatId = r.StatId and x.ExportToDestinationId > 0 ) THEN 1 ELSE 0 END

	, [CheckForExistenceSql] = 
			REPLACE( 
				REPLACE( 
					REPLACE( 
						REPLACE( 
							  N'select [ClientCode] = N''[{ClientCode}]'', [StatName] = N''[{StatName}]'', [StatId] = [{StatId}], [RecCount] = count(1) from [[{ClientCode}]Risk].[dbo].[StatExportBulk] as seb where seb.StatId = [{StatId}] and seb.[{SourceColumn}] is not null'
							, N'[{SourceColumn}]', p.SourceColumn )
						, N'[{StatId}]', CONVERT( nvarchar(10), r.StatId ) )
					, N'[{ClientCode}]', cc.ClientCode )
				, N'[{StatName}]', ISNULL( s.[Name], N'Woops!' ) ) 

	, [FetchSampleRowSql] = 
			REPLACE( 
				REPLACE( 
					REPLACE( 
						  N'select top 1 [ClientCode] = N''[{ClientCode}]'', [StatName] = N''[{StatName}]'', [SourceColumnContainingData] = case when seb.StatValueInt is not null then N''StatValueInt'' when seb.StatValueBigInt is not null then N''StatValueBigInt'' when seb.StatValueDecimal1602 is not null then N''StatValueDecimal1602'' when seb.StatValueDecimal1604 is not null then N''StatValueDecimal1604'' when seb.StatValueNChar100 is not null then N''StatValueNChar100'' when seb.StatValueDate is not null then N''StatValueDate'' when seb.StatValueBit is not null then N''StatValueBit'' when seb.StatValueDatetime is not null then N''StatValueDatetime'' else NULL end, seb.* from [[{ClientCode}]Risk].[dbo].[StatExportBulk] as seb where seb.StatId = [{StatId}] and ( seb.StatValueInt is not null or seb.StatValueBigInt is not null or seb.StatValueDecimal1602 is not null or seb.StatValueDecimal1604 is not null or seb.StatValueNChar100 is not null or seb.StatValueDate is not null or seb.StatValueBit is not null or seb.StatValueDatetime is not null )'
						, N'[{StatId}]', CONVERT( nvarchar(10), r.StatId ) )
					, N'[{ClientCode}]', cc.ClientCode )
				, N'[{StatName}]', ISNULL( s.[Name], N'Woops!' ) )

into #StatBase0
from (
		-- Fetch data from all of the [???Risk].[dbo].[AtomicStatXref] tables...
		select [AncestorStatGroupName] = N'FTB', StatId, StatName, DataTypeDesc from [FTBRisk].[dbo].[AtomicStatXref] where ActiveFlag = 1
		union -- allow the removal of duplicates
		select [AncestorStatGroupName] = N'TDB', StatId, StatName, DataTypeDesc from [TDBRisk].[dbo].[AtomicStatXref] where ActiveFlag = 1
		union -- allow the removal of duplicates
		select [AncestorStatGroupName] = N'MTB', StatId, StatName, DataTypeDesc from [MTBRisk].[dbo].[AtomicStatXref] where ActiveFlag = 1
		-- ...in theory, the content from any [???Risk].[dbo].[AtomicStatXref] table is the same as any other.  But...
	) as r
	inner join cteClientCode as cc on r.AncestorStatGroupName = cc.ClientCode
	left join [stat].[Stat] as s on r.StatId = s.StatId
	left join #StatList as p on r.StatId = p.StatId -- #StatList is the list of StatId values inside the client specific sproc
where 1 = 1
--	and ( r.DataTypeDesc <> REPLACE( REPLACE( REPLACE( s.DataType, N'numeric', N'decimal' ), N'nchar', N'nvarchar' ), N'(50)', N'(100)' ) or p.SourceColumn is null ) -- looking for differences
----	and exists( select 'X' from [report].[vwStatGroupStatKeyType] as x where x.AncestorStatGroupName = cc.ClientCode and x.StatId = r.StatId )
order by r.StatId
;

select *
into #StatBase
from #StatBase0
where 1 = 1
	and ( 
			-- DataType difference
			[Risk_DataTypeDesc] <> REPLACE( REPLACE( REPLACE( [AtomicStat_DataType], N'numeric', N'decimal' ), N'nchar', N'nvarchar' ), N'(50)', N'(100)' ) 
			
			or ( 
					[AtomicStat_sproc_expectedSourceColumn] is null 
					and [HasStatGroupXref] = 1
				)
			
			or (
					[AtomicStat_sproc_expectedSourceColumn] is not null 
					and [HasStatGroupXref] = 0
				)
		) -- looking for differences

--select * from #StatBase

select @siStatIdMax = max( StatId ) from #StatBase
;
--PRINT N'@siStatIdMax = ' + CONVERT( nvarchar(10), @siStatIdMax )
;

-- Build the query to pull one sample row per stat from [???Risk].[dbo].[StatExportBulk]...
select 
	@nvFetchAllSampleRowsSql = 
		( 
			@nvFetchAllSampleRowsSql 
				+ isnull( 
						sb.[FetchSampleRowSql] 
							+ case 
									when sb.StatId <> @siStatIdMax 
										then NCHAR(013) + NCHAR(010) + N'union all' + NCHAR(013) + NCHAR(010) 
									else N'' 
								end 
						, N'' )
		) 
from #StatBase as sb 
--where StatId = @siStatIdMax -- for testing; ensuring only 1 sample query is used.
order by StatId
;

--PRINT N''
--PRINT @nvFetchAllSampleRowsSql
--EXEC [DBA].[dbo].[uspPrintFullString] @pnvString = @nvFetchAllSampleRowsSql
;

RAISERROR ( N'Please be patient.  This could take a while.  Fetching sample rows...', 0, 1 ) WITH NOWAIT
;
WAITFOR DELAY '00:00:01'
;
RAISERROR ( N'', 0, 1 ) WITH NOWAIT
;

INSERT INTO #SampleRow
EXEC (@nvFetchAllSampleRowsSql)
;

select
	 b.[ClientCode]
	,b.[StatId]
	,b.[StatName]
	,b.[Risk_DataTypeDesc]
	,b.[AtomicStat_DataType]
	,b.[TargetTable]
	,b.[AtomicStat_sproc_expectedSourceColumn]
	,[Risk_StatExportBulk_actualSourceColumn] = r.SourceColumnContainingData
	,b.[HasStatGroupXref]
	,b.[IsFlaggedForExportToHub]
	,b.[CheckForExistenceSql]
	,b.[FetchSampleRowSql]
from #StatBase as b
	left join #SampleRow as r
		on b.StatId = r.StatId
;

END -- [report].[uspStatCrossCheck]
;

GO
