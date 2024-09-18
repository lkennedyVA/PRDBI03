USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspAccountUtilizationUpsert]
	Created By: Larry Dugger
	Description: AccountUtilization insert update
		Performs no calcuation simply takes a data set of new batch data 
		and populates the new structures.
		BatchLogId must be passed in.

	Tables: [stat].[KeyElementType]
		,[stat].[StatTypeNumeric0109]
		,[work].[AccountUtilization]

	Procedures: [stat].[uspKeyElementMultiOnlyInsert]
		,[error].[uspLogErrorDetailInsertOut]

	Functions: [stat].[ufnStatId]
		,[stat].[ufnKeyTypeId]
		,[stat].[ufnStatDefaultValue]

	History:
		2018-01-15 - LBD - Created
		2018-01-23 - LBD - Modified, adjusted to run pagesize sets, pagesize is 10k
		2018-01-25 - LBD - Modified, adjusted to run using new MultiOnly
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspAccountUtilizationUpsert](
	 @psiBatchLogId SMALLINT
)
AS
BEGIN
	SET NOCOUNT ON;
	
	IF OBJECT_ID('tempdb..#tblSubject', 'U') IS NOT NULL DROP TABLE #tblSubject;
	CREATE TABLE #tblSubject (
		 HashId binary(64) not null default(0x0) 
		,PartitionId tinyint not null default(0) 
		,KeyElementId bigint not null default(0)
		,KeyReferenceId bigint
		,KeyTypeId smallint 
		,TokenSetId bigint
	);
	CREATE TABLE #tblSubjectPageSize (
		 HashId binary(64) not null default(0x0) 
		,PartitionId tinyint not null default(0) 
		,KeyElementId bigint not null default(0)
		,KeyReferenceId bigint
		,KeyTypeId smallint 
		,TokenSetId bigint
	);
	CREATE INDEX #01Subject on #tblSubjectPageSize(TokenSetId);
	CREATE INDEX #02Subject on #tblSubjectPageSize(KeyReferenceId, KeyTypeId) include (TokenSetId);

	IF OBJECT_ID('tempdb..#tblPredicate', 'U') IS NOT NULL DROP TABLE #tblPredicate;
	CREATE TABLE #tblPredicatePageSize (
		 PartitionId tinyint not null default(0) 
		,KeyElementId bigint not null default(0)
		,TokenSetId bigint 
	);
	CREATE INDEX #01Predicate on #tblPredicatePageSize(TokenSetId) INCLUDE(PartitionId,KeyElementId);

	IF OBJECT_ID('tempdb..#tblObject', 'U') IS NOT NULL DROP TABLE #tblObject;
	CREATE TABLE #tblObjectPageSize (
		 TokenSetId bigint 
		,StatId smallint
		,StatValue numeric(9,3)
	);
	CREATE INDEX #01Object on #tblObjectPageSize(TokenSetId,StatId) INCLUDE(StatValue);

	DECLARE @tblKeyElementList [stat].[KeyElementType];

	DECLARE @iErrorDetailId int 
		,@sSchemaName sysname = N'stat'
		,@dtDateActivated datetime2(7) = SYSDATETIME()
		,@siAccountUtilizationBalanceStatId smallint = [stat].[ufnStatId]('AccountUtilizationBalance')	
		,@siAccountUtilizationDepositsStatId smallint = [stat].[ufnStatId]('AccountUtilizationDeposits')
		,@siAccountUtilizationSpendStatId smallint = [stat].[ufnStatId]('AccountUtilizationSpend')
		,@siAccountUtilizationOverallStatId smallint = [stat].[ufnStatId]('AccountUtilizationOverall')
		,@siCustomerIdKeyTypeId smallint = [stat].[ufnKeyTypeId]('Account Utilization CustomerId')   
		,@siOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeId]('Account Utilization OrgId') 
		,@siAccountUtilizationKeyTypeId smallint = [stat].[ufnKeyTypeId]('Account Utilization')
		,@xTableSet xml
		,@iPageSize int = 10000;

	--Only need one keytype here, full load
	INSERT INTO #tblSubject (
		 KeyReferenceId
		,KeyTypeId
		,TokenSetId
	)
	SELECT 
		 CustomerId AS KeyReferenceId
		,@siCustomerIdKeyTypeId AS KeyTypeId
		,CustomerId AS TokenSetId
	FROM [work].[AccountUtilization]
	WHERE Processed = 0;

	--LOOP PROCESSING @iPageSize at a time
	WHILE 1 = 1 
	BEGIN
		--take the first pagesize
		INSERT INTO #tblSubjectPageSize (
			 KeyReferenceId
			,KeyTypeId
			,TokenSetId
		)
		SELECT TOP (@iPageSize)
			 KeyReferenceId
			,KeyTypeId
			,TokenSetId
		FROM #tblSubject
		ORDER BY KeyReferenceId;
			
		--Put Stat and Values here
		INSERT INTO #tblObjectPageSize(TokenSetId,StatId,StatValue)
		SELECT
			 au.CustomerId AS TokenSetId 
			,@siAccountUtilizationBalanceStatId AS StatId
			,au.AccountUtilizationBalance AS StatValue
		FROM [work].[AccountUtilization] au
		INNER JOIN #tblSubjectPageSize sps ON au.CustomerId = sps.KeyReferenceId
		UNION
		SELECT 
			 au.CustomerId AS TokenSetId
			,@siAccountUtilizationDepositsStatId AS StatId
			,au.AccountUtilizationDeposits AS StatValue
		FROM [work].[AccountUtilization] au
		INNER JOIN #tblSubjectPageSize sps ON au.CustomerId = sps.KeyReferenceId
		UNION
		SELECT 
			 au.CustomerId AS TokenSetId
			,@siAccountUtilizationSpendStatId AS StatId
			,au.AccountUtilizationSpend AS StatValue
		FROM [work].[AccountUtilization] au
		INNER JOIN #tblSubjectPageSize sps ON au.CustomerId = sps.KeyReferenceId
		UNION
		SELECT 
			 au.CustomerId AS TokenSetId
			,@siAccountUtilizationOverallStatId AS StatId
			,au.AccountUtilizationOverall AS StatValue
		FROM [work].[AccountUtilization] au
		INNER JOIN #tblSubjectPageSize sps ON au.CustomerId = sps.KeyReferenceId;

		--For this table we need a record for each keymember the set.
		INSERT INTO @tblKeyElementList (
			 HashId
			,PartitionId
			,KeyElementId
			,KeyReferenceId
			,KeyTypeId
			,TokenSetId
		) 
		SELECT 
			 HashId
			,PartitionId
			,KeyElementId
			,KeyReferenceId
			,KeyTypeId
			,TokenSetId
		FROM #tblSubjectPageSize
		UNION 	--ADD in the OrgId KeyType
		SELECT 
			 s.HashId
			,s.PartitionId
			,s.KeyElementId 
			,OrgId AS KeyReferenceId
			,@siOrgIdKeyTypeId
			,s.TokenSetId 
		FROM #tblSubjectPageSize s
		INNER JOIN [work].[AccountUtilization] au on s.KeyReferenceId = au.CustomerId;

		--Only execute the insert proc if there are records to process
		IF @@ROWCOUNT > 0 
			EXEC [stat].[uspKeyElementMultiOnlyInsert]@ptblKeyElementList=@tblKeyElementList,@pxTableSet=@xTableSet OUTPUT; 

		--Retrieve from xml
		--Insert into Predicate (Just what we need from xml)
		INSERT INTO #tblPredicatePageSize(PartitionId,KeyElementId,TokenSetId)
		SELECT DISTINCT CONVERT(tinyint,r.a.value('PartitionId[1]','VARCHAR(512)')) AS PartitionId
			,CONVERT(bigint,r.a.value('KeyElementId[1]','VARCHAR(512)')) AS KeyElementId	
			,CONVERT(bigint,r.a.value('TokenSetId[1]','VARCHAR(512)')) AS TokenSetId 
		FROM @xTableSet.nodes('KeyElement/.') r(a);

		BEGIN TRY 	
			--If we find a matching KeyMemberId and StatId
			--in the typed table, update those records with our 
			--calculated values
			UPDATE st
				SET StatValue = s.StatValue
					,BatchLogId = @psiBatchLogId 
			FROM [stat].[StatTypeNumeric0109] st 
			INNER JOIN (SELECT p.PartitionId, p.KeyElementId, o.StatId, o.StatValue 
						FROM #tblSubjectPageSize s0
 						INNER JOIN #tblPredicatePageSize p ON s0.TokenSetId = p.TokenSetId
						INNER JOIN #tblObjectPageSize o ON s0.TokenSetId = o.TokenSetId) s 
				ON st.PartitionId = s.PartitionId	
					AND st.KeyElementId = s.KeyElementId
					AND  st.StatId = s.StatId
			WHERE st.StatValue <> s.StatValue; 

			--If we dont find a matching KeyMemberId and StatId
			--in the typed table, insert a new record
			INSERT INTO [stat].[StatTypeNumeric0109](
				  PartitionId
				 ,KeyElementId
				 ,StatId
				 ,StatValue
				 ,BatchLogId
			)
			SELECT DISTINCT
				 s.PartitionId
				,s.KeyElementId
				,s.StatId
				,s.StatValue
				,@psiBatchLogId
			FROM (SELECT p.PartitionId, p.KeyElementId, o.StatId, o.StatValue 
					FROM #tblSubjectPageSize s0
 					INNER JOIN #tblPredicatePageSize p ON s0.TokenSetId = p.TokenSetId
					INNER JOIN #tblObjectPageSize o ON s0.TokenSetId = o.TokenSetId) s
			WHERE NOT EXISTS (SELECT 'X'
						FROM [stat].[StatTypeNumeric0109] 
						WHERE s.PartitionId = PartitionId	
							AND s.KeyElementId = KeyElementId
							AND s.StatId = StatId);
		END TRY
		BEGIN CATCH
			BEGIN
				EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
				SET @iErrorDetailId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
				THROW;
			END
		END CATCH
		--Update the Processed Flag
		UPDATE au
			SET Processed = 1
		FROM [work].[AccountUtilization] au
		INNER JOIN #tblSubjectPageSize s on au.CustomerId = s.TokenSetId;

		IF @@ROWCOUNT = 0
			BREAK;
		--Remove the PageSize processed records, from source
		DELETE s
		FROM #tblSubject s
		INNER JOIN #tblSubjectPageSize sps on s.TokenSetId = sps.TokenSetId;
		--Clean the PageSize tables
		DELETE FROM @tblKeyElementList;
		TRUNCATE TABLE #tblSubjectPageSize;
		TRUNCATE TABLE #tblPredicatePageSize;
		TRUNCATE TABLE #tblObjectPageSize;
	END
END

GO
