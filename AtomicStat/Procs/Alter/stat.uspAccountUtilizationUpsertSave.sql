USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspAccountUtilizationUpsertSave]
	Created By: Larry Dugger
	Description: AccountUtilization insert update
		Performs no calcuation simply takes a data set of new batch data 
		and populates the new structures.
		BatchLogId must be passed in.

	Tables: [stat].[KeyElementType]
		,[stat].[StatTypeNumeric0109]
		,[work].[AccountUtilization]

	Procedures: [stat].[uspKeyElementMultiInsert]
		,[error].[uspLogErrorDetailInsertOut]

	Functions: [stat].[ufnStatId]
		,[stat].[ufnKeyTypeId]
		,[stat].[ufnStatDefaultValue]

	History:
		2018-01-15 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspAccountUtilizationUpsertSave](
	 @psiBatchLogId INT
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
 	CREATE INDEX #01Subject on #tblSubject(TokenSetId);
	CREATE INDEX #02Subject on #tblSubject(KeyReferenceId, KeyTypeId) include (TokenSetId);

	IF OBJECT_ID('tempdb..#tblPredicate', 'U') IS NOT NULL DROP TABLE #tblPredicate;
	CREATE TABLE #tblPredicate (
		 PartitionId tinyint not null default(0) 
		,KeyElementId bigint not null default(0)
		,TokenSetId bigint 
	);
	CREATE INDEX #01Predicate on #tblPredicate(TokenSetId) INCLUDE(PartitionId,KeyElementId);

	IF OBJECT_ID('tempdb..#tblObject', 'U') IS NOT NULL DROP TABLE #tblObject;
	CREATE TABLE #tblObject (
		 TokenSetId bigint 
		,StatId smallint
		,StatValue numeric(9,3)
	);
	CREATE INDEX #01Object on #tblObject(TokenSetId,StatId) INCLUDE(StatValue);

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
		,@xTableSet xml;

	--Only need one keytype here
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

	--Put Stat and Values here
	INSERT INTO #tblObject(TokenSetId,StatId,StatValue)
	SELECT
		 CustomerId AS TokenSetId 
		,@siAccountUtilizationBalanceStatId AS StatId
		,AccountUtilizationBalance AS StatValue
	FROM [work].[AccountUtilization]
	UNION
	SELECT 
		 CustomerId AS TokenSetId
		,@siAccountUtilizationDepositsStatId AS StatId
		,AccountUtilizationDeposits AS StatValue
	FROM [work].[AccountUtilization]
	UNION
	SELECT 
		 CustomerId AS TokenSetId
		,@siAccountUtilizationSpendStatId AS StatId
		,AccountUtilizationSpend AS StatValue
	FROM [work].[AccountUtilization]
	UNION
	SELECT 
		 CustomerId AS TokenSetId
		,@siAccountUtilizationOverallStatId AS StatId
		,AccountUtilizationOverall AS StatValue
	FROM [work].[AccountUtilization];

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
	FROM #tblSubject
	UNION 	--ADD in the OrgId KeyType
	SELECT 
		 s.HashId
		,s.PartitionId
		,s.KeyElementId 
		,OrgId AS KeyReferenceId
		,@siOrgIdKeyTypeId
		,s.TokenSetId 
	FROM #tblSubject s
	INNER JOIN [work].[AccountUtilization] au on s.KeyReferenceId = au.CustomerId
												and s.KeyTypeId = @siCustomerIdKeyTypeId;

	--Only execute the insert proc if there are records to process
	IF @@ROWCOUNT > 0 
		EXEC [stat].[uspKeyElementMultiInsert]@ptblKeyElementList=@tblKeyElementList,@pxTableSet=@xTableSet OUTPUT; 

	--Retrieve from xml
	--Insert into Predicate (Just what we need from xml)
	INSERT INTO #tblPredicate(PartitionId,KeyElementId,TokenSetId)
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
					FROM #tblSubject s0
 					INNER JOIN #tblPredicate p ON s0.TokenSetId = p.TokenSetId
					INNER JOIN #tblObject o ON s0.TokenSetId = o.TokenSetId) s 
			ON st.PartitionId = s.PartitionId	
				AND st.KeyElementId = s.KeyElementId
				AND  st.StatId = s.StatId
		WHERE st.StatValue <> s.StatValue
			AND st.BatchLogId <> @psiBatchLogId; 

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
				FROM #tblSubject s0
 				INNER JOIN #tblPredicate p ON s0.TokenSetId = p.TokenSetId
				INNER JOIN #tblObject o ON s0.TokenSetId = o.TokenSetId) s
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
END

GO
