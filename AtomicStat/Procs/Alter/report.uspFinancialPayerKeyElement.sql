USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [report].[uspFinancialPayerKeyElement]
	Created By: Chris Sharp 
	Description: 
		Assign the KeyTemplatePreprocessed string for the KeyTypeId 
		the source tables for this procedure are specific to AtomicStat and Financial Payer.

		We don't anticipate records in [kegen].[KCP_PNC_SurrogateKeyXref] without a HashId 
		but we check for them and create them if necessary.   

		Using [kegen].[KCP_PNC_SurrogateKeyXref]  as a source, we page through the records
		a PageSize at a time looking for records where the KeyElementId doesn't exist in
		[report].[KeyElement].

		We create KeyReferenceIds for any RoutingNumbers or AccountNumbers that don't have 
		a record in [report].[KeyReference].
		
		Finally, we insert the Financial Payer KeyElement set into [report].[KeyElement].
			** The check for existance was already performed so for a single threaded process, 
				we shouldn't need a secondary check for existance into [report].[KeyElement] 
				prior to inserting.  If we have multiple processes processing in tandem, 
				we may want to uncomment the where not exists in lines 490-492 as a precaution.

	Parameters: @psiBatchLogId INT 
		,@piPageSize INT 
		,@pbiMinKeyElementId BIGINT (OPTIONAL)	

	Table(s): [stat].[KeyType]
		,[kegen].[KCP_PNC_SurrogateKeyXref] 
		,[stat].[KeyElement]
		,[report].[KeyElement]
		,[report].[KeyReference]
		,[ValidFI].[dbo].[Payer]

	Function(s): [report].[ufnSourceDataTypeIdByName]
		,[stat].[ufnKeyTypeIdByName]

	Procedure(s): [error].[uspLogErrorDetailInsertOut]

	History:
		2018-11-06 - CBS - Created	
*****************************************************************************************/
ALTER PROCEDURE [report].[uspFinancialPayerKeyElement](
	 @psiBatchLogId INT --= 36
	,@piPageSize INT --= 750000
	,@pbiMinKeyElementId BIGINT = NULL	--Optional, discussed storing the value of the last run in precalc.BatchRunValue
)
AS 
BEGIN
	SET NOCOUNT ON;
	IF OBJECT_ID('tempdb..#tblSubject', 'U') IS NOT NULL DROP TABLE #tblSubject;
	CREATE TABLE #tblSubject ( 
		 PayerId int
		,KeyElementId bigint 
		,PartitionId tinyint 
		,HashId binary(64) not null default(0x0)	--We don't anticipate getting to this point with records without a HashId but if we do, we'll create them
		,PRIMARY KEY CLUSTERED						
		( 
			 PayerId ASC
			,KeyElementId ASC
			,PartitionId ASC
		) WITH ( FILLFACTOR = 100 )
	);	
	IF OBJECT_ID('tempdb..#tblSubjectPageSize', 'U') IS NOT NULL DROP TABLE #tblSubjectPageSize; 
	CREATE TABLE #tblSubjectPageSize ( 
		 PartitionId tinyint 
		,RoutingNumber nvarchar(50)
		,AccountNumber nvarchar(50)
		,HashId binary(64) not null default(0x0)
		,KeyElementId bigint not null default(0)
		,PRIMARY KEY CLUSTERED 
		( 
			 PartitionId ASC
			,KeyElementId ASC
			,RoutingNumber ASC
			,AccountNumber ASC
			,HashId ASC
		) WITH ( FILLFACTOR = 100 )
	);	
	IF OBJECT_ID('tempdb..#tblKeyReference', 'U') IS NOT NULL DROP TABLE #tblKeyReference;
	CREATE TABLE #tblKeyReference ( 
		 KeyTypeId smallint 
		,ExternalReferenceValue nvarchar(100)
		,SourceDataTypeId tinyint
		,KeyReferenceValue nvarchar(100)
		,KeyReferenceId bigint default(0)	
		,KeyElementId bigint not null default(0)
		,PartitionId tinyint not null default(0)
		,HashId binary(64) not null default(0x0)
		,PRIMARY KEY CLUSTERED 
		( 
			 KeyReferenceId ASC			--KeyReferenceId added as first field in PK to avoid a table scan in line 408
			,KeyTypeId ASC				--		IF EXISTS (SELECT 'X' FROM #tblKeyReference WHERE KeyReferenceId = 0) 
			,KeyReferenceValue ASC
			,SourceDataTypeId ASC
			,KeyElementId ASC
			,PartitionId ASC
			,HashId ASC
		) WITH ( FILLFACTOR = 100 )
	);
	IF OBJECT_ID('tempdb..#tblKeyReferenceInsertTemplate', 'U') IS NOT NULL DROP TABLE #tblKeyReferenceInsertTemplate;
	CREATE TABLE #tblKeyReferenceInsertTemplate ( 
		 KeyTypeId smallint not null
		,SourceDataTypeId tinyint not null
		,KeyReferenceValue nvarchar(100) not null
		,KeyReferenceId bigint not null 
	);
	DECLARE @iPageSize int = @piPageSize
		,@siBatchLogId INT = @psiBatchLogId
		,@biMinKeyElementIdParameter bigint = ISNULL(@pbiMinKeyElementId, 0)
		,@biMinKeyElementId bigint
		,@biUpperBoundKeyElementId bigint
		,@siKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Financial Payer')
		,@iPartitionNumber int 
		,@iPartitionCount int 
		,@iOrgId int = 100009 --Do We Really Need Multiple Variables for PNC Client OrgId Going Forward?
		,@iRowCount int 
		,@nvMessageText nvarchar(256)
		,@nvOrgId nvarchar(100)  
		,@nvKeyTemplatePreprocessed nvarchar(1024) 
		,@siClientOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('ClientOrgId')
		,@siRoutingNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Routing Number')
		,@siAccountNumberKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('Account Number')
		,@siOrgIdKeyTypeId smallint = [stat].[ufnKeyTypeIdByName]('OrgId')
		,@tiCustomerNumberSourceDataTypeId tinyint = [report].[ufnSourceDataTypeIdByName]('nvarchar(50)')					--Source Data TypeIds
		,@tiRoutingNumberSourceDataTypeId tinyint = [report].[ufnSourceDataTypeIdByName]('nchar(9)')
		,@tiAccountNumberSourceDataTypeId tinyint = [report].[ufnSourceDataTypeIdByName]('nvarchar(50)')
		,@tiIdTypeSourceDataTypeId tinyint = [report].[ufnSourceDataTypeIdByName]('int')
		,@tiClientOrgIdSourceDataTypeId tinyint = [report].[ufnSourceDataTypeIdByName]('int')
		,@iErrorDetailId int	
		,@sSchemaName sysname = 'stat';
	
	SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Beginning Execution'; 
	RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; 

	--Pre-converting OrgId and CustomerNumberIdTypeId to a string 
	--Instead of converting these values with each iteration
	SET @nvOrgId = TRY_CONVERT(nvarchar(100), @iOrgId);

	--Assign the KeyTemplatePreprocessed string for the @psiKeyTypeId passed in 
	--The source tables for this procedure are specific to AtomicStat and Financial KCP
	SELECT @nvKeyTemplatePreprocessed = KeyTemplatePreprocessed
	FROM [stat].[KeyType]
	WHERE KeyTypeId = @siKeyTypeId;

	--@iPartitionCount: total number of iterations given @piPageSize
	SELECT @iPartitionCount = (COUNT(1) / @iPageSize) + 1
		,@biMinKeyElementId = MIN(KeyElementId) 
		,@iPartitionNumber = 0
	FROM [kegen].[KCP_PNC_SurrogateKeyXref] 
	WHERE BatchLogId = @siBatchLogId;

	--If @biMinKeyElementIdParameter has a value, set @biMinKeyElementId equal to 
	--That value and roll with it
	IF @biMinKeyElementIdParameter <> 0 
		SET @biMinKeyElementId = @biMinKeyElementIdParameter;

	SET @biUpperBoundKeyElementId = @biMinKeyElementId + @iPageSize;

	WHILE @iPartitionNumber <= @iPartitionCount
	BEGIN
		SET @iRowCount = 0;
		SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'......... Beginning Outer Loop '+ CONVERT( nvarchar(50), @iPartitionNumber ) +' Of ' +CONVERT( nvarchar(50), @iPartitionCount ); 
		RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; 

		INSERT INTO #tblSubject ( 
			 PayerId 
			,KeyElementId
			,PartitionId
			,HashId 
		)
		SELECT DISTINCT kcp.PayerId
			,kcp.KeyElementId
			,kcp.PartitionId
			,ISNULL(ke.HashId, 0x0)
		FROM [kegen].[KCP_PNC_SurrogateKeyXref]  kcp  
		LEFT JOIN [stat].[KeyElement] ke								
			ON kcp.KeyElementId = ke.KeyElementId						
		WHERE kcp.KeyElementId >= @biMinKeyElementId						
			AND kcp.KeyElementId <= @biUpperBoundKeyElementId
			AND NOT EXISTS (SELECT 'X'
						FROM [report].[KeyElement] ke 
						WHERE kcp.KeyElementId = ke.KeyElementId)
		ORDER BY kcp.PayerId
			,kcp.KeyElementId
			,kcp.PartitionId;

		SET @iRowCount = @@ROWCOUNT;

		IF @iRowCount <> 0 
		BEGIN
			SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + CONVERT(nvarchar(50), @iRowCount) +' Records Inserted into #tblSubject'; 
			RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; 
		END;
			
		--If no records are inserted into #tblSubject that dont already exist in report.KeyElement, 
		--Jump to end of the loop...
		IF @iRowCount <> 0
		BEGIN
			SET @iRowCount = 0;

			--Records that exist without a HashId shouldnt exist but we create them if necessary
			IF EXISTS (SELECT 'X' FROM #tblSubject WHERE HashId = 0x0)
			BEGIN 				
				UPDATE s
				SET s.HashId = CONVERT(binary(64)
							,HASHBYTES( N'SHA2_512'
								,CONVERT(varbinary(512)
								,CONVERT(nvarchar(512)
									,REPLACE( REPLACE( @nvKeyTemplatePreprocessed
										,'[{RoutingNumber}]', CONVERT(nvarchar(50),p.RoutingNumber ) )
											,'[{AccountNumber}]', CONVERT(nvarchar(50),p.AccountNumber ))					
									))),1)		
				FROM #tblSubject s 
				INNER JOIN [ValidFI].[dbo].[Payer] p
					ON s.PayerId = p.PayerId
				WHERE HashId = 0x0;
			END
			
			--Grab Customer and Payer information from ValidFI and storing as nvarchar(50) within #tblSubjectPageSize
			INSERT INTO #tblSubjectPageSize ( 
				 RoutingNumber			
				,AccountNumber			
				,KeyElementId
				,PartitionId 
				,HashId 
			)
			SELECT TRY_CONVERT(nvarchar(50),p.RoutingNumber)	
				,TRY_CONVERT(nvarchar(50),p.AccountNumber)
				,s.KeyElementId
				,s.PartitionId
				,s.HashId 
			FROM #tblSubject s 
			INNER JOIN [ValidFI].[dbo].[Payer] p
				ON s.PayerId = p.PayerId
			ORDER BY s.PayerId ASC;

			--ClientOrgId KeyReference Creation 
			INSERT INTO #tblKeyReference(
				 KeyTypeId 
				,ExternalReferenceValue 
				,SourceDataTypeId 
				,KeyReferenceValue
				,KeyReferenceId 
				,KeyElementId
				,PartitionId
				,HashId
			)
			SELECT DISTINCT @siClientOrgIdKeyTypeId AS KeyTypeId
				,@nvOrgId AS ExternalReferenceValue 
				,@tiClientOrgIdSourceDataTypeId AS SourceDataTypeId
				,@nvOrgId AS KeyReferenceValue
				,ISNULL(kr.KeyReferenceId, 0) AS KeyReferenceId
				,s.KeyElementId
				,s.PartitionId
				,s.HashId
			FROM #tblSubjectPageSize s 
			LEFT JOIN [report].[KeyReference] kr
				ON kr.KeyTypeId = @siClientOrgIdKeyTypeId
					AND kr.KeyReferenceValue = @nvOrgId;
					
			--RoutingNumber KeyReference Creation  
			INSERT INTO #tblKeyReference(
				 KeyTypeId 
				,ExternalReferenceValue 
				,SourceDataTypeId 
				,KeyReferenceValue
				,KeyReferenceId 
				,KeyElementId
				,PartitionId
				,HashId
			)
			SELECT DISTINCT @siRoutingNumberKeyTypeId  
				,s.RoutingNumber
				,@tiRoutingNumberSourceDataTypeId 
				,s.RoutingNumber AS KeyReferenceValue
				,ISNULL(kr.KeyReferenceId, 0)
				,s.KeyElementId
				,s.PartitionId
				,s.HashId
			FROM #tblSubjectPageSize s
			LEFT JOIN [report].[KeyReference] kr
				ON kr.KeyTypeId = @siRoutingNumberKeyTypeId
					AND kr.SourceDataTypeId = @tiRoutingNumberSourceDataTypeId
					AND kr.KeyReferenceValue = s.RoutingNumber;

			--AccountNumber KeyReference Creation 
			INSERT INTO #tblKeyReference(
				 KeyTypeId 
				,ExternalReferenceValue 
				,SourceDataTypeId 
				,KeyReferenceValue
				,KeyReferenceId 
				,KeyElementId
				,PartitionId
				,HashId
			)
			SELECT DISTINCT @siAccountNumberKeyTypeId  
				,s.AccountNumber
				,@tiAccountNumberSourceDataTypeId 
				,s.AccountNumber AS KeyReferenceValue
				,ISNULL(kr.KeyReferenceId, 0)
				,s.KeyElementId
				,s.PartitionId
				,s.HashId
			FROM #tblSubjectPageSize s
			LEFT JOIN [report].[KeyReference] kr
				ON kr.KeyTypeId = @siAccountNumberKeyTypeId
					AND kr.SourceDataTypeId = @tiAccountNumberSourceDataTypeId
					AND kr.KeyReferenceValue = s.AccountNumber;

			BEGIN TRY
					
				--For performance, output any created KeyReferenceIds into a temp table (heap)
				IF OBJECT_ID('tempdb..#tblKeyReferenceInsert', 'U') IS NOT NULL DROP TABLE #tblKeyReferenceInsert;
				SELECT KeyTypeId 
					,SourceDataTypeId 
					,KeyReferenceValue
					,KeyReferenceId 
				INTO #tblKeyReferenceInsert
				FROM #tblKeyReferenceInsertTemplate;

				--If we have KeyReferenceIds to create well handle that in this section
				IF EXISTS (SELECT 'X' FROM #tblKeyReference WHERE KeyReferenceId = 0)
				BEGIN
					INSERT INTO [report].[KeyReference](
						 KeyTypeId
						,KeyReferenceValue
						,SourceDataTypeId
					)
					OUTPUT inserted.KeyTypeId
						,inserted.SourceDataTypeId	
						,inserted.KeyReferenceValue
						,inserted.KeyReferenceId
					INTO #tblKeyReferenceInsert
					SELECT DISTINCT KeyTypeId
						,KeyReferenceValue
						,SourceDataTypeId
					FROM #tblKeyReference  
					WHERE KeyReferenceId = 0;

					--Now that weve inserted our new values, we need to add a non-named primary key to the table
					--If we were to create a named primary key, the name would need to be unique globally in tempdb
					--CAUTION FROM .Lee, its possible to remove an existing primary key on a different table if 
					--One exists...  Hence the non-named PK
					ALTER TABLE #tblKeyReferenceInsert ADD PRIMARY KEY CLUSTERED ( 
						 KeyTypeId ASC
						,KeyReferenceValue ASC
						,SourceDataTypeId ASC
						,KeyReferenceId ASC 
						) WITH ( FILLFACTOR = 100 );

					--Update the KeyReferenceId in #tblKeyReference with any newly created KeyReferenceIds
					UPDATE kr
					SET kr.KeyReferenceId = kri.KeyReferenceId
					FROM #tblKeyReference kr
					INNER JOIN #tblKeyReferenceInsert kri
						ON kri.KeyTypeId = kr.KeyTypeId
							AND kri.KeyReferenceValue = kr.KeyReferenceValue
							AND kri.SourceDataTypeId = kr.SourceDataTypeId
					WHERE kr.KeyReferenceId = 0;
				END		
							
				--Insert into [report].[KeyElement]
				--At this point, we've already checked for existance so for a single threaded process, we
				--Shouldn't need a check for existance prior to inserting.  If multiple processes are processing
				--the same dataset, it may make sense to add a check for existence here
				INSERT INTO [report].[KeyElement] ( 
					 HashId
					,PartitionId
					,KeyElementId
					,KeyReferenceId
					,KeyTypeId
					,BatchLogId
				)
				SELECT kr.HashId
					,kr.PartitionId
					,kr.KeyElementId
					,kr.KeyReferenceId
					,kr.KeyTypeId
					,@siBatchLogId
				FROM #tblKeyReference kr
				--WHERE NOT EXISTS (SELECT 'X'
				--				FROM [report].[KeyElement] ke 
				--				WHERE kr.KeyElementId = ke.KeyElementId);	
	 
				SET @iRowCount = @@ROWCOUNT;	
			
				SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'Inserted Another '+CONVERT(nvarchar(50),@iRowCount)+' Records into [report].[KeyElement]'; 
				RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; 
				SET @iRowCount = 0;
				
				--Clean temp table in preparation for the next iteration
				TRUNCATE TABLE #tblKeyReference;

				--Since we avoided naming the PK on #tblKeyReferenceInsert due to concerns with dropping a named PK
				--We drop the table and recreate it with each iteration using #tblKeyReferenceTemplate as our source
				DROP TABLE #tblKeyReferenceInsert;
							
				END TRY
				BEGIN CATCH
					EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId = @iErrorDetailId OUTPUT;
					SET @iErrorDetailId = -1 * @iErrorDetailId; 
					THROW;
				END CATCH	
			END
			
		--Clean temp table in preparation for the next iteration	
		TRUNCATE TABLE #tblSubject;
		TRUNCATE TABLE #tblSubjectPageSize;

		--We need to increment @biMinKeyElementId and @biUpperBoundKeyElementId to capture the next @iPageSize of records
		SET @biMinKeyElementId = @biUpperBoundKeyElementId + 1; 
		SET @biUpperBoundKeyElementId = @biUpperBoundKeyElementId + @iPageSize;
		SET @iPartitionNumber += 1;
			
		--Outputting the values of MinKeyElementId and UpperBoundKeyElementId every 10th iteration for Development purposes
		--If we need to restart the script this gives us the most recent value of MinKeyElementId as a starting point
		IF @iPartitionNumber % 20 = 0
		BEGIN
			CHECKPOINT;
			SET @nvMessageText = NCHAR(009) + CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'MinKeyElement Value '+CONVERT(nvarchar(50),@biMinKeyElementId)+', UpperBoundKeyElement Value '+CONVERT(nvarchar(50),@biUpperBoundKeyElementId)+', Partition Value '+CONVERT(nvarchar(50),@iPartitionNumber);   
			RAISERROR( @nvMessageText, 0, 1 ) WITH NOWAIT; 
		END			

		WAITFOR DELAY '00:00:00.1';
				
	END
END

GO
