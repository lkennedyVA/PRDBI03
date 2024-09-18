USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspBatchLogUpsertOut]
	Created By: Chris Sharp
	Description: This procedure inserts a record instanciating a Stat Batch.

	Tables: [stat].[BatchLog]

	Procedures: [error].[uspLogErrorDetailInsertOut]

	History:
		2017-08-29 - CBS - Created
		2017-09-27 - CBS - Modified, adjusted @piBatchLogId to small int data type
		2018-01-21 - LBD - Modified, adjusted for new structure, now allows for
			external batches to be added to this table. If inserting external, 
			the record will be completed 
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspBatchLogUpsertOut](
	 @psiBatchLogId SMALLINT OUTPUT --2017-09-27
	,@piOrgId INT = NULL
	,@pdtBatchStartDate DATETIME2(7) = NULL
	,@pdtBatchEndDate DATETIME2(7) = NULL
	,@puiBatchUId UNIQUEIDENTIFIER = 0x
	,@psiBatchProcessId SMALLINT = 0
	,@puiBatchProcessUId UNIQUEIDENTIFIER = 0x
)
AS
BEGIN	
	SET NOCOUNT ON;
	SET @pdtBatchStartDate = ISNULL( @pdtBatchStartDate, SYSDATETIME() )
	;
	DECLARE @tblBatchLog TABLE (
		 BatchLogId smallint null
		,OrgId int not null
		,BatchStartDate datetime2(7) null 
		,BatchEndDate datetime2(7) null
		,ProcessBeginDate datetime2(7) not null
		,ProcessingEndDate datetime2(7) null 
		,BatchUId uniqueidentifier not null 
		,BatchProcessId smallint not null
		,BatchProcessUId uniqueidentifier not null
	);
	DECLARE @iErrorDetailId int
		,@sSchemaName sysname = N'stat'
		,@dtDateActivated datetime2(7) = SYSDATETIME();

	BEGIN TRY

		IF NOT EXISTS (SELECT 'X'
					FROM [stat].[BatchLog] 
					WHERE BatchLogId = @psiBatchLogId)
		BEGIN
			INSERT INTO [stat].[BatchLog]( 
				 OrgId
				,BatchStartDate
				,BatchEndDate
				,ProcessBeginDate
				,ProcessingEndDate 
				,BatchUId
				,BatchProcessId
				,BatchProcessUId
			)
			OUTPUT inserted.BatchLogId
				 ,inserted.OrgId
				 ,inserted.BatchStartDate
				 ,inserted.BatchEndDate
				 ,inserted.ProcessBeginDate
				 ,inserted.ProcessingEndDate
				 ,inserted.BatchUId
				 ,inserted.BatchProcessId
				 ,inserted.BatchProcessUId
			  INTO @tblBatchLog
			SELECT 
				 @piOrgId
				,@pdtBatchStartDate AS BatchStartDate
				,@pdtBatchEndDate AS BatchEndDate
				,@dtDateActivated AS ProcessBeginDate
				,CASE WHEN @psiBatchProcessId = 0 THEN NULL ELSE @dtDateActivated END AS ProcessingEndDate
				,CASE WHEN ISNULL(@puiBatchUId,0x) = 0x THEN NEWID() ELSE @puiBatchUId END AS BatchUId
				,@psiBatchProcessId
				,@puiBatchProcessUId;

			SELECT @psiBatchLogId = BatchLogId
			FROM @tblBatchLog;
		END	
		ELSE IF EXISTS (SELECT 'X'
					FROM [stat].[BatchLog] 
					WHERE BatchLogId = @psiBatchLogId)
		BEGIN
			UPDATE bl
				SET ProcessingEndDate = @dtDateActivated
			FROM [stat].[BatchLog] bl 
			WHERE BatchLogId = @psiBatchLogId;
		END	

	END TRY
	BEGIN CATCH
	BEGIN
		EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
		SET @psiBatchLogId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
		THROW
	END
	END CATCH;
END

GO
