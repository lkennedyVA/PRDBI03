USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspStatGroupMaxBatchUpsertOut]
	Created By: Larry Dugger
	Description: This upserts the StatGroupMaxBatch table.

	Tables: [stat].[BatchLog]
		,[stat].[StatGroupMaxBatch]

	Procedures: [error].[uspLogErrorDetailInsertOut]

	History:
		2018-01-21 - LBD - Created
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspStatGroupMaxBatchUpsertOut](
	 @pnvStatGroupName NVARCHAR(50)
	,@psiBatchProcessId SMALLINT
	,@puBatchProcessUId VARCHAR(50)
	,@puBatchUId VARCHAR(50)
)
AS
BEGIN	
	SET NOCOUNT ON;
	DECLARE @tblStatGroupMaxBatch TABLE (
		 StatGroupId smallint null
		,MaxBatchLogId smallint not null
	);
	DECLARE @iErrorDetailId int
		,@sSchemaName sysname = N'stat'
		,@dtDateActivated datetime2(7) = SYSDATETIME()
		,@iStatGroupId int
		,@siBatchLogId smallint
		,@uiBatchProcessUId UNIQUEIDENTIFIER = @puBatchProcessUId
		,@uiBatchUId UNIQUEIDENTIFIER = @puBatchUId;

	SELECT @iStatGroupId = StatGroupId
	FROM [stat].[StatGroup]
	WHERE [Name] = 'Account Utilization';

	BEGIN TRY
		IF NOT EXISTS (SELECT 'X'
						FROM [stat].[StatGroupMaxBatch] sgmb
						WHERE StatGroupId = @iStatGroupId)
		BEGIN
			INSERT INTO [stat].[StatGroupMaxBatch]( 
				 StatGroupId
				,MaxBatchLogId
			)
			OUTPUT inserted.StatGroupId
					,inserted.MaxBatchLogId
				INTO @tblStatGroupMaxBatch
			SELECT 
				 @iStatGroupId
				,BatchLogId
			FROM [stat].[BatchLog]
			WHERE BatchUId = @uiBatchUId
				AND BatchProcessId = @psiBatchProcessId
				AND BatchProcessUId = @uiBatchProcessUId
		END	
		ELSE --Prior record exists.
		BEGIN
			SELECT @siBatchLogId = BatchLogId
			FROM [stat].[BatchLog]
			WHERE BatchUId = @uiBatchUId
				AND BatchProcessId = @psiBatchProcessId
				AND BatchProcessUId = @uiBatchProcessUId;
			UPDATE sgmb
				SET MaxBatchLogId = @siBatchLogId
			FROM [stat].[StatGroupMaxBatch] sgmb
			WHERE sgmb.StatGroupId = @iStatGroupId;
		END
 	END TRY
	BEGIN CATCH
		EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
		SET @siBatchLogId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
		THROW
	END CATCH;
END

GO
