USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspProcessAccountUtilization]
	Created By: Larry Dugger
	Description: Determines if Account Utilization needs to be processed, if so it removes
		old stat records, and adds new ones.

	Procedures: [error].[uspLogErrorDetailInsertOut]

	History:
		2018-01-21 - LBD - Created, varchar is used for ssis convenience
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspProcessAccountUtilization](
	 @psiBatchProcessId INT
	,@puBatchProcessUId VARCHAR(50)
	,@puBatchUId VARCHAR(50)
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @iErrorDetailId int
		,@sSchemaName sysname = N'stat'
		,@siMaxBatchLogId INT = 0
		,@siAUBatchLogId INT = 0
		,@iStatGroupId int
		,@iOrgId int = 100009
		,@dt datetime2(7) = SYSDATETIME()
		,@uiBatchProcessUId UNIQUEIDENTIFIER = @puBatchProcessUId
		,@uiBatchUId UNIQUEIDENTIFIER = @puBatchUId;

	SELECT @iStatGroupId = StatGroupId
	FROM [stat].[StatGroup]
	WHERE [Name] = 'Account Utilization';

	SELECT @siAUBatchLogId = BatchLogId
	FROM [stat].[BatchLog]
	WHERE BatchUId = @uiBatchUId
		AND BatchProcessId = @psiBatchProcessId
		AND BatchProcessUId = @uiBatchProcessUId;

	--Not in BatchLog yet
	IF @siAUBatchLogId = 0
		EXECUTE [stat].[uspBatchLogUpsertOut]
			 @psiBatchLogId = @siAUBatchLogId OUTPUT
			,@piOrgId =	@iOrgId
			,@pdtBatchStartDate	= @dt
			,@pdtBatchEndDate = @dt
			,@puiBatchUId = @uiBatchUId
			,@psiBatchProcessId = @psiBatchProcessId
			,@puiBatchProcessUId = @uiBatchProcessUId;
	
	--Has this batch already been processed?
	SET @siMaxBatchLogId = [stat].[ufnMaxBatchLog]('Account Utilization');

	--Is there a new one to process
	IF @siAUBatchLogId > @siMaxBatchLogId
	BEGIN
		BEGIN TRY
		EXECUTE [stat].[uspAccountUtilizationUpsert] @psiBatchLogId = @siAUBatchLogId;
		END TRY
		BEGIN CATCH
			EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
			SET @siAUBatchLogId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
			THROW
		END CATCH
		DELETE FROM [work].[AccountUtilization];
		SELECT @siAUBatchLogId as BatchLogId;
	END
	ELSE
		SELECT -1 as BatchLogId
END

GO
