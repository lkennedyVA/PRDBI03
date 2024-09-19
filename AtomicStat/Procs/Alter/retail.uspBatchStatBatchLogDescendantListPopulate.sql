USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	EXEC pnc.uspBatchStatBatchLogDescendantListPopulate @piBatchId = 336

*/
ALTER PROCEDURE [retail].[uspBatchStatBatchLogDescendantListPopulate]
	(
		@piBatchId INT
	)
AS
BEGIN
	DROP TABLE IF EXISTS ##BatchStatBatchLogDescendant
	;
	CREATE TABLE ##BatchStatBatchLogDescendant
		(
			 StatBatchLogId INT PRIMARY KEY NOT NULL
			,BatchId INT NOT NULL
		)
	;
	INSERT INTO ##BatchStatBatchLogDescendant
		(
			 StatBatchLogId
			,BatchId
		)
	SELECT
		 StatBatchLogId
		,BatchId
	FROM retail.BatchStatBatchLogXref
	WHERE BatchId > @piBatchId
	;
END

GO
