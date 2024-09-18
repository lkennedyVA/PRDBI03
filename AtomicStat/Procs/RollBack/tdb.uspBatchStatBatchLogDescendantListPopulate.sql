USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	EXEC tdb.uspBatchStatBatchLogDescendantListPopulate @piBatchId = 336

*/
ALTER PROCEDURE [tdb].[uspBatchStatBatchLogDescendantListPopulate]
	(
		@piBatchId smallint
	)
AS
BEGIN
	DROP TABLE IF EXISTS ##BatchStatBatchLogDescendant
	;
	CREATE TABLE ##BatchStatBatchLogDescendant
		(
			 StatBatchLogId smallint PRIMARY KEY NOT NULL
			,BatchId smallint NOT NULL
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
	FROM tdb.BatchStatBatchLogXref
	WHERE BatchId > @piBatchId
	;
END

GO
