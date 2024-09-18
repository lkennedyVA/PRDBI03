USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	EXEC mtb.uspMTBBatchStatBatchLogDescendantListPopulate @psiMTBBatchId = 13

*/
ALTER PROCEDURE [mtb].[uspMTBBatchStatBatchLogDescendantListPopulate]
	(
		@psiMTBBatchId smallint
	)
AS
BEGIN
	DROP TABLE IF EXISTS ##MTBBatchStatBatchLogDescendant
	;
	CREATE TABLE ##MTBBatchStatBatchLogDescendant
		(
			 StatBatchLogId smallint PRIMARY KEY NOT NULL
			,MTBBatchId smallint NOT NULL
		)
	;
	INSERT INTO ##MTBBatchStatBatchLogDescendant
		(
			 StatBatchLogId
			,MTBBatchId
		)
	SELECT
		 StatBatchLogId
		,MTBBatchId
	FROM mtb.MTBBatchStatBatchLogXref
	WHERE MTBBatchId > @psiMTBBatchId
	;
END

GO
