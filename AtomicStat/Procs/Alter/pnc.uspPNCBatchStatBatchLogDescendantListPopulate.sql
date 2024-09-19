USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

	EXEC pnc.uspPNCBatchStatBatchLogDescendantListPopulate @psiPNCBatchId = 13

*/
ALTER PROCEDURE [pnc].[uspPNCBatchStatBatchLogDescendantListPopulate]
	(
		@psiPNCBatchId INT
	)
AS
BEGIN
	DROP TABLE IF EXISTS ##PNCBatchStatBatchLogDescendant
	;
	CREATE TABLE ##PNCBatchStatBatchLogDescendant
		(
			 StatBatchLogId INT PRIMARY KEY NOT NULL
			,PNCBatchId INT NOT NULL
		)
	;
	INSERT INTO ##PNCBatchStatBatchLogDescendant
		(
			 StatBatchLogId
			,PNCBatchId
		)
	SELECT
		 StatBatchLogId
		,PNCBatchId
	FROM pnc.PNCBatchStatBatchLogXref
	WHERE PNCBatchId > @psiPNCBatchId
	;
END

GO
