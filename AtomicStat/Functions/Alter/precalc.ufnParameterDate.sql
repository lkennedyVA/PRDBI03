USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [precalc].[ufnParameterDate]
	Created By: Lee Whiting
	Description: This function accepts a Parameter Reference (Name), and if found,
		returns the parameter's assigned date value.

	Tables: [precalc].[Parameter]

	Example: SELECT ParameterValue AS IncrementalStartCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalStartCycleDate', default );
				SELECT ParameterValue AS IncrementalStartCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalStartCycleDate', 13 );
				SELECT ParameterValue AS IncrementalEndCycleDate FROM [precalc].[ufnParameterDate]( 'IncrementalEndCycleDate', default );
				SELECT ParameterValue AS Nonsense FROM [precalc].[ufnParameterDate]( 'nonSense', -42 );

	History:
		2018-09-18 - LSW - Created
		2018-10-08 - LSW - Added parameter @psiBatchLogId
							If "default" or NULL, then the query looks for the matching 
								ParameterReference in the associated last BatchLogId inserted rows.
*****************************************************************************************/
ALTER FUNCTION [precalc].[ufnParameterDate]
(
	 @pnvParameterReference nvarchar(128)
	,@psiBatchLogId int = NULL
)
RETURNS table
AS RETURN ( 
		SELECT TOP 1 @pnvParameterReference AS ParameterReference, TRY_CONVERT( date, brv.[RunValueAsString] ) AS ParameterValue
		FROM [precalc].[BatchRunValue] AS brv
		WHERE brv.[RunValueKeyReference] = @pnvParameterReference
			AND (
					( @psiBatchLogId IS NULL AND brv.BatchLogId = ( SELECT ISNULL( MAX( x.BatchLogId ), 0 ) FROM [precalc].[BatchRunValue] AS x ) )
					OR
					( @psiBatchLogId IS NOT NULL AND brv.BatchLogId = @psiBatchLogId )
				)
	);
