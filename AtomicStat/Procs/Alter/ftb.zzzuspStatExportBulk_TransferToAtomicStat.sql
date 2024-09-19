USE [AtomicStat]
 /*********NO BATCHID, WAS PULLED BECAUSE OF THE SMALLINT FOR KEYID*********/
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON 
GO
/****************************************************************************************
	Name: ftb.uspStatExportBulk_TransferToAtomicStat

	Description: 
		Used [t d b].[uspStatExportBulk_TransferToAtomicStat_manualesque_2] as a base.

	Example:

		First)
			SELECT TOP 3 'Before initiating the transfer:', * FROM [FTBRisk].[dbo].[StatBatch] ORDER BY 1 DESC
			;
			DECLARE @iStatBatchLogId int = -1 -- supplying -1 for StatBatchLogId causes a new row to be inserted into stat.BatchLog.
				,@iBatchId int = #p1# -- being manualesque, this must be supplied from [FTBRisk].[dbo].[StatBatch]
			;
			EXEC [ftb].[uspStatExportBulk_TransferToAtomicStat] @piStatBatchLogId = @iStatBatchLogId OUTPUT, @ptiDebug = 1
			;
			SELECT @iBatchId = BatchId FROM [ftb].[BatchStatBatchLogXref] WHERE StatBatchLogId = @iStatBatchLogId
			;
			SELECT 'Id values established and/or determined:', StatBatchLogId, BatchId FROM [ftb].[BatchStatBatchLogXref] WHERE StatBatchLogId = @iStatBatchLogId
			;
			SELECT '[FTBRisk].[dbo].[StatBatch]', * FROM [FTBRisk].[dbo].[StatBatch] WHERE StatBatchId = @iBatchId
			;
			SELECT '[stat].[BatchLog]', * FROM [stat].[BatchLog] WHERE BatchLogId = @iStatBatchLogId
			;
			SELECT '[ftb].[BatchStatBatchLogXref]', * FROM [ftb].[BatchStatBatchLogXref] WHERE BatchId = @iBatchId OR StatBatchLogId = @iStatBatchLogId
			;
			SELECT '[ftb].[StatValueBulk]', @iStatBatchLogId AS StatBatchLogId, COUNT(1) AS RecCount FROM [ftb].[StatValueBulk] WHERE StatBatchLogId = @iStatBatchLogId
			;
			SELECT '[ftb].[vwBatchTransferToHubAvailable]', * FROM [ftb].[vwBatchTransferToHubAvailable]
			;
			--SELECT * FROM [stat].[vwBatchTransferToHubAvailable]
			;

		Second)
			EXEC [ftb].[uspQueueStatTransferToHub] @piStatBatchLogId = #p2# -- the value output'd to @iStatBatchLogId by "ftb.uspStatExportBulk_TransferToAtomicStat" above.
			;
			If a value is not supplied for @piStatBatchLogId, the sproc will determine the earliest available batch and add it to the Hub's queue.
		
	History:
		2020-08-17 - VALIDRS\LWhiting - Created.  Based upon [m t b].[uspStatExportBulk_TransferToAtomicStat].
		2020-09-14 - VALIDRS\LWhiting - Final prep before enabling the Stat Batch Management job.  ie: go from a dev-test posture to a live/production posture.
		2020-09-27 - VALIDRS\LWhiting - Added automatic sub-batching via bin packing 
												(ie: it attempts to "fill each bin" as close to 100% capacity as reasonably possible)
												in order to reduce pressure/contention regarding the log and object/resource locking.
		2021-04-02 - VALIDRS\LWhiting - CCF2483: default StatId 103 to a NULL value as step 1 towards changing its default value to 0.
		.
		2022-03-23 - VALIDRS\CSharp - VALID-154: Update the logic to properly promote bit stats with a non-default value to bring them in line with PNC
*****************************************************************************************/
ALTER PROCEDURE [ftb].[zzzuspStatExportBulk_TransferToAtomicStat]
(
		 @piBatchId int -- @piBatchId is a required parameter - it does not have a default value.
		,@piStatBatchLogId int = -1 OUTPUT -- -32337 /*OUTPUT*/
		,@ptiDebug tinyint = 1 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
		,@pbiBinCapacity bigint = 10000000 -- defaults to 10 million rows per bin
--declare
--		 @piBatchId int = 73 -- 1 --TO-DO: remove the "= 1" when done testing as a script
--		,@piStatBatchLogId int = 2317 -- -1 --OUTPUT -- -32337 /*OUTPUT*/
--		,@ptiDebug tinyint = 1 -- 0 = no feedback, 1+ = depth of output feedback to Message tab.
	)
AS
BEGIN
----print 'Intentionally stalled [ftb].[uspStatExportBulk_TransferToAtomicStat] to avoid accidental execution.'; return; --TO-DO: remove this line once ready to run this sproc	
	SET NOCOUNT ON;
	DECLARE @iStatBatchLogId int = @piStatBatchLogId
		,@iBatchId int = ISNULL( @piBatchId, 0 )
		,@tiDebug tinyint = CASE WHEN ISNULL( @ptiDebug, 0 ) > 0 THEN @ptiDebug + 1 ELSE 0 END
		,@nvThisSourceCode nvarchar(4000)
		,@biRecCount bigint = 0
		,@biDeltaCheck bigint = 0
		,@biUpdateAvailableRecCount bigint = 0
		,@biInsertAvailableRecCount bigint = 0
		,@nvMessage nvarchar(4000)
		,@iSubBatch_RowSeq int = 0
		,@iSubBatchSeq int = 0
		,@iSubBatchStatCount int = 0
		,@dCycleDate date
		,@nvStatGenerator nvarchar(128)
		,@nvExec nvarchar(max)
		,@iStatBatchLogId_Max int
		,@iClientOrgId int = ( SELECT sg.OrgId FROM stat.StatGroup AS sg WHERE sg.[Name] = N'FTB' AND sg.[StatGroupId] = sg.[AncestorStatGroupId] )
		,@dtThisBegin datetime = getdate()
		,@dtThisEnd datetime
		,@tThisDuration time
		,@iError int = 0
		,@biBinCapacity bigint = @pbiBinCapacity
		,@iBinRowSeqMin int = 0
		,@iBinRowSeqMax int = 0
		,@iBinRowSeq int = 0
		,@iBinSeq int = 0
	;

	SET @tiDebug = CASE WHEN ISNULL( @ptiDebug, 0 ) > 0 THEN @ptiDebug + 1 ELSE 1 END 
	;
		SET @nvThisSourceCode = dbo.ufnExecutingObjectString( @@SPID, @@PROCID, HOST_NAME(), @@SERVERNAME, DB_NAME(), SUSER_SNAME() )
		;
		PRINT N''
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvThisSourceCode
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N' Begin...'
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT
		;

	DECLARE @nvSubject nvarchar(256)
		,@nvBody nvarchar(4000)
		,@nvServerName nvarchar(128) = N'[' + @@ServerName + N']'
		,@nvJobName nvarchar(128) = N''
		,@nvStepName nvarchar(128) = N''
		,@nvJobRef nvarchar(256) = N'"' + @nvThisSourceCode + N'"'
	;

	SELECT 
		 @nvJobName = ISNULL( p.JobName, N'{not running under a job}' )
		,@nvStepName = ISNULL( p.JobStepName, N'{not running under a job}' )
		,@nvJobRef = ISNULL( p.ProgramNameParsed, N'"' + @nvThisSourceCode + N'"' ) -- N'{not running under a job}'
	FROM [DBA].[dbo].[ufnSysProcessesProgramNameParse]( ( SELECT TOP 1 s.program_name FROM master.sys.dm_exec_sessions s WHERE s.session_id = @@SPID ) ) AS p
	;


	DROP TABLE IF EXISTS #tmpGeoUnique
	;
	SELECT * INTO #tmpGeoUnique FROM [stat].[GeoUnique] gu
	;
	ALTER TABLE #tmpGeoUnique ADD PRIMARY KEY CLUSTERED ( [GeoCode] )
	;
	DROP TABLE IF EXISTS #tmpDollarStratRange
	;
	SELECT * INTO #tmpDollarStratRange FROM [stat].[DollarStratRange] dsr
	;
	ALTER TABLE #tmpDollarStratRange ADD PRIMARY KEY CLUSTERED ( [DollarStratRangeId] )
	;
	DROP TABLE IF EXISTS #tmpChannel
	;
	SELECT * INTO #tmpChannel FROM [stat].[Channel] chan
	;
	ALTER TABLE #tmpChannel ADD PRIMARY KEY CLUSTERED ( [ChannelId] )
	;

/* uncomment to perform sub-batching... (comment block [A] begin)
	IF ISNULL( @iBatchId, 0 ) < 1
		select
				@iBatchId = MIN( sb.BatchId )
		from [ftb].[SubBatch] as sb
		WHERE 1 = 1
			AND sb.BatchId > 0
			AND sb.SubBatchStatSeq = 1
			AND sb.SubBatchBegin IS NULL
			AND sb.SubBatchEnd IS NULL
	;
	IF ISNULL( @iBatchId, 0 ) > 0
		SELECT
				@iSubBatch_RowSeq = MIN( sb.[RowSeq] )
		FROM [ftb].[SubBatch] AS sb
		WHERE 1 = 1
			AND sb.BatchId = @iBatchId 
			AND sb.SubBatchStatSeq = 1
			AND sb.SubBatchBegin IS NULL
			AND sb.SubBatchEnd IS NULL
	;
	IF ISNULL( @iSubBatch_RowSeq, 0 ) > 0
		SELECT
				 @iSubBatchSeq = [SubBatchSeq]
		FROM [ftb].[SubBatch]
		WHERE RowSeq = @iSubBatch_RowSeq
	;
	if ISNULL( @iStatBatchLogId, -1 ) < 1
		and ISNULL( @iSubBatch_RowSeq, 0 ) > 0
		set @iStatBatchLogId = isnull( (
				select
					case when sb.StatBatchLogId > 0 then sb.StatBatchLogId else @iStatBatchLogId end
				from [ftb].[SubBatch] as sb
				WHERE 1 = 1
					AND sb.RowSeq = @iSubBatch_RowSeq
					--AND sb.BatchId = @iBatchId
					--AND sb.SubBatchSeq = @iSubBatchSeq
					--AND sb.StatBatchLogId > 0
					--AND sb.SubBatchStatSeq = 1
					--AND sb.SubBatchBegin IS NULL
					--AND sb.SubBatchEnd IS NULL
			), @iStatBatchLogId )
	;

--print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N' = ' + isnull( convert( nvarchar(50),  ), N'{null}' )
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'        @iBatchId = ' + isnull( convert( nvarchar(50), @iBatchId ), N'{null}' )
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'@iSubBatch_RowSeq = ' + isnull( convert( nvarchar(50), @iSubBatch_RowSeq ), N'{null}' )
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'    @iSubBatchSeq = ' + isnull( convert( nvarchar(50), @iSubBatchSeq ), N'{null}' )
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N' @iStatBatchLogId = ' + isnull( convert( nvarchar(50), @iStatBatchLogId ), N'{null}' )

uncomment to perform sub-batching... (comment block [A] begin) */


if @iBatchId = 0 begin print N'@iBatchId = 0!  Stopping execution with "SET NOEXEC ON"'; set noexec on; end else begin print N'First available batch where start/end are null: @iBatchId = ' + convert( nvarchar(50), @iBatchId ); end;
--TO-DO: reverse these 2 lines: if @iBatchId = 0 begin print N'@iBatchId = 0!  Stopping execution.  Exiting sproc.'; return -1; end else begin print N'First available batch where start/end are null: @iBatchId = ' + convert( nvarchar(50), @iBatchId ); end
;

if isnull( @iStatBatchLogId, -1 ) = -1
begin
			PRINT ''
			PRINT REPLICATE( '-', 80 )
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'@piStatBatchLogId is less than 0 or {null}.  Calling stat.uspBatchLogUpsertOut...'
			SET @iStatBatchLogId = -1; 
			EXEC [stat].[uspBatchLogUpsertOut] @psiBatchLogId = @iStatBatchLogId OUTPUT, @piOrgId = @iClientOrgId; -- 163769
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'New batch added to AtomicStat.stat.BatchLog.'
			PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'New BatchLogId = ' + CONVERT( nvarchar(50), @iStatBatchLogId )
			PRINT REPLICATE( '-', 80 )
			PRINT ''

	print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N' @iStatBatchLogId = ' + isnull( convert( nvarchar(50), @iStatBatchLogId ), N'{null}' )
	print ''
end


----EXEC [ftb].[uspBatchStatBatchLogXrefEstablish] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId, @ptiDebug = @tiDebug;
EXEC [ftb].[uspBatchTransferInitiate] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId /*OUTPUT*/, @ptiDebug = @tiDebug


if not exists( select 'x' from [ftb].[BatchStatBatchLogXref] as x where x.BatchId = @iBatchId and x.StatBatchLogId = @iStatBatchLogId ) begin print N'BatchId and StatBatchLogId not found in [ftb].[BatchStatBatchLogXref].  Stopping execution with "SET NOEXEC ON"'; set noexec on; end
--TO-DO: reverse these 2 lines: if not exists( select 'x' from [ftb].[BatchStatBatchLogXref] as x where x.BatchId = @iBatchId and x.StatBatchLogId = @iStatBatchLogId ) begin print N'BatchId and StatBatchLogId not found in [ftb].[BatchStatBatchLogXref].  Exiting sproc.'; return -1; end
;
/* ...TO-DO: uncomment when ready again (comment block [A] end) */


/*
drop table if exists #BatchKeyStatCount
;
create table #BatchKeyStatCount
	(
	 StatBuildBatchId int
	,KeyTypeId int
	,StatId int
	,RecCount bigint
	)
;
insert into #BatchKeyStatCount
select
	 StatBuildBatchId
	,KeyTypeId
	,StatId
	,count(1) as RecCount
from [FTBRisk].[dbo].[StatExportBulk]
where [StatBuildBatchId] = @iBatchId
group by 
	 [StatBuildBatchId]
	,KeyTypeId
	,StatId
order by
	 [StatBuildBatchId]
	,KeyTypeId
	,StatId
;


drop table if exists #preSort;
;
create table #preSort
	(
		 rowSeq int
		,preSort int
		,preCount bigint
		,absDiff bigint
		,absShuffle bigint
		,IsFirst tinyint
		,StatBuildBatchId int
		,KeyTypeId int
		,StatId int
		,RecCount bigint
	)
;
with ctePreSort as 
(  
	select 
		 row_number() over ( partition by StatBuildBatchId, KeyTypeId, ABS( ( @biBinCapacity / 2 ) - RecCount ) order by StatBuildBatchId, KeyTypeId, RecCount ) as preSort
		,count_big(1)     over ( partition by StatBuildBatchId, KeyTypeId, ABS( ( @biBinCapacity / 2 ) - RecCount )                                                ) as preCount
		,StatBuildBatchId
		,KeyTypeId
		,StatId
		,RecCount
		,ABS( ( @biBinCapacity / 2 ) - RecCount ) as absDiff
	from #BatchKeyStatCount 
)
insert into #preSort
	(
		 rowSeq
		,preSort
		,preCount
		,absDiff
		,absShuffle
		,IsFirst
		,StatBuildBatchId
		,KeyTypeId
		,StatId
		,RecCount
	)
select
	 row_number() over (                               order by StatBuildBatchId, KeyTypeId, absDiff, ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ), RecCount, preSort ) as rowSeq 
	,row_number() over ( partition by StatBuildBatchId order by StatBuildBatchId, KeyTypeId, absDiff, ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ), RecCount, preSort ) as preSort
	,preCount
	,absDiff
	,ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ) as absShuffle

	,case when ( row_number() over ( partition by StatBuildBatchId order by StatBuildBatchId, KeyTypeId, absDiff, ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ), RecCount, preSort ) ) = 1 then 1 else 0 end as IsFirst

	,StatBuildBatchId
	,KeyTypeId
	,StatId
	,RecCount
from ctePreSort
order by rowSeq
;
create clustered index #cix_preSort on #preSort ( StatBuildBatchId, KeyTypeId, preSort, absDiff, absShuffle )
;


drop table if exists #BinContent;
;
create table #BinContent
	(
		 RowSeq int
		,StatBuildBatchId int
		,BinSeq int
		,preSort int
		,KeyTypeId int
		,StatId int
		,RecCount bigint
		,RollingSum bigint
		,IsFirst tinyint
		,IsNewBin tinyint
		,RollingMod bigint
	)
;
with cteBinSort as
	( 
		-- Establish the root with preSort = 1
		select StatBuildBatchId, KeyTypeId, preSort, StatId, RecCount
			, RecCount as RollingSum
			, IsFirst
			, convert( tinyint, 1 ) as IsNewBin
			, ( RecCount % @biBinCapacity ) as RollingMod
		from #preSort
		where preSort = 1

		-- rowSeq int
		--,preSort int
		--,preCount bigint
		--,absDiff bigint
		--,absShuffle bigint
		--,IsFirst tinyint
		--,StatBuildBatchId int
		--,KeyTypeId int
		--,StatId int
		--,RecCount bigint
   
		union all

		--	Add the latest RecCount to the RollingSum of the previous record.
		--		If RollingSum is less-than-or-equal-to the bin capacity, then do nothing.
		--		If RollingSum exceeds the bin capacity, then the NextSum will be added to a new bin.
		select ps.StatBuildBatchId, ps.KeyTypeId, ps.preSort, ps.StatId, ps.RecCount, convert( bigint, case
				when ( ( ns.NextSum / @biBinCapacity ) = ( cbs.RollingSum / @biBinCapacity ) ) or ( ( ns.NextSum % @biBinCapacity ) = 0 ) then ns.NextSum -- continue rolling
				else ( ( ( ns.NextSum / @biBinCapacity ) * @biBinCapacity ) + ps.RecCount ) -- start a new bin
			 end ) as RollingSum
			, ps.IsFirst
			, convert( tinyint, case
				when ( ( ns.NextSum / @biBinCapacity ) = ( cbs.RollingSum / @biBinCapacity ) ) or ( ( ns.NextSum % @biBinCapacity ) = 0 ) then 0 -- continue rolling
				else 1 -- start a new bin
			 end ) as IsNewBin
			, ( convert( bigint, case
					when ( ( ns.NextSum / @biBinCapacity ) = ( cbs.RollingSum / @biBinCapacity ) ) or ( ( ns.NextSum % @biBinCapacity ) = 0 ) then ns.NextSum -- continue rolling
					else ( ( ( ns.NextSum / @biBinCapacity ) * @biBinCapacity ) + ps.RecCount ) -- start a new bin
				 end ) % @biBinCapacity ) as RollingMod
		from #preSort as ps
			inner join cteBinSort as cbs 
				on ps.StatBuildBatchId = cbs.StatBuildBatchId 
					--and ps.KeyTypeId = cbs.KeyTypeId
					and ps.preSort = ( cbs.preSort + 1 )
			cross apply ( select ( cbs.RollingSum + ps.RecCount ) as NextSum ) as ns
	)
,cteBinSort2 as
	(
select 
		 StatBuildBatchId
		,dense_rank() over ( partition by StatBuildBatchId order by StatBuildBatchId, ( ( RollingSum - RecCount ) / @biBinCapacity ) ) as BinSeq
		,preSort
		,KeyTypeId
		,StatId
		,RecCount
		,RollingSum
		,IsFirst
		,IsNewBin
		,RollingMod
from cteBinSort
	)
insert into #BinContent
	(
		 RowSeq
		,StatBuildBatchId
		,BinSeq
		,preSort
		,KeyTypeId
		,StatId
		,RecCount
		,RollingSum
		,IsFirst
		,IsNewBin
		,RollingMod
	)
select
		 row_number() over( order by StatBuildBatchId, BinSeq, KeyTypeId, StatId ) as RowSeq
		,StatBuildBatchId
		,BinSeq
		,preSort
		,KeyTypeId
		,StatId
		,RecCount
		,RollingSum
		,IsFirst
		,IsNewBin
		,RollingMod
from cteBinSort2
option ( maxrecursion 0 )
;
create clustered index #cix_BinContent on #BinContent ( StatBuildBatchId, BinSeq, KeyTypeId, StatId )
;


drop table if exists #Bin
;
create table #Bin
	(
		 RowSeq int
		,StatBuildBatchId int
		,BinSeq int
		,RecCount bigint
	)
;
insert into #Bin
	(
		 RowSeq
		,StatBuildBatchId
		,BinSeq
		,RecCount
	)
select
	 row_number() over( order by StatBuildBatchId, BinSeq ) as RowSeq
	,StatBuildBatchId
	,BinSeq
	,sum( RecCount ) as RecCount
	--, RollingSum, IsFirst, IsNewBin, RollingMod
from #BinContent
group by StatBuildBatchId, BinSeq
order by StatBuildBatchId, BinSeq
;
select @iBinRowSeq = min( RowSeq ), @iBinRowSeqMin = min( RowSeq ), @iBinRowSeqMax = max( RowSeq ) from #Bin
;
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Bin count: ' + convert( nvarchar(50), @iBinRowSeqMax )
;
*/

	DECLARE @tbStatList table ( StatId int NOT NULL PRIMARY KEY, KeyTypeId smallint NULL );
	insert into @tbStatList ( StatId, KeyTypeId )
	--We use this only when sub-batching: SELECT sb.StatId, v.KeyTypeId FROM [ftb].[SubBatch] sb inner join report.vwStatGroupStatKeyType as v on sb.StatId = v.StatId where sb.SubBatchSeq = @iSubBatchSeq and v.AncestorStatGroupId = 54 and v.KeyTypeId in( 107, 108, 109, 110, 111, 112, 113, 127  ) -- TO-DO: uncomment this when sub-batching
	select distinct StatId, KeyTypeId from report.vwStatGroupStatKeyType as v where v.AncestorStatGroupId = 54 and v.KeyTypeId in( -- TO-DO: uncomment this when not sub-batching
			  107 -- FTB - Customer
			, 108 -- FTB - Payer
			, 109 -- FTB - KCP
			, 110 -- FTB - CC (Customer Channel)
			, 111 -- FTB - CCL (Customer Channel Location)
			, 112 -- FTB - DSGL
			, 113 -- FTB - DSGS
			, 127 -- FTB - Customer Account Level
		) order by StatId
	;
--select * from @tbStatList;
	DECLARE @tbKeyTypeList table ( KeyTypeId int NOT NULL PRIMARY KEY );
	insert into @tbKeyTypeList
	select distinct KeyTypeId from @tbStatList
	;


select @iSubBatchStatCount = count(1) from @tbStatList
;
declare @nvStatList nvarchar(max)
;
select @nvStatList = COALESCE( @nvStatList + N', ', N'' ) + CONVERT( nvarchar(50), t.StatId ) FROM @tbStatList AS t
;
print replicate( N'-', 80 )
print N''
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'StatId list for sub batch sequence ' + CONVERT( nvarchar(50), @iSubBatchSeq ) +  N': (stat count = ' + convert( nvarchar(50), @iSubBatchStatCount ) + N')'
print REPLICATE( NCHAR(009), @tiDebug ) + @nvStatList
print N''
print replicate( N'-', 80 )
;


drop table if exists #BatchKeyStatCount
;
create table #BatchKeyStatCount
	(
	 StatBuildBatchId int
	,KeyTypeId int
	,StatId int
	,RecCount bigint
	)
;
insert into #BatchKeyStatCount
	(
		 StatBuildBatchId
		,KeyTypeId
		,StatId
		,RecCount
	)
select 
		 seb.StatBuildBatchId
		,seb.KeyTypeId
		,seb.StatId
		,seb.RecCount
from (
		select
			 StatBuildBatchId
			,KeyTypeId
			,StatId
			,count(1) as RecCount
		from [FTBRisk].[dbo].[StatExportBulk]
		where [StatBuildBatchId] = @iBatchId
		group by 
			 [StatBuildBatchId]
			,KeyTypeId
			,StatId
	) seb
	inner join @tbStatList sl
		on seb.StatId = sl.StatId
order by
	 seb.StatBuildBatchId
	,seb.KeyTypeId
	,seb.StatId
;


drop table if exists #preSort;
;
create table #preSort
	(
		 rowSeq int
		,preSort int
		,preCount bigint
		,absDiff bigint
		,absShuffle bigint
		,IsFirst tinyint
		,StatBuildBatchId int
		,KeyTypeId int
		,StatId int
		,RecCount bigint
	)
;
with ctePreSort as 
(  
	select 
		 row_number() over ( partition by StatBuildBatchId, KeyTypeId, ABS( ( @biBinCapacity / 2 ) - RecCount ) order by StatBuildBatchId, KeyTypeId, RecCount ) as preSort
		,count_big(1)     over ( partition by StatBuildBatchId, KeyTypeId, ABS( ( @biBinCapacity / 2 ) - RecCount )                                                ) as preCount
		,StatBuildBatchId
		,KeyTypeId
		,StatId
		,RecCount
		,ABS( ( @biBinCapacity / 2 ) - RecCount ) as absDiff
	from #BatchKeyStatCount 
)
insert into #preSort
	(
		 rowSeq
		,preSort
		,preCount
		,absDiff
		,absShuffle
		,IsFirst
		,StatBuildBatchId
		,KeyTypeId
		,StatId
		,RecCount
	)
select
	 row_number() over (                               order by StatBuildBatchId, KeyTypeId, absDiff, ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ), RecCount, preSort ) as rowSeq 
	,row_number() over ( partition by StatBuildBatchId order by StatBuildBatchId, KeyTypeId, absDiff, ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ), RecCount, preSort ) as preSort
	,preCount
	,absDiff
	,ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ) as absShuffle

	,case when ( row_number() over ( partition by StatBuildBatchId order by StatBuildBatchId, KeyTypeId, absDiff, ABS( preSort - ( ABS( preCount - preSort ) + 1 ) ), RecCount, preSort ) ) = 1 then 1 else 0 end as IsFirst

	,StatBuildBatchId
	,KeyTypeId
	,StatId
	,RecCount
from ctePreSort
order by rowSeq
;
create clustered index #cix_preSort on #preSort ( StatBuildBatchId, KeyTypeId, preSort, absDiff, absShuffle )
;


drop table if exists #BinContent;
;
create table #BinContent
	(
		 RowSeq int
		,StatBuildBatchId int
		,BinSeq int
		,preSort int
		,KeyTypeId int
		,StatId int
		,RecCount bigint
		,RollingSum bigint
		,IsFirst tinyint
		,IsNewBin tinyint
		,RollingMod bigint
	)
;
with cteBinSort as
	( 
		-- Establish the root with preSort = 1
		select StatBuildBatchId, KeyTypeId, preSort, StatId, RecCount
			, RecCount as RollingSum
			, IsFirst
			, convert( tinyint, 1 ) as IsNewBin
			, ( RecCount % @biBinCapacity ) as RollingMod
		from #preSort
		where preSort = 1

		-- rowSeq int
		--,preSort int
		--,preCount bigint
		--,absDiff bigint
		--,absShuffle bigint
		--,IsFirst tinyint
		--,StatBuildBatchId int
		--,KeyTypeId int
		--,StatId int
		--,RecCount bigint
   
		union all

		--	Add the latest RecCount to the RollingSum of the previous record.
		--		If RollingSum is less-than-or-equal-to the bin capacity, then do nothing.
		--		If RollingSum exceeds the bin capacity, then the NextSum will be added to a new bin.
		select ps.StatBuildBatchId, ps.KeyTypeId, ps.preSort, ps.StatId, ps.RecCount, convert( bigint, case
				when ( ( ns.NextSum / @biBinCapacity ) = ( cbs.RollingSum / @biBinCapacity ) ) or ( ( ns.NextSum % @biBinCapacity ) = 0 ) then ns.NextSum -- continue rolling
				else ( ( ( ns.NextSum / @biBinCapacity ) * @biBinCapacity ) + ps.RecCount ) -- start a new bin
			 end ) as RollingSum
			, ps.IsFirst
			, convert( tinyint, case
				when ( ( ns.NextSum / @biBinCapacity ) = ( cbs.RollingSum / @biBinCapacity ) ) or ( ( ns.NextSum % @biBinCapacity ) = 0 ) then 0 -- continue rolling
				else 1 -- start a new bin
			 end ) as IsNewBin
			, ( convert( bigint, case
					when ( ( ns.NextSum / @biBinCapacity ) = ( cbs.RollingSum / @biBinCapacity ) ) or ( ( ns.NextSum % @biBinCapacity ) = 0 ) then ns.NextSum -- continue rolling
					else ( ( ( ns.NextSum / @biBinCapacity ) * @biBinCapacity ) + ps.RecCount ) -- start a new bin
				 end ) % @biBinCapacity ) as RollingMod
		from #preSort as ps
			inner join cteBinSort as cbs 
				on ps.StatBuildBatchId = cbs.StatBuildBatchId 
					--and ps.KeyTypeId = cbs.KeyTypeId
					and ps.preSort = ( cbs.preSort + 1 )
			cross apply ( select ( cbs.RollingSum + ps.RecCount ) as NextSum ) as ns
	)
,cteBinSort2 as
	(
select 
		 StatBuildBatchId
		,dense_rank() over ( partition by StatBuildBatchId order by StatBuildBatchId, ( ( RollingSum - RecCount ) / @biBinCapacity ) ) as BinSeq
		,preSort
		,KeyTypeId
		,StatId
		,RecCount
		,RollingSum
		,IsFirst
		,IsNewBin
		,RollingMod
from cteBinSort
	)
insert into #BinContent
	(
		 RowSeq
		,StatBuildBatchId
		,BinSeq
		,preSort
		,KeyTypeId
		,StatId
		,RecCount
		,RollingSum
		,IsFirst
		,IsNewBin
		,RollingMod
	)
select
		 row_number() over( order by StatBuildBatchId, BinSeq, KeyTypeId, StatId ) as RowSeq
		,StatBuildBatchId
		,BinSeq
		,preSort
		,KeyTypeId
		,StatId
		,RecCount
		,RollingSum
		,IsFirst
		,IsNewBin
		,RollingMod
from cteBinSort2
option ( maxrecursion 0 )
;
--create clustered index #cix_BinContent on #BinContent ( StatBuildBatchId, BinSeq, KeyTypeId, StatId )
create clustered index #cix_BinContent on #BinContent ( BinSeq, KeyTypeId, StatId )
;


drop table if exists #Bin
;
create table #Bin
	(
		 RowSeq int
		,StatBuildBatchId int
		,BinSeq int
		,RecCount bigint
	)
;
insert into #Bin
	(
		 RowSeq
		,StatBuildBatchId
		,BinSeq
		,RecCount
	)
select
	 row_number() over( order by StatBuildBatchId, BinSeq ) as RowSeq
	,StatBuildBatchId
	,BinSeq
	,sum( RecCount ) as RecCount
	--, RollingSum, IsFirst, IsNewBin, RollingMod
from #BinContent
group by StatBuildBatchId, BinSeq
order by StatBuildBatchId, BinSeq
;
select @iBinRowSeq = min( RowSeq ), @iBinRowSeqMin = min( RowSeq ), @iBinRowSeqMax = max( RowSeq ) from #Bin
;
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Bin count: ' + convert( nvarchar(50), @iBinRowSeqMax )
;



SET @nvExec = N'USE [FTBRisk];
		DROP INDEX IF EXISTS [fx01StatExportBulk] ON [dbo].[StatExportBulk];
		CREATE NONCLUSTERED INDEX [fx01StatExportBulk] ON [dbo].[StatExportBulk]
		(
			 [StatBuildBatchId] ASC
			,[KeyTypeId] ASC
			,[StatId] ASC
			,[StatExportBulkId] ASC
		)
		INCLUDE ( [StatBuildCycleDate], [InsertDatetime], [ParentOrgId], [CustomerIdIFA], [IdTypeId], [IdOrgId], [IdStateId], [IdMac], [CustomerNumberStringFromClient], [CustomerAccountNumber], [ClientOrgId], [PayerClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LengthPayerAccountNumber], [ChannelId], [LocationOrgId], [LocationOrgCode], [GeoLarge], [GeoSmall], [GeoZip4], [RetailDollarStrat], [DollarStrat], [StatValueInt], [StatValueBigInt], [StatValueDecimal1602], [StatValueDecimal1604], [StatValueNChar100], [StatValueDate], [StatValueBit], [StatValueDatetime] )
		WHERE ( [StatBuildBatchId] = (' + CONVERT( nvarchar(50), @iBatchId ) + N') )
		WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
	'
SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
EXEC (@nvExec)
;

set @biRecCount = 0
select 
 @biRecCount = count_big(1) -- the raw count
from [FTBRisk].[dbo].[StatExportBulk] as svb
where svb.StatBuildBatchId = @iBatchId and
	exists( select 'X' from @tbStatList as x where x.StatId = svb.StatId )
;
print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'      Risk [dbo].[StatExportBulk] applicable raw row count = ' + isnull( convert( nvarchar(50), @biRecCount ), N'{null}' )
print ''


insert into [ftb].[SubBatch] ( BatchId, SubBatchSeq, StatId, SubBatchStatSeq, RawRowCount, StatBatchLogId, SubBatchDuration_Second, HashBulkCount, HashExceptionCount, StatValueBulkUpdateCount, StatValueBulkInsertDeltaCount, StatValueBulkInsertFullCount )
select BatchId, SubBatchSeq, StatId, SubBatchStatSeq, RawRowCount, StatBatchLogId, SubBatchDuration_Second, HashBulkCount, HashExceptionCount, StatValueBulkUpdateCount, StatValueBulkInsertDeltaCount, StatValueBulkInsertFullCount
from (
		select BatchId = @iBatchId, SubBatchSeq = ( select max( x.SubBatchSeq ) + 1 from [ftb].[SubBatch] as x ), StatId = 0, SubBatchStatSeq = 1, RawRowCount = @biRecCount, StatBatchLogId = @iStatBatchLogId, SubBatchDuration_Second = 0, HashBulkCount = 0, HashExceptionCount = 0, StatValueBulkUpdateCount = 0, StatValueBulkInsertDeltaCount = 0, StatValueBulkInsertFullCount = 0
	) as n
where not exists( select 'x' from [ftb].[SubBatch] as x where x.BatchId = n.BatchId and x.SubBatchStatSeq = 1 )
;











	SELECT
			--@iSubBatchSeq = MIN( [SubBatchSeq] )
			@iSubBatch_RowSeq = MIN( [RowSeq] )
	FROM [ftb].[SubBatch]
	WHERE BatchId = @iBatchId
		AND SubBatchStatSeq = 1
		AND SubBatchBegin IS NULL
		AND SubBatchEnd IS NULL
	;
--print N'@iSubBatch_RowSeq = ' + convert( nvarchar(50), isnull( @iSubBatch_RowSeq, N'{null}' ) );
	SELECT
			 @iSubBatchSeq = [SubBatchSeq]
	FROM [ftb].[SubBatch]
	WHERE RowSeq = @iSubBatch_RowSeq
	;
--print N'@iSubBatchSeq = ' + convert( nvarchar(50), isnull( @iSubBatchSeq, N'{null}' ) );



----select top 1 @iBatchId = seb.StatBuildBatchId from [FTBRisk].[dbo].[StatExportBulk] seb --  ...just until dev/test is done...  (It took 00:02:55 to table scan 1.4 billion rows in [FTBRisk].[dbo].[StatExportBulk] due to @iBatchId = 0!)
--select @iBatchId = sub.BatchId from [ftb].[SubBatch] as sub where sub.RowSeq = @iSubBatch_RowSeq --  ...just until dev/test is done...  (It took 00:02:55 to table scan 1.4 billion rows in [FTBRisk].[dbo].[StatExportBulk] due to @iBatchId = 0!)
--print N'Where sub.RowSeq = @iSubBatch_RowSeq (' + convert( nvarchar(50), @iSubBatch_RowSeq ) + N'): @iBatchId = ' + convert( nvarchar(50), isnull( @iBatchId, N'{null}' ) );

SELECT @dCycleDate = CycleDate, @nvStatGenerator = StatBatchDataSetName FROM [FTBRisk].[dbo].[StatBatch] AS b WHERE b.StatBatchId = @iBatchId
;
	
	IF @tiDebug > 0
	BEGIN
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 )
		PRINT REPLICATE( NCHAR(009), @tiDebug ) + N'StatBatchLogId = ' + CONVERT( nvarchar(50), @iStatBatchLogId )
		PRINT REPLICATE( NCHAR(009), @tiDebug ) + N'     CycleDate = ' + CONVERT( nvarchar(50), @dCycleDate, 121 )
		PRINT REPLICATE( NCHAR(009), @tiDebug ) + N'       BatchId = ' + CONVERT( nvarchar(50), @iBatchId )
		PRINT REPLICATE( NCHAR(009), @tiDebug ) + N' StatGenerator = ' + @nvStatGenerator
		;
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END


/*
	IF ISNULL( @iStatBatchLogId, -1 ) = -1 AND EXISTS( SELECT 'X' FROM [FTBRisk].[dbo].[StatBatch] b WHERE b.StatBatchTransferInitiatedDatetime IS NULL AND b.StatBatchCompletedDatetime > DATEADD( day, -7, SYSDATETIME() ) AND b.StatBatchDeltaPromotionComplete = 0 )
	BEGIN
		IF ( SELECT COUNT(1) FROM [FTBRisk].[dbo].[StatBatch] b WHERE b.StatBatchTransferInitiatedDatetime IS NULL AND b.StatBatchCompletedDatetime > DATEADD( day, -7, SYSDATETIME() ) ) > 0
		BEGIN
*/
--if isnull( @iStatBatchLogId, -1 ) = -1
--begin
--			PRINT N'@piStatBatchLogId is less than 0 or {null}.  Calling stat.uspBatchLogUpsertOut...'
--			SET @iStatBatchLogId = -1; 
--			EXEC [stat].[uspBatchLogUpsertOut] @psiBatchLogId = @iStatBatchLogId OUTPUT, @piOrgId = @iClientOrgId; -- 163769
--			PRINT N'New batch added to AtomicStat.stat.BatchLog.'
--			PRINT N'New BatchLogId = ' + CONVERT( nvarchar(50), @iStatBatchLogId )
--end
/*
		END
	END
	ELSE IF @iStatBatchLogId IS NULL AND EXISTS( SELECT 'X' FROM [FTBRisk].[dbo].[StatBatch] AS b WHERE b.StatBatchCompletedDatetime IS NOT NULL AND b.StatBatchDeltaPromotionComplete = 0 AND EXISTS( SELECT 'X' FROM [ftb].[BatchStatBatchLogXref] x WHERE b.StatBatchId = x.BatchId ) )
	BEGIN
		IF ( SELECT COUNT(1) FROM [FTBRisk].[dbo].[StatBatch] AS b WHERE b.StatBatchTransferInitiatedDatetime IS NULL ) > 0
		BEGIN
			PRINT N'@piStatBatchLogId is {null}.'
			SELECT TOP 1 @iStatBatchLogId = x.StatBatchLogId, @iBatchId = x.BatchId FROM [FTBRisk].[dbo].[StatBatch] b INNER JOIN [ftb].[BatchStatBatchLogXref] x ON b.StatBatchId = x.BatchId WHERE b.StatBatchCompletedDatetime IS NOT NULL ORDER BY x.BatchId; 
			PRINT N'Batch fetched from [stat].[BatchLog].'
			PRINT N'BatchLogId = ' + CONVERT( nvarchar(50), @iStatBatchLogId )
		END
	END
*/
--select top 1 @iStatBatchLogId = x.StatBatchLogId, @iBatchId = x.BatchId FROM [FTBRisk].[dbo].[StatBatch] b INNER JOIN [ftb].[BatchStatBatchLogXref] x ON b.StatBatchId = x.BatchId WHERE b.StatBatchCompletedDatetime IS NOT NULL ORDER BY x.BatchId; 
--select top 1 x.StatBatchLogId, x.BatchId FROM [FTBRisk].[dbo].[StatBatch] b INNER JOIN [ftb].[BatchStatBatchLogXref] x ON b.StatBatchId = x.BatchId WHERE b.StatBatchCompletedDatetime IS NOT NULL ORDER BY x.BatchId; 


/*
	--SET @iBatchId = [ftb].[ufnBatchGetByStatBatchLogId]( @iStatBatchLogId )
	--;
	--IF @iBatchId IS NOT NULL 
	--	AND EXISTS( SELECT 'X' FROM [FTBRisk].[dbo].[StatExportBulk] AS x WHERE x.StatBuildBatchId = @iBatchId AND x.StatBuildName = 'FTB' )
	IF ISNULL( @iStatBatchLogId, 0 ) > 0
	BEGIN 

		EXEC [ftb].[uspBatchTransferInitiate] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId OUTPUT, @ptiDebug = @tiDebug
		;
*/

if isnull( @iStatBatchLogId, 0 ) > 0 and isnull( @iBatchId, 0 ) > 0 and isnull( @iSubBatchStatCount, 0 ) > 0
begin 
	PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 )
	print REPLICATE( NCHAR(009), @tiDebug ) + N'       @iBatchId = ' + CONVERT( nvarchar(50), @iBatchId )
	print REPLICATE( NCHAR(009), @tiDebug ) + N'@iStatBatchLogId = ' + CONVERT( nvarchar(50), @iStatBatchLogId )
	;
	insert into [ftb].[BatchStatBatchLogXref]( BatchId, StatBatchLogId )
	select @iBatchId as BatchId, @iStatBatchLogId as StatBatchLogId where not exists( select 'x' from [ftb].[BatchStatBatchLogXref] as x where /*x.BatchId = @iBatchId and*/ x.StatBatchLogId = @iStatBatchLogId )
	;
end
else
begin
	if isnull( @iStatBatchLogId, 0 ) = 0 or isnull( @iBatchId, 0 ) = 0 print N'Both @iStatBatchLogId and @iBatchId must have values greater than 0 (zero)' + REPLICATE( N'!', 40 )
	if isnull( @iSubBatchStatCount, 0 ) = 0 print N'@iSubBatchStatCount cannot be less than 1 ' + REPLICATE( N'!', 40 )
	print N'Setting NOEXEC to ON, terminating this run.'
	;
	set noexec on; -- set noexec off
	;
end


update [ftb].[SubBatch] set [StatBatchLogId] = @iStatBatchLogId, [SubBatchBegin] = sysdatetime(), [HashWorkComplete] = NULL, [StatValueBulkUpdateComplete] = NULL, [StatValueBulkInsertDeltaComplete] = NULL, [StatValueBulkInsertFullComplete] = NULL, 
	[HashWorkRowCount] = 0, [RawRowCount] = 0, [StatValueBulkUpdateCount] = 0, [StatValueBulkInsertDeltaCount] = 0, [StatValueBulkInsertFullCount] = 0 where [RowSeq] = @iSubBatch_RowSeq
;


		-- ----------------------------------------------------------------------------------------------------------------------------------
		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[HashWork] Begin...'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		TRUNCATE TABLE [ftb].[HashWork]
		;
		IF EXISTS (SELECT 'X' FROM sys.objects AS o INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id WHERE s.name = N'ftb' AND o.name = N'pkHashWork' AND o.type = N'PK')
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + NCHAR(009) + N'dropping pkHashWork from [ftb].[HashWork].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			ALTER TABLE [ftb].[HashWork] DROP CONSTRAINT [pkHashWork] 
			;
		END

		SET @biRecCount = 0
		;

drop table if exists #BinFetch;
;
create table #BinFetch
	(
		 StatBuildBatchId int
		,KeyTypeId int
		,StatId int

		,BinSeq int
		,RowSeq int
	)
;
--create clustered index #cix_BinFetch on #BinFetch ( StatBuildBatchId, KeyTypeId, StatId )
create clustered index #cix_BinFetch on #BinFetch ( KeyTypeId, StatId )
;


	WHILE ISNULL( @iBinRowSeq, ( @iBinRowSeqMax + 1 ) ) <= @iBinRowSeqMax
	BEGIN -- @iBinRowSeq <= @iBinRowSeqMax
	
		select @iBinSeq = b.BinSeq from #Bin b where b.RowSeq = @iBinRowSeq
		;
		print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Importing Bin: ' + convert( nvarchar(50), @iBinRowSeq ) + N' of ' + convert( nvarchar(50), @iBinRowSeqMax )
		;

		truncate table #BinFetch
		;
		insert into #BinFetch
			(
				 KeyTypeId
				,StatId
				,StatBuildBatchId

				,BinSeq
				,RowSeq
			)
		select
				 KeyTypeId
				,StatId
				,StatBuildBatchId

				,BinSeq
				,RowSeq
		from #BinContent as bc
		where bc.BinSeq = @iBinSeq
		order by bc.KeyTypeId, bc.StatId
		;

		INSERT INTO [ftb].[HashWork] ( [StatExportBulkId], [StatBatchLogId], [KeyTypeId], [StatId], [Seq], [Seq2], [HashId], [ParentOrgId], [CustomerIdIFA], [IdMac], [IdTypeId], [IdOrgId], [IdStateId], [CustomerNumberStringFromClient], [CustomerAccountNumber], [ClientOrgId], [PayerClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LengthPayerAccountNumber], [ChannelOrgId], [ChannelId], [LocationOrgId], [LocationOrgCode], [GeoOrgId], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStratRangeId], [DollarStrat], [StatValue] )
		SELECT -- 17,483,046 rows in 00:01:52
--		top 100
		-- declare @iStatBuildBatchId int = 6, @iStatBatchLogId int = 547; select top 3
			 StatExportBulkId
			--,StatBuildBatchId
			,@iStatBatchLogId AS StatBatchLogId
			--,seb.KeyTypeId -- TO-DO: revert back to this when the following CASE structure is no longer needed.
			,case when ( seb.KeyTypeId = 107 and seb.StatId in( 206, 207 ) ) then 127 else seb.KeyTypeId end KeyTypeId
			,seb.StatId

			,ROW_NUMBER() OVER(
					PARTITION BY
						 seb.KeyTypeId
						--,ParentOrgId
						----,CustomerIdIFA
						,CustomerNumberStringFromClient -- TO-DO: revert [CustomerNumberStringFromClient] back to this and remove the following CASE structure...
						--,case when seb.StatId in( 206, 207) and CustomerNumberStringFromClient is null then CustomerAccountNumber else CustomerNumberStringFromClient end
						,CustomerAccountNumber
						,IdTypeId
						,IdOrgId
						,IdStateId
						,IdMac
						,seb.ClientOrgId
						,PayerClientOrgId
						,PayerRoutingNumber
						,PayerAccountNumber
						,LengthPayerAccountNumber
						,seb.ChannelId
						,LocationOrgId
						,LocationOrgCode
						,GeoLarge
						,GeoSmall
						,GeoZip4
						,DollarStrat
					ORDER BY
						 case when ( seb.KeyTypeId = 107 and seb.StatId in( 206, 207 ) ) then 127 else seb.KeyTypeId end -- KeyTypeId
						--,ParentOrgId
						--,CustomerIdIFA
						,CustomerNumberStringFromClient -- TO-DO: revert [CustomerNumberStringFromClient] back to this and remove the following CASE structure...
						--,case when seb.StatId in( 206, 207) and CustomerNumberStringFromClient is null then CustomerAccountNumber else CustomerNumberStringFromClient end
						,CustomerAccountNumber
						,IdTypeId
						,IdOrgId
						,IdStateId
						,IdMac
						,seb.ClientOrgId
						,PayerClientOrgId
						,PayerRoutingNumber
						,PayerAccountNumber
						,LengthPayerAccountNumber
						,seb.ChannelId
						,LocationOrgId
						,LocationOrgCode
						,GeoLarge
						,GeoSmall
						,GeoZip4
						,DollarStrat
						,seb.StatId
				) AS Seq -- this will be our shortcut to a "DISTINCT" list of HashId values (without actually performing a DISTINCT in the SELECT)
--			,0 AS Seq -- stub for row_number() column

			--,ROW_NUMBER() OVER(
			--		PARTITION BY
			--			 seb.KeyTypeId
			--			,seb.StatId
			--			--,ParentOrgId
			--			----,CustomerIdIFA
			--			,IdTypeId
			--			,IdOrgId
			--			,IdStateId
			--			,IdMac
			--			,CustomerNumberStringFromClient
			--			,CustomerAccountNumber
			--			,seb.ClientOrgId
			--			,PayerClientOrgId
			--			,PayerRoutingNumber
			--			,PayerAccountNumber
			--			,LengthPayerAccountNumber
			--			,ChannelId
			--			,LocationOrgId
			--			,LocationOrgCode
			--			,GeoLarge
			--			,GeoSmall
			--			,GeoZip4
			--			,DollarStrat
			--		ORDER BY
			--			 seb.KeyTypeId
			--			,seb.StatId
			--			--,ParentOrgId
			--			--,CustomerIdIFA
			--			,IdTypeId
			--			,IdOrgId
			--			,IdStateId
			--			,IdMac
			--			,CustomerNumberStringFromClient
			--			,CustomerAccountNumber
			--			,seb.ClientOrgId
			--			,PayerClientOrgId
			--			,PayerRoutingNumber
			--			,PayerAccountNumber
			--			,LengthPayerAccountNumber
			--			,ChannelId
			--			,LocationOrgId
			--			,LocationOrgCode
			--			,GeoLarge
			--			,GeoSmall
			--			,GeoZip4
			--			,DollarStrat
			--			,StatExportBulkId DESC
			--	) AS Seq2
			,0 AS Seq2 -- stub for 2nd row_number() column

/*
Name	KeyTypeId	KeyTemplatePreprocessedToConcat
*/
			,ISNULL( -- If a Hash key turns up NULL, then we set it to 0x0 so that it will be pushed into [ftb].[HashException] in order to troubleshoot it.
				CASE -- KeyTypeId is evaluated in order of greatest volume of StatId's to least volume of StatId's
					WHEN seb.KeyTypeId = 127 or ( seb.KeyTypeId = 107 and seb.StatId in( 206, 207 ) )
						THEN -- FTB - Customer Account Level
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|128|' + CONVERT( nvarchar(256), seb.CustomerAccountNumber ) -- [{FTBCustomerAccountLevelAccount}]
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 107
						THEN -- FTB - Customer
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|119|' + CONVERT( nvarchar(256), seb.CustomerAccountNumber )
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 108
						THEN -- FTB - Payer
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|120|' + CONVERT( nvarchar(256), seb.PayerRoutingNumber ) + N'|121|' + CONVERT( nvarchar(256), seb.PayerAccountNumber )
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 109
						THEN -- FTB - KCP
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|119|' + CONVERT( nvarchar(256), seb.CustomerAccountNumber ) + N'|120|' + CONVERT( nvarchar(256), seb.PayerRoutingNumber ) + N'|121|' + CONVERT( nvarchar(256), seb.PayerAccountNumber )
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 110
						THEN -- FTB - CC (Customer Channel)
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|119|' + CONVERT( nvarchar(256), seb.CustomerAccountNumber ) + N'|122|' + CONVERT( nvarchar(256), chan.OrgId )
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 111
						THEN -- FTB - CCL (Customer Channel Location)
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
	--    NA: come back and resolve [LocationOrgCode] to [LocationOrgId] when [LocationOrgId] is null and [LocationOrgCode] is not null
	--    NA: do NOT allow FTB into the hub until FTB locations are loaded into the Org tables and stat.Location.
	--  DONE: Need a new table for client specific locations ([stat].[ClientLocation] ParentOrgId int, OrgId int, LocationCode string, LocationName string?).  [stat].[Location] does not support client specific locations.
										,N'118|163769|119|' + CONVERT( nvarchar(256), seb.CustomerAccountNumber ) + N'|122|' + CONVERT( nvarchar(256), chan.OrgId ) + N'|123|' + CONVERT( nvarchar(256), seb.LocationOrgId ) 
										--END
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 112
						THEN -- FTB - DSGL
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|124|' + CONVERT( nvarchar(256), gl.OrgId ) + N'|126|' + CONVERT( nvarchar(256), dsr.DollarStratRangeId ) -- [{FTBDollarStratRangeId}]
									) 
								, 1 ) 
					WHEN seb.KeyTypeId = 113
						THEN -- FTB - DSGS
							CONVERT(binary(64)
								,HASHBYTES( N'SHA2_512'
										,N'118|163769|125|' + CONVERT( nvarchar(256), gs.OrgId ) + N'|126|' + CONVERT( nvarchar(256), dsr.DollarStratRangeId ) -- [{FTBDollarStratRangeId}]
									) 
								, 1 ) 

					ELSE CONVERT( binary(64), 1/0 ) -- if we have encountered an unhandled KeyType, then we do not want this query to succeed in committing its results since there would be malformed HashId's.
					--ELSE CONVERT( binary(64), 0x0 ) -- !!! if we have encountered an unhandled KeyType, then we do not want this query to succeed in committing its results since there would be malformed HashId's.
				END
				, 0x0 ) AS HashId
			,ParentOrgId
			,CustomerIdIFA

			,IdMac
			,IdTypeId
			,IdOrgId
			,IdStateId

			--,CASE WHEN seb.KeyTypeId = ?? THEN ISNULL( CustomerNumberStringFromClient, CustomerAccountNumber ) ELSE CustomerNumberStringFromClient END AS CustomerNumberStringFromClient -- because StatId's 206 and 207 are coming across without CustomerNumberStringFromClient nor ClientOrgId
			,CustomerNumberStringFromClient -- TO-DO: revert [CustomerNumberStringFromClient] back to this and remove the following CASE structure...
			--,case when seb.StatId in( 206, 207) and CustomerNumberStringFromClient is null then CustomerAccountNumber else CustomerNumberStringFromClient end as CustomerNumberStringFromClient
			,CustomerAccountNumber
			--,CASE WHEN seb.KeyTypeId = ?? THEN ISNULL( seb.ClientOrgId, @iClientOrgId ) ELSE seb.ClientOrgId END ClientOrgId -- because StatId's 206 and 207 are coming across without CustomerNumberStringFromClient nor ClientOrgId
			,seb.ClientOrgId
			,PayerClientOrgId
			,PayerRoutingNumber
			,PayerAccountNumber
			,LengthPayerAccountNumber
			,chan.OrgId AS ChannelOrgId
			,seb.ChannelId
			--,ISNULL( LocationOrgId, lo.OrgId ) AS LocationOrgId
			,LocationOrgId
			,LocationOrgCode
			,COALESCE( gl.OrgId, gs.OrgId, NULL ) AS GeoOrgId
			,GeoLarge
			,GeoSmall
			,GeoZip4
			,dsr.DollarStratRangeId AS DollarStratRangeId
			,DollarStrat

/* for FTB...

	statCount	StatTargetTable
	64	[stat].[StatTypeInt]
	43	[stat].[StatTypeDecimal1602]
	14	[stat].[StatTypeBigint]
	12	[stat].[StatTypeDate]
	7	[stat].[StatTypeNumeric1604]
	5	[stat].[StatTypeBit]
	4	[stat].[StatTypeNchar100]
	4	[stat].[StatTypeNchar50]
	4	[stat].[StatTypeNumeric0109]
	1	[stat].[StatTypeNumeric1312]

	StatTargetTable	StatListByCountDesc
	[stat].[StatTypeInt]	243,450,79,164,106,80,449,108,435,154,83,448,167,253,177,175,159,109,245,446,445,444,63,59,46,141,319,149,61,43,239,328,367,238,345,454,241,368,48,55,73,152,138,201,57,45,50,67,381,382,74,75,77,76,78,68,69,70,71,72,82,100,169,251
	[stat].[StatTypeBigint]	94,99,93,98,102,92,97,96,247,249,91,246,248,185
	[stat].[StatTypeDecimal1602]	242,451,163,41,439,107,244,158,105,162,165,176,174,173,95,160,110,254,447,58,62,47,142,38,318,147,60,42,453,240,134,151,137,342,315,337,56,44,39,81,168,184,250
	[stat].[StatTypeNumeric0109]	117,115,116,113
	[stat].[StatTypeDate]	156,90,155,87,66,84,49,54,64,170,171,89
	[stat].[StatTypeNchar50]	157,161,207,206
	[stat].[StatTypeBit]	65,103,101,51,85
	[stat].[StatTypeNchar100]	104,52,53,86
	[stat].[StatTypeNumeric1604]	452,494,493,492,474,000,000
	[stat].[StatTypeNumeric1312]	200

*/
 			,CASE -- StatId is evaluated in order of greatest volume in TargetTable to least volume of in TargetTable, and then evaluated by order of greatest volume in a stat to least volume in a stat
					WHEN seb.StatId IN( 670, 243,450,79,108,164,435,106,154,83,80,449,448,167,177,175,253,159,109,245,446,445,444,63,59,46,141,61,43,239,319,328,149,367,238,345,454,241,368,48,55,73,152,138,201,57,45,50,67,381,382,74,75,76,77,78,68,69,70,71,72,82,100,169 ,663,664,654,656,657,658 ,660,661 ,135,136 ) -- int
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueInt, 0 ) ) -- int (has the greatest number of rows in the initial Full Load)

					WHEN seb.StatId IN( 157,161, 102,92,97,94,99,93,98,247,249,91,96,246,248,185 ) -- bigint (has the 3rd greatest number of rows in the initial Full Load)
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueBigint, 0 ) )
											  
					WHEN seb.StatId IN( 242,451,107,163,439,41,244,158,105,165,174,176,173,95,162,160,110,254,447,62,58,47,142,60,38,42,318,147,453,240,134,151,137,342,315,337,56,44,39,81,168 ,653,659,662 ) -- decimal(16,2)
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1602, 0.00 ) ) -- decimal(16,2)  (has the 2nd greatest number of rows in the initial Full Load)

					WHEN seb.StatId IN( 155,156,87,90,66,84,49,54,64,170,171,89 ) -- date
						THEN CONVERT( nvarchar(100), seb.StatValueDate, 121 )

					WHEN seb.StatId IN( 101 ) -- bit default 1
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueBit, 1 ) ) --(VALID-154: Update the logic to properly promote bit stats with a non-default value to bring them in line with PNC and FNB, reverting the adjustments made as part of CCF2483)

					--WHEN seb.StatId IN( 101,103,65,51,85 ) -- bit default NULL (CCF2483: this forces both values, 0 and 1, to pass through to IFA.  This is meant to be temporary, while changing from a default of 1 to a default of 0.  We must also correct the values in IFA, hence leveraging NULL to get the correct values to IFA.)
					--	THEN CONVERT( nvarchar(100), seb.StatValueBit )

					WHEN seb.StatId IN( 103,65,51,85 ) -- bit default 0
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueBit, 0 ) ) --(VALID-154: Update the logic to properly promote bit stats with a non-default value to bring them in line with PNC and FNB, reverting the adjustments made as part of CCF2483)

					WHEN seb.StatId IN( 207,206 ,655 ) -- nchar(50)
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueNchar100, N'' ) ) -- Houston, we have a (minor) problem.  These stats are configured as nchar(50) in stat.Stat.

					WHEN seb.StatId IN( 104,52,53,86 ) -- nchar(100)
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueNchar100, N'' ) )

					WHEN seb.StatId IN( 452,494,493,492,474,473,472 ) -- numeric(16,4) -- ,472,473
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1604, 0.0000 ) ) -- Note: in [FTBRisk].[dbo].[StatExportBulk], Diana has the column as "Decimal" rather than "Numeric".

					-- vv -- AccountUtilization -- vv --
-- TO-DO: find out how bad the default value X.000 vs X.00 situation really is.
					WHEN seb.StatId IN( 117 ) -- numeric(9,3)/[stat].[StatTypeNumeric0109] -- AccountUtilizationOverall
						--THEN CONVERT( nvarchar(100), NULLIF( NULLIF( seb.StatValueNumeric0109, N'0.000' ), N'0.00' ) )
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1604, 0.0000 ) )

					WHEN seb.StatId IN( 115 ) -- numeric(9,3)/[stat].[StatTypeNumeric0109] -- AccountUtilizationDeposits
						--THEN CONVERT( nvarchar(100), NULLIF( NULLIF( seb.StatValueNumeric0109, N'2.000' ), N'2.00' ) )
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1604, 2.0000 ) )

					WHEN seb.StatId IN( 116 ) -- numeric(9,3)/[stat].[StatTypeNumeric0109] -- AccountUtilizationSpend
						--THEN CONVERT( nvarchar(100), NULLIF( NULLIF( seb.StatValueNumeric0109, N'3.000' ), N'3.00' ) )
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1604, 3.0000 ) )

					WHEN seb.StatId IN( 113 ) -- numeric(9,3)/[stat].[StatTypeNumeric0109] -- AccountUtilizationBalance
						--THEN CONVERT( nvarchar(100), NULLIF( NULLIF( seb.StatValueNumeric0109, N'1.000' ), N'1.00' ) )
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1604, 1.0000 ) )

					-- ^^ -- AccountUtilization -- ^^ --

					WHEN seb.StatId IN( 200 ) -- numeric(13,12)
						THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueDecimal1604, 0.000000000000 ) )

					--WHEN seb.StatId IN( -666 ) 
					--	THEN CONVERT( nvarchar(100), NULLIF( seb.StatValueInt, N'0' ) )

					--ELSE CONVERT( nvarchar(100), 1/0 ) -- the StatId is not accounted for.
					ELSE CONVERT( nvarchar(100), N'StatId = ' + CONVERT( nvarchar(100), seb.StatId ) + N' is not accounted for.' )
				END AS StatValue

		FROM #BinFetch bin
			INNER JOIN [FTBRisk].[dbo].[StatExportBulk] seb
				ON bin.StatBuildBatchId = seb.StatBuildBatchId
					AND bin.KeyTypeId = seb.KeyTypeId
					AND bin.StatId = seb.StatId

			------ ... select top 50 * from [stat].[vwGeo]
			------ ... select top 50 * from [stat].[Location]
			------ ... select top 50 * from [ftb].[tmpLocation]
			------ ... select distinct LocationOrgCode into [ftb].[tmpLocationOrgCode] from [FTBRisk].[dbo].[StatExportBulk] seb where LocationOrgCode is not null
			----LEFT JOIN [ftb].[tmpLocation] li ON seb.LocationOrgId = li.OrgId
			----LEFT JOIN [ftb].[tmpLocation] lo ON seb.LocationOrgCode = lo.LocationCode
			----LEFT JOIN [stat].[vwLocation] lo ON seb.LocationOrgCode = lo.LocationCode
			--LEFT JOIN [stat].[ClientLocation] lo ON seb.LocationOrgCode = lo.ClientLocationCode AND seb.ParentOrgId = lo.ClientOrgId -- lo.ClientOrgId = @iClientOrgId (FTB)

			--LEFT JOIN [stat].[vwGeo] gl ON seb.GeoLarge = gl.GeoCode -- gl.[Name]
			--LEFT JOIN [stat].[vwGeo] gs ON seb.GeoSmall = gs.GeoCode -- gs.[Name]
			--LEFT JOIN [stat].[vwGeo] gz ON seb.GeoZip4 = gz.GeoCode -- gz.[Name]
			--LEFT JOIN [stat].[GeoUnique] gl ON seb.GeoLarge = gl.GeoCode -- gl.[Name]
			--LEFT JOIN [stat].[GeoUnique] gs ON seb.GeoSmall = gs.GeoCode -- gs.[Name]
			LEFT JOIN #tmpGeoUnique gl ON seb.GeoLarge = gl.GeoCode -- gl.[Name]
			LEFT JOIN #tmpGeoUnique gs ON seb.GeoSmall = gs.GeoCode -- gs.[Name]

--  DONE: join [stat].[DollarStratRange] when DollarStrat stats become a part of the StatExportBulk set.
			--LEFT JOIN [stat].[DollarStratRange] dsr	ON seb.DollarStrat >= dsr.RangeFloor AND seb.DollarStrat <= dsr.RangeCeiling
			LEFT JOIN #tmpDollarStratRange dsr	ON seb.DollarStrat >= dsr.RangeFloor AND seb.DollarStrat <= dsr.RangeCeiling

			--LEFT JOIN [stat].[Channel] chan ON seb.ChannelId = chan.ChannelId
			LEFT JOIN #tmpChannel chan ON seb.ChannelId = chan.ChannelId

		WHERE 1 = 1
			AND seb.StatBuildBatchId = @iBatchId
			and exists( select 'x' from @tbStatList as x where x.StatId = seb.StatId ) -- and x.KeyTypeId = seb.KeyTypeId
			and exists( select 'x' from @tbKeyTypeList as x where x.KeyTypeId = seb.KeyTypeId ) 
			AND ISNULL( GeoLarge, N'☠' ) <> N'XX - XX'
			AND ISNULL( GeoSmall, N'☠' ) <> N'XX - XXX'
			AND ISNULL( GeoZip4, N'☠' )  <> N'XX - XXXX'

--			AND seb.StatBuildName = 'FTB'
--and StatExportBulkId in( 1, 2, 3, 4, 5 )
--and seb.KeyTypeId in -- just until we figure out the oddball key types we haven't configured yet, or lack supporting data.
--	(
--		 107
--		,108
--		,109
--		,110
--		,111 -- FTB - CCL (Customer Channel Location)
--		,112
--		,113
--		,127
--	)
OPTION ( MAXDOP 4 ) 
		;
select @iError = @@ERROR, @biRecCount = ROWCOUNT_BIG()
;
if @iError > 0
begin
	print N'Insert into [HashWork] failed.  Stopping execution with "SET NOEXEC ON"';
	set noexec on;
end
;
--		SET @biRecCount = @@ROWCOUNT -- ROWCOUNT_BIG()
		;
		IF @tiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + CONVERT( nvarchar(50), @biRecCount ) + N' bulk rows inserted in [ftb].[HashWork].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END

update [ftb].[SubBatch] set [HashWorkComplete] = sysdatetime(), [HashWorkRowCount] = [HashWorkRowCount] + @biRecCount, [RawRowCount] = [RawRowCount] + @biRecCount where [RowSeq] = @iSubBatch_RowSeq
;

		select @iBinRowSeq = min( b.RowSeq ) from #Bin b where b.RowSeq > @iBinRowSeq
		;
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;

	END -- @iBinRowSeq <= @iBinRowSeqMax
	;
if @biRecCount = 0 begin print N'No rows inserted into HashWork.  Stopping execution with "SET NOEXEC ON"'; set noexec on; end;

----select top 10 * from [FTBRisk].[dbo].[StatExportBulk] seb

--select top 100 '' AS [HashWork], * from [ftb].[HashWork]
;

/* commented out and moved to the bottom... (for troubleshooting's sake)
---- drop the filtered index on [FTBRisk].[dbo].[StatExportBulk]
--SET @nvExec = N'USE [FTBRisk];
--		DROP INDEX IF EXISTS [fx01StatExportBulk] ON [dbo].[StatExportBulk];
--	'
--SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
--RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
--EXEC (@nvExec)
--;
*/

		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Begin... [ftb].[HashWork] index check'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		--ALTER INDEX ALL ON [ftb].[HashWork] REBUILD 
		--;
		ALTER INDEX [ccxHashWork] ON [ftb].[HashWork] REBUILD -- performing a REBUILD on a clustered columnstore index compacts the content and purges the holes (dead zones) that build up over time from inserts, updates and deletes.
		;

		IF NOT EXISTS (SELECT 'X' FROM sys.objects AS o INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id WHERE s.name = N'ftb' AND o.name = N'pkHashWork' AND o.type = N'PK')
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...adding [pkHashWork] to [ftb].[HashWork]...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			SET @nvExec = N'ALTER TABLE [ftb].[HashWork] ADD  CONSTRAINT [pkHashWork] PRIMARY KEY --CLUSTERED 
				(
					[Seq] ASC, [HashId] ASC, [StatBatchLogId] ASC, [KeyTypeId] ASC, [StatId] ASC
				) WITH ( PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON ) ON [FG_ftb]'
			;
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			EXEC (@nvExec)
		END

		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[HashWork] index check ...end.'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		-- ----------------------------------------------------------------------------------------------------------------------------------




		-- ----------------------------------------------------------------------------------------------------------------------------------
		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Begin... [ftb].[StatValueBulk] upsert'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		set @iBinRowSeq = @iBinRowSeqMin -- reset to the first bin
		;

		SELECT @biRecCount = COUNT_BIG(1) FROM [ftb].[StatValueBulk]
		;
		SET @biDeltaCheck = @biRecCount
		;
		IF @tiDebug > 0 
		BEGIN
			IF @biDeltaCheck > 0 
				SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'* This is not a Full Load *'
			;
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
		;

		--ALTER INDEX ALL ON [ftb].[StatValueBulk] DISABLE 
		--;
		DROP INDEX IF EXISTS [fx01StatValueBulk] ON [ftb].[StatValueBulk]
		;

		-- delta update, because [ftb].[StatValueBulk] already has rows
		IF @tiDebug > 0 AND @biDeltaCheck > 0
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[StatValueBulk] ...delta update...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
		SELECT @biUpdateAvailableRecCount = COUNT_BIG(1)
		FROM [ftb].[HashWork] AS t
			INNER JOIN [ftb].[StatValueBulk] AS u
				ON t.KeyTypeId = u.KeyTypeId
					AND t.StatId = u.StatId
					AND t.HashId = u.HashId
		WHERE 1 = 1
			AND @biDeltaCheck > 0
			--AND t.StatBatchLogId = @iStatBatchLogId
			AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			AND ISNULL( t.StatValue, N'☠' ) <> ISNULL( u.StatValue, N'☠' )
			AND u.StatBatchLogId < @iStatBatchLogId
		;


	WHILE ISNULL( @iBinRowSeq, ( @iBinRowSeqMax + 1 ) ) <= @iBinRowSeqMax
	BEGIN -- @iBinRowSeq <= @iBinRowSeqMax
	
		select @iBinSeq = b.BinSeq from #Bin b where b.RowSeq = @iBinRowSeq
		;
		print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Upserting Bin: ' + convert( nvarchar(50), @iBinRowSeq ) + N' of ' + convert( nvarchar(50), @iBinRowSeqMax )
		;

		truncate table #BinFetch
		;
		insert into #BinFetch
			(
				 KeyTypeId
				,StatId
				,StatBuildBatchId

				,BinSeq
				,RowSeq
			)
		select
				 KeyTypeId
				,StatId
				,StatBuildBatchId

				,BinSeq
				,RowSeq
		from #BinContent as bc
		where bc.BinSeq = @iBinSeq
		order by bc.KeyTypeId, bc.StatId
		;

		SET @biRecCount = 0
		;
--print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...This is when the UPDATE of [ftb].[HashWork] would occur...'
/* TO-DO: remove coment block, re-enabling the UPDATE */
		UPDATE u -- Update existing rows in [ftb].[StatValueBulk]
		SET StatBatchLogId = @iStatBatchLogId
			--,StatValue = NULLIF( t.StatValue, N'☠' )
			,StatValue = t.StatValue
		FROM #BinFetch bin
			INNER JOIN [ftb].[HashWork] AS t
				ON bin.KeyTypeId = t.KeyTypeId
					AND bin.StatId = t.StatId
			INNER JOIN [ftb].[StatValueBulk] AS u
				ON t.KeyTypeId = u.KeyTypeId
					AND t.StatId = u.StatId
					AND t.HashId = u.HashId
		WHERE 1 = 1
			AND @biDeltaCheck > 0
			--AND t.StatBatchLogId = @iStatBatchLogId
			AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			AND ISNULL( t.StatValue, N'☠' ) <> ISNULL( u.StatValue, N'☠' )
--and u.StatBatchLogId > 874
			AND u.StatBatchLogId < @iStatBatchLogId
		OPTION (RECOMPILE)
		;
/* TO-DO: remove coment block, re-enabling the UPDATE */

		SET @biRecCount = @@ROWCOUNT
		;
		IF @tiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + CONVERT( nvarchar(50), @biRecCount ) + N' delta rows updated in [ftb].[StatValueBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
update [ftb].[SubBatch] set [StatValueBulkUpdateComplete] = sysdatetime(), [StatValueBulkUpdateCount] = @biRecCount where [RowSeq] = @iSubBatch_RowSeq
;


		/*
		-- delta insert, because [ftb].[StatValueBulk] already has rows
		IF @tiDebug > 0 AND @biDeltaCheck > 0
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[StatValueBulk] ...delta insert...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
		*/

		--SELECT @biInsertAvailableRecCount = COUNT_BIG(1)
		--FROM [ftb].[HashWork] AS t
		--WHERE 1 = 1
		--	AND @biDeltaCheck > 0
		--	AND t.StatBatchLogId = @iStatBatchLogId
		--	AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
		--	--AND ISNULL( @iBatchDataSetRefreshLogId, 0 ) > 0
		--	--AND t.StatValue <> N'☠' -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
		--	AND t.StatValue IS NOT NULL -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
		--	AND NOT EXISTS
		--		(
		--			SELECT 'X'
		--			FROM [ftb].[StatValueBulk] x
		--			--WHERE x.StatBatchLogId = @siStatBatchLogId
		--			WHERE x.KeyTypeId = t.KeyTypeId
		--				AND x.StatId = t.StatId
		--				AND x.HashId = t.HashId
		--		)
		--;
		SET @biRecCount = 0
		;

--if @biDeltaCheck > 0 print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...This is when the Delta INSERT of [ftb].[HashWork] would occur...'
/* TO-DO: remove coment block, re-enabling the INSERT */
		INSERT INTO [ftb].[StatValueBulk]
			(
				 StatBatchLogId
				,KeyTypeId
				,StatId
				,HashId
				,StatValue
			)
		SELECT
			 --@iStatBatchLogId AS StatBatchLogId
			 t.StatBatchLogId
			,t.KeyTypeId
			,t.StatId
			,t.HashId
			--,NULLIF( t.StatValue, N'☠' ) AS StatValue
			,t.StatValue AS StatValue -- FullSet mode _and_ Delta mode( the WHERE filters out any NULLs from being inserted )
		FROM #BinFetch bin
			INNER JOIN [ftb].[HashWork] AS t
				ON bin.KeyTypeId = t.KeyTypeId
					AND bin.StatId = t.StatId
		WHERE 1 = 1
			AND @biDeltaCheck > 0
			--AND t.StatBatchLogId = @iStatBatchLogId
			AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			--AND ISNULL( @iBatchDataSetRefreshLogId, 0 ) > 0
			--AND t.StatValue <> N'☠' -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
			AND t.StatValue IS NOT NULL -- FullSet mode _and_ Delta mode ( filters out any NULLs from being inserted );
			AND NOT EXISTS
				(
					SELECT 'X'
					FROM [ftb].[StatValueBulk] x
					--WHERE x.StatBatchLogId = @siStatBatchLogId
					WHERE x.KeyTypeId = t.KeyTypeId
						AND x.StatId = t.StatId
						AND x.HashId = t.HashId
				)
		OPTION (RECOMPILE)
		;
/* TO-DO: remove coment block, re-enabling the INSERT */

		SET @biRecCount = @@ROWCOUNT
		;
		SET @biInsertAvailableRecCount = @biRecCount
		;
		IF @tiDebug > 0 
		BEGIN
			IF @biDeltaCheck > 0
			begin
				SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + CONVERT( nvarchar(50), @biRecCount ) + N' delta rows inserted into [ftb].[StatValueBulk]'
				--ELSE
				--	SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'** This is not a Delta Load **'
				--;
				RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			end
		END
		;
update [ftb].[SubBatch] set [StatValueBulkInsertDeltaComplete] = sysdatetime(), [StatValueBulkInsertDeltaCount] = [StatValueBulkInsertDeltaCount] + @biRecCount where @biDeltaCheck > 0 and [RowSeq] = @iSubBatch_RowSeq
;

		-- get full load row count _before_ performing the full load insert, because if >0, then we will drop the pk, disable [ix01StatValueBulk], and recreate/rebuild them after the full load is inserted
		SELECT @biInsertAvailableRecCount = ISNULL( @biInsertAvailableRecCount, 0 ) + COUNT_BIG(1)
		FROM [ftb].[HashWork] AS t
		WHERE 1 = 1
			AND @biDeltaCheck = 0
			--AND t.StatBatchLogId = @iStatBatchLogId
			AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			--AND t.Seq2 = 1
			AND t.StatValue IS NOT NULL
		;

		-- full load, because [ftb].[StatValueBulk] is empty and not all HashWork.StatValue's are null.
		IF @biDeltaCheck = 0 AND @biInsertAvailableRecCount > 0
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[StatValueBulk] ...full insert... ...disabling indexes and dropping PK'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			--ALTER INDEX [ix01StatValueBulk_StatBatchLogId_KeyTypeId_HashId] ON [ftb].[StatValueBulk] DISABLE;
			ALTER TABLE [ftb].[StatValueBulk] DROP CONSTRAINT IF EXISTS [pkStatValueBulk] WITH ( ONLINE = OFF )
			;
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[StatValueBulk] ...full insert... Begin...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT
			;
		END

		SET @biRecCount = 0
		;

--if @biDeltaCheck = 0 print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...This is when the Full INSERT of [ftb].[HashWork] would occur...'
/* TO-DO: remove coment block, re-enabling the INSERT-full */
		INSERT INTO [ftb].[StatValueBulk] ( [StatBatchLogId], [KeyTypeId], [HashId], [StatId], [StatValue] ) -- , [Tid]
		SELECT
			-- t.[StatExportBulkId]
			 t.[StatBatchLogId]
			,t.[KeyTypeId]
			,t.[HashId]
			,t.[StatId]
 			,t.[StatValue]
		FROM #BinFetch bin
			INNER JOIN [ftb].[HashWork] AS t
				ON bin.KeyTypeId = t.KeyTypeId
					AND bin.StatId = t.StatId
		WHERE 1 = 1
			AND @biDeltaCheck = 0
			--AND t.StatBatchLogId = @iStatBatchLogId
			AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			--AND t.Seq2 = 1
			AND t.StatValue IS NOT NULL
		OPTION (RECOMPILE)
		;
/* TO-DO: remove coment block, re-enabling the INSERT-full */

		SET @biRecCount = @@ROWCOUNT
		;
		IF @tiDebug > 0 
		BEGIN
			IF @biDeltaCheck = 0 
			begin
				SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'All/FullLoad (' + CONVERT( nvarchar(50), @biRecCount ) + N') rows inserted into [ftb].[StatValueBulk].'
				--ELSE
				--	SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'* This is not a Full Load *'
				;
				RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			end
		END
		;
update [ftb].[SubBatch] set [StatValueBulkInsertFullComplete] = sysdatetime(), [StatValueBulkInsertFullCount] = [StatValueBulkInsertFullCount] + @biRecCount where @biDeltaCheck = 0 and [RowSeq] = @iSubBatch_RowSeq
;

		select @iBinRowSeq = min( b.RowSeq ) from #Bin b where b.RowSeq > @iBinRowSeq
		;
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;

	END -- @iBinRowSeq <= @iBinRowSeqMax
	;

		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N' Updated row count = ' + ISNULL( CONVERT( nvarchar(50), @biUpdateAvailableRecCount ), N'{null}' )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Inserted row count = ' + ISNULL( CONVERT( nvarchar(50), @biInsertAvailableRecCount ), N'{null}' )

		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuilding index...'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		IF @biInsertAvailableRecCount > 0
		BEGIN -- @biInsertAvailableRecCount > 0
			-- full load, because [ftb].[StatValueBulk] was empty
			IF @biDeltaCheck = 0
			BEGIN
				--SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + SPACE(1) + N'...full insert... ([ftb].[StatValueBulk])'
				--RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

				IF NOT EXISTS (SELECT 'X' FROM sys.objects AS o INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id WHERE s.name = N'ftb' AND o.name = N'pkStatValueBulk' AND o.type = N'PK')
				BEGIN
					SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...adding [pkStatValueBulk] to [ftb].[StatValueBulk]...'
					RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
					-- full load, so we recreate the PK
					SET @nvExec = N'ALTER TABLE [ftb].[StatValueBulk] ADD  CONSTRAINT [pkStatValueBulk] PRIMARY KEY --CLUSTERED 
					(
						[StatId] ASC,
						[KeyTypeId] ASC,
						[HashId] ASC
					)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_ftb]'
					;
					SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
					RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
					EXEC (@nvExec)
				END
			END
			ELSE -- delta load, so rebuild PK instead
			BEGIN
				IF DATENAME( weekday, SYSDATETIME() ) IN( 'Saturday', 'Sunday' ) AND @nvStatGenerator IN( N'Weekly AU Stats', N'FTB Weekly AU Stats' ) -- rebuild PK during weekends
				BEGIN
					SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuilding pkStatValueBulk on [ftb].[StatValueBulk]...'
					RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
--					ALTER INDEX [pkStatValueBulk] ON [ftb].[StatValueBulk] REBUILD 
					;
				END
			END
		END -- @biInsertAvailableRecCount > 0

		
			SELECT @iStatBatchLogId_Max = MAX( x1.StatBatchLogId ) 
			FROM [ftb].[vwBatchStatBatchLogXref] x1
			WHERE x1.StatBatchLogId < ( 
										--SELECT MAX( x2.StatBatchLogId ) AS StatBatchLogId 
										--FROM [ftb].[vwBatchStatBatchLogXref] x2
										--WHERE CycleDate > DATEADD( dd, -15, SYSDATETIME() ) 
										--	AND x2.HubBatchId IS NOT NULL 
										SELECT MIN( x2.StatBatchLogId ) AS StatBatchLogId 
										FROM [ftb].[vwBatchStatBatchLogXref] x2
										WHERE CycleDate = @dCycleDate 
									)
			;
			IF @iStatBatchLogId_Max IS NULL SET @iStatBatchLogId_Max = ISNULL( ( SELECT MAX( StatBatchLogId ) - 1 FROM [ftb].[vwBatchStatBatchLogXref] ), 0 )
			;
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'fx01StatValueBulk filter by "[StatBatchLogId] > ' + CONVERT( nvarchar(50), @iStatBatchLogId_Max ) + N'"'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

			SET @nvExec = N'
					DROP INDEX IF EXISTS [fx01StatValueBulk] ON [ftb].[StatValueBulk];
					CREATE NONCLUSTERED INDEX [fx01StatValueBulk] ON [ftb].[StatValueBulk]
					(
						[StatBatchLogId] ASC,
						[KeyTypeId] ASC,
						[HashId] ASC,
						[StatId] ASC
					)
					INCLUDE ( [StatValue] )
					WHERE ( [StatBatchLogId] > (' + CONVERT( nvarchar(50), @iStatBatchLogId_Max ) + N') ) AND ( [StatBatchLogId] <= (' + CONVERT( nvarchar(50), @iStatBatchLogId ) + N') )
					WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_ftb]
				' -- fx01StatValueBulk
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			EXEC (@nvExec)
			;


		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[StatValueBulk] ...end.'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		-- ----------------------------------------------------------------------------------------------------------------------------------






		-- ----------------------------------------------------------------------------------------------------------------------------------
		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Begin... [ftb].[HashBulk]'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		--ALTER INDEX ALL ON [ftb].[HashBulk] REBUILD 
		--;
		ALTER INDEX [ccxHashBulk] ON [ftb].[HashBulk] REBUILD -- performing a REBUILD on a clustered columnstore index compacts the content and purges the holes (dead zones) that build up over time from inserts, updates and deletes.
		;

		IF NOT EXISTS (SELECT 'X' FROM sys.objects AS o INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id WHERE s.name = N'ftb' AND o.name = N'pkHashBulk' AND o.type = N'PK')
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...adding [pkHashBulk] to [ftb].[HashBulk]...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			-- full load, so we recreate the PK
			SET @nvExec = N'ALTER TABLE [ftb].[HashBulk] ADD  CONSTRAINT [pkHashBulk] PRIMARY KEY --CLUSTERED 
			(
				[StatBatchLogId] ASC,
				[HashId] ASC
			)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_ftb]'
			;
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			EXEC (@nvExec)
		END


-- declare @iStatBatchLogId int = 819; set nocount off;
		--   ( [StatExportBulkId], [StatBatchLogId], [KeyTypeId], [StatId], [Seq], [Seq2], [HashId], [ParentOrgId], [CustomerIdIFA], [IdMac], [IdTypeId], [IdOrgId], [IdStateId], [CustomerNumberStringFromClient], [CustomerAccountNumber], [ClientOrgId], [PayerClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LengthPayerAccountNumber], [ChannelId], [LocationOrgId], [LocationOrgCode], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStrat], [StatValue] ) -- ftb.HashWork
		INSERT INTO [ftb].[HashBulk] ( [StatBatchLogId], [KeyTypeId],                          [HashId], [ParentOrgId], [CustomerIdIFA], [IdMac], [IdTypeId], [IdOrgId], [IdStateId], [CustomerNumberStringFromClient], [CustomerAccountNumber], [ClientOrgId], [PayerClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LengthPayerAccountNumber], [ChannelOrgId], [ChannelId], [LocationOrgId], [LocationOrgCode], [GeoOrgId], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStratRangeId], [DollarStrat] )
		--INSERT INTO [ftb].[HashBulk] ( [StatBatchLogId], [KeyTypeId], [HashId], [ParentOrgId], [CustomerIdIFA], [IdMac], [IdTypeId], [IdOrgId], [IdStateId], [CustomerNumberStringFromClient], [ClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LocationOrgId], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStrat] )
		SELECT 
		DISTINCT -- this has become necessary, once more, because of the introduction of bin packing. LSW 2020-09-30
		---- ISNULL( t.IdMac, 1/0 )
			----[StatBatchLogId], [KeyTypeId], [HashId], [ParentOrgId], [CustomerIdIFA], t.IdMac, [IdTypeId], [IdOrgId], [IdStateId], [CustomerNumberStringFromClient], [ClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LocationOrgId], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStrat]
			  t.[StatBatchLogId], t.[KeyTypeId]
			, t.[HashId]
			, t.[ParentOrgId]
			, t.[CustomerIdIFA], t.[IdMac], t.[IdTypeId], t.[IdOrgId], t.[IdStateId]
--TO-DO: check that this lines up with T D B and not M T B ==>> ----, CASE WHEN StatId IN( 206, 207 ) THEN t.[CustomerNumberStringFromClient] ELSE NULL END AS [CustomerNumberStringFromClient] -- is ?FTB? is Customer AccountNumber based.  So, we null out Customer NumberStringFromClient
			, NULL AS [CustomerNumberStringFromClient]
			, t.[CustomerAccountNumber]
			, t.[ClientOrgId], t.[PayerClientOrgId], t.[PayerRoutingNumber], t.[PayerAccountNumber], t.[LengthPayerAccountNumber]
			, t.[ChannelOrgId], t.[ChannelId], t.[LocationOrgId], t.[LocationOrgCode], t.[GeoOrgId], t.[GeoLarge], t.[GeoSmall], t.[GeoZip4], t.[DollarStratRangeId], t.[DollarStrat]
		FROM [ftb].[HashWork] AS t
		--FROM [ftb].[StatValueBulk] AS svb
		--	inner join [ftb].[HashWork] AS t
		--		on svb.StatBatchLogId = t.StatBatchLogId
		--			and svb.KeyTypeId = t.KeyTypeId
		--			and svb.HashId = t.HashId
		----WHERE t.StatBatchLogId = @iStatBatchLogId
		WHERE 1 = 1
			AND t.StatBatchLogId = @iStatBatchLogId
			AND t.Seq = 1 -- this is our shortcut to a "DISTINCT" list of HashId values (without actually performing a DISTINCT in the SELECT)
			----AND t.HashId IS NOT NULL -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			AND t.HashId <> 0x0 -- 2019-10-18 02:49 - LSW - Because GeoCodes "XX - XX", "XX - XXX", and "XX - XXXX" blow.
			--AND EXISTS( SELECT 'X' FROM [ftb].[StatValueBulk] AS x WHERE x.KeyTypeId = t.KeyTypeId AND x.HashId = t.HashId ) -- only add the HashId to [HashBulk] if it has a row in [StatValueBulk].
			AND EXISTS( SELECT 'X' FROM [ftb].[StatValueBulk] AS x WHERE t.StatBatchLogId = x.StatBatchLogId AND x.KeyTypeId = t.KeyTypeId AND x.HashId = t.HashId ) -- only add the HashId to [HashBulk] if it has a row in [StatValueBulk].
			AND NOT EXISTS( SELECT 'X' FROM [ftb].[HashBulk] AS x WHERE x.HashId = t.HashId ) -- only add the HashId to [HashBulk] if it does not already exist in [HashBulk].
		OPTION (RECOMPILE)
		;

		SET @biRecCount = @@ROWCOUNT
		;
		IF @tiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + CONVERT( nvarchar(50), @biRecCount ) + N' new rows inserted in [ftb].[HashBulk].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
set nocount off
update [ftb].[SubBatch] set [HashBulkComplete] = sysdatetime(), [HashBulkCount] = case when @biRecCount > [HashBulkCount] then @biRecCount else [HashBulkCount] end where [RowSeq] = @iSubBatch_RowSeq
;

		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[HashBulk] ...end.'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;



		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'Begin... [ftb].[HashException]'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		INSERT INTO [ftb].[HashException] ( [ParentOrgId], [StatBatchLogId], [KeyTypeId], [Seq], [Seq2], [HashId], [CustomerIdIFA], [IdMac], [IdTypeId], [IdOrgId], [IdStateId], [CustomerNumberStringFromClient], [CustomerAccountNumber], [ClientOrgId], [PayerClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LengthPayerAccountNumber], [ChannelOrgId], [ChannelId], [LocationOrgId], [LocationOrgCode], [GeoOrgId], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStratRangeId], [DollarStrat] )
		SELECT 
		DISTINCT
			  [ParentOrgId]
			, [StatBatchLogId], [KeyTypeId]
			, [Seq]
			, [Seq2]
			, [HashId]
			, [CustomerIdIFA], [IdMac], [IdTypeId], [IdOrgId], [IdStateId]
			, [CustomerNumberStringFromClient] 
			, [CustomerAccountNumber]
			, [ClientOrgId], [PayerClientOrgId], [PayerRoutingNumber], [PayerAccountNumber], [LengthPayerAccountNumber]
			, [ChannelOrgId], [ChannelId], [LocationOrgId], [LocationOrgCode], [GeoOrgId], [GeoLarge], [GeoSmall], [GeoZip4], [DollarStratRangeId], [DollarStrat]
		FROM [ftb].[HashWork] AS t
		WHERE 1 = 1
			AND t.StatBatchLogId = @iStatBatchLogId
			AND t.Seq = 1 -- this is our shortcut to a "DISTINCT" list of HashId values (without actually performing a DISTINCT in the SELECT)
			AND t.HashId = 0x0
		OPTION (RECOMPILE)
		;

		SET @biRecCount = @@ROWCOUNT
		;
		IF @tiDebug > 0 
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + CONVERT( nvarchar(50), @biRecCount ) + N' new rows inserted in [ftb].[HashException].'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		END
update [ftb].[SubBatch] set [HashExceptionCount] = case when @biRecCount > [HashExceptionCount] then @biRecCount else [HashExceptionCount] end where [RowSeq] = @iSubBatch_RowSeq
;

		IF @biRecCount > 0
		BEGIN
			SELECT @nvSubject = N'Warning: Unresolved hashes added to [ftb].[HashException]'
				,@nvBody = N'Warning.  Investigation required.  Some hashes could not be resolved by ' + @nvJobRef + NCHAR(013) + NCHAR(010) + N'{MT:' + CONVERT( nvarchar(50), NEWID() ) + N'}' -- "MT:" means "Message Tag"
			;
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'*** ' + @nvBody + N' ***'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			EXEC [stat].[uspSendEmail]
				 @pnvRecipients = 'dbmonitoralerts@validsystems.net;lwhiting@validadvantage.com;'
				---- @pnvRecipients = 'dwsupport@validsystems.net;'
				-- @pnvRecipients = 'lwhiting@validadvantage.com;'
				,@pnvSubject = @nvSubject
				,@pnvBodyText = @nvBody
			;
		END

		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[HashException] ...end.'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;



		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuilding [ftb].[HashBulk] index...'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

		IF DATENAME( weekday, SYSDATETIME() ) IN( 'Saturday', 'Sunday' ) AND @nvStatGenerator IN( N'Weekly AU Stats', N'FTB Weekly AU Stats' ) -- rebuild PK during weekends
		BEGIN
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuilding [pkHashBulk] on [ftb].[HashBulk]...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			--ALTER INDEX ALL ON [ftb].[HashBulk] REBUILD 
			ALTER INDEX [pkHashBulk] ON [ftb].[HashBulk] REBUILD 
			;
		END

		--ELSE -- just rebuild the unique index during the week.
		IF @nvStatGenerator IN( N'Daily Stats - Account', N'FTB Daily Stats - Account' )
		BEGIN -- always rebuild the unique index
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuilding ux01HashBulk on [ftb].[HashBulk]...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			ALTER INDEX [ux01HashBulk] ON [ftb].[HashBulk] REBUILD
			;
		END


		--IF ( @biUpdateAvailableRecCount + @biInsertAvailableRecCount ) > 0
		IF @nvStatGenerator IN( N'Weekly AU Stats', N'FTB Weekly AU Stats' ) -- rebuild after AU Stats have completed
		BEGIN	-- if there were updates and/or inserts, then rebuild non PK indexes

			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuild indexes on [ftb].[StatValueBulk]...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

			/*
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuilding fx01StatValueBulk on [ftb].[StatValueBulk]...'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
				SET ANSI_PADDING OFF
				GO
				CREATE NONCLUSTERED INDEX [ix01StatValueBulk_StatBatchLogId_KeyTypeId_HashId] ON [ftb].[StatValueBulk]
				(
					[StatBatchLogId] ASC,
					[KeyTypeId] ASC,
					[HashId] ASC
				)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_ftb]
			*/
			--ALTER INDEX [ix01StatValueBulk_StatBatchLogId_KeyTypeId_HashId] ON [ftb].[StatValueBulk] REBUILD;
			
			/*
				SET ANSI_PADDING OFF
				GO
				CREATE NONCLUSTERED INDEX [ix02StatValueBulk_KeyTypeId_HashId] ON [ftb].[StatValueBulk]
				(
					[KeyTypeId] ASC,
					[HashId] ASC
				)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_ftb]
			*/
			--ALTER INDEX [ix02StatValueBulk_KeyTypeId_HashId] ON [ftb].[StatValueBulk] REBUILD 
			
/* 2021-05-02 Commented out because it keeps colliding with the full backup and system index manager.

			IF DATENAME( weekday, SYSDATETIME() ) IN( 'Saturday', 'Sunday' ) -- rebuild during weekends
			BEGIN

				SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuild [ccxStatValueBulk] on [ftb].[StatValueBulk]...'
				RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

				ALTER INDEX [ccxStatValueBulk] ON [ftb].[StatValueBulk] REBUILD -- performing a REBUILD on a clustered columnstore index compacts the content and purges the holes (dead zones) that build up over time from inserts, updates and deletes.
				;

				SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...rebuild [pkStatValueBulk] on [ftb].[StatValueBulk]...'
				RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

				ALTER INDEX [pkStatValueBulk] ON [ftb].[StatValueBulk] REBUILD 
				;

			END
			;
*/

		END -- ...if there were updates and/or inserts, then rebuild non PK indexes.

		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'[ftb].[HashBulk] ...end.'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		-- ----------------------------------------------------------------------------------------------------------------------------------

--SELECT '' AS [HashBulk], * FROM [ftb].[HashBulk]
--SELECT '' AS [HashException], * FROM [ftb].[HashException]


update [ftb].[SubBatch] set [SubBatchEnd] = sysdatetime(), [SubBatchDuration_Second] = DATEDIFF( second, [SubBatchBegin], sysdatetime() ) where [RowSeq] = @iSubBatch_RowSeq
;


/*
		-- ----------------------------------------------------------------------------------------------------------------------------------
		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'split into legacy Bulk tables: Begin...'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;

			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'   Updating [KCPBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			update u set u.StatBatchLogId = svb.StatBatchLogId, u.StatValue = svb.StatValue 
			from [ftb].[StatValueBulk] svb inner join [ftb].[KCPBulk] u on svb.HashId = u.HashId and 
			svb.StatId = u.StatId where svb.StatBatchLogId = @iStatBatchLogId and 
			svb.KeyTypeId = 109 and 
			isnull( svb.StatValue, N'☠' ) <> isnull( u.StatValue, N'☠' )
			;

			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'   Inserting [KCPBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			insert into [ftb].[KCPBulk] ( [StatId], [HashId], [StatBatchLogId], [StatValue], [CustomerAccountNumber], [RoutingNumber], [AccountNumber], [ClientOrgId] )
			select svb.StatId, svb.HashId, svb.StatBatchLogId, svb.StatValue, hb.CustomerAccountNumber, hb.PayerRoutingNumber, hb.PayerAccountNumber, hb.ClientOrgId 
			from [ftb].[StatValueBulk] svb inner join [ftb].HashBulk hb on svb.HashId = hb.HashId where svb.StatBatchLogId = @iStatBatchLogId and svb.KeyTypeId = 109 and 
			not exists
			(
				select 'x' 
				from [ftb].[KCPBulk] x
				where x.StatId = svb.StatId and x.HashId = svb.HashId
			)
			;

		
		
			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'   Updating [CustomerAccountBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			update u set u.StatBatchLogId = svb.StatBatchLogId, u.StatValue = svb.StatValue 
			from [ftb].[StatValueBulk] svb inner join [ftb].[CustomerAccountBulk] u on svb.HashId = u.HashId and 
			svb.StatId = u.StatId where svb.StatBatchLogId = @iStatBatchLogId and 
			svb.KeyTypeId = 107 and 
			isnull( svb.StatValue, N'☠' ) <> isnull( u.StatValue, N'☠' ) 
			;

			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'   Inserting [CustomerAccountBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			insert into [ftb].[CustomerAccountBulk] ( [StatId], [HashId], [StatBatchLogId], [StatValue], [CustomerAccountNumber], [ClientOrgId] )
			select svb.StatId, svb.HashId, svb.StatBatchLogId, svb.StatValue, hb.CustomerAccountNumber, hb.ClientOrgId 
			from [ftb].[StatValueBulk] svb inner join [ftb].[HashBulk] hb on svb.HashId = hb.HashId where svb.StatBatchLogId = @iStatBatchLogId and svb.KeyTypeId = 107 and 
			not exists
			(
				select 'x' 
				from [ftb].[CustomerAccountBulk] x
				where x.StatId = svb.StatId and x.HashId = svb.HashId
			)
			;



			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'   Updating [PayerBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			update u set u.StatBatchLogId = svb.StatBatchLogId, u.StatValue = svb.StatValue 
			from [ftb].[StatValueBulk] svb inner join [ftb].[PayerBulk] u on svb.HashId = u.HashId and 
			svb.StatId = u.StatId where svb.StatBatchLogId = @iStatBatchLogId and 
			svb.KeyTypeId = 108 and 
			isnull( svb.StatValue, N'☠' ) <> isnull( u.StatValue, N'☠' )
			;

			SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'   Inserting [PayerBulk]'
			RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
			insert into [ftb].[PayerBulk] ( [StatId], [HashId], [StatBatchLogId], [StatValue], [RoutingNumber], [AccountNumber], [ClientOrgId] )
			select svb.StatId, svb.HashId, svb.StatBatchLogId, svb.StatValue, hb.PayerRoutingNumber, hb.PayerAccountNumber, isnull( hb.ClientOrgId, @iClientOrgId ) as ClientOrgId
			from [ftb].[StatValueBulk] svb inner join [ftb].[HashBulk] hb on svb.HashId = hb.HashId where svb.StatBatchLogId = @iStatBatchLogId and svb.KeyTypeId = 108 and 
			not exists
			(
				select 'x' 
				from [ftb].[PayerBulk] x
				where x.StatId = svb.StatId and x.HashId = svb.HashId
			)
			;
		
		SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'split into legacy Bulk tables: ...end.'
		RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
		-- ----------------------------------------------------------------------------------------------------------------------------------
*/



--		EXEC [ftb].[uspBatchTransferComplete] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId OUTPUT, @ptiDebug = @tiDebug
		;

		SET @piStatBatchLogId = @iStatBatchLogId -- setting the output value
		;

--	END 


/* TO-DO: uncomment when ready to have the filtered index dropped at the end of each run
-- drop the filtered index on [FTBRisk].[dbo].[StatExportBulk]
SET @nvExec = N'USE [FTBRisk];
		DROP INDEX IF EXISTS [fx01StatExportBulk] ON [dbo].[StatExportBulk];
	'
SET @nvMessage = CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvExec
RAISERROR ( @nvMessage, 0, 1 ) WITH NOWAIT;
EXEC (@nvExec)
;
*/

	IF @tiDebug > 0 
	BEGIN
		PRINT N''
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'StatBatchLogId = ' + ISNULL( CONVERT( nvarchar(50), @iStatBatchLogId ), N'{null}' )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'     CycleDate = ' + ISNULL( CONVERT( nvarchar(50), @dCycleDate, 121 ), N'{null}' )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'       BatchId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchId ), N'{null}' )
		PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N' StatGenerator = ' + ISNULL( @nvStatGenerator, N'{null}' )
		--PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'@iBatchDataSetRefreshLogId = ' + ISNULL( CONVERT( nvarchar(50), @iBatchDataSetRefreshLogId ), N'{null}' )
		RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
	END
SET @dtThisEnd = getdate()


--print CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...This is when the oddball 5 tables would be upserted/handled...'
/* TO-DO: *** this block is commented out during testing *** ... */
IF @nvStatGenerator = N'Daily Stats - Trxn and Return'
	BEGIN
		EXEC [ftb].[uspDollarStratStatsDelta_TransferToAtomicStat] @piStatBatchLogId = @iStatBatchLogId, @ptiDebug = @tiDebug
		;
	END
;
IF @nvStatGenerator IN( N'Daily Stats - Account', N'FTB Daily Stats - Account' )
	BEGIN
		EXEC [ftb].[uspABAAcctLengthStatsDelta_TransferToAtomicStat] @piStatBatchLogId = @iStatBatchLogId, @ptiDebug = @tiDebug
		;
		EXEC [ftb].[uspABAStatsDelta_TransferToAtomicStat] @piStatBatchLogId = @iStatBatchLogId, @ptiDebug = @tiDebug
		;
		EXEC [ftb].[uspOnUsAccountStatsDelta_TransferToAtomicStat] @piStatBatchLogId = @iStatBatchLogId, @ptiDebug = @tiDebug
		;
		EXEC [ftb].[uspOnUsPayerRoutingNumber_TransferToAtomicStat] @piStatBatchLogId = @iStatBatchLogId, @ptiDebug = @tiDebug
		;
	END
;
EXEC [ftb].[uspBatchTransferComplete] @piStatBatchLogId = @iStatBatchLogId, @piBatchId = @iBatchId OUTPUT, @ptiDebug = @tiDebug --TO-DO: uncomment this line when ready to update the Risk database with progress datetime...
;
/* TO-DO: ... *** this block is commented out during testing *** . */



--print N' ____                               '
--print N'/\  _`\                             '
--print N'\ \ \/\ \    ___     ___       __   '
--print N' \ \ \ \ \  / __`\ /| _ `\   / __`\ '
--print N'  \ \ \_\ \/\ \L\ \/\ \/\ \ /\  __/ '
--print N'   \ \____/\ \____/\ \_\ \_\\ \____\'
--print N'    \/___/  \/___/  \/_/\/_/ \/____/'
--print N''
	PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + N'...Done.'
	PRINT CONVERT( nvarchar(50), SYSDATETIME(), 121 ) + REPLICATE( NCHAR(009), @tiDebug ) + @nvThisSourceCode
	PRINT N''

set @tThisDuration = @dtThisEnd - @dtThisBegin
;
print N'Execution duration = ' + convert( nvarchar(8), @tThisDuration )
	RAISERROR ( N'', 0, 1 ) WITH NOWAIT;
;

END
;

GO
