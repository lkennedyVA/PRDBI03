/*
ALTER TABLE [hadoop].[HadoopBatchExecutionEvent] DROP CONSTRAINT [pkHadoopBatchExecutionEvent] WITH ( ONLINE = OFF )
ALTER TABLE [hadoop].[HadoopBatchExecutionEvent] DROP CONSTRAINT [dfHadoopBatchExecutionEvent_HadoopBatchExecutionStatusId]

ALTER TABLE hadoop.HadoopBatchExecutionEvent ALTER COLUMN HadoopBatchExecutionStatusId INT NOT NULL;
ALTER TABLE hadoop.HadoopBatchExecutionEvent ALTER COLUMN HadoopBatchTypeId INT NOT NULL;

ALTER TABLE [hadoop].[HadoopBatchExecutionEvent] ADD  CONSTRAINT [dfHadoopBatchExecutionEvent_HadoopBatchExecutionStatusId]  DEFAULT ((0)) FOR [HadoopBatchExecutionStatusId]
ALTER TABLE [hadoop].[HadoopBatchExecutionEvent] ADD  CONSTRAINT [pkHadoopBatchExecutionEvent] PRIMARY KEY CLUSTERED 
(
	[HadoopBatchExecutionEventId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Hadoop]
GO
*/

/*
ALTER TABLE [hadoop].[HadoopBatchExecutionStatus] DROP CONSTRAINT [pkHadoopBatchExecutionStatus] WITH ( ONLINE = OFF )

ALTER TABLE hadoop.HadoopBatchExecutionStatus ALTER COLUMN HadoopBatchExecutionStatusId INT NOT NULL;

ALTER TABLE [hadoop].[HadoopBatchExecutionStatus] ADD  CONSTRAINT [pkHadoopBatchExecutionStatus] PRIMARY KEY CLUSTERED 
(
	[HadoopBatchExecutionStatusId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Hadoop]
GO
*/

/*
ALTER TABLE [hadoop].[HadoopBatchType] DROP CONSTRAINT [pkHadoopBatchType] WITH ( ONLINE = OFF )

ALTER TABLE hadoop.HadoopBatchType ALTER COLUMN HadoopBatchTypeId INT NOT NULL;

ALTER TABLE [hadoop].[HadoopBatchType] ADD  CONSTRAINT [pkHadoopBatchType] PRIMARY KEY CLUSTERED 
(
	[HadoopBatchTypeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Hadoop]
GO
*/

/*
DROP INDEX [ix01HashKCP] ON [kegen].[HashKCP]
DROP INDEX [ix02HashKCP] ON [kegen].[HashKCP]
DROP INDEX [ux01HashKCP] ON [kegen].[HashKCP]

ALTER TABLE kegen.HashKCP ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01HashKCP] ON [kegen].[HashKCP]
(
	[PartitionId] ASC,
	[BatchLogId] ASC
)
INCLUDE([ValidFI_CustomerId],[ValidFI_PayerId],[KeyElementId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Main]
GO

CREATE NONCLUSTERED INDEX [ix02HashKCP] ON [kegen].[HashKCP]
(
	[PartitionId] ASC,
	[KeyElementId] ASC
)
INCLUDE([ValidFI_CustomerId],[ValidFI_PayerId],[BatchLogId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Kegen]
GO

CREATE UNIQUE NONCLUSTERED INDEX [ux01HashKCP] ON [kegen].[HashKCP]
(
	[BatchLogId] ASC,
	[ValidFI_CustomerId] ASC,
	[ValidFI_PayerId] ASC
)
INCLUDE([PartitionId],[KeyElementId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Kegen]
GO

*/


ALTER TABLE kegen.KCP_PNC_SurrogateKeyXref ALTER COLUMN BatchLogId INT NOT NULL;
ALTER TABLE mtb.Hash ALTER COLUMN StatBatchLogId INT NOT NULL;

/*
ALTER TABLE [mtb].[MTBBatchStatBatchLogXref] DROP CONSTRAINT [pkMTBBatchStatBatchLogXref] WITH ( ONLINE = OFF )

ALTER TABLE mtb.MTBBatchStatBatchLogXref ALTER COLUMN MTBBatchId INT NOT NULL;
ALTER TABLE mtb.MTBBatchStatBatchLogXref ALTER COLUMN StatBatchLogId INT NOT NULL;

ALTER TABLE [mtb].[MTBBatchStatBatchLogXref] ADD  CONSTRAINT [pkMTBBatchStatBatchLogXref] PRIMARY KEY CLUSTERED 
(
	[StatBatchLogId] ASC,
	[MTBBatchId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Main]
GO
*/


ALTER TABLE mtb.MTBBatchStatBatchLogXref_backup_20190316_0101 ALTER COLUMN MTBBatchId INT NOT NULL;
ALTER TABLE mtb.MTBBatchStatBatchLogXref_backup_20190316_0101 ALTER COLUMN StatBatchLogId INT NOT NULL;
ALTER TABLE mtb.MTBHashKey ALTER COLUMN StatBatchLogId INT NOT NULL;
ALTER TABLE precalc.BatchRunValue ALTER COLUMN BatchLogId INT NOT NULL;
ALTER TABLE precalc.Parameter ALTER COLUMN BatchLogId INT NOT NULL;

/*
ALTER TABLE [report].[KeyElement] DROP CONSTRAINT [dfKeyElementBatchLogId]

ALTER TABLE report.KeyElement ALTER COLUMN BatchLogId INT NOT NULL;

ALTER TABLE [report].[KeyElement] ADD  CONSTRAINT [dfKeyElementBatchLogId]  DEFAULT ((0)) FOR [BatchLogId]
*/

/*
ALTER TABLE [report].[KeyReference] DROP CONSTRAINT [dfKeyReferenceBatchLogId]

ALTER TABLE report.KeyReference ALTER COLUMN BatchLogId INT NOT NULL;

ALTER TABLE [report].[KeyReference] ADD  CONSTRAINT [dfKeyReferenceBatchLogId]  DEFAULT ((0)) FOR [BatchLogId]
*/

/*
DROP INDEX [ix01BatchLog] ON [stat].[BatchLog]

ALTER TABLE stat.BatchLog ALTER COLUMN BatchLogId INT NOT NULL;
ALTER TABLE stat.BatchLog ALTER COLUMN BatchProcessId INT NOT NULL;

CREATE UNIQUE NONCLUSTERED INDEX [ix01BatchLog] ON [stat].[BatchLog]
(
	[BatchLogId] ASC,
	[OrgId] ASC,
	[BatchEndDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Main]
GO
*/

ALTER TABLE stat.KCPKeyElementXref ALTER COLUMN BatchLogId INT NOT NULL;

/*
ALTER TABLE [stat].[KeyElement] DROP CONSTRAINT [dfKeyElement_BatchLogId]

ALTER TABLE stat.KeyElement ALTER COLUMN BatchLogId INT NULL;

ALTER TABLE [stat].[KeyElement] ADD  CONSTRAINT [dfKeyElement_BatchLogId]  DEFAULT ((0)) FOR [BatchLogId]
*/

ALTER TABLE stat.StatGroupMaxBatch ALTER COLUMN MaxBatchLogId INT NOT NULL;

/*
DROP INDEX [ix01StatTypeBigint] ON [stat].[StatTypeBigint]

ALTER TABLE stat.StatTypeBigint ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeBigint] ON [stat].[StatTypeBigint]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeBit] ON [stat].[StatTypeBit]

ALTER TABLE stat.StatTypeBit ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeBit] ON [stat].[StatTypeBit]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeDate] ON [stat].[StatTypeDate]

ALTER TABLE stat.StatTypeDate ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeDate] ON [stat].[StatTypeDate]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeDatetime] ON [stat].[StatTypeDatetime]

ALTER TABLE stat.StatTypeDatetime ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeDatetime] ON [stat].[StatTypeDatetime]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeFloat] ON [stat].[StatTypeFloat]

ALTER TABLE stat.StatTypeFloat ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeFloat] ON [stat].[StatTypeFloat]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeInt] ON [stat].[StatTypeInt]

ALTER TABLE stat.StatTypeInt ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeInt] ON [stat].[StatTypeInt]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeMoney] ON [stat].[StatTypeMoney]

ALTER TABLE stat.StatTypeMoney ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeMoney] ON [stat].[StatTypeMoney]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeNchar100] ON [stat].[StatTypeNchar100]

ALTER TABLE stat.StatTypeNchar100 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeNchar100] ON [stat].[StatTypeNchar100]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeNchar50] ON [stat].[StatTypeNchar50]

ALTER TABLE stat.StatTypeNchar50 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeNchar50] ON [stat].[StatTypeNchar50]
(
	[BatchLogId] ASC
)
INCLUDE([PartitionId],[KeyElementId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeNumeric0109] ON [stat].[StatTypeNumeric0109]

ALTER TABLE stat.StatTypeNumeric0109 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeNumeric0109] ON [stat].[StatTypeNumeric0109]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeNumeric2028] ON [stat].[StatTypeNumeric2028]

ALTER TABLE stat.StatTypeNumeric2028 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeNumeric2028] ON [stat].[StatTypeNumeric2028]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeDecimal1602] ON [stat].[StatTypeDecimal1602]

ALTER TABLE stat.StatTypeDecimal1602 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeDecimal1602] ON [stat].[StatTypeDecimal1602]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/

/*
DROP INDEX [ix01StatTypeNumeric1019] ON [stat].[StatTypeNumeric1019]

ALTER TABLE stat.StatTypeNumeric1019 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeNumeric1019] ON [stat].[StatTypeNumeric1019]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO

*/

/*
DROP INDEX [ix01StatTypeNumeric2938] ON [stat].[StatTypeNumeric2938]

ALTER TABLE stat.StatTypeNumeric2938 ALTER COLUMN BatchLogId INT NULL;

CREATE NONCLUSTERED INDEX [ix01StatTypeNumeric2938] ON [stat].[StatTypeNumeric2938]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO
*/