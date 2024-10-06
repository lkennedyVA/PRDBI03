truncate table dbo.Account_StatType
truncate table dbo.Account_StatValue
truncate table dbo.Account_StatValue
truncate table dbo.Account_StatValue_with_KeyTypeId_HashBulkId
truncate table dbo.Deposit_StatType
truncate table dbo.Deposit_StatValue
truncate table dbo.Deposit_StatValue
truncate table dbo.Deposit_StatValue_with_KeyTypeId_HashBulkId

ALTER TABLE dbo.Account_StatType ALTER COLUMN BatchLogId INT NULL;
ALTER TABLE dbo.Account_StatValue ALTER COLUMN AtomicStatBatchLogId INT NOT NULL;
ALTER TABLE dbo.Account_StatValue ALTER COLUMN BatchLogId INT NULL;
ALTER TABLE dbo.Account_StatValue_with_KeyTypeId_HashBulkId ALTER COLUMN AtomicStatBatchLogId INT NOT NULL;
ALTER TABLE dbo.Deposit_StatType ALTER COLUMN BatchLogId INT NULL;
ALTER TABLE dbo.Deposit_StatValue ALTER COLUMN AtomicStatBatchLogId INT NOT NULL;
ALTER TABLE dbo.Deposit_StatValue ALTER COLUMN BatchLogId INT NULL;
ALTER TABLE dbo.Deposit_StatValue_with_KeyTypeId_HashBulkId ALTER COLUMN AtomicStatBatchLogId INT NOT NULL;


ALTER TABLE [devstat].[BatchLog] DROP CONSTRAINT [dfBatchLogBatchProcessUId]
ALTER TABLE [devstat].[BatchLog] DROP CONSTRAINT [dfBatchLogBatchUId]
ALTER TABLE [devstat].[BatchLog] DROP CONSTRAINT [dfBatchProcessId]
ALTER TABLE [devstat].[BatchLog] DROP CONSTRAINT [dfDBatchLogDateActivated]

DROP INDEX [ix01BatchLog] ON [devstat].[BatchLog]
ALTER TABLE [devstat].[BatchLog] DROP CONSTRAINT [pkStatBatch] WITH ( ONLINE = OFF )


ALTER TABLE devstat.BatchLog ALTER COLUMN BatchLogId INT NOT NULL;
ALTER TABLE devstat.BatchLog ALTER COLUMN BatchProcessId INT NOT NULL;


ALTER TABLE [devstat].[BatchLog] ADD  CONSTRAINT [pkStatBatch] PRIMARY KEY CLUSTERED 
(
	[BatchLogId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Main]
GO

CREATE UNIQUE NONCLUSTERED INDEX [ix01BatchLog] ON [devstat].[BatchLog]
(
	[BatchLogId] ASC,
	[OrgId] ASC,
	[BatchEndDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_Main]
GO

ALTER TABLE [devstat].[BatchLog] ADD  CONSTRAINT [dfDBatchLogDateActivated]  DEFAULT (sysdatetime()) FOR [DateActivated]
ALTER TABLE [devstat].[BatchLog] ADD  CONSTRAINT [dfBatchProcessId]  DEFAULT ((0)) FOR [BatchProcessId]
ALTER TABLE [devstat].[BatchLog] ADD  CONSTRAINT [dfBatchLogBatchUId]  DEFAULT (newid()) FOR [BatchUId]
ALTER TABLE [devstat].[BatchLog] ADD  CONSTRAINT [dfBatchLogBatchProcessUId]  DEFAULT (0x) FOR [BatchProcessUId]