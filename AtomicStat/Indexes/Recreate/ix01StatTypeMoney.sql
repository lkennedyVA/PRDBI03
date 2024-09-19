USE [AtomicStat]
GO

/****** Object:  Index [ix01StatTypeMoney]    Script Date: 9/19/2024 10:41:22 AM ******/
CREATE NONCLUSTERED INDEX [ix01StatTypeMoney] ON [stat].[StatTypeMoney]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO


