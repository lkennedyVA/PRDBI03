USE [AtomicStat]
GO

/****** Object:  Index [ix01StatTypeNumeric0109]    Script Date: 9/19/2024 10:42:28 AM ******/
CREATE NONCLUSTERED INDEX [ix01StatTypeNumeric0109] ON [stat].[StatTypeNumeric0109]
(
	[KeyElementId] ASC
)
INCLUDE([StatId],[StatValue],[BatchLogId],[PartitionId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO


