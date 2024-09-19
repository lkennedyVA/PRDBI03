USE [AtomicStat]
GO

/****** Object:  Index [ix01StatTypeNchar50]    Script Date: 9/19/2024 10:42:07 AM ******/
CREATE NONCLUSTERED INDEX [ix01StatTypeNchar50] ON [stat].[StatTypeNchar50]
(
	[BatchLogId] ASC
)
INCLUDE([PartitionId],[KeyElementId]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [FG_StatType]
GO


