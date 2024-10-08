USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER function [hadoop].[HadoopBatchType_Get](
		  @batchTypeId int = null
		, @batchTypeReference nvarchar(64) = null
		, @batchTypeName nvarchar(128) = null
		, @batchTypeDescription nvarchar(512) = null
)
returns table
/*
	select * from [hadoop].[HadoopBatchType_Get]( default, default, default, N'Stats' )
	;
	select * from [hadoop].[HadoopBatchType_Get]( default, default, N'Hadoop PNC Stat Generation', default )
	;
	select * from [hadoop].[HadoopBatchType_Get]( default, N'PNCStats', default, default )
	;
	select * from [hadoop].[HadoopBatchType_Get]( 1, default, default, default )
	;
*/
as return (
	select [HadoopBatchTypeId]
	   , [HadoopBatchTypeReference]
	   , [HadoopBatchTypeName]
	   , [HadoopBatchTypeDescription]
	   , [RowInsertDateTime]
	from [hadoop].[HadoopBatchType]
		where 
            ( case when @batchTypeId is not null then case when @batchTypeId = [HadoopBatchTypeId] then 1 else 0 end else 0 end ) = 1
         or ( case when @batchTypeReference is not null then case when @batchTypeReference = [HadoopBatchTypeReference] then 1 else 0 end else 0 end ) = 1
         or ( case when @batchTypeName is not null then case when @batchTypeName = [HadoopBatchTypeName] then 1 else 0 end else 0 end ) = 1
         or ( case when @batchTypeDescription is not null then case when [HadoopBatchTypeDescription] like ( N'%' + @batchTypeDescription + N'%' ) then 1 else 0 end else 0 end ) = 1
);
