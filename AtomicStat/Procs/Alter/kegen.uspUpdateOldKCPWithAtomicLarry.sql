USE [AtomicStat]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [keygen].[uspUpdateOldKCPWithAtomic]
	Created By: Larry Dugger
	Descr: Load the records from AtomicStat to Stat Using the max BatchLogId
			
	Tables: [AtomicStat].[kegen].[HashKCP]
		,[AtomicStat].[stat].[StatTypeNchar50]
		,[Stat].[stat].[Allkcp]
	History:
		2019-01-13 - LBD - Created
		2019-02-20 - LBD - Modifed, replaced with new methodology:
			Collapsed operations into a single pass UPDATE inside the paging loop,
			reducing the amount of tempdb involvement.
*****************************************************************************************/
ALTER PROCEDURE [kegen].[uspUpdateOldKCPWithAtomicLarry](
	  @piPageSize INT = 1000000
	 ,@pncDelay nchar(11) = '00:00:00.01'
)
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @siBatchLogId INT
		,@bExists bit = 0
		,@iPageNumber int = 1
		,@iPageCount int = 1
		,@iPageSize int = @piPageSize
		,@ncDelay nchar(11) = @pncDelay -- '00:00:00.01'
		,@iErrorDetailId int
		,@sSchemaName nvarchar(128) = OBJECT_SCHEMA_NAME( @@PROCID ) -- 'kegen'
	;

	SELECT @siBatchLogId = MAX([BatchLogId])
	FROM [report].[vwStat_BatchLog]
	;
	--Any to Process 
	SELECT @bExists = COUNT(*) 
	FROM [kegen].[HashKCP] 
	WHERE BatchLogId = @siBatchLogId
		AND PartitionId IS NOT NULL
	;
	SELECT @iPageCount = CEILING((COUNT(1)/(@iPageSize*1.0))) --returns same integer, or +1 if fraction exists
	FROM [kegen].[HashKCP]
	WHERE BatchLogId = @siBatchLogId
		AND @bExists = 1
	;
	BEGIN TRY
		WHILE @bExists = 1
			AND @iPageNumber <= @iPageCount
		BEGIN -- page loop A
			--UPDATES
			UPDATE ak
				SET KCPMinCheckNumberCleared = stn.StatValue
					,KCPMaxCheckNumberCleared = stn2.StatValue
			FROM [Stat].[stat].[AllKCP] ak
				INNER JOIN 
						(
							SELECT 
								 ValidFI_CustomerId AS ValidFICustomerId
								,ValidFI_PayerId AS ValidFIPayerId
								,PartitionId
								,KeyElementId
							FROM [kegen].[HashKCP]
							WHERE BatchLogId = @siBatchLogId
							ORDER BY ValidFI_CustomerId
								,ValidFI_PayerId
							OFFSET @iPageSize * ( @iPageNumber - 1 ) ROWS
							FETCH NEXT @iPageSize ROWS ONLY
						) h 
					ON ak.ValidFICustomerId = h.ValidFICustomerId
						AND ak.ValidFIPayerId = h.ValidFIPayerId
				LEFT JOIN [AtomicStat].[stat].[StatTypeNchar50] stn 
					ON h.PartitionId = stn.PartitionId
						AND h.KeyElementId = stn.KeyElementId
						AND stn.StatId = 161 -- KCPMinCheckNumberCleared
				LEFT JOIN [AtomicStat].[stat].[StatTypeNchar50] stn2 
					ON h.PartitionId = stn2.PartitionId
						AND h.KeyElementId = stn2.KeyElementId
						AND stn2.StatId = 157 -- KCPMaxCheckNumberCleared
			; -- UDPATE ak

			SET @iPageNumber += 1
			;
			WAITFOR DELAY @ncDelay
			;
		END -- page loop A
	END TRY
	BEGIN CATCH
		EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
		RETURN		
	END CATCH

	--IF OBJECT_ID('tempdb..#tblHashKCP', 'U') IS NOT NULL DROP TABLE #tblHashKCP; 
	--CREATE table #tblHashKCP(
	--	 ValidFICustomerId bigint
	--	,ValidFIPayerId bigint
	--	,PartitionId tinyint
	--	,KeyElementId bigint
	--	,NtileId int
	--);
	--CREATE UNIQUE CLUSTERED INDEX ux01HashKCP ON #tblHashKCP (NtileId,ValidFICustomerId,ValidFIPayerId);

	--DECLARE @siBatchLogId INT
	--	,@iCount int = 0
	--	,@iPageNumber int = 0 --Partitions start at ZERO
	--	,@iPageSize int = @piPageSize
	--	,@iNtileCount int 
	--	,@iNtileNumber int 
	--	,@iPageCount int = 255 --Partitions end at 255
	--	,@iErrorDetailId int
	--	,@sSchemaName nvarchar(128) = 'kegen';

	--SELECT @siBatchLogId = MAX([BatchLogId])
	--FROM [report].[vwStat_BatchLog];

	--select @iCount = count(*) 
	--from [kegen].[HashKCP] 
	--WHERE BatchLogId = @siBatchLogId
	--	AND PartitionId IS NOT NULL;

	--IF @iCount > 0
	--WHILE @iPageNumber <= @iPageCount
	--BEGIN
	--	BEGIN TRY	
	--		--MINI loop based on @iPageSize
	--		--LETS set theNTileSize
	--		SELECT @iNtileCount = CASE WHEN COUNT(1) < @iPageSize THEN 1 ELSE (COUNT(1)/@iPageSize) +1 END
	--		FROM [kegen].[HashKCP]
	--		WHERE BatchLogId = @siBatchLogId
	--			AND PartitionId = @iPageNumber;
	--		--GRAB the page records
	--		INSERT INTO #tblHashKCP(NtileId,PartitionId,KeyElementId,ValidFICustomerId,ValidFIPayerId)
	--		SELECT NTILE(@iNtileCount) OVER (ORDER BY KeyElementId) AS NTileId  
	--			,PartitionId,KeyElementId,ValidFI_CustomerId,ValidFI_PayerId	
	--		FROM [kegen].[HashKCP]
	--		WHERE BatchLogId = @siBatchLogId
	--			AND PartitionId = @iPageNumber
	--		ORDER BY KeyElementId;
	--		--In preparation for WHILE LOOP
	--		SET @iNtileNumber = 1;
	--		--NOW process each NTile set
	--		WHILE @iNtileNumber <= @iNtileCount
	--		BEGIN
	--			BEGIN TRY
	--				--UPDATES
	--				UPDATE ak
	--					SET KCPMinCheckNumberCleared = stn.StatValue
	--						,KCPMaxCheckNumberCleared = stn2.StatValue
	--				FROM [Stat].[stat].[AllKCP] ak
	--				INNER JOIN #tblHashKCP h on ak.ValidFICustomerId = h.ValidFICustomerId
	--											AND ak.ValidFIPayerId = h.ValidFIPayerId
	--											AND h.NtileId = @iNtileNumber
	--				LEFT JOIN [AtomicStat].[stat].[StatTypeNchar50] stn on h.PartitionId = stn.PartitionId
	--																	AND h.KeyElementId =stn.KeyElementId
	--																	AND stn.StatId = 161	--min
	--				LEFT JOIN [AtomicStat].[stat].[StatTypeNchar50] stn2 on h.PartitionId = stn2.PartitionId
	--																	AND h.KeyElementId = stn2.KeyElementId
	--																	AND stn2.StatId = 157;	--max

	--				--NEXT page
	--				SET @iNtileNumber += 1;
	--				WAITFOR DELAY '00:00:00.01';
	--			END TRY
	--			BEGIN CATCH
	--				EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId = @iErrorDetailId OUTPUT;
	--				SET @iErrorDetailId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
	--				THROW;
	--			END CATCH
	--		END -- WHILE Ntile
	--		--CLEANUP
	--		TRUNCATE TABLE #tblHashKCP;
	--		--NEXT page
	--		SET @iPageNumber += 1;
	--		WAITFOR DELAY '00:00:00.01';

	--	END TRY
	--	BEGIN CATCH
	--		EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId = @iErrorDetailId OUTPUT;
	--		SET @iErrorDetailId = -1 * @iErrorDetailId; --return the errordetailid negative (indicates an error occurred)
	--		THROW;
	--	END CATCH
	--END  --WHILE Page

END

GO
