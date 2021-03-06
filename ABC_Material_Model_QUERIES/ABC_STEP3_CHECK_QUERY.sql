/****** Script for SelectTopNRows command from SSMS  ******/
IF OBJECT_ID(N'tempdb..#TMP_REVERSE') IS NOT NULL
BEGIN
  TRUNCATE TABLE #TMP_REVERSE
  DROP TABLE #TMP_REVERSE
END

--SELECT TOP 1 [Phy Part No Rec Date] FROM [staging].[BIM_L2_REVERSE_FILTERED]

SELECT 
       [Unique Link Key] AS [L2 Reverse Unique Link Key]
	  ,[Received Month] = CASE WHEN ISNULL([Received Month B],'') = '' THEN [Received Month A] ELSE [Received Month B] END
	  ,[Flow Tag] = CASE WHEN ISNULL([Rms Flow Tag Number B],'') = '' THEN [Rms Flow Tag Number A] ELSE [Rms Flow Tag Number B] END
	  ,[Actual Disposition] = CASE WHEN ISNULL([Actual Disposition B],'') = '' THEN [Actual Disposition A] ELSE [Actual Disposition B] END
	  ,[Notification Type A]
      ,[Receipt Type] = CASE WHEN ISNULL([Unique Link Key],'') = '' THEN 'No Receipt'
	                         WHEN [Actual Disposition A] = 'SC99' AND ISNULL([Actual Disposition B],'') = '' THEN 'Logical Receipt'
							 ELSE 'Physical Receipt'
						END
	  ,[CT Code Label] = CASE WHEN ISNULL([Received Part Serial Number B],'') = '' THEN [Received Part Serial Number A] ELSE [Received Part Serial Number B] END
	  ,[Received Part Kit Part Number]
	  ,[Physical Part Number]
	  ,[Received Plant] = CASE WHEN ISNULL([Received Plant Id B],'') = '' THEN [Received Plant Id A] ELSE [Received Plant Id B] END
      ,[Match FL]
      ,[SN Transaction Status]
	  ,[Sales Doc Type]
	  ,[Sales Document]
	  ,[Phy Part No Rec Date] = CASE WHEN ISNULL([Phy Part No Rec Date B],'') = '' THEN [Phy Part No Rec Date A] ELSE [Phy Part No Rec Date B] END
INTO #TMP_REVERSE
FROM [staging].[BIM_L2_REVERSE_FILTERED]

IF OBJECT_ID(N'fact.ABC_S3_FLOWTAG_CHECK') IS NOT NULL
BEGIN
  TRUNCATE TABLE fact.ABC_S3_FLOWTAG_CHECK
  DROP TABLE fact.ABC_S3_FLOWTAG_CHECK
END

SELECT AL3.[Unique Link Key]
      ,AL3.[Master Event Activity Month]
      ,AL3.[Source System]
      ,AL3.[Region]
      ,AL3.[Event Hwpl]
      ,AL3.[Part Number]
      ,AL3.[Part Actual SL]
      ,AL3.[Part Shipment Month]
      ,AL3.[Part Qty Consumed]
      ,AL3.[DMR Flag]
      ,AL3.[Event SPL]
      ,AL3.[Sc SPL]
      ,AL3.[Part SPL]
      ,AL3.[Part Commodity]
      ,AL3.[Part Description]
      ,AL3.[Safari Warranty Flag]
      ,AL2.[L2 Reverse Unique Link Key]
      ,AL2.[Received Month]
	  ,AL2.[Phy Part No Rec Date]
	  ,AL2.[Flow Tag]
	  ,AL2.[Actual Disposition]
	  ,AL2.[Notification Type A]
	  ,AL2.[Receipt Type]
	  ,AL2.[CT Code Label]
	  ,AL2.[Received Part Kit Part Number]
	  ,AL2.[Physical Part Number]
	  ,AL2.[Received Plant] 
	  ,AL2.[Match FL]
	  ,AL2.[SN Transaction Status]
	  ,AL2.[Sales Doc Type]
	  ,AL2.[Sales Document]
      ,AL1.[Qspeak Vendor]
      ,AL1.[Qspeak Flowtag]
      ,AL1.[Qspeak Flowtag Used]
      ,AL1.[Qspeak OEM Name]
      ,AL1.[Qspeak Disposition]
      ,AL1.[Qspeak Warranty Status]
      ,AL1.[Qspeak Upload Date]
	  ,[Qspeak TAT] = 
	   CASE 
	       WHEN ISNULL(AL1.[Qspeak Upload Date],'') = '' THEN NULL
		   WHEN ISNULL(AL2.[Phy Part No Rec Date],'') = '' THEN NULL 
		   ELSE DATEDIFF(DAY,AL2.[Phy Part No Rec Date],AL1.[Qspeak Upload Date])
	   END
  INTO [fact].[ABC_S3_FLOWTAG_CHECK]
  FROM [ABC].[staging].[tmp_S3_CLEAN_QSPEAK] AL1
  LEFT JOIN #TMP_REVERSE AL2 ON AL1.[Qspeak Flowtag] = AL2.[Flow Tag]
  LEFT JOIN [staging].[ABC_S2_EVENT_PART_REVERSE] AL3 ON AL2.[L2 Reverse Unique Link Key] = AL3.[Unique Link Key] 

  select count(*) from #TMP_REVERSE