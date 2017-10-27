
--exec sp_rename 'dbo.BIM_S2_EVENT_PART_REVERSE', 'BIM_S2_EVENT_PART_REVERSE_20161122'
--[staging].[ABC_S2_EVENT_PART_REVERSE]
IF OBJECT_ID(N'fact.ABC_S2_EVENT_PART_REVERSE') IS NOT NULL
BEGIN
  TRUNCATE TABLE fact.ABC_S2_EVENT_PART_REVERSE
  DROP TABLE fact.ABC_S2_EVENT_PART_REVERSE
END;

SELECT * INTO fact.ABC_S2_EVENT_PART_REVERSE
FROM OPENQUERY([BIM_DB],
'SELECT
	   AL1.[Unique Link Key]
      ,AL1.[Master Event Activity Month]
	  ,AL1.[Source System]
	  ,AL1.Region
	  ,AL1.[Event Country Iso Cd]
	  ,AL1.[Event Hwpl]
	  ,AL1.[Part Number]
	  ,AL1.[Part Actual SL]
	  ,AL1.[Part Shipment Month]
	  ,AL1.[Part Qty Demanded]
	  ,AL1.[Part Qty Consumed]
	  ,AL1.[DMR Flag]
	  ,AL3.Spl as [Event SPL]
	  ,AL5.[Event Spl] AS [Sc SPL]
	  --,AL5.[Safari Spl Adj] AS [Sc SPL Adj]
	  ,AL3.Spl AS [Part SPL]
	  ,AL2.[Part Commodity]
	  ,AL2.[Material Description] AS [Part Description]
	  ,AL1.[Safari Warranty Flag]
	  ,AL4.[Unique Link Key] AS [L2 Reverse Unique Link Key]
	  --,AL4.[Received Month A]
      --,AL4.[Received Month B]
	  ,[Received Month] = CASE WHEN ISNULL(AL4.[Received Month B],'''') = '''' THEN AL4.[Received Month A] ELSE AL4.[Received Month B] END
	  --,AL4.[Rms Flow Tag Number A]
	  --,AL4.[Rms Flow Tag Number B]
	  ,[Flow Tag] = CASE WHEN ISNULL(AL4.[Rms Flow Tag Number B],'''') = '''' THEN AL4.[Rms Flow Tag Number A] ELSE AL4.[Rms Flow Tag Number B] END
	  --,AL4.[Actual Disposition A]
	  --,AL4.[Actual Disposition B]
	  ,[Actual Disposition] = CASE WHEN ISNULL(AL4.[Actual Disposition B],'''') = '''' THEN AL4.[Actual Disposition A] ELSE AL4.[Actual Disposition B] END
	  ,AL4.[Notification Type A]
      ,[Receipt Type] = CASE WHEN ISNULL(AL4.[Unique Link Key],'''') = '''' THEN ''No Receipt''
	                         WHEN AL4.[Actual Disposition A] = ''SC99'' AND ISNULL(AL4.[Actual Disposition B],'''') = '''' THEN ''Logical Receipt''
							 ELSE ''Physical Receipt''
						END
	  --,AL4.[Received Part Serial Number A]
	  --,AL4.[Received Part Serial Number B]
	  ,[CT Code Label] = CASE WHEN ISNULL(AL4.[Received Part Serial Number B],'''') = '''' THEN AL4.[Received Part Serial Number A] ELSE AL4.[Received Part Serial Number B] END
	  ,AL4.[Received Part Kit Part Number]
	  ,AL4.[Physical Part Number]
	  ,AL4.[Part Qty Received]
	  --,AL4.[Received Plant Id A]
	  --,AL4.[Received Plant Id B]
	  ,[Received Plant] = CASE WHEN ISNULL(AL4.[Received Plant Id B],'''') = '''' THEN AL4.[Received Plant Id A] ELSE AL4.[Received Plant Id B] END
      ,AL4.[Match FL]
      ,AL4.[SN Transaction Status]
	  ,AL4.[Sales Doc Type]
	  ,AL4.[Sales Document]
FROM BIM.BMA_L2_Events AL1
LEFT JOIN BIM.BMA_L1_PART_MASTER AL2 ON AL1.[Part Number] = AL2.Material AND AL1.Region = AL2.Region
LEFT JOIN BIM.BMA_L0_HWPL_SPL AL3 ON AL1.[Event Hwpl] = AL3.Hwpl
LEFT JOIN BIM.BMA_L2_Reverse AL4 ON AL1.[Unique Link Key] = AL4.[Unique Link Key]
LEFT JOIN CME.CME.CME_L2_Activity_Costs_BreakFix_view AL5 ON AL1.[Unique Link Key] = AL5.[Unique Link Key]
WHERE AL1.[Master Event Activity Month] BETWEEN ''201608'' AND ''201610''
AND AL1.[Source System] IN (''SFDC'',''WFM-IM'',''CSN'')
AND AL1.Region != ''China''
--AND AL2.[Part Commodity] in(''Hard Drive'', ''Solid State Drive'')
AND (NOT AL1.[Part Qty Demanded] = 0)');


