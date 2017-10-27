
IF OBJECT_ID(N'dbo.BIM_S1_EVENT_CONSUM_PART') IS NOT NULL
BEGIN
  TRUNCATE TABLE dbo.BIM_S1_EVENT_CONSUM_PART
  DROP TABLE dbo.BIM_S1_EVENT_CONSUM_PART
END;

SELECT * INTO dbo.BIM_S1_EVENT_CONSUM_PART
FROM OPENQUERY([BIM_DB],
'SELECT 
	   AL1.[Unique Link Key]
      ,AL1.[Master Event Activity Month]
	  ,AL1.[Source System]
	  ,AL1.Region
	  ,AL1.[Event Hwpl]
	  ,AL1.[Part Number]
	  ,AL1.[Part Actual SL]
	  ,AL1.[Part Shipment Month]
	  ,AL1.[Part Qty Consumed]
	  ,AL1.[DMR Flag]
	  ,NULL as [Event SPL]
	  ,AL3.Spl AS [Part SPL]
	  ,AL2.[Part Commodity]
	  ,AL2.[Material Description] AS [Part Description]
	  ,NULL AS [Safari Warranty Flag]
FROM BIM.BMA_L2_Events AL1
LEFT JOIN BIM.BMA_L1_PART_MASTER AL2 ON AL1.[Part Number] = AL2.Material AND AL1.Region = AL2.Region
LEFT JOIN BIM.BMA_L0_HWPL_SPL AL3 ON AL1.[Event Hwpl] = AL3.Hwpl
WHERE AL1.[Master Event Activity Month] BETWEEN ''201608'' AND ''201610''
AND AL1.[Source System] IN (''SFDC'',''WFM-IM'',''CSN'')
AND AL1.Region != ''China''
AND AL2.[Part Commodity] in(''Hard Drive'', ''Solid State Drive'')
AND (NOT AL1.[Part Qty Demanded] = 0)');