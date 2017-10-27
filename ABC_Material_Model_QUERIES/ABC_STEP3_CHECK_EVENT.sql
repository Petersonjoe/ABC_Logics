
--exec sp_rename 'dbo.BIM_S2_EVENT_PART_REVERSE', 'BIM_S2_EVENT_PART_REVERSE_20161122'

--select top 0* into staging.ABC_S2_EVENT_PART_REVERSE from [fact].[ABC_S2_EVENT_PART_REVERSE]

IF OBJECT_ID(N'staging.ABC_S2_EVENT_PART_REVERSE') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.ABC_S2_EVENT_PART_REVERSE
  DROP TABLE staging.ABC_S2_EVENT_PART_REVERSE
END;

SELECT * INTO staging.ABC_S2_EVENT_PART_REVERSE
FROM OPENQUERY([BIM_DB],
'SELECT top 10 *
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
	  ,AL3.Spl as [Event SPL]
	  ,AL5.[Event Spl] AS [Sc SPL]
	  ,AL3.Spl AS [Part SPL]
	  ,AL2.[Part Commodity]
	  ,AL2.[Material Description] AS [Part Description]
	  ,AL1.[Safari Warranty Flag]
FROM BIM.BMA_L2_Events AL1
LEFT JOIN BIM.BMA_L1_PART_MASTER AL2 ON AL1.[Part Number] = AL2.Material AND AL1.Region = AL2.Region
LEFT JOIN BIM.BMA_L0_HWPL_SPL AL3 ON AL1.[Event Hwpl] = AL3.Hwpl
LEFT JOIN CME.CME.CME_L2_Activity_Costs_BreakFix_view AL5 ON AL1.[Unique Link Key] = AL5.[Unique Link Key]
--WHERE AL1.[Master Event Activity Month] BETWEEN ''201608'' AND ''201611''
AND AL1.[Source System] IN (''SFDC'',''WFM-IM'',''CSN'')
AND AL1.Region != ''China''
AND (NOT AL1.[Part Qty Demanded] = 0)');

declare @sql nvarchar(max)
set @sql = 'SELECT top 10 *
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
	  ,AL3.Spl as [Event SPL]
	  ,AL5.[Event Spl] AS [Sc SPL]
	  ,AL3.Spl AS [Part SPL]
	  ,AL2.[Part Commodity]
	  ,AL2.[Material Description] AS [Part Description]
	  ,AL1.[Safari Warranty Flag]
FROM BIM.BMA_L2_Events AL1
LEFT JOIN BIM.BMA_L1_PART_MASTER AL2 ON AL1.[Part Number] = AL2.Material AND AL1.Region = AL2.Region
LEFT JOIN BIM.BMA_L0_HWPL_SPL AL3 ON AL1.[Event Hwpl] = AL3.Hwpl
LEFT JOIN CME.CME.CME_L2_Activity_Costs_BreakFix_view AL5 ON AL1.[Unique Link Key] = AL5.[Unique Link Key]
--WHERE AL1.[Master Event Activity Month] BETWEEN ''201608'' AND ''201611''
AND AL1.[Source System] IN (''SFDC'',''WFM-IM'',''CSN'')
AND AL1.Region != ''China''
AND (NOT AL1.[Part Qty Demanded] = 0)'

select @sql


