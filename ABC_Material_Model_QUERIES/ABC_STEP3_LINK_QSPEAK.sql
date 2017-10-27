
--exec sp_rename 'dbo.BIM_S2_EVENT_PART_REVERSE', 'BIM_S2_EVENT_PART_REVERSE_20161122'

--CREATE SCHEMA fact;

-- Add flag for duplicate flow tag

IF OBJECT_ID(N'staging.tmp_S3_LINK_QSPEAK') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.tmp_S3_LINK_QSPEAK
  DROP TABLE staging.tmp_S3_LINK_QSPEAK
END;

SELECT AL1.*,
       AL2.[QspkVendorName_Q] AS [Qspeak Vendor],
	   [Qspeak Flowtag] = 
	   CASE 
	       WHEN AL2.FlowTag_Q like '%FT%' THEN RIGHT(AL2.FlowTag_Q,len(AL2.FlowTag_Q) - charindex('FT',AL2.FlowTag_Q,1)+1)
		   WHEN AL2.FlowTag_Q like '$T%' THEN 'F' + RIGHT(AL2.FlowTag_Q,LEN(AL2.FlowTag_Q)-1)
		   WHEN AL2.FlowTag_Q like '0T%' THEN 'F' + RIGHT(AL2.FlowTag_Q,LEN(AL2.FlowTag_Q)-1)
		   ELSE AL2.FlowTag_Q 
	   END,
	   ROW_NUMBER() OVER(PARTITION BY AL2.FlowTag_Q ORDER BY AL2.[Qspeak Upload Date_Q] DESC) AS [Qspeak Flowtag Used],
	   AL2.[OEM Name_Q] AS [Qspeak OEM Name],
	   AL2.[FinalDisposition_D] AS [Qspeak Disposition],
	   AL2.[Warranty Vendor_Q] AS [Qspeak Warranty Status],
	   AL2.[Qspeak Upload Date_Q] AS [Qspeak Upload Date],
	   [Qspeak TAT] = 
	   CASE 
	       WHEN ISNULL([Qspeak Upload Date_Q],'') = '' THEN NULL
		   WHEN ISNULL([Phy Part No Rec Date],'') = '' THEN NULL 
		   ELSE DATEDIFF(DAY,[Phy Part No Rec Date],[Qspeak Upload Date_Q])
	   END
INTO staging.tmp_S3_LINK_QSPEAK
FROM fact.ABC_S2_EVENT_PART_REVERSE_ALL AL1
LEFT JOIN (
     SELECT * FROM BEPROD04.Z_End2End_PQ.dbo.Z_FACT_REPAIR_DETAIL
     WHERE [Record Status_Q] in('Valid' , 'Warning') and 
	 FlowTag_Q like 'FT%' 
	 OR FlowTag_Q LIKE '%FT%'
	 OR FlowTag_Q LIKE '$T%'
	 OR FlowTag_Q LIKE '0T%'
     ) AL2
ON AL1.[Flow Tag] = AL2.FlowTag_Q;

ALTER TABLE staging.tmp_S3_LINK_QSPEAK
ALTER COLUMN [Qspeak Flowtag Used] NVARCHAR(255);

UPDATE staging.tmp_S3_LINK_QSPEAK
SET [Qspeak Flowtag Used] = CASE WHEN [Qspeak Flowtag Used] = '1' THEN 'Y' ELSE 'N' END;

UPDATE staging.tmp_S3_LINK_QSPEAK
SET [Qspeak Flowtag Used] = NULL 
WHERE ISNULL([Qspeak Flowtag],'') = ''; 

IF OBJECT_ID(N'fact.ABC_S3_LINK_QSPEAK') IS NOT NULL
BEGIN
  TRUNCATE TABLE fact.ABC_S3_LINK_QSPEAK
  DROP TABLE fact.ABC_S3_LINK_QSPEAK
END;

SELECT * INTO fact.ABC_S3_LINK_QSPEAK
FROM staging.tmp_S3_LINK_QSPEAK


--SELECT AL1.*,
--       AL2.[QspkVendorName_Q] AS [Qspeak Vendor],
--	   AL2.FlowTag_Q AS [Qspeak Flowtag],
--	   AL2.[OEM Name_Q] AS [Qspeak OEM Name],
--	   AL2.[FinalDisposition_D] AS [Qspeak Disposition],
--	   AL2.[Warranty Vendor_Q] AS [Qspeak Warranty Status],
--	   AL2.[Qspeak Upload Date_Q] AS [Qspeak Upload Date]
--INTO fact.BIM_S3_LINK_QSPEAK
--FROM dbo.BIM_S2_EVENT_PART_REVERSE AL1
--LEFT JOIN BEPROD04.Z_End2End_PQ.dbo.Z_FACT_REPAIR_DETAIL AL2
--ON AL1.[Flow Tag] = AL2.FlowTag_Q
--where AL2.[Record Status_Q] in('Valid' , 'Warning') and AL2.FlowTag_Q like 'FT%'
	
-- Looking for duplicates -- without filter 1033, 1173
-- 159, 159
/*
select region, count(*) as Duplicate_FlowTag_records from 
BIM_S3_LINK_QSPEAK_20161209
where [Qspeak Flowtag] in
(
select distinct [Qspeak Flowtag]
from BIM_S3_LINK_QSPEAK_20161209
group by [Qspeak Flowtag]
having count([Qspeak Flowtag]) > 1)
group by region
*/


