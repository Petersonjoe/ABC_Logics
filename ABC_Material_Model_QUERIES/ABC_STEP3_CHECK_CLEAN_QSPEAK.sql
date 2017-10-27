-- PROCESS QSPEAK
IF OBJECT_ID(N'staging.tmp_S3_CLEAN_QSPEAK') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.tmp_S3_CLEAN_QSPEAK
  DROP TABLE staging.tmp_S3_CLEAN_QSPEAK
END;

SELECT 
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
	   AL2.[Qspeak Upload Date_Q] AS [Qspeak Upload Date]--,
	   --[Qspeak TAT] = 
	   --CASE 
	   --    WHEN ISNULL(AL2.[Qspeak Upload Date_Q],'') = '' THEN NULL
		  -- WHEN ISNULL(AL2.[Phy Part No Rec Date],'') = '' THEN NULL 
		  -- ELSE DATEDIFF(DAY,AL2.[Phy Part No Rec Date],AL2.[Qspeak Upload Date_Q])
	   --END
INTO staging.tmp_S3_CLEAN_QSPEAK
FROM(
     SELECT * FROM BEPROD04.Z_End2End_PQ.dbo.Z_FACT_REPAIR_DETAIL
     WHERE [Upload Month_D] BETWEEN '201608' AND '201610'
	 AND [Record Status_Q] in('Valid' , 'Warning') 
	 AND ( 
	 FlowTag_Q like 'FT%' 
	 OR FlowTag_Q LIKE '%FT%'
	 OR FlowTag_Q LIKE '$T%'
	 OR FlowTag_Q LIKE '0T%')
     ) AL2;

ALTER TABLE staging.tmp_S3_CLEAN_QSPEAK
ALTER COLUMN [Qspeak Flowtag Used] NVARCHAR(255);

UPDATE staging.tmp_S3_CLEAN_QSPEAK
SET [Qspeak Flowtag Used] = CASE WHEN [Qspeak Flowtag Used] = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM staging.tmp_S3_CLEAN_QSPEAK
WHERE [Qspeak Flowtag Used] = 'N'

