/****** Script for SelectTopNRows command from SSMS  ******/
-- Add flag for duplicate flow tag
-- pre-steps:
-- 1. using ssis package to load ABC_S2_EVENT_PART_REVERSE_ALL table, about 1 hour
-- 2. using ssis package to load iReturn, about 8 hours
-- 3. using ssis package to load RMA, about 15 mins

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

-- clean ireturn

-- #### use ssis package flow the ireturn data ####
IF OBJECT_ID(N'tempdb..#tmp_iRETURN') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_iRETURN
  DROP TABLE #tmp_iRETURN
END;

SELECT 
        [FLOWTAG]  -- for mapping used
	   ,[SPARE_NO]  -- for mapping used
	   ,[RECVD_PART]  -- for mapping used
       ,[RCVG_PLANT] 
	   ,[DISP_LOCATION] 
	   ,[Report_Month] 
	   ,[FL_VENDOR_ID]
	   ,[FL_VENDOR_NAME]
	   ,ROW_NUMBER() OVER(PARTITION BY [FLOWTAG] ORDER BY [Report_Month] DESC, [CHANGE_DATE_LTZ] DESC) AS [IRETURN_USED]
   INTO #tmp_iRETURN
   FROM staging.tmp_ABC_S3_LINK_iRETURN
  WHERE [DISP_LOCATION]<>'XXXX'

ALTER TABLE #tmp_iRETURN
ALTER COLUMN [IRETURN_USED] NVARCHAR(255);

UPDATE #tmp_iRETURN
SET [IRETURN_USED] = CASE WHEN [IRETURN_USED] = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #tmp_iRETURN
WHERE [IRETURN_USED] = 'N'

-- link iReturn
IF OBJECT_ID(N'staging.ABC_S3_LINK_iRETURN') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.ABC_S3_LINK_iRETURN
  DROP TABLE staging.ABC_S3_LINK_iRETURN
END;

-- add columns
SELECT  AL1.*
       ,CONVERT(VARCHAR(50),NULL) AS [iReturn Received Plant]
	   ,CONVERT(VARCHAR(50),NULL) AS [iReturn Disposition]
	   ,CONVERT(VARCHAR(50),NULL) AS [iReturn Report Month]
	   ,CONVERT(VARCHAR(50),NULL) AS [iReturn Vendor ID]
	   ,CONVERT(VARCHAR(50),NULL) AS [iReturn Vendor Name]
  INTO staging.ABC_S3_LINK_iRETURN
  FROM staging.tmp_S3_LINK_QSPEAK AL1

--SELECT  AL1.*
--       ,AL2.[RCVG_PLANT] AS [iReturn Received Plant]
--	   ,AL2.[DISP_LOCATION] AS [iReturn Disposition]
--	   ,AL2.[Report_Month] AS [iReturn Report Month]
--	   ,AL2.[FL_VENDOR_ID] AS [iReturn Vendor ID]
--	   ,AL2.[FL_VENDOR_NAME] AS [iReturn Vendor Name]
--  INTO staging.ABC_S3_LINK_iRETURN
--  FROM staging.tmp_S3_LINK_QSPEAK AL1
--  LEFT JOIN #tmp_iRETURN AL2 ON AL1.[Flow Tag] = AL2.FLOWTAG
--  AND AL1.[Received Part Kit Part Number] = AL2.[SPARE_NO]
--  WHERE AL1.[REGION] IN ('APJ','EMEA') AND AL1.[Qspeak Flowtag used] <> 'N'

UPDATE AA
   SET AA.[iReturn Received Plant] = BB.[RCVG_PLANT],
       AA.[iReturn Disposition] = BB.[DISP_LOCATION],
	   AA.[iReturn Report Month] = BB.[Report_Month],
	   AA.[iReturn Vendor ID] = BB.[FL_VENDOR_ID],
	   AA.[iReturn Vendor Name] = BB.[FL_VENDOR_NAME]
  FROM staging.ABC_S3_LINK_iRETURN AA, #tmp_iRETURN BB
 WHERE AA.[Flow Tag] = BB.FLOWTAG
   AND AA.[Received Part Kit Part Number] = BB.[SPARE_NO]
   AND AA.[REGION] IN ('APJ','EMEA') AND AA.[Qspeak Flowtag used] <> 'N'

UPDATE AA
   SET AA.[iReturn Received Plant] = BB.[RCVG_PLANT],
       AA.[iReturn Disposition] = BB.[DISP_LOCATION],
	   AA.[iReturn Report Month] = BB.[Report_Month],
	   AA.[iReturn Vendor ID] = BB.[FL_VENDOR_ID],
	   AA.[iReturn Vendor Name] = BB.[FL_VENDOR_NAME]
  FROM staging.ABC_S3_LINK_iRETURN AA, #tmp_iRETURN BB
 WHERE AA.[Flow Tag] = BB.FLOWTAG
   AND AA.[Physical Part Number] = BB.[RECVD_PART]
   AND ISNULL(AA.[iReturn Disposition],'') = '' AND AA.[REGION] IN ('APJ','EMEA')
   AND AA.[Qspeak Flowtag used] <> 'N'

-- clean RMA tool data
IF OBJECT_ID(N'tempdb..#tmp_RMA') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_RMA
  DROP TABLE #tmp_RMA
END;

--#### using ssis package to flow RMA data ####
SELECT 
       FlowTagNumber,
	   HPPartNumber,
       [HPDISPOSITION] as [RWT Disposition],
       VendorCode as [RWT Vendor ID],
       VendorName  as [RWT Vendor Name],
       ROW_NUMBER() OVER(PARTITION BY FlowTagNumber ORDER BY ContainerReleaseDateTime DESC) AS [RMA_USED] -- TBD
INTO #tmp_RMA
FROM staging.tmp_ABC_S3_LINK_RMA

ALTER TABLE #tmp_RMA
ALTER COLUMN [RMA_USED] NVARCHAR(255);

UPDATE #tmp_RMA
SET [RMA_USED] = CASE WHEN [RMA_USED] = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #tmp_RMA
WHERE [RMA_USED] = 'N'

IF OBJECT_ID(N'staging.ABC_S3_LINK_SUPPLIER') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.ABC_S3_LINK_SUPPLIER
  DROP TABLE staging.ABC_S3_LINK_SUPPLIER
END;

SELECT AL1.*
      ,CONVERT(VARCHAR(50),NULL) AS [RWT Disposition]
      ,CONVERT(VARCHAR(50),NULL) AS [RWT Vendor ID]
      ,CONVERT(VARCHAR(50),NULL) AS [RWT Vendor Name]
INTO staging.ABC_S3_LINK_SUPPLIER
FROM staging.ABC_S3_LINK_iRETURN AL1

UPDATE AA
SET AA.[RWT Disposition] = BB.[RWT Disposition]
   ,AA.[RWT Vendor ID] = BB.[RWT Vendor ID]
   ,AA.[RWT Vendor Name] = BB.[RWT Vendor Name]
FROM staging.ABC_S3_LINK_SUPPLIER AA, #tmp_RMA BB
WHERE AA.[Flow Tag] = BB.FlowTagNumber AND AA.[Received Part Kit Part Number] = BB.HPPartNumber
AND AA.[REGION] = 'APJ' AND AA.[Qspeak Flowtag used] <> 'N'
AND ISNULL(AA.[iReturn Received Plant], AA.[Received Plant]) = 'H499'

UPDATE AA
SET AA.[RWT Disposition] = BB.[RWT Disposition]
   ,AA.[RWT Vendor ID] = BB.[RWT Vendor ID]
   ,AA.[RWT Vendor Name] = BB.[RWT Vendor Name]
FROM staging.ABC_S3_LINK_SUPPLIER AA, #tmp_RMA BB
WHERE AA.[Flow Tag] = BB.FlowTagNumber AND AA.[Physical Part Number] = BB.HPPartNumber
AND AA.[REGION] = 'APJ' AND AA.[Qspeak Flowtag used] <> 'N'
AND ISNULL(AA.[iReturn Received Plant], AA.[Received Plant]) = 'H499'
AND ISNULL(AA.[RWT Disposition],'') = ''

-- SUPPLIER UPDATE
ALTER TABLE staging.ABC_S3_LINK_SUPPLIER
ADD [L1 Supplier] varchar(255) null,
[L1 Supplier Source] varchar(255) null

-- APJ
UPDATE staging.ABC_S3_LINK_SUPPLIER
SET [L1 Supplier] = [RWT Vendor Name],
    [L1 Supplier Source] = 'RWT' 
WHERE Region = 'APJ' AND [RWT Vendor Name] IS NOT NULL 

-- AME
UPDATE AA
SET AA.[L1 Supplier] = BB.[L1 Supplier Name],
    AA.[L1 Supplier Source] = 'Received Plant'
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_AMS_PLANT_SUPPLIER] BB
WHERE AA.[Received Plant] = BB.[Received Plant] and AA.Region = BB.Region

-- EMEA
UPDATE AA
SET AA.[L1 Supplier] = BB.[L1 Supplier Name],
    AA.[L1 Supplier Source] = 'iReturn'
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_EMEA_DISPO_SUPPLIER] BB
WHERE ISNULL(AA.[iReturn Disposition],AA.[Actual Disposition]) = BB.[Dispostion]
AND AA.Region = BB.Region