/****** Script for ABC data process flow from design team ******/
-- Add flag for duplicate flow tag
-- pre-steps:
-- 0. process each upload master data, update the region to 'AME', update the data period
--    for some data, update necessary field
-- 1. using ssis package to load ABC_S2_EVENT_PART_REVERSE_ALL table, about 1 hour
-- 2. using ssis package to load iReturn, about 8 hours
-- 3. using ssis package to load RMA, about 15 mins
-- 4. regular CCS file for all regions, contact GSPV and provide them logics
DECLARE @VERSION NVARCHAR(6) = '2017Q1'
DECLARE @iCOST_MONTH NVARCHAR(10) = '201701' -- used in line 814
DECLARE @MID_Q_DATE DATETIME

SET @MID_Q_DATE = CONVERT(DATETIME,'20161215') -- Changed every quarter


-- ADD DMR FLAG FROM HPRT4
-- TO BE DISCUSSED

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
	   convert(nvarchar(255),year(AL2.[Qspeak Upload Date_Q])) +
       convert(nvarchar(255),right(100+month(AL2.[Qspeak Upload Date_Q]),2)) AS [Qspeak Upload Month],
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
     SELECT * FROM [rawdata].[BEPROD04_Z_FACT_REPAIR_DETAIL]
     WHERE [Record Status_Q] in('Valid' , 'Warning') and 
	 [Qspeak Upload Date_Q] < '2017-06-09 00:00:00.000'
	 AND (FlowTag_Q like 'FT%' 
	 OR FlowTag_Q LIKE '%FT%'
	 OR FlowTag_Q LIKE '$T%'
	 OR FlowTag_Q LIKE '0T%')
     ) AL2
ON AL1.[Flow Tag] = AL2.FlowTag_Q
WHERE AL1.[Data Period] = @VERSION;

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

DELETE FROM staging.tmp_S3_LINK_QSPEAK
WHERE ISNULL([Qspeak Flowtag Used],'') = 'N'

-- Clear the dash with hyphen
UPDATE AL1
SET AL1.[Received Part Kit Part Number] = REPLACE(AL1.[Received Part Kit Part Number],' ','-'),
    AL1.[Physical Part Number] = REPLACE(AL1.[Physical Part Number],' ','-')
FROM fact.ABC_S3_LINK_QSPEAK AL1

UPDATE AL1
SET AL1.[Received Part Kit Part Number] = REPLACE(AL1.[Received Part Kit Part Number],' ','-'),
    AL1.[Physical Part Number] = REPLACE(AL1.[Physical Part Number],' ','-')
FROM staging.tmp_S3_LINK_QSPEAK AL1

-- clean ireturn
-- #### use ssis package flow the ireturn data ####
-- each time pull all the raw iReturn data, No need for Data Period
IF OBJECT_ID(N'tempdb..#tmp_iRETURN') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_iRETURN
  DROP TABLE #tmp_iRETURN
END;

SELECT 
        [FLOWTAG]  -- for mapping
	   ,[SPARE_NO]  -- for mapping
	   ,[RECVD_PART]  -- for mapping
       ,[RCVG_PLANT] 
	   ,[DISP_LOCATION] 
	   ,[Report_Month] 
	   ,[FL_VENDOR_ID]
	   ,[FL_VENDOR_NAME]
	   ,[Disposition]
	   ,ROW_NUMBER() OVER(PARTITION BY [FLOWTAG] ORDER BY [Report_Month] DESC, [CHANGE_DATE_LTZ] DESC) AS [IRETURN_USED]
   INTO #tmp_iRETURN
   FROM staging.tmp_ABC_S3_LINK_iRETURN
  WHERE [DISP_LOCATION]<>'XXXX'
  AND [Report_Month] <= '201703'

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
	   ,CONVERT(VARCHAR(50),NULL) AS [APJ Received Plant]
	   ,CONVERT(VARCHAR(50),NULL) AS [APJ Disposition]
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
   AND AA.[REGION] = 'APJ' AND ISNULL(AA.[Qspeak Flowtag used],'') <> 'N'

UPDATE AA
   SET AA.[iReturn Received Plant] = BB.[RCVG_PLANT],
       AA.[iReturn Disposition] = BB.[Disposition],
	   AA.[iReturn Report Month] = BB.[Report_Month],
	   AA.[iReturn Vendor ID] = BB.[FL_VENDOR_ID],
	   AA.[iReturn Vendor Name] = BB.[FL_VENDOR_NAME]
  FROM staging.ABC_S3_LINK_iRETURN AA, #tmp_iRETURN BB
 WHERE AA.[Flow Tag] = BB.FLOWTAG
   AND AA.[Received Part Kit Part Number] = BB.[SPARE_NO]
   AND AA.[REGION] = 'EMEA' AND ISNULL(AA.[Qspeak Flowtag used],'') <> 'N'

UPDATE AA
   SET AA.[iReturn Received Plant] = BB.[RCVG_PLANT],
       AA.[iReturn Disposition] = BB.[DISP_LOCATION],
	   AA.[iReturn Report Month] = BB.[Report_Month],
	   AA.[iReturn Vendor ID] = BB.[FL_VENDOR_ID],
	   AA.[iReturn Vendor Name] = BB.[FL_VENDOR_NAME]
  FROM staging.ABC_S3_LINK_iRETURN AA, #tmp_iRETURN BB
 WHERE AA.[Flow Tag] = BB.FLOWTAG
   AND AA.[Physical Part Number] = BB.[RECVD_PART]
   AND ISNULL(AA.[iReturn Disposition],'') = '' AND AA.[REGION] = 'APJ'
   AND ISNULL(AA.[Qspeak Flowtag used],'') <> 'N'

UPDATE AA
   SET AA.[iReturn Received Plant] = BB.[RCVG_PLANT],
       AA.[iReturn Disposition] = BB.[Disposition],
	   AA.[iReturn Report Month] = BB.[Report_Month],
	   AA.[iReturn Vendor ID] = BB.[FL_VENDOR_ID],
	   AA.[iReturn Vendor Name] = BB.[FL_VENDOR_NAME]
  FROM staging.ABC_S3_LINK_iRETURN AA, #tmp_iRETURN BB
 WHERE AA.[Flow Tag] = BB.FLOWTAG
   AND AA.[Physical Part Number] = BB.[RECVD_PART]
   AND ISNULL(AA.[iReturn Disposition],'') = '' AND AA.[REGION] = 'EMEA'
   AND ISNULL(AA.[Qspeak Flowtag used],'') <> 'N'

-- clean RMA tool data
IF OBJECT_ID(N'tempdb..#tmp_RMA') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_RMA
  DROP TABLE #tmp_RMA
END;

--#### using ssis package to flow RMA data ####
--each time pull all the RMA data, No need for Data Period
SELECT 
       FlowTagNumber,
	   HPPartNumber,
	   CID AS [RWT CID],
	   ActualCredit AS [RWT Credit],
       [HPDISPOSITION] as [RWT Disposition],
       VendorCode as [RWT Vendor ID],
       VendorName  as [RWT Vendor Name],
       EngineerApproval as [RWT Engineer Approval],
       NFFTestResults as [RWT NFF Results],
	   convert(nvarchar(255),year([ContainerReleaseDateTime])) +
       convert(nvarchar(255),right(100+month([ContainerReleaseDateTime]),2)) as [RWT Release Month],
       ROW_NUMBER() OVER(PARTITION BY FlowTagNumber ORDER BY ContainerReleaseDateTime DESC) AS [RMA_USED] -- TBD
INTO #tmp_RMA
FROM staging.tmp_ABC_S3_LINK_RMA
WHERE CONVERT(VARCHAR(6),ContainerReleaseDateTime,112) <= '201704'

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
      ,CONVERT(VARCHAR(50),NULL) AS [RWT CID]
	  ,CONVERT(money,NULL) AS [RWT Credit]
      ,CONVERT(VARCHAR(50),NULL) AS [RWT Disposition]
      ,CONVERT(VARCHAR(50),NULL) AS [RWT Vendor ID]
      ,CONVERT(VARCHAR(50),NULL) AS [RWT Vendor Name]
	  ,CONVERT(VARCHAR(50),NULL) AS [RWT Engineer Approval]
	  ,CONVERT(VARCHAR(50),NULL) AS [RWT NFF Results]
	  ,CONVERT(VARCHAR(50),NULL) AS [RWT Release Month]
INTO staging.ABC_S3_LINK_SUPPLIER
FROM staging.ABC_S3_LINK_iRETURN AL1

UPDATE AA
SET AA.[RWT CID] = BB.[RWT CID]
   ,AA.[RWT Credit] = BB.[RWT Credit]
   ,AA.[RWT Disposition] = BB.[RWT Disposition]
   ,AA.[RWT Vendor ID] = BB.[RWT Vendor ID]
   ,AA.[RWT Vendor Name] = BB.[RWT Vendor Name]
   ,AA.[RWT Engineer Approval] = BB.[RWT Engineer Approval]
   ,AA.[RWT NFF Results] = BB.[RWT NFF Results]
   ,AA.[RWT Release Month] = BB.[RWT Release Month]
FROM staging.ABC_S3_LINK_SUPPLIER AA, #tmp_RMA BB
WHERE AA.[Flow Tag] = BB.FlowTagNumber AND AA.[Received Part Kit Part Number] = BB.HPPartNumber
AND AA.[REGION] = 'APJ' AND ISNULL(AA.[Qspeak Flowtag used], '') <> 'N'
AND ISNULL(AA.[iReturn Received Plant], AA.[Received Plant]) = 'H499'

UPDATE AA
SET AA.[RWT CID] = BB.[RWT CID]
   ,AA.[RWT Credit] = BB.[RWT Credit]
   ,AA.[RWT Disposition] = BB.[RWT Disposition]
   ,AA.[RWT Vendor ID] = BB.[RWT Vendor ID]
   ,AA.[RWT Vendor Name] = BB.[RWT Vendor Name]
   ,AA.[RWT Engineer Approval] = BB.[RWT Engineer Approval]
   ,AA.[RWT NFF Results] = BB.[RWT NFF Results]
   ,AA.[RWT Release Month] = BB.[RWT Release Month]
FROM staging.ABC_S3_LINK_SUPPLIER AA, #tmp_RMA BB
WHERE AA.[Flow Tag] = BB.FlowTagNumber AND AA.[Physical Part Number] = BB.HPPartNumber
AND AA.[REGION] = 'APJ' AND ISNULL(AA.[Qspeak Flowtag used],'') <> 'N'
AND ISNULL(AA.[iReturn Received Plant], AA.[Received Plant]) = 'H499'
AND ISNULL(AA.[RWT Disposition],'') = ''

UPDATE AA
SET AA.[APJ Received Plant] = ISNULL(AA.[iReturn Received Plant],AA.[Received Plant]),
	AA.[APJ Disposition] = ISNULL(ISNULL([RWT Disposition],[Qspeak Disposition]),[iReturn Disposition])
FROM staging.ABC_S3_LINK_SUPPLIER AA
WHERE Region = 'APJ'

UPDATE AA
SET AA.[Qspeak Disposition] = BB.[Qspeak Disposition],
    AA.[Qspeak Warranty Status] = BB.[Qspeak Warranty Status]
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_AMS_QSPEAK_FIX_FOXCONN] BB
WHERE AA.Region = 'AME'
AND AA.[Flow Tag] = BB.[Flow Tag]

UPDATE AA
SET AA.[Qspeak Disposition] = BB.[Qspeak Disposition],
    AA.[Qspeak Warranty Status] = BB.[Qspeak Warranty Status]
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_AMS_QSPEAK_FIX_INVENTEC] BB
WHERE AA.Region = 'AME'
AND AA.[Flow Tag] = BB.[Flow Tag]

UPDATE AA
SET AA.[Qspeak Disposition] = BB.[Qspeak Disposition],
    AA.[Qspeak Warranty Status] = BB.[Qspeak Warranty Status]
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_EMEA_QSPEAK_FIX_FOXCONN] BB
WHERE AA.Region = 'EMEA'
AND AA.[Flow Tag] = BB.[Flow Tag]

UPDATE AA
SET AA.[Qspeak Disposition] = BB.[Qspeak Disposition],
    AA.[Qspeak Warranty Status] = BB.[Qspeak Warranty Status]
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_EMEA_QSPEAK_FIX_INVENTEC] BB
WHERE AA.Region = 'EMEA'
AND AA.[Flow Tag] = BB.[Flow Tag]

-- SUPPLIER UPDATE
ALTER TABLE staging.ABC_S3_LINK_SUPPLIER
ADD [L1 Supplier] varchar(255) null,
    [L1 Supplier Source] varchar(255) null,
	[L2 Supplier] varchar(255) null,
    [Supplier Type] varchar(255) null

-- APJ MODIFIED AT 6:36PM 1.5
UPDATE AA
SET AA.[L1 Supplier] = ISNULL(ISNULL(AA.[RWT Vendor Name],AA.[Qspeak Vendor]),AA.[iReturn Vendor Name]),
    AA.[L1 Supplier Source] = 'RWT',
	AA.[Supplier Type] = CASE WHEN ISNULL(BB.[Supplier Type], '') != '' THEN BB.[Supplier Type] ELSE 'Repair' END
FROM staging.ABC_S3_LINK_SUPPLIER AA
LEFT JOIN [ABC].[fact].[ABC_MASTER_APJ_RFC_SUPPLIER] BB
ON AA.[L1 Supplier] = BB.[L1 Supplier Name]
WHERE AA.Region = 'APJ' 
AND ISNULL(ISNULL(AA.[RWT Vendor Name],AA.[Qspeak Vendor]),AA.[iReturn Vendor Name]) != ''

-- AME
UPDATE AA
SET AA.[L1 Supplier] = BB.[L1 Supplier Name],
    AA.[L1 Supplier Source] = 'Received Plant',
	AA.[Supplier Type] = BB.[Supplier Type]
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_AMS_PLANT_SUPPLIER] BB
WHERE AA.[Received Plant] = BB.[Received Plant] and AA.Region = BB.Region

-- EMEA
UPDATE AA
SET AA.[L1 Supplier] = BB.[L1 Supplier Name],
    AA.[L1 Supplier Source] = 'iReturns Disposition',
	AA.[Supplier Type] = BB.[Supplier Type]
FROM staging.ABC_S3_LINK_SUPPLIER AA, [fact].[ABC_MASTER_EMEA_DISPO_SUPPLIER] BB
WHERE ISNULL(AA.[iReturn Disposition], AA.[Actual Disposition]) = BB.[Dispostion]
AND AA.Region = BB.Region

-- APJ Step 2
IF OBJECT_ID(N'tempdb..#tmp_APJ_CCS') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_APJ_CCS
  DROP TABLE #tmp_APJ_CCS
END

SELECT --[Plant] 
       [Part] 
	  ,[Trans Type] 
	  ,[VENDOR DESCRIPTION] AS [Vendor]
	  --,[Cost Change] = CASE WHEN LEFT([Vendor],2) = '00' THEN SUBSTRING([Vendor],3,10) ELSE [Vendor] END	
	  ,ROW_NUMBER() OVER (PARTITION BY [Part] ORDER BY convert(datetime,[Cost Change]) DESC) AS [APJ CCS USED] 
  INTO #tmp_APJ_CCS
  FROM [ABC].[fact].[ABC_MASTER_APJ_CCS] 
 WHERE [Cost Type] = 'C'
   AND [Plant] = 'H499'
   AND [Data Period] = @VERSION

ALTER TABLE #tmp_APJ_CCS 
ALTER COLUMN [APJ CCS USED] NVARCHAR(255);

UPDATE #tmp_APJ_CCS
SET [APJ CCS USED] = CASE WHEN [APJ CCS USED]  = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #tmp_APJ_CCS 
WHERE [APJ CCS USED] = 'N' 

UPDATE AA
SET AA.[L1 Supplier] = BB.[Vendor], 
    AA.[L1 Supplier Source] = 'CCS Return',
    AA.[Supplier Type] = 'Repair'
FROM [ABC].[staging].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_APJ_CCS BB 
WHERE AA.[Received Part Kit Part Number] =BB.[Part]
AND AA.[Region] = 'APJ'
AND ISNULL(AA.[L1 Supplier],'') = ''

UPDATE AA
SET AA.[L1 Supplier] = BB.[Vendor], 
    AA.[L1 Supplier Source] = 'CCS Return',
    AA.[Supplier Type] = 'Repair'
FROM [ABC].[staging].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_APJ_CCS BB
WHERE AA.[Part Number] =BB.[Part]
AND AA.[Region] = 'APJ'
AND ISNULL(AA.[L1 Supplier],'') = ''

--AMS Step 2
IF OBJECT_ID(N'tempdb..#tmp_AMS_CCS') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_AMS_CCS
  DROP TABLE #tmp_AMS_CCS
END

SELECT --[Plant] 
       [Part] 
	  ,[Trans Type] 
	  ,[VENDOR DESCRIPTION] AS [Vendor]
	  ,[Cost Change]
	  --,[Cost Change] = CASE WHEN LEFT([Vendor],2) = '00' THEN SUBSTRING([Vendor],3,10) ELSE [Vendor] END	
	  ,ROW_NUMBER() OVER (PARTITION BY [Part] ORDER BY convert(datetime,[Cost Change]) DESC) AS [AMS CCS USED] 
  INTO #tmp_AMS_CCS
  FROM [ABC].[fact].[ABC_MASTER_AMS_CCS] 
 WHERE [Data Period] = @VERSION
   AND [Plant] IN ('C299', 'C208', 'C212', 'C240')
   AND [Cost Type] = 'F'

ALTER TABLE #tmp_AMS_CCS 
ALTER COLUMN [AMS CCS USED]  NVARCHAR(255);

UPDATE #tmp_AMS_CCS
SET [AMS CCS USED] = CASE WHEN [AMS CCS USED]  = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #tmp_AMS_CCS 
WHERE [AMS CCS USED] = 'N' 

UPDATE AA
SET AA.[L1 Supplier] = BB.[Vendor], 
    AA.[L1 Supplier Source] = 'CCS Return',
    AA.[Supplier Type] = 'Repair'
FROM [ABC].[staging].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_AMS_CCS BB 
WHERE AA.[Received Part Kit Part Number] =BB.[Part]
AND AA.[Region] = 'AME'
AND ISNULL(AA.[L1 Supplier],'') = ''

UPDATE AA
SET AA.[L1 Supplier] = BB.[Vendor], 
    AA.[L1 Supplier Source] = 'CCS Return',
    AA.[Supplier Type] = 'Repair'
FROM [ABC].[staging].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_AMS_CCS BB
WHERE AA.[Part Number] =BB.[Part]
AND AA.[Region] = 'AME'
AND ISNULL(AA.[L1 Supplier],'') = ''

-- EMEA Step 2
IF OBJECT_ID(N'tempdb..#tmp_EMEA_CCS') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_EMEA_CCS
  DROP TABLE #tmp_EMEA_CCS
END

SELECT --[Plant]
       [Part] 
	  ,[Trans Type] 
	  ,[VENDOR DESCRIPTION] AS [Vendor]
	  ,[Cost Change] = CASE WHEN LEFT([Vendor],2) = '00' THEN SUBSTRING([Vendor],3,10) ELSE [Vendor] END
	  ,ROW_NUMBER() OVER(PARTITION BY [Part] ORDER BY convert(datetime,[Cost Change]) DESC) AS [AMS CCS USED] 
  INTO #tmp_EMEA_CCS
  FROM [ABC].[fact].[ABC_MASTER_EMEA_CCS] 
 WHERE [Data Period] = @VERSION
   AND [Plant] = 'AM02'
   AND [Cost Type] = 'F'

ALTER TABLE #tmp_EMEA_CCS 
ALTER COLUMN [AMS CCS USED]  NVARCHAR(255);

UPDATE #tmp_EMEA_CCS
SET [AMS CCS USED]  = CASE WHEN [AMS CCS USED]  = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #tmp_EMEA_CCS 
WHERE [AMS CCS USED] = 'N' 

UPDATE AA
SET AA.[L1 Supplier] = BB.[Vendor],
    AA.[L1 Supplier Source] = 'CCS Return',
    AA.[Supplier Type] = 'Repair'
from [ABC].[staging].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_EMEA_CCS BB
where AA.[Received Part Kit Part Number] = BB.[Part]
AND AA.[Region] = 'EMEA'
and ISNULL(AA.[L1 Supplier],'') = ''

UPDATE AA
SET AA.[L1 Supplier] = BB.[Vendor],
    AA.[L1 Supplier Source] = 'CCS Return',
    AA.[Supplier Type] = 'Repair'
from [ABC].[staging].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_EMEA_CCS BB
where AA.[Part Number] = BB.[Part]
AND AA.[Region] = 'EMEA'
and ISNULL(AA.[L1 Supplier],'') = ''

-- APJ STEP 3
IF OBJECT_ID(N'tempdb..#TMP_APJ_NB_CCS') IS NOT NULL
BEGIN
  TRUNCATE TABLE #TMP_APJ_NB_CCS
  DROP TABLE #TMP_APJ_NB_CCS
END

SELECT --[PLANT],
	   [PART] ,
	   [TRANS TYPE] ,
       [VENDOR DESCRIPTION] AS [VENDOR],
       [COST CHANGE]
	   ,ROW_NUMBER() OVER(PARTITION BY [PART] ORDER BY convert(datetime,[COST CHANGE]) DESC) AS [APJ CCS USED] 
  INTO #TMP_APJ_NB_CCS
  FROM [ABC].[FACT].[ABC_MASTER_APJ_NB_CCS] 
 WHERE [COST TYPE] = 'C' AND [TRANS TYPE] = 'NB'
   AND [Plant] = 'H499'
   AND [Data Period] = @VERSION

ALTER TABLE #TMP_APJ_NB_CCS
ALTER COLUMN [APJ CCS USED]  NVARCHAR(255);

UPDATE #TMP_APJ_NB_CCS
SET [APJ CCS USED]  = CASE WHEN [APJ CCS USED]  = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #TMP_APJ_NB_CCS
WHERE [APJ CCS USED]  = 'N' 

UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS NB',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER]  AA, #TMP_APJ_NB_CCS BB
WHERE AA.[Received Part Kit Part Number] =BB.[PART]
AND AA.[Region] = 'APJ'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS NB',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER]  AA, #TMP_APJ_NB_CCS BB
WHERE AA.[PART NUMBER] =BB.[PART]
AND AA.[Region] = 'APJ'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

UPDATE AA
SET AA.[L1 Supplier] = BB.[Final Vendor],
	AA.[L2 Supplier] = BB.[Final Vendor]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_JP_FINAL_DISP_VENDER_REMAP] BB
WHERE AA.[Region] = 'APJ' 
AND AA.[Flow Tag] = BB.[Flow Tag]
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[L1 Supplier] = BB.[L1 Supplier]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_APJ_L1_SUPPLIER_REMAP_BY_FT] BB
WHERE AA.[Region] = 'APJ' 
AND AA.[Flow Tag] = BB.[Flow Tag]
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[L2 Supplier] = BB.[L2 Supplier],
    AA.[Supplier Type] = BB.[Supplier Type]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_APJ_L2_SUPPLIER] BB
WHERE AA.[Region] = 'APJ' 
AND AA.[L1 Supplier] = BB.[L1 Supplier]
AND BB.[Data Period] = @VERSION

-- AME STEP 3
IF OBJECT_ID(N'tempdb..#TMP_AMS_NB_CCS') IS NOT NULL
BEGIN
  TRUNCATE TABLE #TMP_AMS_NB_CCS
  DROP TABLE #TMP_AMS_NB_CCS
END

SELECT --[PLANT] ,
	   [PART] ,
	   [TRANS TYPE] ,
       [VENDOR DESCRIPTION] AS [VENDOR],
       [COST CHANGE]
	   ,ROW_NUMBER() OVER(PARTITION BY [PART] ORDER BY convert(datetime,[COST CHANGE]) DESC) AS [AMS CCS USED] 
  INTO #TMP_AMS_NB_CCS
  FROM [ABC].[FACT].[ABC_MASTER_AMS_NB_CCS] 
 WHERE [COST TYPE] = 'F' AND [TRANS TYPE] = 'NB'
   AND [Plant] IN ('C299', 'C208', 'C212', 'C240')
   AND [Data Period] = @VERSION

ALTER TABLE #TMP_AMS_NB_CCS
ALTER COLUMN [AMS CCS USED]  NVARCHAR(255);

UPDATE #TMP_AMS_NB_CCS
SET [AMS CCS USED]  = CASE WHEN [AMS CCS USED]  = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #TMP_AMS_NB_CCS
WHERE [AMS CCS USED]  = 'N' 

UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS NB',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER]  AA, #TMP_AMS_NB_CCS BB
WHERE AA.[Received Part Kit Part Number] =BB.[PART]
AND AA.[Region] = 'AME'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS NB',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER]  AA, #TMP_AMS_NB_CCS BB
WHERE AA.[PART NUMBER] = BB.[PART]
AND AA.[Region] = 'AME'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

-- AME PCG UPDATE
UPDATE AA
SET AA.[Supplier Type] = 'RFC'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[L1 Supplier] = 'CONVERGE INC'

UPDATE AA
SET AA.[L2 Supplier] = BB.[L2 Supplier],
    AA.[Supplier Type] = BB.[Supplier Type]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [ABC].[fact].[ABC_MASTER_AMS_L2_SUPPLIER] BB
WHERE AA.[L1 Supplier] = BB.[L1 Supplier]
AND AA.Region = 'AME'
AND BB.[L1 Supplier] != 'All Else'
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[L2 Supplier] = AA.[L1 Supplier]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'AME' AND AA.[L1 Supplier] NOT IN (
	SELECT DISTINCT [L1 Supplier] 
	FROM [ABC].[fact].[ABC_MASTER_AMS_L2_SUPPLIER]
	WHERE [L1 Supplier] != 'All Else'
	AND [Data Period] = @VERSION
)

-- AME Step 4
UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS Return',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER]  AA, #tmp_AMS_CCS BB
WHERE AA.[Physical Part Number] = BB.[PART]
AND AA.[Region] = 'AME'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

--EMEA STEP 3
IF OBJECT_ID(N'tempdb..#TMP_EMEA_NB_CCS') IS NOT NULL
BEGIN 
  TRUNCATE TABLE #TMP_EMEA_NB_CCS
  DROP TABLE #TMP_EMEA_NB_CCS
END

SELECT --[PLANT] 
       [PART] 
	  ,[TRANS TYPE] 
	  ,[VENDOR DESCRIPTION] AS [VENDOR]
	  ,[COST CHANGE]
	  ,ROW_NUMBER() OVER (PARTITION BY [PART] ORDER BY convert(datetime,[COST CHANGE]) DESC) AS [EMEA CCS USED] 
INTO #TMP_EMEA_NB_CCS
FROM [ABC].[FACT].[ABC_MASTER_EMEA_NB_CCS] 
WHERE [COST TYPE] = 'F' AND [TRANS TYPE]='NB'
AND [Plant] = 'AM02'
AND [Data Period] = @VERSION

ALTER TABLE #TMP_EMEA_NB_CCS
ALTER COLUMN [EMEA CCS USED]  NVARCHAR(255);

UPDATE #TMP_EMEA_NB_CCS
SET [EMEA CCS USED]  = CASE WHEN [EMEA CCS USED]  = '1' THEN 'Y' ELSE 'N' END;

DELETE FROM #TMP_EMEA_NB_CCS
WHERE [EMEA CCS USED]  = 'N' 

UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS NB',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, #TMP_EMEA_NB_CCS BB
WHERE AA.[Received Part Kit Part Number] =BB.[PART]
AND AA.Region = 'EMEA'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

UPDATE AA
SET AA.[L1 SUPPLIER] = BB.[VENDOR],
	AA.[L1 SUPPLIER SOURCE] = 'CCS NB',
	AA.[SUPPLIER TYPE] = 'Repair'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, #TMP_EMEA_NB_CCS BB
WHERE AA.[PART NUMBER] =BB.[PART]
AND AA.Region = 'EMEA'
AND ISNULL(AA.[L1 SUPPLIER],'') = ''

UPDATE AA
SET AA.[L2 Supplier] = BB.[L2 Supplier],
    AA.[Supplier Type] = BB.[Supplier Type]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [ABC].[fact].[ABC_MASTER_EMEA_L2_SUPPLIER] BB
WHERE AA.[L1 Supplier] = BB.[L1 Supplier]
AND AA.[Region] = 'EMEA'
AND BB.[L1 Supplier] != 'All Else'
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[L2 Supplier] = AA.[L1 Supplier]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[L1 Supplier] NOT IN (
	SELECT [L1 Supplier] 
	FROM [ABC].[fact].[ABC_MASTER_EMEA_L2_SUPPLIER]
	WHERE [L1 Supplier] != 'All Else'
	AND [Data Period] = @VERSION
) AND AA.[Region] = 'EMEA'


-- UPDATE [FINAL DISP FLAG]
ALTER TABLE [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER]
ADD [Final Disp Flag] NVARCHAR(255) NULL


-- For APJ NON PCG, if RWT ISNULL THEN QSPAEK, IF QSPEAK ISNULL THEN IRETURN
UPDATE AA
SET AA.[Final Disp Flag] = AA.[APJ Disposition]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'APJ'
AND (ISNULL(AA.[L2 Supplier], '') NOT LIKE 'PCG%' OR ISNULL(AA.[L2 Supplier], '') NOT LIKE 'CONVERGE%')
AND ISNULL(AA.[APJ Disposition],'') != ''

-- For APJ PCG
UPDATE AA
SET AA.[Final Disp Flag] = 'NFF'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'APJ'
AND (ISNULL(AA.[L2 Supplier], '') LIKE 'PCG%' OR ISNULL(AA.[L2 Supplier], '') LIKE 'CONVERGE%')
AND AA.[Qspeak Warranty Status] IN ('Non Warranty Repair', 'Warranty Repair')
AND AA.[Qspeak Disposition] = 'NFF'

UPDATE AA
SET AA.[Final Disp Flag] = 'IW Credit'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'APJ'
AND (ISNULL(AA.[L2 Supplier], '') LIKE 'PCG%' OR ISNULL(AA.[L2 Supplier], '') LIKE 'CONVERGE%')
AND AA.[Qspeak Warranty Status] IN ('Warranty Repair')
AND AA.[Qspeak Disposition] = 'Repaired'

UPDATE AA
SET AA.[Final Disp Flag] = 'OOW Credit'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'APJ'
AND (ISNULL(AA.[L2 Supplier], '') LIKE 'PCG%' OR ISNULL(AA.[L2 Supplier], '') LIKE 'CONVERGE%')
AND AA.[Qspeak Warranty Status] IN ('Non Warranty Repair')
AND AA.[Qspeak Disposition] = 'Repaired'

-- Correct APJ Final disp by mapping table, Jan 6 5:19 
-- Include SC01 SC2  TBD

UPDATE AA
SET AA.[Final Disp Flag] = BB.[HP Dispositon]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [ABC].[fact].[ABC_APJ_FINAL_DISP_REMAP_BY_FT] BB
WHERE AA.[Region] = 'APJ' 
AND AA.[Flow Tag] = BB.[Flow Tag Number]
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Final Disp Flag] = BB.[Final Disposition]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_JP_FINAL_DISP_VENDER_REMAP] BB
WHERE AA.[Region] = 'APJ' 
AND AA.[Flow Tag] = BB.[Flow Tag]
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Final Disp Flag] = 'In Transit'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] = 'CD11'
AND AA.[Region] = 'APJ'

UPDATE AA
SET AA.[Final Disp Flag] = 'Good Unused'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'APJ'
AND (AA.[iReturn Disposition] like 'FG%'
OR AA.[iReturn Disposition] like 'QC%')

UPDATE AA
SET AA.[Final Disp Flag] = CASE WHEN AA.[RWT Engineer Approval] = 'Yes'
                                THEN 'Scrap at Supplier'
								ELSE 'Scrap in Central'
						   END
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'APJ'
AND (AA.[iReturn Disposition] = 'SC23' OR AA.[Final Disp Flag] = 'SC23')

UPDATE AA
SET AA.[Final Disp Flag] = 'Logical Receipt'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Receipt Type] = 'Logical Receipt' 
AND (AA.[iReturn Disposition] = 'SC99' OR AA.[Actual Disposition] = 'SC99')
AND AA.Region = 'APJ'

-- Additional update for [L1 Supplier] & [L2 Supplier]
--UPDATE AA
--SET AA.[L1 Supplier] = 'Scrap in Country',
--    AA.[L2 Supplier] = 'Scrap in Country'
--FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
--WHERE AA.[Region] = 'APJ'
--AND AA.[iReturn Disposition] = 'SC01'
--AND AA.[APJ Received Plant] != 'H499'

UPDATE AA
SET AA.[Final Disp Flag] = 'Scrap in Central'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'APJ'
AND AA.[iReturn Disposition] = 'SC01'

UPDATE AA
SET AA.[Final Disp Flag] = BB.[Correct Disposition]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [ABC].[fact].[ABC_APJ_FINAL_DISP_REMAP] BB
WHERE AA.[Region] = BB.[Region] AND AA.[Final Disp Flag] = BB.[Final Disp Flag]

-- For AME/EMEA, TBD, need further discussion as vague design logic

UPDATE AA
SET AA.[Final Disp Flag] = 'Logical Receipt'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Receipt Type] = 'Logical Receipt' 
AND AA.[Actual Disposition] = 'SC99'
AND AA.Region IN ('AME','EMEA')

-- For RRD, PCG
UPDATE AA
SET AA.[Final Disp Flag] = 'NFF'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region != 'APJ'
AND AA.[L2 Supplier] = 'RRD'
--AND AA.[Qspeak Warranty Status] IN ('Non Warranty Repair', 'Warranty Repair')
AND AA.[Qspeak Disposition] = 'NFF'
AND AA.[Receipt Type] = 'Physical Receipt'

UPDATE AA
SET AA.[Final Disp Flag] = 'NFF'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region != 'APJ'
AND AA.[L2 Supplier] = 'PCG'
--AND AA.[Qspeak Warranty Status] IN ('Non Warranty Repair', 'Warranty Repair')
AND AA.[Qspeak Disposition] = 'NFF'
AND AA.[Receipt Type] = 'Physical Receipt'

UPDATE AA
SET AA.[Final Disp Flag] = 'Scrap at Supplier'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region != 'APJ'
--AND AA.[Qspeak Warranty Status] IN ('Non Warranty Repair', 'Warranty Repair')
AND AA.[Qspeak Disposition] = 'Process Scrapped'
--AND AA.[Supplier Type] = 'RFC'
AND AA.[Receipt Type] = 'Physical Receipt'

UPDATE AA
SET AA.[Final Disp Flag] = 'Scrap in Central'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] IN('AME','EMEA') 
AND AA.[Actual Disposition] IN ('SC01', 'SC12', 'SC02', 'SCRU', 'EC01','EC02','EC11')


UPDATE AA
SET AA.[Final Disp Flag] = BB.[Final Disp Flag]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_FINAL_DISP_FLAG] BB
WHERE AA.[Supplier Type] = BB.[L1 Supplier or Supplier Type]
AND AA.[Qspeak Disposition] = BB.[Qspeak Disposition]
AND AA.[Qspeak Warranty Status] = BB.[Qspeak Warranty Status]
AND AA.[Supplier Type] IN('RFC', 'Repair')
AND AA.[Region] IN('AME','EMEA') 

UPDATE AA
SET AA.[Final Disp Flag] = BB.[Final Disp Flag]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_FINAL_DISP_FLAG] BB
WHERE AA.[L2 Supplier] = BB.[L1 Supplier or Supplier Type]
AND AA.[Qspeak Disposition] = BB.[Qspeak Disposition]
AND AA.[Qspeak Warranty Status] = BB.[Qspeak Warranty Status]
AND AA.[L2 Supplier] IN('Inventec', 'Foxconn','SPC')
AND AA.[Region] IN('AME','EMEA') 

-- NEW ADDED FINAL DISP
UPDATE AA
SET AA.[Final Disp Flag] = 'In Transit'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Actual Disposition] IN ('CD11', 'IT21')
AND AA.[Region] IN('AME','EMEA') 

UPDATE AA
SET AA.[Final Disp Flag] = 'Good Unused'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] IN('AME','EMEA')
AND (AA.[Actual Disposition] like 'FG%'
OR AA.[Actual Disposition] like 'CU%'
OR AA.[Actual Disposition] like 'QC%'
OR AA.[Actual Disposition] like 'DS%')

UPDATE AA
SET AA.[Final Disp Flag] = 'Non Return'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Receipt Type] = 'No Receipt'

--UPDATE AA
--SET AA.[Final Disp Flag] = 'DMR Drive'
--FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
--WHERE AA.[Receipt Type] = 'Logical Receipt' 
--AND AA.[DMR Flag] = 'Y'
--AND AA.[Part Commodity] IN ('Hard Drive', 'Solid State Drive')


-- Country condition TBD
--UPDATE AA
--SET AA.[Final Disp Flag] = 'NA MV Expt SOI'
--FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
--WHERE AA.[Part SPL] = 'UY'
--AND AA.Region = 'AME'
--AND AA.[Event Country Iso Cd] IN ('CA','US')
--AND AA.[L2 supplier] = 'Expresspoint'
--AND AA.[Part Actual SL] = 'Same Day'

UPDATE AA
SET AA.[Final Disp Flag] = 'Part-Level Cost'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE ISNULL(AA.[Flow Tag],'') != '' 
AND ISNULL(AA.[Final Disp Flag],'') = ''

UPDATE AA
SET AA.[Final Disp Flag] = 'Unknown'
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE ISNULL(AA.[Flow Tag],'') = '' 
AND AA.[Receipt Type] != 'No Receipt'

-- link NRS/RVS, always choose the last month each quarter
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Cons Cost] float null, 
    [Event PN RVS] float null, 
	[Event PN NRS] float null,
	[Received Part Kit PN RVS] float null,
    [Physical PN RVS] float null,
	[Physical PN NRS] float null,
	[Physical PN NBS] float null

--DECLARE @iCOST_MONTH NVARCHAR(10) = '201610' 

-- TBD
UPDATE AA
SET AA.[Event PN RVS] = BB.[RVS]
   ,AA.[Event PN NRS] = BB.[NRS]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[HPRT4_iCOST_SVC_REGIONAL] BB
WHERE AA.[Part Number] = BB.[Part No] 
AND AA.[Region] = BB.[Region]
AND BB.[Report Month] = @iCOST_MONTH

-- UPDATE THOSE RVS OR NRS STILL IS NULL
UPDATE AA
SET AA.[Event PN RVS] = BB.[RVS]
   ,AA.[Event PN NRS] = BB.[NRS]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_SAP_MBEW] BB
WHERE AA.[Part Number] = BB.[Part Number] 
AND AA.[Region] = BB.[Region]
AND ISNULL(AA.[Event PN RVS],'') = '' AND ISNULL(AA.[Event PN NRS],'') = ''
AND BB.[Data Period] = @VERSION

-- append the unknown rvs & nrs
UPDATE AA
SET AA.[Event PN RVS] = BB.[RVS]
   ,AA.[Event PN NRS] = BB.[NRS]
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_RVS_NRS_ADJ] BB
WHERE AA.[Part Number] = BB.[Part Number] 
AND AA.[Region] = BB.[Region]
AND ISNULL(AA.[Event PN RVS],'') = '' AND ISNULL(AA.[Event PN NRS],'') = ''
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Received Part Kit PN RVS] = BB.RVS
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[HPRT4_iCOST_SVC_REGIONAL] BB
WHERE AA.Region = BB.[Region]
AND AA.[Received Part Kit Part Number] = BB.[Part No]
AND AA.[Received Month] = BB.[Report Month]

UPDATE AA
SET AA.[Received Part Kit PN RVS] = BB.RVS
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_SAP_MBEW] BB
WHERE AA.Region = BB.[Region]
AND AA.[Received Part Kit Part Number] = BB.[Part Number]
AND ISNULL(AA.[Received Part Kit PN RVS],'') = ''
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Received Part Kit PN RVS] = BB.RVS
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_RVS_NRS_ADJ] BB
WHERE AA.Region = BB.[Region]
AND AA.[Received Part Kit Part Number] = BB.[Part Number]
AND ISNULL(AA.[Received Part Kit PN RVS],'') = ''
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Physical PN RVS] = BB.RVS,
    AA.[Physical PN NRS] = BB.NRS
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[HPRT4_iCOST_SVC_REGIONAL] BB
WHERE AA.Region = BB.[Region]
AND AA.[Physical Part Number] = BB.[Part No]
AND AA.[Received Month] = BB.[Report Month]

UPDATE AA
SET AA.[Physical PN RVS] = BB.RVS,
    AA.[Physical PN NRS] = BB.NRS
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_SAP_MBEW] BB
WHERE AA.Region = BB.[Region]
AND AA.[Physical Part Number] = BB.[Part Number]
AND ISNULL(AA.[Physical PN RVS],'') = ''
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Physical PN RVS] = BB.RVS,
    AA.[Physical PN NRS] = BB.NRS
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_RVS_NRS_ADJ] BB
WHERE AA.Region = BB.[Region]
AND AA.[Physical Part Number] = BB.[Part Number]
AND ISNULL(AA.[Physical PN RVS],'') = ''
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Physical PN NBS] = BB.[New Buy]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[BIM_BMA_L1_iCOST_SVC] BB
WHERE AA.Region = BB.Region
AND AA.[Physical Part Number] = BB.[Part]
AND AA.[Received Month] = BB.iCost_period


-- Previous step needs ssis package to load data [staging].[E2_2_AVG_REPAIR_COST]
-- CC6 
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [OOW Avg Repair Cost] FLOAT NULL, [OOW/IW Avg Repair Cost] FLOAT NULL, 
[Is Avg RC GT RVS] NVARCHAR(1) NULL

-- TBD
UPDATE AA
SET AA.[OOW Avg Repair Cost] = BB.[OOW Avg Repair Cost],
    --AA.[OOW/IW Avg Repair Cost] = BB.[OOW/IW Avg Repair Cost],
	AA.[Is Avg RC GT RVS] = CASE WHEN BB.[OOW Avg Repair Cost] > AA.[Event PN RVS] THEN 'Y' ELSE 'N' END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[E2_2_AVG_REPAIR_COST] BB
WHERE AA.[Part Number] = BB.MATERIAL AND AA.Region = BB.REGION
AND BB.[Data Period] = @VERSION

-- CC7 
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Cons Cost Notes] NVARCHAR(255) NULL

UPDATE AA
SET AA.[Cons Cost] = 0.0
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] = 'IW Exch'

UPDATE AA
SET AA.[Cons Cost] = AA.[OOW Avg Repair Cost],
    AA.[Cons Cost Notes] = 'OOW Avg Repair Cost Used'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] = 'OOW Repair'
AND ISNULL(AA.[OOW Avg Repair Cost],-1.0) >= 0.0 

UPDATE AA
SET AA.[Cons Cost] = BB.[Out Warranty Repr],
    AA.[Cons Cost Notes] = 'iCost SVC Repair Std Used'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[BIM_BMA_L1_iCOST_SVC] BB
WHERE AA.[Part Number] = BB.Part
AND AA.[Received Month] = BB.[iCost_period]
AND AA.Region = BB.Region
AND AA.[Final Disp Flag] = 'OOW Repair' 
AND ISNULL(AA.[OOW Avg Repair Cost],-1.0) < 0.0
AND ISNULL(BB.[Out Warranty Repr],-1.0) >= 0.0

UPDATE AA
SET AA.[Cons Cost] = AA.[Event PN NRS],
    AA.[Cons Cost Notes] = 'NRS Used'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] = 'OOW Repair' 
AND ISNULL(AA.[Cons Cost],-1.0) < 0.0

/* should be delete as the [Cons Cost Notes] is uncontrollable 
UPDATE AA
SET AA.[Cons Cost] = ISNULL(ISNULL(AA.[OOW Avg Repair Cost],BB.[Out Warranty Repr]),AA.[Event PN NRS])
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[BIM_BMA_L1_iCOST_SVC] BB
WHERE AA.[Part Number] = BB.Part
AND AA.[Received Month] = BB.[iCost_period]
AND AA.Region = BB.Region
AND AA.[Final Disp Flag] = 'OOW Repair'
*/

-- CC9
UPDATE AA
SET AA.[OOW/IW Avg Repair Cost] = BB.[OOW/IW Avg Repair Cost]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[E2_2_AVG_REPAIR_COST] BB
WHERE AA.[Part Number] = BB.MATERIAL AND AA.Region = BB.REGION
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Cons Cost] = BB.[OOW/IW Avg Repair Cost],
    AA.[Cons Cost Notes] = 'OOW/IW Avg Repair Cost Used'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [rawdata].[E2_2_AVG_REPAIR_COST] BB
WHERE AA.[Part Number] = BB.MATERIAL 
AND AA.Region = BB.REGION
AND AA.[Final Disp Flag] = 'Part-Level Cost'
AND ISNULL(AA.[OOW/IW Avg Repair Cost],-1.0) >= 0.0
AND BB.[Data Period] = @VERSION
 
UPDATE AA
SET AA.[Cons Cost] = AA.[Event PN NRS],
    AA.[Cons Cost Notes] = 'NRS Used'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] = 'Part-Level Cost'
AND (ISNULL(AA.[OOW/IW Avg Repair Cost],-1.0) < 0.0 Or AA.[OOW/IW Avg Repair Cost] = 0.0)

UPDATE AA
SET AA.[Cons Cost] = AA.[Event PN NRS] * AA.[Part Qty Consumed],
    AA.[Cons Cost Notes] = 'NRS Used'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] in('Unknown', 'WT02', 'QT20', 'In Transit')

UPDATE AA
SET AA.[Cons Cost] = 0.0
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] = 'Good Unused'

-- OC4, get standard variance

ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [ESC Variance] float NULL
--update [staging].[ABC_S3_LINK_SUPPLIER] set [ESC Variance] = null
IF OBJECT_ID(N'tempdb..#tmp_Total_Cons_Qty_By_SPL') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_Total_Cons_Qty_By_SPL
  DROP TABLE #tmp_Total_Cons_Qty_By_SPL
END

SELECT [Region],
       [Event SPL],
       [Total Cons Qty] = SUM([Part Qty Consumed])
INTO #tmp_Total_Cons_Qty_By_SPL
FROM [staging].[ABC_S3_LINK_SUPPLIER]
WHERE [Event Country Iso Cd] != 'CN'
GROUP BY [Region], [Event SPL]

UPDATE AA
SET AA.[ESC Variance] = BB.[Actuals A USD]/1.0/CC.[Total Cons Qty] * AA.[Part Qty Consumed]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, 
     [fact].[ABC_MASTER_ESC_VAR] BB,
	 #tmp_Total_Cons_Qty_By_SPL CC
WHERE AA.REGION = BB.REGION AND AA.[Event SPL] = LEFT(BB.[L06 BA Code],2)
AND AA.Region = CC.Region AND AA.[Event SPL] = CC.[Event SPL]
AND [Event Country Iso Cd] != 'CN'
AND BB.[Data Period] = @VERSION

-- CC10, add Defective Scrap 
-- EMEA
--ALTER TABLE [fact].[ABC_MASTER_EMEA_DEFECTIVE_SCRAP]
--ADD [Gross Scrap Cost at RVS] float null,
--    [CTR Release] float null,
--	[Net Scrap Cost at DV] float null

UPDATE [fact].[ABC_MASTER_EMEA_DEFECTIVE_SCRAP]
SET [Gross Scrap Cost at RVS] = NULL,
    [CTR Release] = NULL,
	[Net Scrap Cost at DV] = NULL

UPDATE AA
SET AA.[Gross Scrap Cost at RVS] = AA.[Quantity] * BB.RVS,
    AA.[CTR Release] = AA.[Quantity] * BB.NRS * (-1.0),
	AA.[Net Scrap Cost at DV] = AA.[Quantity] * BB.RVS + AA.[Quantity] * BB.NRS * (-1.0)
FROM [fact].[ABC_MASTER_EMEA_DEFECTIVE_SCRAP] AA, [rawdata].[HPRT4_iCOST_SVC_REGIONAL] BB
WHERE AA.[Material] = BB.[Part No]
AND BB.Region = 'EMEA'
AND BB.[Report Month] = @iCOST_MONTH
AND AA.[Data Period] = @VERSION

-- add DF Scrap
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Def Scrap Cost] float null 

IF OBJECT_ID(N'tempdb..#tmp1_EMEA_DF_Scrap') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_EMEA_DF_Scrap
  DROP TABLE #tmp1_EMEA_DF_Scrap
END

IF OBJECT_ID(N'tempdb..#tmp2_EMEA_DF_Scrap') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_EMEA_DF_Scrap
  DROP TABLE #tmp2_EMEA_DF_Scrap
END

SELECT [Region],
       [Part Number],
       [Total Cons Qty] = SUM([Part Qty Consumed])
INTO #tmp1_EMEA_DF_Scrap
FROM [staging].[ABC_S3_LINK_SUPPLIER]
WHERE [Region] = 'EMEA'
AND ISNULL([Qspeak Flowtag Used],'') != 'N'
AND [Final Disp Flag] = 'Part-Level Cost'
GROUP BY [Region], [Part Number]

SELECT AA.[Material],
	   case when BB.[Total Cons Qty] > 0 then AA.[Net Scrap Cost at DV] / BB.[Total Cons Qty]
	        else 0.0
	   end as [Def Scrap Cost],
	   'EMEA' as [Region]
INTO #tmp2_EMEA_DF_Scrap
FROM [fact].[ABC_MASTER_EMEA_DEFECTIVE_SCRAP] AA, #tmp1_EMEA_DF_Scrap BB
WHERE AA.[Material] = BB.[Part Number]
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[Def Scrap Cost] = BB.[Def Scrap Cost] * AA.[Part Qty Consumed]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_EMEA_DF_Scrap BB
WHERE AA.[Part Number] = BB.[Material]
AND AA.[Region] = 'EMEA'
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[Final Disp Flag] = 'Part-Level Cost'

IF OBJECT_ID(N'staging.tmp_EMEA_Scrap_Cost_Process') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.tmp_EMEA_Scrap_Cost_Process
  DROP TABLE staging.tmp_EMEA_Scrap_Cost_Process
END

SELECT AA.[Region],
       AA.[Part Number],
       AA.[Total Cons Qty],
	   BB.[Net Scrap Cost at DV],
	   CC.[Def Scrap Cost]
  INTO staging.tmp_EMEA_Scrap_Cost_Process
  FROM #tmp1_EMEA_DF_Scrap AA, [fact].[ABC_MASTER_EMEA_DEFECTIVE_SCRAP] BB, #tmp2_EMEA_DF_Scrap CC
 WHERE AA.[Part Number] = BB.[Material] AND AA.[Part Number] = CC.[Material]
   AND BB.[Data Period] = @VERSION

-- AME
--ALTER TABLE [fact].[ABC_MASTER_AMS_DEFECTIVE_SCRAP]
--ADD [Gross Scrap Cost at RVS] float null,
--    [CTR Release] float null,
--	[Net Scrap Cost at DV] float null
UPDATE [fact].[ABC_MASTER_AMS_DEFECTIVE_SCRAP]
SET [Gross Scrap Cost at RVS] = NULL,
    [CTR Release] = NULL,
	[Net Scrap Cost at DV] = NULL

UPDATE AA
SET AA.[Gross Scrap Cost at RVS] = AA.[Quantity] * BB.RVS,
    AA.[CTR Release] = AA.[Quantity] * BB.NRS * (-1.0),
	AA.[Net Scrap Cost at DV] = AA.[Quantity] * BB.RVS + AA.[Quantity] * BB.NRS * (-1.0)
FROM [fact].[ABC_MASTER_AMS_DEFECTIVE_SCRAP] AA, [rawdata].[HPRT4_iCOST_SVC_REGIONAL] BB
WHERE AA.[Material] = BB.[Part No]
AND BB.Region = 'AME'
AND BB.[Report Month] = @iCOST_MONTH
AND AA.[Data Period] = @VERSION

-- add DF Scrap 
IF OBJECT_ID(N'tempdb..#tmp1_AMS_DF_Scrap') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_AMS_DF_Scrap
  DROP TABLE #tmp1_AMS_DF_Scrap
END

IF OBJECT_ID(N'tempdb..#tmp2_AMS_DF_Scrap') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_AMS_DF_Scrap
  DROP TABLE #tmp2_AMS_DF_Scrap
END

SELECT [Region],
       [Part Number],
       [Total Cons Qty] = SUM([Part Qty Consumed])
INTO #tmp1_AMS_DF_Scrap
FROM [staging].[ABC_S3_LINK_SUPPLIER]
WHERE [Region] = 'AME'
AND ISNULL([Qspeak Flowtag Used],'') != 'N'
AND [Final Disp Flag] = 'Part-Level Cost'
GROUP BY [Region], [Part Number]

SELECT AA.[Material],
	   case when BB.[Total Cons Qty] > 0 then AA.[Net Scrap Cost at DV] / BB.[Total Cons Qty]
	        else 0.0
	   end as [Def Scrap Cost],
	   'AME' as [Region]
INTO #tmp2_AMS_DF_Scrap
FROM [fact].[ABC_MASTER_AMS_DEFECTIVE_SCRAP] AA, #tmp1_AMS_DF_Scrap BB
WHERE AA.[Material] = BB.[Part Number]
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[Def Scrap Cost] = BB.[Def Scrap Cost] * AA.[Part Qty Consumed]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_AMS_DF_Scrap BB
WHERE AA.[Part Number] = BB.[Material]
AND AA.[Region] = 'AME'
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[Final Disp Flag] = 'Part-Level Cost'

IF OBJECT_ID(N'staging.tmp_AMS_Scrap_Cost_Process') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.tmp_AMS_Scrap_Cost_Process
  DROP TABLE staging.tmp_AMS_Scrap_Cost_Process
END

SELECT AA.[Region],
       AA.[Part Number],
       AA.[Total Cons Qty],
	   BB.[Net Scrap Cost at DV],
	   CC.[Def Scrap Cost]
  INTO staging.tmp_AMS_Scrap_Cost_Process
  FROM #tmp1_AMS_DF_Scrap AA, [fact].[ABC_MASTER_AMS_DEFECTIVE_SCRAP] BB, #tmp2_AMS_DF_Scrap CC
 WHERE AA.[Part Number] = BB.[Material] AND AA.[Part Number] = CC.[Material]
   AND BB.[Data Period] = @VERSION

-- OC1 NPPO
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [NPPO Cost] float null

-- OC1 NPPO APJ
IF OBJECT_ID(N'tempdb..#tmp_APJ_NPPO') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_APJ_NPPO
  DROP TABLE #tmp_APJ_NPPO
END

SELECT AA.[L2 Supplier],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp_APJ_NPPO
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'APJ' 
AND [Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
GROUP BY AA.[L2 Supplier]

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * CC.[NPPO Spend]/ 1.0 / BB.[Total Cons Qty]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_APJ_NPPO BB, [fact].[ABC_MASTER_APJ_NPPO] CC
WHERE AA.[L2 Supplier] = BB.[L2 Supplier]
AND [Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] = CC.[Supplier]
AND AA.[L2 Supplier] NOT IN ('RRD', 'Seagate')
AND AA.Region = 'APJ'
AND CC.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 
                     (select sum([NPPO Spend])
					  from [fact].[ABC_MASTER_APJ_NPPO] 
					  where [Supplier] in('RRD', 'Seagate')
					    and [Data Period] = @VERSION)
					  / 1.0 / 
					 (select sum([Part Qty Consumed]) 
					  from [staging].[ABC_S3_LINK_SUPPLIER]
					  where [Region] = 'APJ'
					  and [Part Commodity] in ('Hard Drive') 
                      and [Event Country Iso Cd] != 'CN'
					  and [L2 Supplier] in ('RRD', 'Toshiba', 'Seagate')
                      and ISNULL([Qspeak Flowtag Used], '') != 'N')
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'APJ'
AND AA.[Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[Part Commodity] in ('Hard Drive')
AND AA.[L2 Supplier] in ('RRD', 'Toshiba', 'Seagate')


-- OC1 NPPO AME
-- For Inventec * Foxconn
UPDATE AA
SET AA.[NPPO Cost] = BB.[NPPO]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_AMS_NPPO_INVENTEC] BB
WHERE AA.[Flow Tag] = BB.[Customer Service Tag]
AND AA.Region = 'AME'
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = BB.[NPPO]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_AMS_NPPO_FOXCONN] BB
WHERE AA.[Flow Tag] = BB.[Flow Tag]
AND AA.Region = 'AME'
AND BB.[Data Period] = @VERSION

-- NPPO FOR top vendors 
IF OBJECT_ID(N'tempdb..#tmp1_AMS_NPPO') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_AMS_NPPO
  DROP TABLE #tmp1_AMS_NPPO
END

SELECT AA.[L2 Supplier],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_AMS_NPPO
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'AME'
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] in (
    SELECT DISTINCT [Vendor2]
	FROM [fact].[ABC_MASTER_AMS_NPPO_TOP_VENDORS]
	WHERE [Vendor2] NOT IN ('RRD','Expresspoint')
	AND [Data Period] = @VERSION
) 
--AND AA.[L2 Supplier] in ('PCG', 'H3C TECHNOLOGIES CO LIMITED', 'ACCTON TECHNOLOGY CORP') 
GROUP BY AA.[L2 Supplier]

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * CC.[NPPO]/ 1.0 / BB.[Total Cons Qty]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp1_AMS_NPPO BB, [fact].[ABC_MASTER_AMS_NPPO_TOP_VENDORS] CC
WHERE AA.[L2 Supplier] = BB.[L2 Supplier]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] = CC.[Vendor2]
AND AA.Region = 'AME'
AND AA.[L2 Supplier] in (
    SELECT DISTINCT [Vendor2]
	FROM [fact].[ABC_MASTER_AMS_NPPO_TOP_VENDORS]
	WHERE [Vendor2] NOT IN ('RRD','Expresspoint')
	AND [Data Period] = @VERSION
) 
AND CC.[Data Period] = @VERSION

-- NPPO FOR RRD
IF OBJECT_ID(N'tempdb..#tmp2_AMS_NPPO') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_AMS_NPPO
  DROP TABLE #tmp2_AMS_NPPO
END

SELECT AA.[L2 Supplier],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp2_AMS_NPPO
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'AME'
AND AA.[L2 Supplier] = 'RRD' 
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[Final Disp Flag] in ('IW Credit', 'NFF', 'Scrap at Supplier','Scrap in Central')
GROUP BY AA.[L2 Supplier]

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * CC.[NPPO]/ 1.0 / BB.[Total Cons Qty]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_AMS_NPPO BB, [fact].[ABC_MASTER_AMS_NPPO_TOP_VENDORS] CC
WHERE AA.[L2 Supplier] = BB.[L2 Supplier]
AND AA.[L2 Supplier] = CC.[Vendor2]
AND AA.Region = 'AME'
AND AA.[L2 Supplier] = 'RRD' 
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[Final Disp Flag] in ('IW Credit', 'NFF', 'Scrap at Supplier','Scrap in Central')
AND CC.[Data Period] = @VERSION

-- OC1 NPPO EMEA
-- NPPO FOR Inventec & Foxconn
UPDATE AA
SET AA.[NPPO Cost] = BB.[CID Charge]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_EMEA_NPPO_INVENTEC] BB
WHERE AA.[Flow Tag] = BB.[Flow Tag]
AND AA.Region = 'EMEA'
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = BB.[CID Charge]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_EMEA_NPPO_FOXCONN] BB
WHERE AA.[Flow Tag] = BB.[Flow Tag]
AND AA.Region = 'EMEA'
AND BB.[Data Period] = @VERSION

-- NPPO FOR Accton
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_Accton') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_Accton
  DROP TABLE #tmp1_NPPO_Accton
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_Accton
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] in (SELECT DISTINCT [Part Number] 
                         FROM [fact].[ABC_MASTER_EMEA_NPPO_ACCTON]
						 WHERE [Data Period] = @VERSION)
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[L2 Supplier] like 'ACCTON%'
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_Accton') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_Accton
  DROP TABLE #tmp2_NPPO_Accton
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty]
INTO #tmp2_NPPO_Accton
FROM [fact].[ABC_MASTER_EMEA_NPPO_ACCTON] AA, #tmp1_NPPO_Accton BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * BB.[Unit Cons]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_NPPO_Accton BB
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] = BB.[Part Number]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] like 'ACCTON%'

-- NPPO FOR IQOR
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_IQOR') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_IQOR
  DROP TABLE #tmp1_NPPO_IQOR
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_IQOR
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] in (SELECT DISTINCT [Part Number] 
						 FROM [fact].[ABC_MASTER_EMEA_NPPO_IQOR]
						 WHERE [Data Period] = @VERSION)
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[L2 Supplier] like 'IQOR%'
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_IQOR') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_IQOR
  DROP TABLE #tmp2_NPPO_IQOR
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty]
INTO #tmp2_NPPO_IQOR
FROM [fact].[ABC_MASTER_EMEA_NPPO_IQOR] AA, #tmp1_NPPO_IQOR BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * BB.[Unit Cons]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_NPPO_IQOR BB
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] = BB.[Part Number]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] like 'IQOR%'

-- NPPO FOR Eaton
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_Eaton') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_Eaton
  DROP TABLE #tmp1_NPPO_Eaton
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_Eaton
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] in (SELECT DISTINCT [Part Number] 
                         FROM [fact].[ABC_MASTER_EMEA_NPPO_EATON]
						 WHERE [Data Period] = @VERSION)
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[L2 Supplier] like 'EATON%'
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_EATON') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_Eaton
  DROP TABLE #tmp2_NPPO_Eaton
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty]
INTO #tmp2_NPPO_Eaton
FROM [fact].[ABC_MASTER_EMEA_NPPO_EATON] AA, #tmp1_NPPO_Eaton BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * BB.[Unit Cons]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_NPPO_Eaton BB
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] = BB.[Part Number]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] like 'EATON%'

-- NPPO FOR SPC
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_SPC') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_SPC
  DROP TABLE #tmp1_NPPO_SPC
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_SPC
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] in (SELECT DISTINCT [Part Number] 
						 FROM [fact].[ABC_MASTER_EMEA_NPPO_SPC]
						 WHERE [Data Period] = @VERSION)
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[L2 Supplier] like 'SPC%'
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_SPC') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_SPC
  DROP TABLE #tmp2_NPPO_SPC
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty],
	   [Unit Credit] = (SELECT [Sum of charge] 
	                    FROM [fact].[ABC_MASTER_EMEA_NPPO_SPC] 
						WHERE [Part Number] = 'Total Credit'
						AND [Data Period] = @VERSION)
						/1.0/(
						SELECT SUM([Total Cons Qty])
						FROM #tmp1_NPPO_SPC
						)
INTO #tmp2_NPPO_SPC
FROM [fact].[ABC_MASTER_EMEA_NPPO_SPC] AA, #tmp1_NPPO_SPC BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * (BB.[Unit Cons] + BB.[Unit Credit])
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_NPPO_SPC BB
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] = BB.[Part Number]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] like 'SPC%'

-- NPPO FOR REL
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_REL') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_REL
  DROP TABLE #tmp1_NPPO_REL
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_REL
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] in (SELECT DISTINCT [Part Number] 
						 FROM [fact].[ABC_MASTER_EMEA_NPPO_REL]
						 WHERE [Data Period] = @VERSION)
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[L2 Supplier] = 'RENFREWSHIRE ELECTRONICS LTD'
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_REL') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_REL
  DROP TABLE #tmp2_NPPO_REL
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty]
INTO #tmp2_NPPO_REL
FROM [fact].[ABC_MASTER_EMEA_NPPO_REL] AA, #tmp1_NPPO_REL BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * BB.[Unit Cons]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_NPPO_REL BB
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] = BB.[Part Number]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] = 'RENFREWSHIRE ELECTRONICS LTD'

-- NPPO FOR H3C
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_H3C') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_H3C
  DROP TABLE #tmp1_NPPO_H3C
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_H3C
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] in (SELECT DISTINCT [Part Number] 
						 FROM [fact].[ABC_MASTER_EMEA_NPPO_H3C]
						 WHERE [Data Period] = @VERSION)
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N'
AND AA.[L2 Supplier] like 'H3C%'
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_H3C') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_H3C
  DROP TABLE #tmp2_NPPO_H3C
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty]
INTO #tmp2_NPPO_H3C
FROM [fact].[ABC_MASTER_EMEA_NPPO_H3C] AA, #tmp1_NPPO_H3C BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * BB.[Unit Cons]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp2_NPPO_H3C BB
WHERE AA.Region = 'EMEA'
AND AA.[Part Number] = BB.[Part Number]
AND ISNULL(AA.[Qspeak Flowtag Used], '') != 'N'
AND AA.[L2 Supplier] like 'H3C%'

-- NPPO FOR RRD
IF OBJECT_ID(N'fact.ABC_MASTER_EMEA_NPPO_RRD') IS NOT NULL
BEGIN
  TRUNCATE TABLE fact.ABC_MASTER_EMEA_NPPO_RRD
  DROP TABLE fact.ABC_MASTER_EMEA_NPPO_RRD
END

SELECT [Flow Tag] as [RRD Flow Tag],
       [Physical Part Number],
	   [Qspeak Disposition],
	   [Qspeak Warranty Status],
	   [Part Number],
	   [Part Qty Consumed],
	   CONVERT(FLOAT,null) as [NPPO Costs on Flow Tags],
	   CONVERT(FLOAT,null) as [NPPO Costs on Parts],
	   CONVERT(FLOAT,null) as [NPPO Costs on RRD Activity],
	   CONVERT(FLOAT,null) as [Total Costs],
	   @VERSION as [Data Period]
INTO fact.ABC_MASTER_EMEA_NPPO_RRD
FROM [staging].[ABC_S3_LINK_SUPPLIER]
WHERE [L2 Supplier] = 'RRD'
AND [Region] = 'EMEA'
AND ISNULL([Qspeak Flowtag Used],'') != 'N'
AND [Data Period] = @VERSION


-- RRD Flow Tag
UPDATE AA
SET AA.[NPPO Costs on Flow Tags] = BB.[Rate Per Transaction] * AA.[Part Qty Consumed]
FROM fact.ABC_MASTER_EMEA_NPPO_RRD AA, [fact].[ABC_MASTER_EMEA_NPPO_RRD_FLOWTAG] BB
WHERE AA.[Part Qty Consumed] > 0
AND AA.[Qspeak Disposition] = BB.[Qspeak Disposition]
AND AA.[Data Period] = BB.[Data Period]
AND AA.[Data Period] = @VERSION 

-- RRD Part level
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_RRD_BY_PART') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_RRD_BY_PART
  DROP TABLE #tmp1_NPPO_RRD_BY_PART
END

SELECT AA.[Part Number],
       [Total Cons Qty] = SUM(AA.[Part Qty Consumed])
INTO #tmp1_NPPO_RRD_BY_PART
FROM fact.ABC_MASTER_EMEA_NPPO_RRD AA
WHERE [Data Period] = @VERSION
GROUP BY AA.[Part Number]

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_RRD_BY_PART') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_RRD_BY_PART
  DROP TABLE #tmp2_NPPO_RRD_BY_PART
END

SELECT AA.[Part Number],
       [Unit Cons] = AA.[Sum of charge]/1.0/BB.[Total Cons Qty]
INTO #tmp2_NPPO_RRD_BY_PART
FROM fact.ABC_MASTER_EMEA_NPPO_RRD_PART_LEVEL AA, #tmp1_NPPO_RRD_BY_PART BB
WHERE AA.[Part Number] = BB.[Part Number]
AND BB.[Total Cons Qty] > 0 
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Costs on Parts] = AA.[Part Qty Consumed] * 1.0 * BB.[Unit Cons]
FROM fact.ABC_MASTER_EMEA_NPPO_RRD AA, #tmp2_NPPO_RRD_BY_PART BB
WHERE AA.[Part Number] = BB.[Part Number]
AND AA.[Data Period] = @VERSION

-- RRD Activity
IF OBJECT_ID(N'tempdb..#tmp1_NPPO_RRD_BY_ACTIVITY') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_NPPO_RRD_BY_ACTIVITY
  DROP TABLE #tmp1_NPPO_RRD_BY_ACTIVITY
END

SELECT SUM(ISNULL([NPPO Cost],0.0)) AS [Total Activity Cost]
INTO #tmp1_NPPO_RRD_BY_ACTIVITY
FROM [fact].[ABC_MASTER_EMEA_NPPO_RRD_ACTIVITY]
WHERE [Data Period] = @VERSION

IF OBJECT_ID(N'tempdb..#tmp2_NPPO_RRD_BY_ACTIVITY') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp2_NPPO_RRD_BY_ACTIVITY
  DROP TABLE #tmp2_NPPO_RRD_BY_ACTIVITY
END

SELECT SUM([Part Qty Consumed]) AS [Total Qty Consumed]
INTO #tmp2_NPPO_RRD_BY_ACTIVITY
FROM fact.ABC_MASTER_EMEA_NPPO_RRD
WHERE [Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Costs on RRD Activity] = 
    (SELECT [Total Activity Cost] FROM #tmp1_NPPO_RRD_BY_ACTIVITY)
	/1.0/
	(SELECT [Total Qty Consumed] FROM #tmp2_NPPO_RRD_BY_ACTIVITY)
FROM fact.ABC_MASTER_EMEA_NPPO_RRD AA
WHERE AA.[Part Qty Consumed] > 0
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[Total Costs] = ISNULL(AA.[NPPO Costs on Flow Tags],0.0)
                     + ISNULL(AA.[NPPO Costs on Parts],0.0)
					 + ISNULL(AA.[NPPO Costs on RRD Activity],0.0)
FROM [fact].[ABC_MASTER_EMEA_NPPO_RRD] AA
WHERE AA.[Part Qty Consumed] > 0
AND AA.[Data Period] = @VERSION

UPDATE AA
SET AA.[NPPO Cost] = BB.[Total Costs]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_EMEA_NPPO_RRD] BB
WHERE AA.[Flow Tag] = BB.[RRD Flow Tag]
AND AA.Region = 'EMEA'
AND AA.[L2 Supplier] = 'RRD'
--AND AA.[Qspeak Disposition] IN ('NFF','Repaired','Process Scrapped')
AND AA.[Qspeak Flowtag Used] != 'N'
AND ISNULL(BB.[Total Costs], '') != ''
AND BB.[Data Period] = @VERSION


-- CC2 

-- GIVE EACH RECORDS A UNIQUE REMARK
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Relabel Cost] float null,
    [Dekit Cost] float null,
    [Relabel Cost Comment] varchar(255) null,
    [Dekit Cost Comment] varchar(255) null

--UPDATE [staging].[ABC_S3_LINK_SUPPLIER]
--SET [Event PN RVS] = null,
--    [Received Part Kit PN RVS] = null,
--    [Physical PN RVS] = null,
--    [Relabel Cost] = null,
--    [Dekit Cost] = null,
--    [Relabel Cost Comment] = null,
--    [Dekit Cost Comment] = null

-- CC2 APJ RVS
-- add comments

UPDATE AA
SET AA.[Relabel Cost] = 
    CASE WHEN ISNULL(AA.[Event PN RVS],'') != '' 
	      AND ISNULL(AA.[Received Part Kit PN RVS],'') != ''
		 THEN AA.[Event PN RVS] - AA.[Received Part Kit PN RVS]
		 ELSE 0.0
	END,
	AA.[Dekit Cost] = 
	CASE WHEN ISNULL(AA.[Received Part Kit PN RVS],'') != ''
		  AND ISNULL(AA.[Physical PN RVS],'') != ''
		 THEN AA.[Received Part Kit PN RVS] - AA.[Physical PN RVS]
		 ELSE 0.0
	END,
	AA.[Relabel Cost Comment] = 
	CASE WHEN ISNULL(AA.[Event PN RVS],'') != '' 
	      AND ISNULL(AA.[Received Part Kit PN RVS],'') != ''
		 THEN AA.[Relabel Cost Comment]
		 ELSE ISNULL(AA.[Relabel Cost Comment],'') + 'Missing RVS'
	END,
	AA.[Dekit Cost Comment] = 
	CASE WHEN ISNULL(AA.[Physical PN RVS],'') != '' 
	      AND ISNULL(AA.[Received Part Kit PN RVS],'') != ''
		 THEN AA.[Dekit Cost Comment]
		 ELSE ISNULL(AA.[Dekit Cost Comment],'') + 'Missing RVS'
	END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA

UPDATE AA
SET AA.[Relabel Cost Comment] = 
	CASE WHEN ISNULL(AA.[Event PN RVS],'') != ''
		  AND ISNULL(AA.[Received Part Kit PN RVS],'') != ''
		  AND AA.[Event PN RVS] = AA.[Received Part Kit PN RVS]
		 THEN ISNULL(AA.[Relabel Cost Comment],'') + 'Same Kit Part Number'
		 ELSE AA.[Relabel Cost Comment]
	END,
	AA.[Dekit Cost Comment] = 
	CASE WHEN ISNULL(AA.[Physical PN RVS],'') != ''
		  AND ISNULL(AA.[Received Part Kit PN RVS],'') != ''
		  AND AA.[Physical PN RVS] = AA.[Received Part Kit PN RVS]
		 THEN ISNULL(AA.[Dekit Cost Comment],'') + 'Same Physical Part Number'
		 ELSE AA.[Dekit Cost Comment]
	END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA

-- decode CT Label
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Part Age] decimal(18,1) null

--DECLARE @MID_Q_DATE DATETIME
--SET @MID_Q_DATE = CONVERT(DATETIME,'20160915')

UPDATE AA
SET AA.[Part Age] = CONVERT(DECIMAL(18,1),DATEDIFF(MONTH,BB.[Week Beginning],@MID_Q_DATE)/1.0/12)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_MFG_DATES_FOR_CT_LABEL] BB
WHERE LEN(AA.[CT Code Label]) = 14 
AND SUBSTRING(AA.[CT Code Label],10,2) = BB.[Week Code]
AND AA.[Region] in ('AME', 'EMEA')
AND AA.[Receipt Type] = 'Physical Receipt'
AND AA.[L2 Supplier] = 'RRD'
AND AA.[Qspeak OEM Name] = 'Seagate'

UPDATE AA
SET AA.[Part Age] = CONVERT(DECIMAL(18,1),DATEDIFF(MONTH,BB.[Week Beginning],@MID_Q_DATE)/1.0/12)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_MFG_DATES_FOR_CT_LABEL] BB
WHERE LEN(AA.[CT Code Label]) = 14 
AND SUBSTRING(AA.[CT Code Label],10,2) = BB.[Week Code]
AND AA.[Region] = 'APJ'
AND AA.[Receipt Type] = 'Physical Receipt'
AND AA.[L2 Supplier] = 'Seagate'

-- TBD

ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [MFG Date] datetime null

UPDATE AA
SET AA.[MFG Date] = BB.[Week Beginning]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_MFG_DATES_FOR_CT_LABEL] BB
WHERE LEN(AA.[CT Code Label]) = 14 
AND SUBSTRING(AA.[CT Code Label],10,2) = BB.[Week Code]
AND AA.Region IN ('AME', 'EMEA')
AND AA.[Receipt Type] = 'Physical Receipt'
AND AA.[L2 Supplier] = 'RRD'
AND AA.[Qspeak OEM Name] = 'Seagate'

UPDATE AA
SET AA.[MFG Date] = BB.[Week Beginning]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_MFG_DATES_FOR_CT_LABEL] BB
WHERE LEN(AA.[CT Code Label]) = 14 
AND SUBSTRING(AA.[CT Code Label],10,2) = BB.[Week Code]
AND AA.[Region] = 'APJ'
AND AA.[Receipt Type] = 'Physical Receipt'
AND AA.[L2 Supplier] = 'Seagate'

-- CC12
-- ADD GSD SC DMR Flag
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [GSD SC DMR Flag] nvarchar(25) null,
   -- [GSD SC DMR Flag By Master Event Number] nvarchar(25) null,
    [Credits] float null,
	[Credit Comments] nvarchar(255) null

--update [staging].[ABC_S3_LINK_SUPPLIER]
--set [GSD SC DMR Flag By So + Item] = null,
--[GSD SC DMR Flag By Master Event Number] = null

IF OBJECT_ID(N'tempdb..#tmp_DMR_FLAG') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_DMR_FLAG
  DROP TABLE #tmp_DMR_FLAG
END

SELECT DISTINCT 
       [Region]
      ,[Customer Po Number]
	  ,[Shipped Part Number]
      ,[Part Commodity-shipped]
      ,[DMR Flag]
	  ,[So + Item]
  INTO #tmp_DMR_FLAG
  FROM [rawdata].[HPRT4_DMR_FLAG]
 WHERE isnull([DMR Flag],'') != ''

UPDATE AA
SET AA.[GSD SC DMR Flag] = BB.[DMR Flag]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_DMR_FLAG BB
WHERE REPLACE(AA.[So + Item],'-','') = BB.[So + Item]
AND AA.Region = BB.Region

UPDATE AA
SET AA.[GSD SC DMR Flag] = 'No DMR'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE ISNULL(AA.[GSD SC DMR Flag],'') = ''
--AND AA.Region IN('EMEA','AME')

-- CC3
-- calculate virtual credit
UPDATE AA
SET AA.[Credits] = CASE WHEN AA.[Part Commodity] = 'Hard Drive' 
					    THEN AA.[Part Qty Consumed] * AA.[Event PN RVS] * BB.[HDD Credit Rate] / 
						    (
							 SELECT SUM(ISNULL([Part Qty Consumed],0.0) * ISNULL([Event PN RVS],0.0))
							 FROM [staging].[ABC_S3_LINK_SUPPLIER]
						     WHERE [Region] = AA.[Region]
						     AND [Part Commodity] = 'Hard Drive'
                             AND [GSD SC DMR Flag] = 'DMR'
							 AND [Receipt Type] in ('Logical Receipt', 'No Receipt')
							) * -1.0
					    WHEN AA.[Part Commodity] = 'Solid State Drive' 
					    THEN AA.[Part Qty Consumed] * AA.[Event PN RVS] * BB.[SSD Credit Rate] / 
						    (
							 SELECT SUM(ISNULL([Part Qty Consumed],0.0) * ISNULL([Event PN RVS],0.0))
							 FROM [staging].[ABC_S3_LINK_SUPPLIER]
						     WHERE [Region] = AA.[Region]
						     AND [Part Commodity] = 'Solid State Drive'
                             AND [GSD SC DMR Flag] = 'DMR'
							 AND [Receipt Type] in ('Logical Receipt', 'No Receipt')							 
						    )* -1.0
					    ELSE AA.[Credits]
				  END,
	AA.[Credit Comments] = 'DMR Credit'	  
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_VIRTUAL_CREDIT_RATE] BB
WHERE AA.Region = BB.Region
AND AA.[Part Commodity] IN ('Hard Drive', 'Solid State Drive')
AND AA.[GSD SC DMR Flag] = 'DMR'
AND AA.[Receipt Type] in ('Logical Receipt', 'No Receipt')
AND BB.[Data Period] = @VERSION

-- cal PCG Credit
UPDATE AA
SET AA.[Credits] = CASE WHEN AA.[Final Disp Flag] in('IW Credit','OOW Credit') AND BB.[Commodity] = 'Memory'
                        THEN AA.[Part Qty Received] * BB.[Final Price] * 0.88 * -1.0
						WHEN AA.[Final Disp Flag] = 'IW Credit' AND BB.[Commodity] = 'CPU'
						THEN AA.[Part Qty Received] * BB.[Final Price] * -1.0
						WHEN AA.[Final Disp Flag] = 'NFF' AND (BB.[Commodity] = 'CPU' Or BB.[Commodity] = 'Memory')
                        THEN AA.[Part Qty Received] * BB.[Final Price] * -1.0
						WHEN AA.[Final Disp Flag] = 'Scrap at Supplier'
						THEN AA.[Part Qty Received] * BB.[Final Price] * 0.06 * -1.0
						ELSE NULL
				  END,
    AA.[Credit Comments] = CASE WHEN AA.[Final Disp Flag] in('IW Credit','OOW Credit') AND BB.[Commodity] = 'Memory'
								THEN 'Supplier Credit'
						        WHEN AA.[Final Disp Flag] = 'IW Credit' AND BB.[Commodity] = 'CPU'
						        THEN 'Supplier Credit'
								WHEN AA.[Final Disp Flag] = 'NFF' AND (BB.[Commodity] = 'CPU' Or BB.[Commodity] = 'Memory')
								THEN 'Supplier Credit'
								WHEN AA.[Final Disp Flag] = 'Scrap at Supplier'
								THEN 'Supplier Credit'
								ELSE NULL
							END	  
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_WW_PCG_PRICE] BB
WHERE AA.Region = BB.Region
AND AA.[Received Part Kit Part Number] = BB.[Part Number]
AND AA.[L2 Supplier] like 'PCG%'
AND AA.[Final Disp Flag] in ('IW Credit', 'OOW Credit', 'NFF','Scrap at Supplier')
AND ISNULL(AA.[Part Qty Received], 0) = 1
AND BB.[Data Period] = @VERSION

IF OBJECT_ID(N'tempdb..#tmp_ccs_iw') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ccs_iw
  DROP TABLE #tmp_ccs_iw
END

SELECT [Region]
      ,[Month]
	  ,[Part No]
	  ,AVG([Contract Cost]) AS [New Buy]
INTO #tmp_ccs_iw
FROM [rawdata].[HPRT7_CCS_IWC]
GROUP BY [Region], [Month], [Part No]

UPDATE AA
SET AA.[Credits] = BB.[New Buy] * -1.0,
    AA.[Credit Comments] = CASE WHEN ISNULL(AA.[Credit Comments],'') = '' THEN 'CCS IW Credit' END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_iw BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.[L2 Supplier] like 'PCG%'
AND AA.[Final Disp Flag] in ('IW Credit', 'NFF','Scrap at Supplier')
AND ISNULL(AA.[Credits],1.0) > 0.0

IF OBJECT_ID(N'tempdb..#tmp1_ccs_oow') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_ccs_oow
  DROP TABLE #tmp1_ccs_oow
END

SELECT [Region]
      ,[Month]
	  ,[Part No]
	  ,AVG([Contract Cost]) AS [New Buy]
INTO #tmp1_ccs_oow
FROM [rawdata].[HPRT7_CCS_AVERAGE]
WHERE [Transaction Type] = 'OC'
GROUP BY [Region], [Month], [Part No]

UPDATE AA
SET AA.[Credits] = BB.[New Buy] * -1.0,
    AA.[Credit Comments] = CASE WHEN ISNULL(AA.[Credit Comments],'') = '' THEN 'CCS OOW Credit' END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp1_ccs_oow BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.[L2 Supplier] like 'PCG%'
AND AA.[Final Disp Flag] = 'OOW Credit'
AND ISNULL(AA.[Credits],1.0) > 0.0

-- CC4 
-- 1. Seagate NB/IWC, limit VendorCode/DC
--   NB
IF OBJECT_ID(N'tempdb..#tmp_ccs_avg_nb') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ccs_avg_nb
  DROP TABLE #tmp_ccs_avg_nb
END

SELECT AA.[Region]
      ,AA.[Month]
	  ,AA.[Part No]
	  ,AVG(AA.[Forecast Cost]) AS [Forecast]
INTO #tmp_ccs_avg_nb
FROM [rawdata].[HPRT7_CCS_AVERAGE] AA, [fact].[ABC_MASTER_RRD_VENDOR] BB
WHERE AA.[Vendor No] = BB.[Vendor]
AND AA.Region = BB.[Region]
AND AA.[Org CD] IN ('AM02','H499','C299')
AND AA.[Transaction Type] = 'NB'
AND ISNULL(AA.[Forecast Cost],-100.0) >= 0.0
AND BB.[Data Period] = @VERSION
GROUP BY AA.[Region], AA.[Month], AA.[Part No]

--  IWC
IF OBJECT_ID(N'tempdb..#tmp_ccs_avg_iwc') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ccs_avg_iwc
  DROP TABLE #tmp_ccs_avg_iwc
END

SELECT AA.[Region]
      ,AA.[Month]
	  ,AA.[Part No]
	  ,AVG(AA.[Contract Cost]) AS [Contract]
INTO #tmp_ccs_avg_iwc
FROM [rawdata].[HPRT7_CCS_IWC] AA, [fact].[ABC_MASTER_RRD_VENDOR] BB
WHERE AA.[Vendor No] = BB.[Vendor]
AND AA.Region = BB.[Region]
AND AA.[Org CD] IN ('AM02','H499','C299')
AND BB.[Data Period] = @VERSION
GROUP BY AA.[Region], AA.[Month], AA.[Part No]

-- 1.1 for AME/EMEA
UPDATE AA
SET AA.[Credits] = CASE WHEN ISNULL(AA.[Part Age], 1000.0) != 1000.0  
                        AND AA.[Part Age] <= 3.0
                        THEN BB.[Forecast] * -1.0
						WHEN ISNULL(AA.[Part Age], 0.0) != 0.0 
						AND AA.[Part Age] > 3.0
						THEN BB.[Forecast] * 0.5 * -1.0
						WHEN (ISNULL(AA.[Part Age], 1000.0) = 1000.0 
							OR ISNULL(AA.[CT Code Label], '') = '')
						THEN BB.[Forecast] * -1.0
						ELSE AA.[Credits]
					END,
	AA.[Credit Comments] = CASE WHEN ISNULL(AA.[Part Age], 1000.0) != 1000.0  
								THEN 'CCS NB Fcst'
								WHEN (ISNULL(AA.[Part Age], 1000.0) = 1000.0 
									OR ISNULL(AA.[CT Code Label], '') = '')
								THEN 'CCS NB Fcst - Blank Part Age' 
								ELSE AA.[Credit Comments]
							END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_nb BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.[Region] IN ('AME', 'EMEA')
AND AA.[L2 Supplier] = 'RRD'
AND LOWER(AA.[Qspeak OEM Name]) = 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'

UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
	AA.[Credit Comments] = 'CCS IW Credit' 
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_iwc BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.[Region] IN ('AME', 'EMEA')
AND AA.[L2 Supplier] = 'RRD'
AND LOWER(AA.[Qspeak OEM Name]) = 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'
AND ISNULL(AA.[Credits],'') = ''

-- 1.2 for APJ
UPDATE AA
SET AA.[Credits] = CASE WHEN ISNULL(AA.[Part Age], 1000.0) != 1000.0  
                        AND AA.[Part Age] <= 3.0
                        THEN BB.[Forecast] * -1.0
						WHEN ISNULL(AA.[Part Age], 0.0) != 0.0 
						AND AA.[Part Age] > 3.0
						THEN BB.[Forecast] * 0.5 * -1.0
						WHEN (ISNULL(AA.[Part Age], 1000.0) = 1000.0 
							OR ISNULL(AA.[CT Code Label], '') = '')
						THEN BB.[Forecast] * -1.0
						ELSE AA.[Credits]
					END,
	AA.[Credit Comments] = CASE WHEN ISNULL(AA.[Part Age], 1000.0) != 1000.0  
								THEN 'CCS NB Fcst'
								WHEN (ISNULL(AA.[Part Age], 1000.0) = 1000.0 
									OR ISNULL(AA.[CT Code Label], '') = '')
								THEN 'CCS NB Fcst - Blank Part Age' 
								ELSE AA.[Credit Comments]
							END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_nb BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.[Region] = 'APJ'
AND lower(AA.[L2 Supplier]) = 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'

UPDATE AA
SET AA.[Credits] = BB.[Contract]* -1.0,
	AA.[Credit Comments] = 'CCS IW Credit' 
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_iwc BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.[Region] = 'APJ'
AND lower(AA.[L2 Supplier]) = 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'
AND ISNULL(AA.[Credits],'') = ''

UPDATE AA
SET AA.[Credits] = ISNULL(AA.[Credits],0.0) + BB.[Avg Kitting Fee]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_APJ_AVG_KITTING_FEE] BB
WHERE AA.Region = BB.[Region]
AND AA.[Master Event Activity Month] = BB.[Month]
AND lower(AA.[L2 Supplier]) = 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'
AND AA.[Credit Comments] in ('CCS NB Fcst', 'CCS NB Fcst - Blank Part Age')
AND BB.[Data Period] = @VERSION

-- 2. Non seagate NB/IWC, Limit DC
--   NB
IF OBJECT_ID(N'tempdb..#tmp_ccs_avg_nsg_nb') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ccs_avg_nsg_nb
  DROP TABLE #tmp_ccs_avg_nsg_nb
END

SELECT AA.[Region]
      ,AA.[Month]
	  ,AA.[Part No]
	  ,AVG(AA.[Forecast Cost]) AS [Forecast]
INTO #tmp_ccs_avg_nsg_nb
FROM [rawdata].[HPRT7_CCS_AVERAGE] AA
WHERE AA.[Org CD] IN ('AM02','H499','C299')
AND AA.[Transaction Type] = 'NB'
AND ISNULL(AA.[Forecast Cost],-100.0) >= 0.0
GROUP BY AA.[Region], AA.[Month], AA.[Part No]

--  IWC
IF OBJECT_ID(N'tempdb..#tmp_ccs_avg_nsg_iwc') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ccs_avg_nsg_iwc
  DROP TABLE #tmp_ccs_avg_nsg_iwc
END

SELECT AA.[Region]
      ,AA.[Month]
	  ,AA.[Part No]
	  ,AVG(AA.[Contract Cost]) AS [Contract]
INTO #tmp_ccs_avg_nsg_iwc
FROM [rawdata].[HPRT7_CCS_IWC] AA
WHERE AA.[Org CD] IN ('AM02','H499','C299')
GROUP BY AA.[Region], AA.[Month], AA.[Part No]

-- 2.1 for AME/EMEA
UPDATE AA
SET AA.[Credits] = BB.[Forecast] * -1.0,
	AA.[Credit Comments] = 'CCS NB Fcst'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_nsg_nb BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.Region IN ('AME', 'EMEA')
AND AA.[L2 Supplier] = 'RRD'
AND LOWER(AA.[Qspeak OEM Name]) != 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'

UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
	AA.[Credit Comments] = 'CCS IW Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_nsg_iwc BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.Region IN ('AME', 'EMEA')
AND AA.[L2 Supplier] = 'RRD'
AND LOWER(AA.[Qspeak OEM Name]) != 'seagate'
AND AA.[Final Disp Flag] = 'IW Credit'
AND ISNULL(AA.[Credits],'') = ''

-- 2.2 for APJ
UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
	AA.[Credit Comments] = 'CCS IW Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_avg_nsg_iwc BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.Region = 'APJ'
AND AA.[L2 Supplier] IN ('RRD', 'HITACHI GST', 'TOSHIBA', 'WESTERN DIGITAL')
AND AA.[Final Disp Flag] = 'IW Credit'

UPDATE AA
SET AA.[Cons Cost] = 0.0
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[L2 Supplier] = 'RRD'
AND AA.[Final Disp Flag] = 'NFF'

-- CC4.1 For non HDD VENDOR (RRD, PCG,SEAGATE,HITACHI, TOSHIBA, WESTERN DIGITAL)
-- IW
IF OBJECT_ID(N'tempdb..#tmp1_ccs_iw') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp1_ccs_iw
  DROP TABLE #tmp1_ccs_iw
END

SELECT [Region]
      ,[Month]
	  ,[Part No]
	  ,AVG([Contract Cost]) AS [Contract]
INTO #tmp1_ccs_iw
FROM [rawdata].[HPRT7_CCS_IWC]
GROUP BY [Region], [Month], [Part No]

UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
    AA.[Credit Comments] = 'Supplier Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp1_ccs_iw BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.Region IN ('AME', 'EMEA')
AND (AA.[L2 Supplier] != 'RRD' AND ISNULL(AA.[L2 Supplier],'') not like'PCG%')
AND AA.[Final Disp Flag] = 'IW Credit'

UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
    AA.[Credit Comments] = 'Supplier Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp1_ccs_iw BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Month] 
AND AA.Region = 'APJ'
AND (
     AA.[L2 Supplier] NOT IN('RRD','seagate', 'HITACHI GST', 'WESTERN DIGITAL', 'TOSHIBA')  
	 AND ISNULL(AA.[L2 Supplier],'') not like'PCG%' 
	)
AND AA.[Final Disp Flag] = 'IW Credit'

-- OOW
IF OBJECT_ID(N'tempdb..#tmp_ccs_oow') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ccs_oow
  DROP TABLE #tmp_ccs_oow
END

SELECT [Region]
      ,[Month] as [Report Month]
	  ,[Part No]
	  ,AVG([Contract Cost]) AS [Contract]
INTO #tmp_ccs_oow
FROM [rawdata].[HPRT7_CCS_AVERAGE]
WHERE [Transaction Type] = 'OC'
GROUP BY [Region], [Month], [Part No]

UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
    AA.[Credit Comments] = 'Supplier Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_oow BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Report Month] 
AND AA.Region IN ('AME', 'EMEA')
AND (AA.[L2 Supplier] != 'RRD' AND ISNULL(AA.[L2 Supplier],'') not like'PCG%')
AND AA.[Final Disp Flag] = 'OOW Credit'

UPDATE AA
SET AA.[Credits] = BB.[Contract] * -1.0,
    AA.[Credit Comments] = 'Supplier Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ccs_oow BB
WHERE AA.[Physical Part Number] = BB.[Part No]
AND AA.[Region] = BB.Region
AND AA.[Received Month] = BB.[Report Month] 
AND AA.Region = 'APJ'
AND (
     AA.[L2 Supplier] NOT IN('RRD','seagate', 'HITACHI GST', 'WESTERN DIGITAL', 'TOSHIBA')  
	 AND ISNULL(AA.[L2 Supplier],'') not like'PCG%' 
	)
AND AA.[Final Disp Flag] = 'OOW Credit'

-- OC5

-- Logic changed in 20170425
UPDATE AA
SET AA.[Credits] = BB.[Orphan Receipt at DV]/ 
                   (SELECT SUM([Event PN RVS]*[Part Qty Consumed])
				    FROM [staging].[ABC_S3_LINK_SUPPLIER]
					WHERE Region = AA.Region
					AND [Final Disp Flag] = 'Non Return'
                    AND [GSD SC DMR Flag] = 'No DMR'
					AND [Event Country Iso Cd] != 'CN'
                    AND [Part Qty Consumed] > 0.0
					AND ISNULL([Event PN RVS],'') != ''
					) * AA.[Event PN RVS]* AA.[Part Qty Consumed],
    AA.[Credit Comments] = 'Def Orphan Receipt Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_Orphan_Receipt_Costing] BB
WHERE AA.[Region] = BB.Region
AND AA.[Final Disp Flag] = 'Non Return'
AND AA.[GSD SC DMR Flag] = 'No DMR'
AND AA.[Event Country Iso Cd] != 'CN'
AND AA.[Part Qty Consumed] > 0
AND BB.[Data Period] = @VERSION


UPDATE AA
SET AA.[Credits] = BB.[Orphan Receipt at FG]/
                   (SELECT SUM([Event PN RVS])
				    FROM [staging].[ABC_S3_LINK_SUPPLIER]
					WHERE Region = AA.Region
					AND [Final Disp Flag] = 'Non Return'
                   -- AND [GSD SC DMR Flag] = 'No DMR'
					AND [Event Country Iso Cd] != 'CN'
                    AND [Part Qty Consumed] = 0.0
					AND ISNULL([Event PN RVS],'') != ''
					) * AA.[Event PN RVS],
    AA.[Credit Comments] = 'Good Orphan Receipt Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_Orphan_Receipt_Costing] BB
WHERE AA.[Region] = BB.Region
--AND AA.Region IN ('AME','EMEA')
AND AA.[Final Disp Flag] = 'Non Return'
--AND AA.[GSD SC DMR Flag] = 'No DMR'
AND AA.[Event Country Iso Cd] != 'CN'
AND AA.[Part Qty Consumed] = 0.0
AND ISNULL([Event PN RVS],'') != ''
AND BB.[Data Period] = @VERSION

UPDATE AA
SET AA.[Cons Cost] = ISNULL(AA.[Event PN RVS],0.0) + ISNULL(AA.[Credits],0.0)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] in ('Non Return', 'DMR Drive')

-- update CC8
UPDATE AA
SET AA.[Cons Cost] = CASE WHEN AA.[Final Disp Flag] = 'Scrap in Central' 
						  THEN ISNULL(AA.[Physical PN RVS],0.0)
                          WHEN AA.[Final Disp Flag] = 'Scrap at Supplier' 
						  THEN ISNULL(AA.[Physical PN RVS],0.0) + ISNULL(AA.[Credits],0.0)
					 END
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] IN ('Scrap in Central', 'Scrap at Supplier')

-- CC5
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [DI Cost] float null,
    [DI Credit] float null

-- OC2
UPDATE AA
SET AA.[Credits] = AA.[RWT Credit] * -1.0,
    AA.[Credit Comments] = 'Supplier CID Credit'
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Region] = 'APJ'
AND AA.[RWT CID] = 'Y'
AND AA.[Final Disp Flag] in ('IW Credit', 'OOW Credit')

UPDATE AA
SET AA.[DI Cost] = ISNULL(AA.[Physical PN NBS],0.0) + ISNULL(AA.[Credits],0.0),
    AA.[DI Credit] = ISNULL(AA.[Physical PN RVS],0.0) - ISNULL(AA.[Physical PN NBS],0.0),
	AA.[Cons Cost] = ISNULL(AA.[Physical PN RVS],0.0) + ISNULL(AA.[Credits],0.0)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] in ('IW Credit', 'OOW Credit') 

-- update CC1
UPDATE AA
SET AA.[Cons Cost] = ISNULL(AA.[Event PN RVS],0.0) + ISNULL(AA.[Credits],0.0)
FROM [ABC].[STAGING].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Final Disp Flag] IN ('Logical Receipt', 'Non Return')

-- CC11, expresspoint Cons Cost
--UPDATE AA
--SET AA.[Cons Cost] = CASE WHEN ISNULL(BB.[SOI NB Price],'') != ''
--	                      AND ISNULL(BB.[SOI Core Price],'') != ''
--						  THEN BB.[SOI NB Price] - BB.[SOI Core Price]
--						  ELSE AA.[Physical PN NRS]
--						END,
--	AA.[Cons Cost Notes] = CASE WHEN ISNULL(BB.[SOI NB Price],'') != ''
--								AND ISNULL(BB.[SOI Core Price],'') != ''
--								THEN 'MV SOI Pricing'
--								ELSE 'Physical PN NRS'
--							END			  
--FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
--LEFT JOIN [fact].[ABC_MASTER_AMS_EXPT_MATL_PRICE] BB
--ON AA.[Physical Part Number] = BB.Material
--WHERE AA.[Event Country Iso Cd] in ('CA', 'US')
--AND AA.[L2 Supplier] = 'Expresspoint'
--AND AA.[Part Actual SL] = 'Same Day'
--AND AA.[Event SPL] = 'UY'

--DECLARE @VERSION varchar(6)= '2016Q4'

--UPDATE AA
--SET AA.[Cons Cost] = BB.[SOI NB Price] + ISNULL(AA.[Credits],0.0),
--	AA.[Cons Cost Notes] = 'MV SOI Price'	  
--FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_AMS_EXPT_MATL_PRICE] BB
--WHERE AA.[Part Number] = BB.Material
--AND AA.[Final Disp Flag] in ('Logical Receipt', 'Non Return', 'Scrap at Supplier', 'Scrap in Central') 
--AND AA.[Event Country Iso Cd] in ('CA', 'US')
--AND AA.[L2 Supplier] = 'Expresspoint'
--AND AA.[Part Actual SL] = 'Same Day'
--AND AA.[Event SPL] = 'UY'
--AND BB.[Data Period] = @VERSION

--UPDATE AA
--SET AA.[Cons Cost] = ISNULL(BB.[SOI NB Price],0.0) - ISNULL(BB.[SOI Core Price],0.0),
--	AA.[Cons Cost Notes] = 'MV SOI Price - Core Price'	  
--FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_AMS_EXPT_MATL_PRICE] BB
--WHERE AA.[Received Part Kit Part Number] = BB.Material
--AND AA.[Final Disp Flag] in ('Part-Level Cost', 'OOW Repair', 'IW Credit')
--AND AA.[Event Country Iso Cd] in ('CA', 'US')
--AND AA.[L2 Supplier] = 'Expresspoint'
--AND AA.[Part Actual SL] = 'Same Day'
--AND AA.[Event SPL] = 'UY'
--AND BB.[Data Period] = @VERSION

-- NPPO for Expresspoint
IF OBJECT_ID(N'tempdb..#tmp_AMS_EXPT_NPPO') IS NOT NULL
BEGIN
	TRUNCATE TABLE #tmp_AMS_EXPT_NPPO
	DROP TABLE #tmp_AMS_EXPT_NPPO
END

SELECT [Unit Cost] = (SELECT [NPPO] 
						FROM [fact].[ABC_MASTER_AMS_NPPO_TOP_VENDORS] 
						WHERE [Vendor2] = 'Expresspoint'
						AND [Data Period] = @VERSION
						)
						/1.0/SUM(AA.[Part Qty Consumed])
INTO #tmp_AMS_EXPT_NPPO
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Event Country Iso Cd] in ('CA', 'US')
AND AA.[L1 Supplier] = 'EXPRESSPOINT TECHNOLOGY SERVICES'
AND AA.[Part Actual SL] = 'Same Day'
--AND AA.[Event SPL] = 'UY'
AND AA.[Part Qty Consumed] != 0

UPDATE AA
SET AA.[NPPO Cost] = AA.[Part Qty Consumed] * 1.0 * (SELECT [Unit Cost] FROM #tmp_AMS_EXPT_NPPO)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[Event Country Iso Cd] in ('CA', 'US')
AND AA.[L1 Supplier] = 'EXPRESSPOINT TECHNOLOGY SERVICES'
AND AA.[Part Actual SL] = 'Same Day'
--AND AA.[Event SPL] = 'UY'
AND AA.[Part Qty Consumed] != 0

-- PCG NFF Cons cost
UPDATE AA
SET AA.[DI Cost] = ISNULL(AA.[Physical PN NBS],0.0) + ISNULL(AA.[Credits],0.0),
    AA.[DI Credit] = ISNULL(AA.[Physical PN RVS],0.0) - ISNULL(AA.[Physical PN NBS],0.0),
	AA.[Cons Cost] = ISNULL(AA.[Physical PN RVS],0.0) + ISNULL(AA.[Credits],0.0)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE AA.[L2 Supplier] = 'PCG'
AND AA.[Final Disp Flag] = 'NFF'


-- Consumption at Std
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Cons Cost at Std] float null 

UPDATE AA
SET AA.[Cons Cost at Std] = CASE WHEN AA.[Part Qty Consumed] > 0 
								 AND AA.[Receipt Type] in ('Logical Receipt', 'No Receipt')
								 THEN AA.[Event PN RVS]
								 WHEN AA.[Part Qty Consumed] > 0 
								 AND AA.[Receipt Type] = 'Physical Receipt'
								 THEN AA.[Event PN NRS]
								 ELSE AA.[Cons Cost at Std]
							END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA

-- IC1 Inv Cost, need preprocess the rawdata shared by design team 
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Country Inv Costs] float null,
    [Region Inv Costs] float null,
	[Total Inv Costs] float null

---- For Sub Region
--IF OBJECT_ID(N'tempdb..#tmp_inv_cost_subregion') IS NOT NULL
--BEGIN
--  TRUNCATE TABLE #tmp_inv_cost_subregion
--  DROP TABLE #tmp_inv_cost_subregion
--END

--SELECT AA.SPL,
--       AA.[ISO Country Code],
--	   [Unit Inv Cost] = (SELECT TOP 1 [Total Inv Cost]
--	                      FROM [fact].[vw_ABC_Inv_Cost_By_SubRegion]
--						  WHERE [SPL] = AA.SPL
--						  AND [ISO Country Code] = AA.[ISO Country Code]
--						  AND [Data Period] = @VERSION
--						  )/1.0/
--						 (SELECT SUM([Part Qty Demanded])
--						  FROM [staging].[ABC_S3_LINK_SUPPLIER]
--						  WHERE [Event SPL] = AA.SPL
--						  AND [Event Country Iso Cd] IN (SELECT [ISO Country Code] 
--						                                 FROM [fact].[vw_ABC_Inv_Cost_By_SubRegion] 
--						                                 WHERE [SPL] = AA.[SPL] 
--														 AND [Sub Region Correct] = AA.[Sub Region Correct])
--						  AND ISNULL([Qspeak Flowtag Used],'') != 'N' 
--						  AND [Event Country Iso Cd] != 'CN'
--                          AND ISNULL([Part Qty Demanded],0) > 0 
--						  )
--INTO #tmp_inv_cost_subregion
--FROM [fact].[vw_ABC_Inv_Cost_By_SubRegion] AA

--UPDATE AA
--SET AA.[Country Inv Costs] = BB.[Unit Inv Cost] * 1.0 * AA.[Part Qty Demanded]
--FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_inv_cost_subregion BB
--WHERE AA.[Event SPL] = BB.SPL
--AND AA.[Event Country Iso Cd] = BB.[ISO Country Code]
--AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
--AND AA.[Event Country Iso Cd] != 'CN'
--AND ISNULL(AA.[Part Qty Demanded],0) > 0 

---- For Country	
--IF OBJECT_ID(N'tempdb..#tmp_country_cost') IS NOT NULL
--BEGIN
--  TRUNCATE TABLE #tmp_country_cost
--  DROP TABLE #tmp_country_cost
--END

--SELECT [Event Country Iso Cd],
--       [Event SPL],
--	   [Unit Inv Cost] = (SELECT [Total Inv Cost]
--						  FROM [fact].[vw_ABC_Inv_Cost_By_Country]
--						  WHERE [ISO Country Code] = AA.[Event Country Iso Cd]
--						  AND [SPL] = AA.[Event SPL]
--						  AND [Data Period] = @VERSION
--						  )	   
--	                      /1.0/SUM(AA.[Part Qty Demanded])
--INTO #tmp_country_cost
--FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
--WHERE ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
--AND ISNULL(AA.[Part Qty Demanded],0) > 0 
--AND AA.[Event Country Iso Cd] != 'CN'
--GROUP BY [Event Country Iso Cd], [Event SPL]

--UPDATE AA
--SET AA.[Country Inv Costs] = BB.[Unit Inv Cost] * 1.0 * AA.[Part Qty Demanded]
--FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_country_cost BB
--WHERE AA.[Event Country Iso Cd] = BB.[Event Country Iso Cd]
--AND AA.[Event SPL] = BB.[Event SPL]
--AND AA.[Event Country Iso Cd] != 'CN'
--AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
--AND ISNULL(AA.[Part Qty Demanded],0) > 0
--AND ISNULL(AA.[Country Inv Costs],-1.0) = -1.0

-- For Region
IF OBJECT_ID(N'tempdb..#tmp_region_cost') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_region_cost
  DROP TABLE #tmp_region_cost
END

SELECT [Region],
       [Part SPL],
	   [Unit Inv Cost] = (SELECT [Total Inv Cost]
						  FROM [ABC].[fact].[ABC_MASTER_REGIONAL_INV_COST]
						  WHERE [Region] = AA.[Region]
						  AND [SPL] = AA.[Part SPL]
						  AND [Data Period] = @VERSION
						  )	   
	                      /1.0/SUM(AA.[Part Qty Demanded])
INTO #tmp_region_cost
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
AND ISNULL(AA.[Part Qty Demanded],0) > 0 
AND AA.[Event Country Iso Cd] != 'CN'
AND AA.[Region] in ('APJ', 'EMEA')
GROUP BY [Region], [Part SPL]

UPDATE AA
SET AA.[Region Inv Costs] = BB.[Unit Inv Cost] * 1.0 * AA.[Part Qty Demanded]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_region_cost BB
WHERE AA.[Region] = BB.[Region]
AND AA.[Part SPL] = BB.[Part SPL]
AND AA.Region in ('APJ', 'EMEA')
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
AND AA.[Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Part Qty Demanded],0) > 0 
AND ISNULL(AA.[Region Inv Costs],-1.0) = -1.0


IF OBJECT_ID(N'tempdb..#tmp_total_dmd_by_cd') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_total_dmd_by_cd
  DROP TABLE #tmp_total_dmd_by_cd
END

SELECT 
       AA.[Part SPL],
	   [Region Flag] = BB.[NA vs LA],
       [Total Qty Dmd] = SUM([Part Qty Demanded])	   
INTO #tmp_total_dmd_by_cd
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
LEFT JOIN [ABC].[fact].[ABC_MASTER_AMS_REGION_COUNTRY] BB
ON AA.[Event Country Iso Cd] = BB.[Iso Country Code]
WHERE AA.[Region] = 'AME'
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
AND ISNULL(AA.[Part Qty Demanded],0) > 0 
GROUP BY AA.[Part SPL], BB.[NA vs LA]

--declare @VERSION NVARCHAR(6) = '2016Q4'
IF OBJECT_ID(N'tempdb..#tmp_ame_inv_cost') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_ame_inv_cost
  DROP TABLE #tmp_ame_inv_cost
END

SELECT DISTINCT
       AA.[Event Country Iso Cd],
       AA.[Part SPL],
	   [Unit Inv Cost] = (SELECT [Total Inv Cost]
						    FROM [ABC].[fact].[ABC_MASTER_REGIONAL_INV_COST]
						   WHERE [SPL] = AA.[Part SPL]
						     AND [Region] = BB.[NA vs LA]
							 AND [Data Period] = @VERSION
						 )/1.0/
						 (SELECT [Total Qty Dmd]
						    FROM #tmp_total_dmd_by_cd
						   WHERE [Part SPL] = AA.[Part SPL]
						     AND [Region Flag] = BB.[NA vs LA]
						 )
INTO #tmp_ame_inv_cost
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA 
LEFT JOIN [fact].[ABC_MASTER_AMS_REGION_COUNTRY] BB
ON AA.[Event Country Iso Cd] = BB.[Iso Country Code]
WHERE AA.Region = 'AME' 
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
AND ISNULL(AA.[Part Qty Demanded],0) > 0 

UPDATE AA
SET AA.[Region Inv Costs] = BB.[Unit Inv Cost] * 1.0 * AA.[Part Qty Demanded]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_ame_inv_cost BB
WHERE AA.[Event Country Iso Cd] = BB.[Event Country Iso Cd]
AND AA.[Part SPL] = BB.[Part SPL]
AND AA.Region = 'AME'
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
--AND AA.[Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Part Qty Demanded],0) > 0 
AND ISNULL(AA.[Region Inv Costs],-1.0) = -1.0



UPDATE AA
SET AA.[Total Inv Costs] = ISNULL(AA.[Country Inv Costs],0.0) + ISNULL(AA.[Region Inv Costs],0.0)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA

-- OC6 taxes
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Taxes] money null

IF OBJECT_ID(N'tempdb..#tmp_tax_rate') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_tax_rate
  DROP TABLE #tmp_tax_rate
END

SELECT [Event Country Iso Cd],
       [Tax Rate] = (SELECT [Total Taxes] 
	                 FROM fact.vw_ABC_Taxes_By_Country
					 WHERE [ISO Country Code] = AA.[Event Country Iso Cd]
					 AND [ISO Country Code] != 'CN'
					 AND [Data Period] = @VERSION
	                )/1.0/
					SUM(ISNULL(AA.[Event PN RVS],0.0)*AA.[Part Qty Consumed]
					) 
INTO #tmp_tax_rate
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA
WHERE ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
AND AA.[Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Part Qty Consumed],0) > 0 
GROUP BY [Event Country Iso Cd]

UPDATE AA
SET AA.[Taxes] = AA.[Event PN RVS] * AA.[Part Qty Consumed] * BB.[Tax Rate] * 1.0
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, #tmp_tax_rate BB
WHERE AA.[Event Country Iso Cd] = BB.[Event Country Iso Cd]
AND ISNULL(BB.[Tax Rate],'') != '' 
AND ISNULL(AA.[Qspeak Flowtag Used],'') != 'N' 
AND AA.[Event Country Iso Cd] != 'CN'
AND ISNULL(AA.[Part Qty Consumed],0) > 0 

-- Calc overall results for reporting
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Total E2E Matl Cost] float null,
    [Total E2E Matl - Inv Cost] float null

UPDATE AA
SET AA.[Total E2E Matl Cost] = ISNULL(AA.[Cons Cost],0.0)
							 + ISNULL(AA.[NPPO Cost],0.0)
							 + ISNULL(AA.[Relabel Cost],0.0)
							 + ISNULL(AA.[Dekit Cost],0.0)
							 + ISNULL(AA.[Taxes],0.0)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA

UPDATE AA
SET AA.[Total E2E Matl - Inv Cost] = ISNULL(AA.[Total E2E Matl Cost],0.0)
							       + ISNULL(AA.[Total Inv Costs],0.0)
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA

-- Calc Non-Event Cost
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Non Event Cost] float null

UPDATE AA
SET AA.[Non Event Cost] = (SELECT [Cost Value] 
                        FROM [fact].[ABC_MASTER_NON_EVENT_COST]
						WHERE [Region] = AA.[Region]
						AND [Data Period] = @VERSION
						) / 
                       (SELECT SUM(ISNULL([Part Qty Demanded],0)*[Event PN RVS])
                        FROM [staging].[ABC_S3_LINK_SUPPLIER]
						WHERE [Region] = AA.[Region]
						AND ISNULL([Part Qty Demanded],'') != ''
						) * 1.0 * 
						ISNULL(AA.[Part Qty Demanded],0)*AA.[Event PN RVS]
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA, [fact].[ABC_MASTER_NON_EVENT_COST] BB
WHERE AA.[Region] = BB.[Region]
AND BB.[Data Period] = @VERSION

-- Identify if the PART SPL belongs to EG
ALTER TABLE [staging].[ABC_S3_LINK_SUPPLIER]
ADD [Part SPL Desc] nvarchar(255) null 

	
--update  [staging].[ABC_S3_LINK_SUPPLIER]
--set [Part SPL Desc] = null

UPDATE AA
SET AA.[Part SPL Desc] = CASE WHEN ISNULL(AA.[Part SPL],'') != '' AND ISNULL(BB.[APJ SPL],'') = '' THEN 'PPS'
                              WHEN ISNULL(BB.[APJ SPL],'') != '' THEN 'EG'
						      ELSE AA.[Part SPL Desc]
					     END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA LEFT JOIN [fact].[ABC_MASTER_SPL_by_Region_FY17] BB
ON AA.[Part SPL] = BB.[APJ SPL]
WHERE AA.[Region] = 'APJ' 

UPDATE AA
SET AA.[Part SPL Desc] = CASE WHEN ISNULL(AA.[Part SPL],'') != '' AND ISNULL(BB.[EMEA SPL],'') = '' THEN 'PPS'
                              WHEN ISNULL(BB.[EMEA SPL],'') != '' THEN 'EG'
						      ELSE AA.[Part SPL Desc]
					     END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA LEFT JOIN [fact].[ABC_MASTER_SPL_by_Region_FY17] BB
ON AA.[Part SPL] = BB.[EMEA SPL]
WHERE AA.[Region] = 'EMEA' 

UPDATE AA
SET AA.[Part SPL Desc] = CASE WHEN ISNULL(AA.[Part SPL],'') != '' AND ISNULL(BB.[AME SPL],'') = '' THEN 'PPS'
                              WHEN ISNULL(BB.[AME SPL],'') != '' THEN 'EG'
						      ELSE AA.[Part SPL Desc]
					     END
FROM [staging].[ABC_S3_LINK_SUPPLIER] AA LEFT JOIN [fact].[ABC_MASTER_SPL_by_Region_FY17] BB
ON AA.[Part SPL] = BB.[AME SPL]
WHERE AA.[Region] = 'AME' 



DELETE FROM [fact].[ABC_DATA_CENTER_HISTORY]
WHERE [Data Period] = @VERSION

INSERT INTO [fact].[ABC_DATA_CENTER_HISTORY](
       [Data Period]
      ,[Unique Link Key]
      ,[Master Event Activity Month]
      ,[Source System]
      ,[Region]
      ,[Master Event Number]
      ,[Event Number]
      ,[Event Country Iso Cd]
      ,[Part Qty Demanded]
      ,[Event Hwpl]
      ,[Part Number]
      ,[Part Actual SL]
      ,[Part Shipment Month]
      ,[Part Qty Consumed]
      ,[So + Item]
      ,[GSD SC DMR Flag]
      ,[Event SPL]
      ,[Sc SPL]
      ,[Part SPL]
      ,[Part SPL Desc]
      ,[Part Commodity]
      ,[Part Description]
      ,[Safari Warranty Flag]
      ,[L2 Reverse Unique Link Key]
      ,[Received Month]
      ,[Phy Part No Rec Date]
      ,[Flow Tag]
      ,[Actual Disposition]
      ,[Notification Type A]
      ,[Receipt Type]
      ,[CT Code Label]
      ,[MFG Date]
      ,[Part Age]
      ,[Received Part Kit Part Number]
      ,[Physical Part Number]
      ,[Part Qty Received]
      ,[Received Plant]
      ,[Match FL]
      ,[SN Transaction Status]
      ,[Sales Doc Type]
      ,[Sales Document]
      ,[Qspeak Vendor]
      ,[Qspeak Flowtag]
      ,[Qspeak Flowtag Used]
      ,[Qspeak OEM Name]
      ,[Qspeak Disposition]
      ,[Qspeak Warranty Status]
      ,[Qspeak Upload Date]
      ,[Qspeak Upload Month]
      ,[Qspeak TAT]
      ,[iReturn Received Plant]
      ,[iReturn Disposition]
      ,[iReturn Report Month]
      ,[iReturn Vendor ID]
      ,[iReturn Vendor Name]
      ,[APJ Received Plant]
      ,[APJ Disposition]
      ,[RWT CID]
      ,[RWT Credit]
      ,[RWT Disposition]
      ,[RWT Vendor ID]
      ,[RWT Vendor Name]
      ,[RWT Engineer Approval]
      ,[RWT NFF Results]
      ,[RWT Release Month]
      ,[L1 Supplier]
      ,[L1 Supplier Source]
      ,[L2 Supplier]
      ,[Supplier Type]
      ,[Final Disp Flag]
      ,[Cons Cost]
      ,[DI Cost]
      ,[DI Credit]
      ,[OOW Avg Repair Cost]
      ,[OOW/IW Avg Repair Cost]
      ,[Is Avg RC GT RVS]
      ,[Cons Cost Notes]
      ,[ESC Variance]
      ,[Def Scrap Cost]
      ,[NPPO Cost]
      ,[Event PN RVS]
      ,[Event PN NRS]
      ,[Received Part Kit PN RVS]
      ,[Physical PN RVS]
      ,[Physical PN NRS]
      ,[Physical PN NBS]
      ,[Relabel Cost]
      ,[Dekit Cost]
      ,[Relabel Cost Comment]
      ,[Dekit Cost Comment]
      ,[Credits]
      ,[Credit Comments]
      ,[Cons Cost at Std]
      ,[Country Inv Costs]
      ,[Region Inv Costs]
      ,[Total Inv Costs]
	  ,[Total E2E Matl Cost]
	  ,[Total E2E Matl - Inv Cost]
      ,[Taxes]
	  ,[Non Event Cost]
)
SELECT @VERSION as [Data Period]
      ,[Unique Link Key]
      ,[Master Event Activity Month]
      ,[Source System]
      ,[Region]
      ,[Master Event Number]
      ,[Event Number]
      ,[Event Country Iso Cd]
      ,[Part Qty Demanded]
      ,[Event Hwpl]
      ,[Part Number]
      ,[Part Actual SL]
      ,[Part Shipment Month]
      ,[Part Qty Consumed]
      ,[So + Item]
      ,[GSD SC DMR Flag]
      ,[Event SPL]
      ,[Sc SPL]
      ,[Part SPL]
      ,[Part SPL Desc]
      ,[Part Commodity]
      ,[Part Description]
      ,[Safari Warranty Flag]
      ,[L2 Reverse Unique Link Key]
      ,[Received Month]
      ,[Phy Part No Rec Date]
      ,[Flow Tag]
      ,[Actual Disposition]
      ,[Notification Type A]
      ,[Receipt Type]
      ,[CT Code Label]
      ,[MFG Date]
      ,[Part Age]
      ,[Received Part Kit Part Number]
      ,[Physical Part Number]
      ,[Part Qty Received]
      ,[Received Plant]
      ,[Match FL]
      ,[SN Transaction Status]
      ,[Sales Doc Type]
      ,[Sales Document]
      ,[Qspeak Vendor]
      ,[Qspeak Flowtag]
      ,[Qspeak Flowtag Used]
      ,[Qspeak OEM Name]
      ,[Qspeak Disposition]
      ,[Qspeak Warranty Status]
      ,[Qspeak Upload Date]
      ,[Qspeak Upload Month]
      ,[Qspeak TAT]
      ,[iReturn Received Plant]
      ,[iReturn Disposition]
      ,[iReturn Report Month]
      ,[iReturn Vendor ID]
      ,[iReturn Vendor Name]
      ,[APJ Received Plant]
      ,[APJ Disposition]
      ,[RWT CID]
      ,[RWT Credit]
      ,[RWT Disposition]
      ,[RWT Vendor ID]
      ,[RWT Vendor Name]
      ,[RWT Engineer Approval]
      ,[RWT NFF Results]
      ,[RWT Release Month]
      ,[L1 Supplier]
      ,[L1 Supplier Source]
      ,[L2 Supplier]
      ,[Supplier Type]
      ,[Final Disp Flag]
      ,[Cons Cost]
      ,[DI Cost]
      ,[DI Credit]
      ,[OOW Avg Repair Cost]
      ,[OOW/IW Avg Repair Cost]
      ,[Is Avg RC GT RVS]
      ,[Cons Cost Notes]
      ,[ESC Variance]
      ,[Def Scrap Cost]
      ,[NPPO Cost]
      ,[Event PN RVS]
      ,[Event PN NRS]
      ,[Received Part Kit PN RVS]
      ,[Physical PN RVS]
      ,[Physical PN NRS]
      ,[Physical PN NBS]
      ,[Relabel Cost]
      ,[Dekit Cost]
      ,[Relabel Cost Comment]
      ,[Dekit Cost Comment]
      ,[Credits]
      ,[Credit Comments]
      ,[Cons Cost at Std]
      ,[Country Inv Costs]
      ,[Region Inv Costs]
      ,[Total Inv Costs]
	  ,[Total E2E Matl Cost]
	  ,[Total E2E Matl - Inv Cost]
      ,[Taxes]
	  ,[Non Event Cost]
FROM [staging].[ABC_S3_LINK_SUPPLIER]


