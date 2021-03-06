/***
	RUN SSIS PKG - ABC_Load_S1_Event_Reverse.dtsx - Container: ABC Comparison SAP VS BIM,
	TO REFRESH THE MOST RECENT CONSUMPTION FROM BIM DATABASE;
	MANUALLY IMPORT THE RAW DATA FROM Aaron INTO rawdata.ABC_Cons_Fr_SAP;
	MODIFY THE VIEW OF vw_ABC_Cons_Fr_SAP;
	USED FOR COMPARING CONSUMPTION IN COUNTRY/SUBREGION - SPL LEVEL;
	APJ - COUNTRY LEVEL;
	AME/EMEA - SUBREGION LEVEL;
!!! CHANGE THE VIEW CODING TO ADD THE NEWEST MONTH;
***/
--======================================================================================
--SELECT * INTO [rawdata].[ABC_Cons_Fr_SAP_170505_bk]
--FROM [rawdata].[ABC_Cons_Fr_SAP]

--ALTER TABLE [rawdata].[ABC_Cons_Fr_SAP] ADD [201709] FLOAT NULL
--TRUNCATE TABLE [rawdata].[ABC_Cons_Fr_SAP]

-- UPDATE
IF OBJECT_ID(N'tempdb..#BIM') IS NOT NULL
  DROP TABLE #BIM

SELECT 
       [Region]
      ,[Country Cd]
      ,[Month]
      ,[Qtr] as [Fiscal Qtr]
      ,[Event SPL in BIM]
	  ,CONVERT(VARCHAR(255),NULL) AS [SPL in SAP]
      ,SUM(ISNULL([Part Qty Consumed],0)) AS [Cons Qty in BIM]
	  ,CONVERT(BIGINT,NULL) AS [Cons Qty in SAP]
  INTO #BIM
  FROM [ABC].[staging].[tmp_ABC_CONS_FR_BIM]
-- WHERE [Region] in ('AME','APJ')
 GROUP BY [Region]
      ,[Country Cd]
      ,[Month]
      ,[Qtr]
      ,[Event SPL in BIM]


select * from #BIM where Region = 'AME' and Month = '201706' and [Country Cd] = 'BR'

--select AA.[Region],
--	   AA.[Month], 
--       AA.[Country Cd], 
--       AA.[Event SPL in BIM], 
--	   AA.[Cons Qty in BIM],
--       CC.[Region],CC.[Month],CC.[Country Cd],CC.SPL,CC.SPL, CC.[Cons Qty in SAP],AA.[SPL in SAP]
--  FROM #BIM AA
--  LEFT JOIN [fact].[vw_ABC_Cons_Fr_SAP] CC  -- use left join to reserve all data from BIM
--    ON AA.[Region] = isnull(CC.[Region],'')
--   AND AA.[Month] = isnull(CC.[Month],'')
--   AND AA.[Country Cd] = isnull(CC.[Country Cd], '')
--   AND AA.[Event SPL in BIM] = isnull(CC.SPL,'')
-- WHERE AA.[Region] = 'APJ' and aa.[Month] = '201705' and aa.[Event SPL in BIM] = 'VR'

--select * from fact.ABC_CONS_SAP_vs_BIM_Latest aa
--where AA.[Region] = 'APJ' and aa.[Month] = '201705' and aa.[Event SPL in BIM] = 'VR'

-- APJ, in country level
UPDATE AA
   SET AA.[SPL in SAP] = CC.SPL,
       AA.[Cons Qty in SAP] = CC.[Cons Qty in SAP]
  FROM #BIM AA
  LEFT JOIN [fact].[vw_ABC_Cons_Fr_SAP] CC  -- use left join to reserve all data from BIM
    ON AA.[Region] = CC.[Region]
   AND AA.[Month] = CC.[Month]
   AND AA.[Country Cd] = CC.[Country Cd]
   AND AA.[Event SPL in BIM] = CC.SPL
 WHERE AA.[Region] = 'APJ'

IF OBJECT_ID(N'fact.ABC_CONS_SAP_vs_BIM_Latest') IS NOT NULL
  DROP TABLE fact.ABC_CONS_SAP_vs_BIM_Latest

SELECT AA.[Region]
      ,AA.[Country Cd] as [Country/SubRegion]
      ,AA.[Month]
      ,AA.[Fiscal Qtr]
      ,AA.[Event SPL in BIM]
	  ,AA.[SPL in SAP]
      ,AA.[Cons Qty in BIM]
	  ,AA.[Cons Qty in SAP] 
 INTO fact.ABC_CONS_SAP_vs_BIM_Latest 
 FROM #BIM AA 
WHERE AA.REGION = 'APJ'

--select AA.[Country/SubRegion], BB.[BIM Country]
--  FROM fact.ABC_CONS_SAP_vs_BIM_Latest AA
--  LEFT JOIN (SELECT DISTINCT [BIM Country],[Country Cd]
--               FROM [fact].[vw_ABC_Cons_Fr_SAP]
--		      WHERE REGION = 'APJ') BB
--    ON AA.[Country/SubRegion] = BB.[Country Cd]
-- where AA.[Region] = 'APJ' and aa.[Month] = '201705' and aa.[Event SPL in BIM] = 'VR'

UPDATE AA
   SET AA.[Country/SubRegion] = BB.[BIM Country]
  FROM fact.ABC_CONS_SAP_vs_BIM_Latest AA
  LEFT JOIN (SELECT DISTINCT [BIM Country],[Country Cd]
               FROM [fact].[vw_ABC_Cons_Fr_SAP]
		      WHERE REGION = 'APJ') BB
    ON AA.[Country/SubRegion] = BB.[Country Cd]




--select * from #BIM
--where region = 'APJ' and [Country Cd] = 'JP'
--order by [Event SPL in BIM], [Month]

-- AME, in subregion level
-- preprocess AME data using BIM subregion 
IF OBJECT_ID(N'tempdb..#AME') IS NOT NULL
  DROP TABLE #AME

SELECT 
       A.[Region]
      ,[Country Cd]
      ,[Month]
      ,[Qtr]
	  ,B.[SubRegion]
      ,[Event SPL in BIM]
      ,[Part Qty Consumed]
  INTO #AME
  FROM [staging].[tmp_ABC_CONS_FR_BIM] A
  LEFT JOIN [fact].[BMA_L0_Country_Mapping] B 
    ON A.[REGION] = B.[REGION] AND A.[Country Cd] = B.[ISO Country Code]
  --LEFT JOIN [rawdata].[ABC_EMEA_Country_SubRegion] C
  --  ON B.[Country Name] = C.[Country]
 WHERE A.[Region] = 'AME'

IF OBJECT_ID(N'tempdb..#AME_CALC') IS NOT NULL
  DROP TABLE #AME_CALC

SELECT 
       [Region]
	  ,[SubRegion]
      ,[Month]
      ,[Qtr]
      ,[Event SPL in BIM]
	  ,CONVERT(VARCHAR(255),NULL) AS [SPL in SAP]
      ,SUM(ISNULL([Part Qty Consumed],0)) AS [Cons Qty in BIM]
	  ,CONVERT(BIGINT,NULL) AS [Cons Qty in SAP]
  INTO #AME_CALC
  FROM #AME
 GROUP BY [Region]
      ,[Month]
      ,[Qtr]
	  ,[SubRegion]
      ,[Event SPL in BIM]

UPDATE AA
   SET AA.[SPL in SAP] = CC.SPL,
       AA.[Cons Qty in SAP] = CC.[Cons Qty in SAP]
  FROM #AME_CALC AA
  --LEFT JOIN [fact].[BMA_L0_Country_Mapping] BB
  --  ON AA.[Region] = BB.[Region] 
  -- AND AA.[Country Cd] = BB.[ISO Country Code]
  LEFT JOIN [fact].[vw_ABC_Cons_Fr_SAP] CC
    ON AA.[Event SPL in BIM] = CC.SPL 
   AND AA.[Month] = CC.[Month] 
   AND AA.[SubRegion] = CC.[BIM Subregion]
   AND AA.[Region] = CC.[Region]
 WHERE AA.[Region] = 'AME'

INSERT INTO fact.ABC_CONS_SAP_vs_BIM_Latest SELECT * FROM #AME_CALC WHERE REGION = 'AME'

-- EMEA in subregion level
-- non 'JN','K3','62'
IF OBJECT_ID(N'tempdb..#EMEA') IS NOT NULL
  DROP TABLE #EMEA

SELECT 
       A.[Region]
      ,[Country Cd]
      ,[Month]
      ,[Qtr]
	  ,B.[SubRegion]
      ,[Event SPL in BIM]
      ,[Part Qty Consumed]
  INTO #EMEA
  FROM [staging].[tmp_ABC_CONS_FR_BIM] A
  LEFT JOIN [fact].[BMA_L0_Country_Mapping] B 
    ON A.[REGION] = B.[REGION] 
   AND A.[Country Cd] = B.[ISO Country Code]
  --LEFT JOIN [rawdata].[ABC_EMEA_Country_SubRegion] C
  --  ON B.[Country Name] = C.[Country]
 WHERE A.[Region] = 'EMEA' 
   --AND A.[Event SPL in BIM] NOT IN ('JN','K3','62')

IF OBJECT_ID(N'tempdb..#EMEA_CALC') IS NOT NULL
  DROP TABLE #EMEA_CALC

SELECT 
       [Region]
	  ,[SubRegion]
      ,[Month]
      ,[Qtr]
      ,[Event SPL in BIM]
	  ,CONVERT(VARCHAR(255),NULL) AS [SPL in SAP]
      ,SUM(ISNULL([Part Qty Consumed],0)) AS [Cons Qty in BIM]
	  ,CONVERT(BIGINT,NULL) AS [Cons Qty in SAP]
  INTO #EMEA_CALC
  FROM #EMEA
 GROUP BY [Region]
      ,[Month]
      ,[Qtr]
	  ,[SubRegion]
      ,[Event SPL in BIM]

UPDATE AA
   SET AA.[SPL in SAP] = CC.SPL,
       AA.[Cons Qty in SAP] = CC.[Cons Qty in SAP]
  FROM #EMEA_CALC AA
  --LEFT JOIN [fact].[BMA_L0_Country_Mapping] BB
  --  ON AA.[Region] = BB.[Region] 
  -- AND AA.[Country Cd] = BB.[ISO Country Code]
  LEFT JOIN [fact].[vw_ABC_Cons_Fr_SAP] CC
    ON AA.[Event SPL in BIM] = CC.SPL 
   AND AA.[Month] = CC.[Month] 
   AND AA.[SubRegion] = CC.[BIM Subregion]
   AND AA.[Region] = CC.[Region]
 WHERE AA.[Region] = 'EMEA'

INSERT INTO fact.ABC_CONS_SAP_vs_BIM_Latest SELECT * FROM #EMEA_CALC WHERE REGION = 'EMEA' 

--AND [Event SPL in BIM] NOT in ('JN','K3','62')

-- for 'JN','K3','62'
--IF OBJECT_ID(N'tempdb..#EMEA_PLUS') IS NOT NULL
--  DROP TABLE #EMEA_PLUS

--SELECT 
--       A.[Region]
--      ,[Country Cd]
--      ,[Month]
--      ,[Qtr]
--	  ,B.[SubRegion]
--      ,'JN/K3/62' AS [Event SPL in BIM]
--      ,[Part Qty Consumed]
--  INTO #EMEA_PLUS
--  FROM [staging].[tmp_ABC_CONS_FR_BIM] A
--  LEFT JOIN [fact].[BMA_L0_Country_Mapping] B 
--    ON A.[REGION] = B.[REGION] 
--   AND A.[Country Cd] = B.[ISO Country Code]
--  --LEFT JOIN [rawdata].[ABC_EMEA_Country_SubRegion] C
--  --  ON B.[Country Name] = C.[Country]
-- WHERE A.[Region] = 'EMEA' 
--   AND A.[Event SPL in BIM] IN ('JN','K3','62')

--IF OBJECT_ID(N'tempdb..#EMEA_PLUS_CALC') IS NOT NULL
--  DROP TABLE #EMEA_PLUS_CALC

--SELECT 
--       [Region]
--	  ,[SubRegion]
--      ,[Month]
--      ,[Qtr]
--      ,[Event SPL in BIM]
--	  ,CONVERT(VARCHAR(255),NULL) AS [SPL in SAP]
--      ,SUM(ISNULL([Part Qty Consumed],0)) AS [Cons Qty in BIM]
--	  ,CONVERT(BIGINT,NULL) AS [Cons Qty in SAP]
--  INTO #EMEA_PLUS_CALC
--  FROM #EMEA_PLUS
-- GROUP BY [Region]
--      ,[Month]
--      ,[Qtr]
--	  ,[SubRegion]
--      ,[Event SPL in BIM]

--UPDATE AA
--   SET AA.[SPL in SAP] = CC.SPL,
--       AA.[Cons Qty in SAP] = CC.[Cons Qty in SAP]
--  FROM #EMEA_PLUS_CALC AA
--  --LEFT JOIN [fact].[BMA_L0_Country_Mapping] BB
--  --  ON AA.[Region] = BB.[Region] 
--  -- AND AA.[Country Cd] = BB.[ISO Country Code]
--  LEFT JOIN [fact].[vw_ABC_Cons_Fr_SAP] CC
--    ON AA.[Event SPL in BIM] = CC.SPL 
--   AND AA.[Month] = CC.[Month] 
--   AND AA.[SubRegion] = CC.[BIM Subregion]
--   AND AA.[Region] = CC.[Region]
-- WHERE AA.[Region] = 'EMEA' 

--INSERT INTO fact.ABC_CONS_SAP_vs_BIM_Latest SELECT * FROM #EMEA_PLUS_CALC 
--WHERE REGION = 'EMEA' 

SELECT * FROM fact.ABC_CONS_SAP_vs_BIM_Latest WHERE MONTH = '201710'







