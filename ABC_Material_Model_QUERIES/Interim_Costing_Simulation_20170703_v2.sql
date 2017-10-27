/*** Interim costing simulation instrunction
	20170622 - Add the mapping fields, region, Event Part Number, Safari Warranty Flag, GSD SC DMR Flag
	           to make the logic more clear.
	20170703 - Add the detailed parts of the total cost, Con Cost, NPPO Cost, Relabel Cost, Dekit Cost.
	20170804 - Add an alternative version with keys being shrinked to region, Event Part Number, Safari Warranty Flag
***/
-- NEXT quarterly data as basis to link previous quarter's data

--IF OBJECT_ID(N'staging.ABC_INTERIM_COSTING_SIMULATION_v2') IS NOT NULL
--BEGIN
--  DROP TABLE staging.ABC_INTERIM_COSTING_SIMULATION_v2;
--END
--GO

--CREATE TABLE staging.ABC_INTERIM_COSTING_SIMULATION_v2(
--	[Unique Link Key] nvarchar(255) null,
--	[Region] nvarchar(25) null,
--	[Event Part Number] nvarchar(255) null,
--	[Safari Warranty Flag] nvarchar(25) null,
--	[Interim Cons Cost] money null,
--	[Interim NPPO Cost] money null,
--	[Interim Relabel Cost] money null,
--	[Interim Dekit Cost] money null,
--	[Interim Taxes] money null,
--	[Interim Non Event Cost] money null,
--	[Total E2E Interim Cost] money null,
--	[Total E2E + Non Event Cost] money null,
--	[Interim Costing Option] nvarchar(255) null,
--	[Interim Period] nvarchar(255) null
--);

--/*
--   Application: modify the column name with SQL scripts
--*/

--BEGIN TRANSACTION
--SET QUOTED_IDENTIFIER ON
--SET ARITHABORT ON
--SET NUMERIC_ROUNDABORT OFF
--SET CONCAT_NULL_YIELDS_NULL ON
--SET ANSI_NULLS ON
--SET ANSI_PADDING ON
--SET ANSI_WARNINGS ON
--COMMIT
--BEGIN TRANSACTION
--GO
--EXECUTE sp_rename N'staging.ABC_INTERIM_COSTING_SIMULATION_v2.[Interim Con Cost]', N'Tmp_Interim Cons Cost_1', 'COLUMN' 
--GO
--EXECUTE sp_rename N'staging.ABC_INTERIM_COSTING_SIMULATION_v2.[Tmp_Interim Cons Cost_1]', N'Interim Cons Cost', 'COLUMN' 
--GO
--ALTER TABLE staging.ABC_INTERIM_COSTING_SIMULATION_v2 SET (LOCK_ESCALATION = TABLE)
--GO
--COMMIT

DECLARE @VERSION NVARCHAR(25) = '2017Q1';
DECLARE @PRE_VERSION NVARCHAR(25);
DECLARE @INTERIM_Period NVARCHAR(255);

IF RIGHT(@VERSION,2) = 'Q1'
BEGIN
  SELECT @PRE_VERSION = CONVERT(NVARCHAR,LEFT(@VERSION,4) - 1) + 'Q4';
  SELECT @INTERIM_Period = @PRE_VERSION + '-' + @VERSION;
END
ELSE
BEGIN
  SELECT @PRE_VERSION = LEFT(@VERSION,5) + CONVERT(NVARCHAR,RIGHT(@VERSION,1) - 1);
  SELECT @INTERIM_Period = @PRE_VERSION + '-' + @VERSION;
END

SELECT @PRE_VERSION
SELECT @INTERIM_Period 

DELETE FROM staging.ABC_INTERIM_COSTING_SIMULATION_v2
WHERE [Interim Period] = @INTERIM_Period

-- Main Body - the current quarter's data (or ULK)
IF OBJECT_ID(N'tempdb..#tmp_curr_quarter_data') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_curr_quarter_data
  DROP TABLE #tmp_curr_quarter_data
END

SELECT * 
INTO #tmp_curr_quarter_data
FROM [fact].[ABC_DATA_CENTER_HISTORY]
WHERE [Data Period] = @VERSION

-- Option 1
-- Previous Q's average
IF OBJECT_ID(N'tempdb..#tmp_interim_results_opt1_1') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_interim_results_opt1_1
  DROP TABLE #tmp_interim_results_opt1_1
END

SELECT [Region],
       [Part Number] AS [Event Part Number],
	   [Safari Warranty Flag],
	   SUM(ISNULL([Part Qty Consumed],0.0) * ISNULL([Cons Cost],0.0))/Count(*) AS [Interim Cons Cost],
	   SUM(ISNULL([Part Qty Consumed],0.0) * ISNULL([NPPO Cost],0.0))/Count(*) AS [Interim NPPO Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Relabel Cost],0.0))/Count(*) AS [Interim Relabel Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Dekit Cost],0.0))/Count(*) AS [Interim Dekit Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Taxes],0.0))/Count(*) AS [Interim Taxes],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Total E2E Matl Cost],0.0))/Count(*) AS [Total E2E Interim Cost]
INTO #tmp_interim_results_opt1_1
FROM [fact].[ABC_DATA_CENTER_HISTORY]
WHERE [Data Period] = @PRE_VERSION
AND [Part Qty Consumed] > 0
GROUP BY [Region],
         [Part Number],
	     [Safari Warranty Flag]

INSERT INTO staging.ABC_INTERIM_COSTING_SIMULATION_v2(
      [Unique Link Key],
      [Region],
	  [Event Part Number],
	  [Safari Warranty Flag],
      [Interim Cons Cost],
      [Interim NPPO Cost],
      [Interim Relabel Cost],
      [Interim Dekit Cost],
	  [Interim Taxes],
	  [Total E2E Interim Cost],
	  [Interim Costing Option],
	  [Interim Period]
)
SELECT AA.[Unique Link Key],
       AA.[Region],
	   AA.[Part Number],
	   AA.[Safari Warranty Flag],
	   BB.[Interim Cons Cost],
	   BB.[Interim NPPO Cost],
	   BB.[Interim Relabel Cost],
	   BB.[Interim Dekit Cost],
	   BB.[Interim Taxes],
       BB.[Total E2E Interim Cost],
	   'Option 1 - PART CONS' as [Interim Costing Option],
	   @INTERIM_Period as [Interim Period]
FROM #tmp_curr_quarter_data AA
INNER JOIN #tmp_interim_results_opt1_1 BB
ON AA.Region = BB.Region
AND AA.[Part Number] = BB.[Event Part Number]
AND AA.[Safari Warranty Flag] = BB.[Safari Warranty Flag]
WHERE AA.[Part Qty Consumed] > 0 AND AA.[Data Period] = @VERSION 

IF OBJECT_ID(N'tempdb..#tmp_interim_results_opt1_2') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_interim_results_opt1_2
  DROP TABLE #tmp_interim_results_opt1_2
END

SELECT [Region],
       [Part Number] AS [Event Part Number],
	   [Safari Warranty Flag],
	   SUM(ISNULL([Part Qty Consumed],0.0) * ISNULL([Cons Cost],0.0))/Count(*) AS [Interim Cons Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([NPPO Cost],0.0))/Count(*) AS [Interim NPPO Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Relabel Cost],0.0))/Count(*) AS [Interim Relabel Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Dekit Cost],0.0))/Count(*) AS [Interim Dekit Cost],
	   SUM(ISNULL([Part Qty Consumed],0) * ISNULL([Taxes],0.0))/Count(*) AS [Interim Taxes],
	   SUM(ISNULL([Part Qty Demanded],0) * ISNULL([Total E2E Matl Cost],0.0))/Count(*) AS [Total E2E Interim Cost]
INTO #tmp_interim_results_opt1_2
FROM [fact].[ABC_DATA_CENTER_HISTORY]
WHERE [Data Period] = @PRE_VERSION
AND [Part Qty Consumed] = 0 AND [Part Qty Demanded] > 0
GROUP BY [Region],
       [Part Number],
	   [Safari Warranty Flag]

INSERT INTO staging.ABC_INTERIM_COSTING_SIMULATION_v2(
	  [Unique Link Key],
	  [Region],
	  [Event Part Number],
	  [Safari Warranty Flag],
      [Interim Cons Cost],
      [Interim NPPO Cost],
      [Interim Relabel Cost],
      [Interim Dekit Cost],
	  [Interim Taxes],
	  [Total E2E Interim Cost],
	  [Interim Costing Option],
	  [Interim Period]
)
SELECT AA.[Unique Link Key],
       AA.[Region],
	   AA.[Part Number],
	   AA.[Safari Warranty Flag],
	   BB.[Interim Cons Cost],
	   BB.[Interim NPPO Cost],
	   BB.[Interim Relabel Cost],
	   BB.[Interim Dekit Cost],
	   BB.[Interim Taxes],
       BB.[Total E2E Interim Cost],
	   'Option 1 - DEMAND BUT NO CONS' as [Interim Costing Option],
	   @INTERIM_Period as [Interim Period]
FROM #tmp_curr_quarter_data AA
INNER JOIN #tmp_interim_results_opt1_2 BB
ON AA.Region = BB.Region
AND AA.[Part Number] = BB.[Event Part Number]
AND AA.[Safari Warranty Flag] = BB.[Safari Warranty Flag]
WHERE AA.[Part Qty Consumed] = 0 AND AA.[Part Qty Demanded] > 0
AND AA.[Data Period] = @VERSION

-- Option 2
INSERT INTO staging.ABC_INTERIM_COSTING_SIMULATION_v2(
	  [Unique Link Key],
	  [Region],
	  [Event Part Number],
	  [Safari Warranty Flag],
	  [Total E2E Interim Cost],
	  [Interim Costing Option],
	  [Interim Period]
)
SELECT AA.[Unique Link Key],
       AA.[Region],
	   AA.[Part Number],
	   AA.[Safari Warranty Flag],
       ISNULL(AA.[Part Qty Consumed],0.0) * ISNULL(AA.[Event PN NRS],0.0)
	   + ISNULL(AA.[Part Qty Consumed],0.0) 
	   * CASE WHEN ISNULL(AA.[Event SPL],'') != '' AND ISNULL(BB.[Event SPL],'') != '' 
	          THEN ISNULL(BB.[Non NRS Rate],0.0)
			  WHEN (ISNULL(AA.[Event SPL],'') != '' AND ISNULL(BB.[Event SPL],'') = '')
			    OR (ISNULL(AA.[Event SPL],'') = '')
	          THEN (SELECT DISTINCT [Non NRS Rate] 
			        FROM [staging].[ABC_INTERIM_COSTING_COST_ADDER]
					WHERE [Region] = AA.Region
					AND [Event SPL] = 'All Others'
					AND [Data Period] = @PRE_VERSION)
         END
	   AS [Total E2E Interim Cost], -- + [Cost Adder by SPL],
	   'Option 2' as [Interim Costing Option],
	   @INTERIM_Period as [Interim Period]
FROM #tmp_curr_quarter_data AA
LEFT JOIN [staging].[ABC_INTERIM_COSTING_COST_ADDER] BB
ON AA.[Event SPL] = BB.[Event SPL]
AND AA.[Region]  = BB.[Region]
AND AA.[Data Period] = BB.[Data Period]
WHERE AA.[Data Period] = @VERSION 
AND AA.[Unique Link Key] NOT IN (
	SELECT DISTINCT [Unique Link Key] 
	FROM staging.ABC_INTERIM_COSTING_SIMULATION_v2
	WHERE [Interim Period] = @INTERIM_Period
	)

UPDATE AA
SET AA.[Total E2E + Non Event Cost] = ISNULL(AA.[Total E2E Interim Cost],0.0) + ISNULL(AA.[Interim Non Event Cost],0.0)
FROM staging.ABC_INTERIM_COSTING_SIMULATION_v2 AA
WHERE AA.[Interim Period] = @INTERIM_Period

