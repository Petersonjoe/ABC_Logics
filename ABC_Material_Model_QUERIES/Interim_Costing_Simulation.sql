-- For interim costing simulation

-- NEXT quarterly data as basis to link previous quarter's data

--IF OBJECT_ID(N'staging.ABC_INTERIM_COSTING_SIMULATION') IS NOT NULL
--BEGIN
--  DROP TABLE staging.ABC_INTERIM_COSTING_SIMULATION;

--  CREATE TABLE staging.ABC_INTERIM_COSTING_SIMULATION(
--	  [Unique Link Key] nvarchar(255) null,
--	  [Region] nvarchar(25) null,
--	  [Event Part Number] nvarchar(255) null,
--	  [Safari Warranty Flag] nvarchar(25) null,
--	  [GSD SC DMR Flag] nvarchar(25) null,
--	  [Total E2E Interim Cost] float null,
--	  [Interim Costing Option] nvarchar(255) null,
--	  [Interim Period] nvarchar(255) null
--  );
--END

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

DELETE FROM staging.ABC_INTERIM_COSTING_SIMULATION
WHERE [Interim Period] = @INTERIM_Period

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
	   [GSD SC DMR Flag],
	   SUM(ISNULL([Part Qty Consumed],0) * [Total E2E Matl Cost])/Count(*) AS [Total E2E Interim Cost]
INTO #tmp_interim_results_opt1_1
FROM [fact].[ABC_DATA_CENTER_HISTORY]
WHERE [Data Period] = @PRE_VERSION
AND [Part Qty Consumed] > 0
GROUP BY [Region],
         [Part Number],
	     [Safari Warranty Flag],
	     [GSD SC DMR Flag]

INSERT INTO staging.ABC_INTERIM_COSTING_SIMULATION(
      [Unique Link Key],
      [Region],
	  [Event Part Number],
	  [Safari Warranty Flag],
	  [GSD SC DMR Flag],
	  [Total E2E Interim Cost],
	  [Interim Costing Option],
	  [Interim Period]
)
SELECT AA.[Unique Link Key],
       AA.[Region],
	   AA.[Part Number],
	   AA.[Safari Warranty Flag],
	   AA.[GSD SC DMR Flag],
       BB.[Total E2E Interim Cost],
	   'Option 1 - PART CONS' as [Interim Costing Option],
	   @INTERIM_Period as [Interim Period]
FROM [fact].[ABC_DATA_CENTER_HISTORY] AA
INNER JOIN #tmp_interim_results_opt1_1 BB
ON AA.Region = BB.Region
AND AA.[Part Number] = BB.[Event Part Number]
AND AA.[Safari Warranty Flag] = BB.[Safari Warranty Flag]
AND AA.[GSD SC DMR Flag] = BB.[GSD SC DMR Flag]
WHERE AA.[Part Qty Consumed] > 0 AND AA.[Data Period] = @VERSION 

IF OBJECT_ID(N'tempdb..#tmp_interim_results_opt1_2') IS NOT NULL
BEGIN
  TRUNCATE TABLE #tmp_interim_results_opt1_2
  DROP TABLE #tmp_interim_results_opt1_2
END

SELECT [Region],
       [Part Number] AS [Event Part Number],
	   [Safari Warranty Flag],
	   [GSD SC DMR Flag],
	   SUM(ISNULL([Part Qty Demanded],0) * [Total E2E Matl Cost])/Count(*) AS [Total E2E Interim Cost]
INTO #tmp_interim_results_opt1_2
FROM [fact].[ABC_DATA_CENTER_HISTORY]
WHERE [Data Period] = @PRE_VERSION
AND [Part Qty Consumed] = 0 AND [Part Qty Demanded] > 0
GROUP BY [Region],
       [Part Number],
	   [Safari Warranty Flag],
	   [GSD SC DMR Flag]

INSERT INTO staging.ABC_INTERIM_COSTING_SIMULATION(
	  [Unique Link Key],
	  [Region],
	  [Event Part Number],
	  [Safari Warranty Flag],
	  [GSD SC DMR Flag],
	  [Total E2E Interim Cost],
	  [Interim Costing Option],
	  [Interim Period]
)
SELECT AA.[Unique Link Key],
       AA.[Region],
	   AA.[Part Number],
	   AA.[Safari Warranty Flag],
	   AA.[GSD SC DMR Flag],
       BB.[Total E2E Interim Cost],
	   'Option 1 - DEMAND BUT NO CONS' as [Interim Costing Option],
	   @INTERIM_Period as [Interim Period]
FROM fact.ABC_DATA_CENTER_HISTORY AA
INNER JOIN #tmp_interim_results_opt1_2 BB
ON AA.Region = BB.Region
AND AA.[Part Number] = BB.[Event Part Number]
AND AA.[Safari Warranty Flag] = BB.[Safari Warranty Flag]
AND AA.[GSD SC DMR Flag] = BB.[GSD SC DMR Flag]
WHERE AA.[Part Qty Consumed] = 0 AND AA.[Part Qty Demanded] > 0
AND AA.[Data Period] = @VERSION

-- Option 2
INSERT INTO staging.ABC_INTERIM_COSTING_SIMULATION(
	  [Unique Link Key],
	  [Region],
	  [Event Part Number],
	  [Safari Warranty Flag],
	  [GSD SC DMR Flag],
	  [Total E2E Interim Cost],
	  [Interim Costing Option],
	  [Interim Period]
)
SELECT AA.[Unique Link Key],
       AA.[Region],
	   AA.[Part Number],
	   AA.[Safari Warranty Flag],
	   AA.[GSD SC DMR Flag],
       AA.[Part Qty Consumed] * AA.[Event PN NRS], -- + [Cost Adder by SPL],
	   'Option 2' as [Interim Costing Option],
	   @INTERIM_Period as [Interim Period]
FROM fact.ABC_DATA_CENTER_HISTORY AA
WHERE AA.[Unique Link Key] NOT IN (
SELECT DISTINCT [Unique Link Key] 
FROM staging.ABC_INTERIM_COSTING_SIMULATION
WHERE [Interim Period] = @INTERIM_Period
) AND AA.[Data Period] = @VERSION

