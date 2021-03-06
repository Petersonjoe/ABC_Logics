/*** update INV COST ***/

/**
Preprocess Instruction:
  1. Change Region LA/NA --> AME
  2. For EMEA, 3 countrie --> France, Germany, Italy specially process
  3. Try to directily Update the rawdata using below coding
  4. Upload mapping list:
     Sub-Region3 --> Sub Region
	 L06 BA Code --> SPL Code
	 Actual A$ --> Total Inv Cost
	 Allocation Driver --> Allocation Driver
	 ABC Field to Populate Cost Into: --> ABC Field Category
**/

--update fact.ABC_MASTER_WW_INV_COST
--set [Data Period] = 'Q117v1'
--where [Data Period] = '2017Q1'

declare @version nvarchar(6) = '2017Q1'
select * from [ABC].[fact].[ABC_MASTER_WW_INV_COST]

update fact.ABC_MASTER_WW_INV_COST
set [Data Period] = @version
where isnull([Data Period],'') = ''

-- check point, count(*)
select [Data Period], count(*)
from [ABC].[fact].[ABC_MASTER_WW_INV_COST]
group by [Data Period]
order by [Data Period]


update fact.ABC_MASTER_WW_INV_COST
set [Region] = case when [Region] = 'APJeC' then 'APJ'
                    when [Region] = 'AMS' then 'AME'
					when [Region] in ('LA','NA') then 'AME'
					else [Region]
				end

declare @version nvarchar(6) = '2017Q1'
update fact.ABC_MASTER_WW_INV_COST
set [Allocation Key] = case when patindex('%COUNTRY%',[Allocation Driver]) > 0 then 'COUNTRY'
                            when patindex('%REGION%',[Allocation Driver]) > 0 then 'REGION'
					        else 'Invalid'
						end
where [Data Period] = @version

update fact.ABC_MASTER_WW_INV_COST
set [Sub Region Correct] = case when [Sub Region] = 'CEE' then 'CEEnI'
                                when [Sub Region] = 'UK & I' then 'UKI'
								when [Sub Region] = 'AEC' then 'Invalid'
								when [Sub Region] = 'United States' then 'USA'
								else [Sub Region]
							end
where [Data Period] = @version


--create view fact.vw_ABC_Inv_Cost_By_Region
--as
--select distinct a.[Region],left(a.[SPL Code],2) as SPL,a.[Total Inv Cost],a.[Data Period]
--from [fact].[ABC_MASTER_WW_INV_COST] a
--where [Allocation Key] = 'REGION'

create view fact.vw_ABC_Inv_Cost_By_SubRegion
as
select distinct a.[Sub Region Correct],left(a.[SPL Code],2) as SPL,b.[ISO Country Code],a.[Total Inv Cost],a.[Data Period]
from [fact].[ABC_MASTER_WW_INV_COST] a, [fact].[BMA_L0_Country_Mapping] b
where a.[Sub Region Correct] = b.[SubRegion]
and a.[Sub Region Correct] in (
select distinct [Sub Region Correct] from [fact].[ABC_MASTER_WW_INV_COST]
where [Allocation Key] = 'Country'
except
select distinct [Country Name] from [fact].[BMA_L0_Country_Mapping]
)
--union 
--select distinct a.[Sub Region Correct],left(a.[SPL Code],2) as SPL,b.[ISO Country Code],a.[Total Inv Cost],a.[Data Period]
--from [fact].[ABC_MASTER_WW_INV_COST] a, [fact].[BMA_L0_Country_Mapping] b
--where a.[Sub Region Correct] = b.[Country Name]
--and a.[Allocation Key] = 'COUNTRY'
--and a.[Sub Region Correct] not in (
--select distinct [Sub Region Correct] from [fact].[ABC_MASTER_WW_INV_COST]
--where [Allocation Key] = 'Country'
--except
--select distinct [Country Name] from [fact].[BMA_L0_Country_Mapping]
--)


--select distinct [SubRegion], [Country Name] from [fact].[BMA_L0_Country_Mapping]
--where [SubRegion] = 'Iberia'


--select patindex('%country%', 'Allocate based on COUNTRY part demand qty')

/*** NEW LOGIC FOR INV COST ***/
--CREATE TABLE [fact].[ABC_MASTER_REGIONAL_INV_COST]
--( 
--  [Data Period] NVARCHAR(6) NULL,
--  [SPL] NVARCHAR(2) NULL,
--  [Region] NVARCHAR(25) NULL,
--  [Total Inv Cost] money null
--)
declare @version nvarchar(6) = '2017Q1'

DELETE FROM [fact].[ABC_MASTER_REGIONAL_INV_COST]
WHERE [Data Period] = @version

INSERT INTO [fact].[ABC_MASTER_REGIONAL_INV_COST](
[Data Period],
[SPL],
[Region],
[Total Inv Cost]
)
SELECT [Fiscal Quarter]
      ,[SPL]
      ,[Region]
	  ,[Total Inv Cost]
  FROM [ABC].[rawdata].[ABC_MASTER_REGIONAL_INV_COST]
  unpivot ([Total Inv Cost] for [Region] in ([APJeC], [EMEA], [NA], [LA])) p

--select * from [fact].[ABC_MASTER_REGIONAL_INV_COST]

UPDATE [fact].[ABC_MASTER_REGIONAL_INV_COST]
SET Region = CASE --WHEN Region IN ('NA', 'LA') THEN 'AME'
                  WHEN Region = 'APJeC' THEN 'APJ'
				  ELSE Region
			 END

SELECT [Data Period],
       [SPL],
	   [Region],
	   [Inv Cost] = SUM(ISNULL([Total Inv Cost], 0)) 
INTO #TMP_INV
FROM [fact].[ABC_MASTER_REGIONAL_INV_COST]
GROUP BY [Data Period],
       [SPL],
	   [Region]

TRUNCATE TABLE [fact].[ABC_MASTER_REGIONAL_INV_COST]

INSERT INTO [fact].[ABC_MASTER_REGIONAL_INV_COST](
[Data Period],
       [SPL],
	   [Region],
	   [Total Inv Cost]
)
SELECT [Data Period],
       [SPL],
	   [Region],
	   [Inv Cost]
FROM #TMP_INV
