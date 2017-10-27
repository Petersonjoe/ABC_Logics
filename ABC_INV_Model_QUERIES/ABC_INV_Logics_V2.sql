-------Q Start--------
SELECT 'Q117' as [Data Period]
      ,[Region]
	  ,[Support_Product_Line]
	  ,[SubRegion]
	  ,case when [region]='APJ' then 'APJeC'
	        when [Region]='EMEA' then 'EMEA'
			when [Country] in ('Canada','United States') then 'NA'
			else 'LA' end as [Region L2]
	  ,[Type]
	  ,[Country]
	  ,[Plant]
	  ,[Location]
	  ,[Plant]+[Location] as [MRP Area]
	  ,substring([Location],1,2) as [Location Type]
	  ,[Condition]
	  ,substring([Location],1,2)+'-'+[Condition] as [Location Type-Condition]
	  ,case when substring([Location],1,2) in ('FG','CU','DS','RF','OR') then 'Event'
	        when [Location]='#' and Condition='Good' then 'Event'
			else 'Non Event' end as [Inv Type]
	  ,sum([TotSSRV_ValCurrent]) as [TotSSRV_ValCurrent]
	  ,sum([TotalResCurrent]) as [E&O Reserve (start)]
  FROM [Inventory_SOR].[dbo].[InventorySummary1016_PassI]
  where [source]<>'DWIA LOTS'
  and [country] <> 'China'
  group by [Region]
	  ,[Support_Product_Line]
	  ,[SubRegion]
	  ,case when [region]='APJ' then 'APJeC'
	        when [Region]='EMEA' then 'EMEA'
			when [Country] in ('Canada','United States') then 'NA'
			else 'LA' end
	  ,[Type]
	  ,[Country]
	  ,[Plant]
	  ,[Location]
	  ,[Plant]+[Location]
	  ,[Condition]
	  ,case when substring([Location],1,2) in ('FG','CU','DS','RF','OR') then 'Event'
	        when [Location]='#' and Condition='Good' then 'Event'
			else 'Non Event' end

--------Q Start by SPL and Region Group---------

SELECT [Support_Product_Line]
      ,case when [region]='APJ' then 'APJeC'
	        when [Region]='EMEA' then 'EMEA'
			when [Country] in ('Canada','United States') then 'NA'
			else 'LA' end as [Region L2]
      ,sum([TotSSRV_ValCurrent]) as [TotSSRV_ValCurrent]
FROM [Inventory_SOR].[dbo].[InventorySummary1016_PassI]
  where [source]<>'DWIA LOTS'
  and [country] <> 'China'
group by [Support_Product_Line]
        ,case when [region]='APJ' then 'APJeC'
	        when [Region]='EMEA' then 'EMEA'
			when [Country] in ('Canada','United States') then 'NA'
			else 'LA' end


--------Q End--------
SELECT [Plant]+[Location] as [MRP Area],[Support_Product_Line],[condition]
,sum([TotalResCurrent]) as [E&O Reserve (end)]
from [Inventory_SOR].[dbo].[InventorySummary0117_PassI]
where [source]<>'DWIA LOTS'
  and [country] <> 'China'
group by [Plant]+[Location],[Support_Product_Line],[condition]


--------Update Value--------
update A
set A.[E&O Reserve (end)]=B.[E&O Reserve (end)]
from  [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] A
left outer join [ABC_Inventory].[dbo].[Jan'17 Reserve Value] B
on A.[MRP Area]=B.[Inv Plant-Location]
and A.[Support_Product_Line]=B.[Support_Product_Line]
and A.[Condition]=B.[Condition]

update A
set A.[Special Alloc Key]=B.[Special Alloc Key]
from  [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] A
left outer join [ABC_Inventory].[dbo].[Special Key Mapping Table] B
on A.[MRP Area]=B.[Inv Plant-Location]


update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [Allocation Key]=[Subregion]+'-'+[Type]
where [Special Alloc Key] is null

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [Allocation Key]=[Special Alloc Key]
where [Special Alloc Key] is not null

update A
set A.[TotSSRV_ValCurrent by Allocation Key]=t.[TotSSRV_ValCurrent]
from [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] A
left outer join [ABC_Inventory].[dbo].[Oct'16 Total Value] t
on A.[Support_Product_Line]=t.[Support_Product_Line]
and A.[Region L2]=t.[Region L2]
;

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [Std Rev Alloc %]=null

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [Std Rev Alloc %]=[TotSSRV_ValCurrent]/[TotSSRV_ValCurrent by Allocation Key]
where [TotSSRV_ValCurrent by Allocation Key]<>0

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [Std Rev Alloc %]=0
where [TotSSRV_ValCurrent by Allocation Key]=0

update A
set A.[Cube Std Rev]=B.[2017Q1 Adj Cube]
from [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] A
left outer join [ABC_Inventory].[dbo].[Cube Inv Cost] B
on A.[Region L2]=B.[Region L2]
and A.[Support_Product_Line]=[SPL]
where B.[type]='Std Rev Costs'


update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] 
set [Std Rev $]=[Std Rev Alloc %]*[Cube Std Rev]


update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [E&O Reserve (end)]=0
where [E&O Reserve (end)] is null

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] 
set [E&O True Up]=[E&O Reserve (start)]-[E&O Reserve (end)]
;
with t as (select [Support_Product_Line],[Region L2],sum([E&O True Up]) as [E&O True Up by Allocation Key] from [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
group by [Support_Product_Line],[Region L2])

update A
set A.[E&O True Up by Allocation Key]=t.[E&O True Up by Allocation Key]
from [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] A
left outer join t 
on A.[Support_Product_Line]=t.[Support_Product_Line]
and A.[Region L2]=t.[Region L2]

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [E&O Alloc %]=null

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [E&O Alloc %]=[E&O True Up]/[E&O True Up by Allocation Key]
where [E&O True Up by Allocation Key] <>0

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [E&O Alloc %]=0
where [E&O True Up by Allocation Key] =0

update A
set A.[Cube E&O Cost]=B.[2017Q1 Adj Cube]
from [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117] A
left outer join [ABC_Inventory].[dbo].[Cube Inv Cost] B
on A.[Region L2]=B.[Region L2]
and A.[Support_Product_Line]=B.[SPL]
where B.[Type]='E&O Costs'

update [ABC_Inventory].[dbo].[Inventory ABC_Model Overview_Q117]
set [E&O True Up $]=[E&O Alloc %]*[Cube E&O Cost]



