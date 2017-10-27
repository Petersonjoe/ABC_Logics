/*** Some Master Data ***/
--[fact].[ABC_MASTER_AMS_L2_SUPPLIER]
--[fact].[ABC_MASTER_AMS_PLANT_SUPPLIER]
--[fact].[ABC_MASTER_APJ_L2_SUPPLIER]
--[fact].[ABC_MASTER_APJ_RFC_SUPPLIER]
--[fact].[ABC_MASTER_COMMODITY_CODES]
--[fact].[ABC_MASTER_EMEA_DISPO_SUPPLIER]
--[fact].[ABC_MASTER_FINAL_DISP_FLAG]
--[fact].[ABC_MASTER_MFG_DATES_FOR_CT_LABEL]
--[fact].[ABC_MASTER_SPL_by_Region_FY17]
--[fact].[ABC_APJ_FINAL_DISP_REMAP]



alter table [fact].[ABC_MASTER_EMEA_NPPO_RRD]
add [Data Period] nvarchar(6) null

update [fact].[ABC_MASTER_EMEA_NPPO_RRD]
set [Data Period] = '2016Q4'
