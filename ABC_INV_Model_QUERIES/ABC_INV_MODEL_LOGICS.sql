/*** 
    ABC Inventory Model Logics
	Author: Lei, Ji
	Create Date: 2017-10-26
	Modify Date: 2017-10-16
	Copyright: HPE Pointnext GSD-SC-GC-BE-BI All rights reserved.
***/

use ABC_Inventory;

--select * from sys.all_objects
--select * from sys.all_columns

-- linked in demand qty from shipment
if exists(
	select obj.name, 
		   obj.create_date, 
		   obj.modify_date, 
		   col.name, 
		   col.max_length,
		   col.precision
	from sys.all_objects obj
	left join sys.all_columns col on obj.object_id = col.object_id
	where obj.type = 'U' and obj.name = 'Inventory ABC_Model Overview_Q117'
	and col.name = 'Part Qty Demanded'
	)
	select 1
else
    alter table [Inventory ABC_Model Overview_Q117]
	add [Part Qty Demanded] int null
go

update aa
set aa.[Part Qty Demanded] = null
from [Inventory ABC_Model Overview_Q117] aa

update aa
set aa.[Part Qty Demanded] = bb.[Part Qty Demanded]
from [Inventory ABC_Model Overview_Q117] aa
left join [ABC_INV_Parts_Demand_fact] bb 
on aa.[Data Period] = bb.[Data Period]
and aa.[Support_Product_Line] = bb.[Part SPL]
and aa.[MRP Area] = bb.[MRP Area]

