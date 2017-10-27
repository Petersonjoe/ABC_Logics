IF OBJECT_ID(N'rawdata.BIM_L2_REVERSE_FILTERED') IS NOT NULL
BEGIN
  TRUNCATE TABLE rawdata.BIM_L2_REVERSE_FILTERED
  DROP TABLE rawdata.BIM_L2_REVERSE_FILTERED
END;

SELECT * INTO rawdata.BIM_L2_REVERSE_FILTERED
FROM OPENQUERY([BIM_DB],
'SELECT * FROM (
	SELECT
		   [System]
		  ,[Sales Doc Type]
		  ,[Sales Document]
		  ,[So + Item]
		  ,[Received Month A]
		  ,[Received Month B]
		  ,[Received Month] = CASE WHEN ISNULL([Received Month B],'''') = '''' THEN [Received Month A] ELSE [Received Month B] END
		  ,[Phy Part No Rec Date A]
		  ,[Phy Part No Rec Date B]
		  ,[Event/Case/Sba]
		  ,[Event/Case/Sba Wk]
		  ,[Event/Case/Sba CC]
		  ,[Actgoods Issue Date]
		  ,[Expected Return Date]
		  ,[Cust/Pur Group]
		  ,[Notification Type A]
		  ,[Notification Type B]
		  ,[Sn + Item A]
		  ,[Service Notification A]
		  ,[Service Notification Line Item A]
		  ,[Sn + Item B]
		  ,[RMS Flow Tag Number A]
		  ,[RMS Flow Tag Number B]
		  ,[RMA Scan by Receiver A]
		  ,[RMA Scan by Receiver B]
		  ,[Blind Receipt RMA]
		  ,[ZM FL]
		  ,[Fake ZD FL]
		  ,[Financially Impacted BR]
		  ,[BR of SC99 FL]
		  ,[Fulfilled Plant]
		  ,[Received Plant Id A]
		  ,[Received Plant Id B]
		  ,[Returning Country Iso Cd]
		  ,[Physical Part Number]
		  ,[Physical Part Number A]
		  ,[Physical Part Number B]
		  ,[Received Part Kit Part Number]
		  ,[Received Part Kit Part Number A]
		  ,[Received Part Kit Part Number B]
		  ,[Part Qty Received]
		  ,[Part Return Overide]
		  ,[Return Type]
		  ,[Receiver Comments A]
		  ,[Receiver Comments B]
		  ,[Expected Return Part Cond]
		  ,[Physical Part Return Cond A]
		  ,[Physical Part Return Cond B]
		  ,[Reject Code]
		  ,[Activity Code]
		  ,[Failure Code]
		  ,[Special Processing]
		  ,[Actual Disposition A]
		  ,[Actual Disposition B]
		  ,[Delivery Disposition]
		  ,[Return Reason Code]
		  ,[Scanned Return AWB]
		  ,[Scanned Return Awb CC]
		  ,[Match FL]
		  ,[Unique Link Key]
		  ,[Region]
		  ,[Received Part Serial Number A]
		  ,[Received Part Serial Number B]
		  ,[SN Transaction Status]
	  FROM [BIM].[BIM].[Bma_L2_Reverse]) A
 WHERE A.[Received Month] >= ''201511''
 ');