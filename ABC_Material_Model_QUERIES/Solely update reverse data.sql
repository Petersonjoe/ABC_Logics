UPDATE AL1 	  
SET   AL1.[L2 Reverse Unique Link Key] = AL4.[Unique Link Key]
     ,AL1.[Received Month] = CASE WHEN ISNULL(AL4.[Received Month B],'') = '' THEN AL4.[Received Month A] ELSE AL4.[Received Month B] END
	 ,AL1.[Phy Part No Rec Date] = CASE WHEN ISNULL(AL4.[Phy Part No Rec Date B],'') = '' THEN AL4.[Phy Part No Rec Date A] ELSE AL4.[Phy Part No Rec Date B] END
	 ,AL1.[Flow Tag] = CASE WHEN ISNULL(AL4.[Rms Flow Tag Number B],'') = '' THEN AL4.[Rms Flow Tag Number A] ELSE AL4.[Rms Flow Tag Number B] END
	 ,AL1.[Actual Disposition] = CASE WHEN ISNULL(AL4.[Actual Disposition B],'') = '' THEN AL4.[Actual Disposition A] ELSE AL4.[Actual Disposition B] END
	 ,AL1.[Notification Type A] = AL4.[Notification Type A]
     ,AL1.[Receipt Type] = CASE WHEN ISNULL(AL4.[Unique Link Key],'') = '' THEN 'No Receipt'
	                         WHEN AL4.[Actual Disposition A] = 'SC99' AND ISNULL(AL4.[Actual Disposition B],'') = '' THEN 'Logical Receipt'
							 ELSE 'Physical Receipt'
						END
	 ,AL1.[CT Code Label] = CASE WHEN ISNULL(AL4.[Received Part Serial Number B],'') = '' THEN AL4.[Received Part Serial Number A] ELSE AL4.[Received Part Serial Number B] END
	 ,AL1.[Received Part Kit Part Number] = AL4.[Received Part Kit Part Number]
	 ,AL1.[Physical Part Number] = AL4.[Physical Part Number]
     ,AL1.[Part Qty Received] = AL4.[Part Qty Received]
	 ,AL1.[Received Plant] = CASE WHEN ISNULL(AL4.[Received Plant Id B],'') = '' THEN AL4.[Received Plant Id A] ELSE AL4.[Received Plant Id B] END
     ,AL1.[Match FL] = AL4.[Match FL]
     ,AL1.[SN Transaction Status] = AL4.[SN Transaction Status]
	 ,AL1.[Sales Doc Type] = AL4.[Sales Doc Type]
	 ,AL1.[Sales Document] = AL4.[Sales Document]
FROM [fact].[ABC_S2_EVENT_PART_REVERSE_ALL] AL1 
LEFT JOIN [rawdata].[BIM_L2_REVERSE_FILTERED] AL4 ON AL1.[Unique Link Key] = AL4.[Unique Link Key]
WHERE AL1.[Data Period] = '2017Q2'
