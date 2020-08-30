
-- Surface assay records -- in [DRILLHOLES].[dbo].[SurfaceSampleAttr]

-- db counts for surface sample geochem

-- total surface samples --8,567,605
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[SurfaceSample] s

-- surface samples that do not have attribute data. Why not? -- 18,520
SELECT	count (Id) 
FROM	[DRILLHOLES].[dbo].[SurfaceSample] 
WHERE	Id not in 
		( SELECT	distinct SurfaceSampleId FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] )

-- total surface sample attribute records	--229,442,733
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] 

-- surface samples for which there are attribute records -- 8,549,085 (diff = 18,520 OK)
SELECT	count (distinct SurfaceSampleId)
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] 


-- attribute records with detail: left join count shows that all sa records have detail	--229,442,733
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	

-- attribute records with sample info --229,442,733	OK all have sample data
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId

-- attribute records with detail and sample info --229,442,733 OK
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId

-- 'geochem assay' attribute records	-- 150,139,250
SELECT	count(*) 
		--count (distinct sa.SurfaceSampleId)	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId
WHERE	len(sa.AttributeColumn) < 30
		and TRY_CAST(sa.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	

-- geochem assays attribs in match table --   150,033,005  
SELECT	count(*) 
		--count (distinct sa.SurfaceSampleId)	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId
		INNER JOIN [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] am on 
					am.AttributeColumn = sa.AttributeColumn 
WHERE	len(sa.AttributeColumn) < 30
		and TRY_CAST(sa.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	

-- geochem assays with mathched anlayte --   147,110,607  (98% of 'geochem' records)
SELECT	count(*) 
		--count (distinct sa.SurfaceSampleId)	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId
		INNER JOIN [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] am on 
					am.AttributeColumn = sa.AttributeColumn 
WHERE	len(sa.AttributeColumn) < 30
		and TRY_CAST(sa.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	
		AND am.Match <> 'Unmatched'



-- samples not in above	--
SELECT	*
FROM	[DRILLHOLES].[dbo].[SurfaceSample] s
where	s.id not in (
			SELECT	distinct sa.SurfaceSampleId	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
	FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
			INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
			INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId
	WHERE	len(sa.AttributeColumn) < 30
			and TRY_CAST(sa.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
			and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	
			) ------------

-- check 
SELECT	*
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId
WHERE	len(sa.AttributeColumn) < 30
		and TRY_CAST(sa.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	
		and s.Id = 392376
--------------------------------------------------------------------------------------------


-- assays with detail and sample info with matching analyte records --190,200,560
SELECT	count(*) 
		 --top 100 *  
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[SurfaceSample] s ON s.Id = sa.SurfaceSampleId	-- 229,442,733
		INNER JOIN [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] am on		-- 177,629,220
					upper(am.AttributeColumn) = upper(sa.AttributeColumn )
		------------
WHERE	len(sa.AttributeColumn) < 30													-- 150,033,005
		and TRY_CAST(sa.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	




-- attributes source attrib table that are not in match list (should be same as stop strint list) -- 11,127 !! ??
SELECT	-- count(distinct sa.AttributeColumn) 
		DISTINCT(AttributeColumn)
FROM	[DRILLHOLES].[dbo].[SurfaceSampleAttr] sa
WHERE	AttributeColumn NOT IN (
			SELECT AttributeColumn FROM [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] )
ORDER BY AttributeColumn
		


