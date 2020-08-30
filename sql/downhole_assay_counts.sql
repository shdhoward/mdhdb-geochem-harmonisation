
-- Downhole assay records -- in [DRILLHOLES].[dbo].[DHGeochemistryAttr]

-- db counts for surface sample geochem

-- total drillhole sites --2,319,783
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[Collar]

-- total downhole samples --52,480,940
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[DHGeochemistry] g

-- downhole samples that do not have attribute data. Why not? -- 254,391
SELECT	count (Id) 
FROM	[DRILLHOLES].[dbo].[DHGeochemistry] 
WHERE	Id not in 
		( SELECT distinct DHGeochemistryId FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] )

-- total downhole sample attribute records	-- 575,106,383
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] 

-- downhole samples for which there are attribute records -- 52,226,549 (diff = 254,391 OK)
SELECT	count (distinct DHGeochemistryId)
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] 


-- attribute records with detail:  join count shows that all ga records have detail	-- 575,106,383
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] ga
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on ga.MRTDetailId = D.Id	

-- attribute records with sample info --  575,106,383	OK all have sample data
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] ga
		INNER JOIN [DRILLHOLES].[dbo].[DHGeochemistry] g ON g.Id = ga.DHGeochemistryId

-- attribute records with detail and sample info -- 575,106,383 OK
SELECT	count(*) 
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] ga
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on ga.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[DHGeochemistry] g ON g.Id = ga.DHGeochemistryId

-- 'geochem assay' attribute records	-- 358,348,797
SELECT	count(*) 
		--count (distinct sa.SurfaceSampleId)	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] ga
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on ga.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[DHGeochemistry] g ON g.Id = ga.DHGeochemistryId
WHERE	len(ga.AttributeColumn) < 30
		and TRY_CAST(ga.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	


-- geochem assays attribs in match table --   353,252,173 
SELECT	count(*) 
		--count (distinct sa.SurfaceSampleId)	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] ga
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on ga.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[DHGeochemistry] g ON g.Id = ga.DHGeochemistryId
		INNER JOIN [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] am on 
					am.AttributeColumn = ga.AttributeColumn 
WHERE	len(ga.AttributeColumn) < 30
		and TRY_CAST(ga.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	


-- geochem assays with mathched anlayte --   331,073,963  (94% of 'geochem' records)  -- 10 mins!!
SELECT	count(*) 
		--count (distinct sa.SurfaceSampleId)	-- number of sample sites with geochem assays	-- 7,612,048 ca. 1M less than samples with atrrib data !!!!
FROM	[DRILLHOLES].[dbo].[DHGeochemistryAttr] ga
		INNER JOIN [DRILLHOLES].[dbo].[MRTDetail] D   on ga.MRTDetailId = D.Id	
		INNER JOIN [DRILLHOLES].[dbo].[DHGeochemistry] g ON g.Id = ga.DHGeochemistryId
		INNER JOIN [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] am on 
					am.AttributeColumn = ga.AttributeColumn 
WHERE	len(ga.AttributeColumn) < 30
		and TRY_CAST(ga.AttributeColumn as float) is  null				-- eliminate records that have 'numeric' names 
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT')	-- concentration units	
		AND am.Match <> 'Unmatched'

--==========================================================
-- TODO -- These from surface... to be changed to DH counts

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
		


