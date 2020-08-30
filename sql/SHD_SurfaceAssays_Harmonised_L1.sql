/* SHD_SurfaceAssays_Harmonised_L1.sql
================================================================================
 Author:		David Howard
 Create date:	29 Aug 2020
 Description:	Extract Surface Sample assays with L1 Harmonisation (standard analyte name and unit )
 
 Process:		To run in OpenFile public extract of MDHDB
				Surface assay data read into temp table from which harmonised data are computed 
				and written to new flat file db table HarmonisedSurfaceAssays
				Index all relevant tables/columns before running
 Output:		Flat file list of surface assays, one row per assay with full sample and assay metadata
				for saving as csv for later import to postgres.
=================================================================================*/

USE DRILLHOLES

DECLARE	@minId bigint = 0
		, @counter int = 1
		, @numBatches int = 1	-- USE SMALL NUMBER WHEN TESTING -- PROCESSSES ca. 1M rows per minute
		, @batchSize int = 100		-- USE SMALL NUMBER WHEN TESTING
		, @rowsProcessed int = 0
		, @maxId bigint				-- max assay id in SurfaceSampleAttr table

SET @minId = (SELECT MAX(AssayId) FROM [shd].[SurfaceAssaysHarmonisedL1])
if @minId is null set @minId = 0

set @maxId = (select max(id) from [DRILLHOLES].[dbo].[SurfaceSampleAttr])

--print 'minId = ' +  cast(@minId as varchar) 
--print 'maxId = ' +  cast(@maxId as varchar) 
--print 'batch no. = ' + cast(@counter as varchar) + ' of ' + cast ( @numBatches as varchar) + ' batches of size = '  + cast ( @batchSize as varchar) 
--print 'total rows processed = ' +  cast(@rowsProcessed as varchar) 

--COMMENT OUT while and end WHEN TESTING 
--WHILE @counter <= @numBatches and @minId < @maxId
--BEGIN

-------------  MESSAGES WHILE RUNNING --------------------
print 'minId = ' +  cast(@minId as varchar) 
print 'batch no. = ' + cast(@counter as varchar) + ' of ' + cast ( @numBatches as varchar) + ' batches of size = '  + cast ( @batchSize as varchar) 
print 'total rows processed = ' +  cast(@rowsProcessed as varchar) 


IF OBJECT_ID('tempdb..#tempAssays') IS NOT NULL
  DROP TABLE #tempAssays

SELECT 
	--top (@batchSize)  -- COMMENT OUT WHEN TESTING
	top 100 
	sa.Id as AssayId
	-- SAMPLES =============
	,s.Id 'SampleId'
	,replace([CompanySampleId],'"','') as CompanySampleId
	,s.[Anumber]
	,cb.Name as Company
	, round(s.[Longitude],6) 'Longitude', round(s.[Latitude],6) 'Latitude'
	,s.SurfaceSampleType as SampleType

	-- COMPANY ASSAY DATA ===============
	,sa.AttributeColumn
	,sa.AttributeValue
	, D.Units 'AttributeUnits'
	, D.Accuracy, D.LabMethod, D.[LowerLimit], D.[UpperLimit], D.Laboratory

	-- STANDARDISED ANALYTES =============
	,am.Match
	,am.Analyte

	-- MARK NITON ASSAYS =============
	,CASE 
		WHEN sa.AttributeColumn LIKE '%niton%' OR D.LabMethod LIKE '%niton%'
		THEN 'NITON' ELSE NULL 
		END AS 'Method'

	-- NUMERIC SOURCE VALUES =============
	, CASE 
		when LEFT(sa.[AttributeValue], 1)='<' then  -- 'less than' values
			case
				when TRY_CAST(stuff(sa.[AttributeValue] ,1,1,'-') as real) is null 
				then -9999  
				else TRY_CAST(stuff(sa.[AttributeValue] ,1,1,'-') as real) 
			end
												
		when LEFT(sa.[AttributeValue], 1)='>' then  -- 'greater than' values
			case 
				when TRY_CAST(stuff(sa.[AttributeValue] ,1,1,'') as real) is null 
				then -99999  
				else TRY_CAST(stuff(sa.[AttributeValue] ,1,1,'') as real) 
			end

		when TRY_CAST(sa.[AttributeValue] as real) is null then NULL
		ELSE CAST(sa.[AttributeValue] as real) 
		END  as 'ImportValue'

	, CASE 
		when LEFT(sa.[AttributeValue], 1)='<' then  '<'
		when LEFT(sa.[AttributeValue], 1)='>' then  '>'
		END  as Qualifier
	, D.GeoChemistryUOM 'ImportUnits'

	-- STANDARDISED UNITS  =============
	,am.StdUnits
	--, conv.xBy
------------------------------
INTO #tempAssays

FROM	[DRILLHOLES].[dbo].[SurfaceSample] s  -- count = 8,567,605 samples
		inner JOIN [DRILLHOLES].[dbo].[SurfaceSampleAttr] sa ON  sa.SurfaceSampleId = s.Id	-- count =  229,442,733 samples with attrib records
		inner join [DRILLHOLES].[dbo].[MRTDetail] D   on sa.MRTDetailId = D.Id				-- count =  229,442,733 samples with attrib records and detail OK
		inner join [DRILLHOLES].[dbo].[CLBODY] cb on cb.CompanyId = s.CompanyId
		inner join [DRILLHOLES].[shd].[20200829_0600__unique_attribs_match] am on 
					am.AttributeColumn = sa.AttributeColumn -- count =  191,075,815  with attribs in match table

	-- MATCHED 'GEOCHEMISTRY' ATTRIBUTES ONLY
where	len(sa.AttributeColumn) < 30
		and TRY_CAST(sa.AttributeColumn as float) is null  -- eliminate records that have 'numeric' names
		and d.GeoChemistryUOM in ('ppm', 'ppb', 'ppt', 'pct',  'GPT') -- concentration units	
		and am.Match <> 'Unmatched'

		--AND sa.Id = 4161937
		AND sa.Id > @minId

order by sa.Id 


----	INSERT INTO HARMONISED TABLE -- COMMENT OUT WHEN TESTING
insert into shd.SurfaceAssaysHarmonisedL1 (
	[AssayId]
    ,[SampleId]
    ,[CompanySampleId]
    ,[Anumber]
    ,[Company]
    ,[Longitude]
    ,[Latitude]
    ,[SampleType]
    ,[AttributeColumn]
    ,[AttributeValue]
    ,[AttributeUnits]
    ,[Accuracy]
    ,[LabMethod]
    ,[LowerLimit]
    ,[UpperLimit]
    ,[Laboratory]
    ,[Match]
    ,[Analyte]
    ,[Method]
    ,[ImportValue]
    ,[Qualifier]
    ,[ImportUnits]
    ,[StdUnits]
    ,[Value_StdUnits]
    ,[Symbol_Unit]
)----------------------------COMMENT OUT TO HER WHEN TESTING
 
SELECT	
		--min(t.sampleid), max(t.sampleid)
		t.* 
		, (ImportValue * conv.xBy) as Value_StdUnits
		, concat (Analyte, '_', StdUnits) AS Symbol_Unit
		--,count(AssayId) as rownum
--INTO	shd.SurfaceAssaysHarmonisedL1						-- first run only to set up table structure, THEN USE INSERT INTO
FROM	#tempAssays t
		left join [DRILLHOLES].[shd].[LU_Geochem_units_conversion_20200731] conv on
					conv.ImportUnits = t.ImportUnits  and conv.StdUnit= t.StdUnits

--where t.AssayId = 		148913657

group by 		-- removes duplicates coming from join (I think this might be in the attrib match table where the same attrib name occurs more than once. Fix this)
	[AssayId]
    ,[SampleId]
    ,[CompanySampleId]
    ,[Anumber]
    ,[Company]
    ,[Longitude]
    ,[Latitude]
    ,[SampleType]
    ,[AttributeColumn]
    ,[AttributeValue]
    ,[AttributeUnits]
    ,[Accuracy]
    ,[LabMethod]
    ,[LowerLimit]
    ,[UpperLimit]
    ,[Laboratory]
    ,[Match]
    ,[Analyte]
    ,[Method]
    ,[ImportValue]
    ,[Qualifier]
    ,t.[ImportUnits]
    ,[StdUnits]
    --,[Value_StdUnits]
    --,[Symbol_Unit]
	  ,conv.xBy

set @rowsProcessed = @rowsProcessed + @@ROWCOUNT
SET @minId = (SELECT MAX(AssayId) FROM [shd].[SurfaceAssaysHarmonisedL1])  -- latest max sampel id
set @counter = @counter + 1

-- COMMENT OUT WHEN TESTING
--END -- WHILE


-- -----------------CHECKING ---------------------------------------

--delete from [DRILLHOLES].[shd].[SurfaceAssaysHarmonisedL1]
--select count(*) as HarmonisedAssays from [shd].[SurfaceAssaysHarmonisedL1] 

--SELECT count(*) 
--  FROM [DRILLHOLES].[shd].[SurfaceAssaysHarmonisedL1]

--  select top 1000 * from [DRILLHOLES].[shd].[SurfaceAssaysHarmonisedL1]

--  -- unique samples 
  select --distinct(sampleid) -- 784545
	SampleId, [CompanySampleId], Longitude, latitude, [Anumber], Company, SampleType
  from [DRILLHOLES].[shd].[SurfaceAssaysHarmonisedL1]
  group by sampleid,[CompanySampleId], longitude, latitude, [Anumber],Company, SampleType