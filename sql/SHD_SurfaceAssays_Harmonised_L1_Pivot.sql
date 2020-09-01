/****** Script for SelectTopNRows command from SSMS  ******/
DECLARE		@PivotColumns AS NVARCHAR(MAX) ,
			@PivotQuery  AS NVARCHAR(MAX)

IF OBJECT_ID('tempdb..#tempAssays') IS NOT NULL
  DROP TABLE #tempAssays

SELECT	top 10000
		[SampleId] ,[Anumber] ,[CompanySampleId] ,[Longitude] ,[Latitude] ,[SampleType]	,Analyte ,[Value_StdUnits]  ,[Symbol_Unit]
into	#tempAssays
FROM	[DRILLHOLES].[shd].[HarmonisedSurfaceAssays]
where	[SampleType] = 'soil'
		and [Value_StdUnits] > 0
group by [SampleId],[Anumber],[CompanySampleId] ,[Longitude] ,[Latitude],[SampleType],Analyte  ,[Value_StdUnits] ,[Symbol_Unit]

SELECT   @PivotColumns = COALESCE(@PivotColumns + ',','') + QUOTENAME([Symbol_Unit]) 
FROM	(SELECT DISTINCT [Symbol_Unit] FROM #tempAssays ) AS Analytes  order by [Symbol_Unit]
print @PivotColumns;

set @PivotQuery  = '
	select * from 
	(
		SELECT	top 10000
				[SampleId] ,[Anumber] ,[CompanySampleId] ,[Longitude] ,[Latitude] ,[SampleType]	,[Value_StdUnits]  ,[Symbol_Unit]
		FROM	[DRILLHOLES].[shd].[HarmonisedSurfaceAssays]
		where	[SampleType] = ''soil''
				and [Value_StdUnits] > 0
		group by [SampleId],[Anumber],[CompanySampleId] ,[Longitude] ,[Latitude],[SampleType],[Value_StdUnits] ,[Symbol_Unit]
	) x
	pivot
	(
		max([Value_StdUnits]) for [Symbol_Unit] in ( ' + @PivotColumns + ')
	) p
	order by [SampleId]';


	execute (@PivotQuery)
