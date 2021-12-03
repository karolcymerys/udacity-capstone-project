
class SQLQueries:
    UK_STAGING_TABLE = '''
            BEGIN; END;
            CREATE EXTERNAL TABLE s3_schema.uk_source (
                date_time   varchar,
                area_name   varchar,
                area_code   varchar,
                new_cases   int)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
                'strip.outer.array' = 'true')
            STORED AS TEXTFILE
            LOCATION 's3://{ENVIRONMENT_NAME}-udacity-capstone-project/raw_data/uk_source/';
         '''
    UK_DATA_QUALITY_CHECK = {
        'query': '''
            SELECT count(*) 
                FROM s3_schema.uk_source s 
                WHERE s.date_time IS NULL OR 
                    area_code is NULL OR 
                    new_cases IS NULL
                    ''',
        'expected_result': 0
    }
    UK_LOAD_DATA_TO_FACT_TABLE = '''
        INSERT INTO factNewCase (date_id, region_id, new_cases)
            SELECT TO_DATE(file.date_time, 'YYYY-MM-DD') as date_id,
                    file.area_code as region_id,
                    file.new_cases as new_cases 
                FROM s3_schema.uk_source file
        '''
    UK_LOAD_DATA_TO_DIM_TIME_TABLE = '''
        INSERT INTO dimTime (date_id, day, week, month, year, weekday)
            SELECT DISTINCT 
                    TO_DATE(file.date_time, 'YYYY-MM-DD') as date_id,
                    EXTRACT(day FROM date_id) as day,
                    EXTRACT(week FROM date_id) as week,
                    EXTRACT(month FROM date_id) as month,
                    EXTRACT(year FROM date_id) as year,
                    EXTRACT(dayofweek FROM date_id) as weekday
                FROM s3_schema.uk_source file
        '''
    UK_LOAD_DATA_TO_DIM_REGION_TABLE = '''
        INSERT INTO dimRegion (region_id, name, super_region, country)
            SELECT DISTINCT 
                    file.area_code as region_id,
                    file.area_name as name,
                    file.area_name as super_region,
                    case when SUBSTRING(file.area_code, 1, 1) = 'E' THEN 'England'
                         when SUBSTRING(file.area_code, 1, 1) = 'N' THEN 'Northern Ireland'
                         when SUBSTRING(file.area_code, 1, 1) = 'S' THEN 'Scotland'
                         when SUBSTRING(file.area_code, 1, 1) = 'W' THEN 'Wales'
                    end as country
                FROM s3_schema.uk_source file
        '''

    USA_STAGING_TABLE = '''
            BEGIN; END;
            CREATE EXTERNAL TABLE s3_schema.usa_source (
                "date"  date,
                county  varchar,
                state   varchar,
                fips    varchar,
                cases   int,
                death   int)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                'separatorChar' = ',',
                'quoteChar' = '\"',
                'escapeChar' = '\\\\')
            STORED AS TEXTFILE
            LOCATION 's3://{ENVIRONMENT_NAME}-udacity-capstone-project/raw_data/usa_source/'
            TABLE PROPERTIES ('skip.header.line.count'='1');
        '''

    USA_DATA_QUALITY_CHECK = {
        'query': '''
            SELECT count(*) 
                FROM s3_schema.usa_source s 
                WHERE s."date" IS NULL OR 
                    cases IS NULL
                    ''',
        'expected_result': 0
    }
    USA_LOAD_DATA_TO_FACT_TABLE = '''
        INSERT INTO factNewCase (date_id, region_id, new_cases)
            SELECT file."date" as date_id,
                    md5(county || state) as region_id,
                    file.cases - coalesce(LAG(file.cases) OVER (PARTITION BY region_id order by date_id), 0) as new_cases
                FROM s3_schema.usa_source file
        '''
    USA_LOAD_DATA_TO_DIM_TIME_TABLE = '''
        INSERT INTO dimTime (date_id, day, week, month, year, weekday)
            SELECT DISTINCT file."date" as date_id,
                    EXTRACT(day FROM date_id) as day,
                    EXTRACT(week FROM date_id) as week,
                    EXTRACT(month FROM date_id) as month,
                    EXTRACT(year FROM date_id) as year,
                    EXTRACT(dayofweek FROM date_id) as weekday
                FROM s3_schema.usa_source file
        '''
    USA_LOAD_DATA_TO_DIM_REGION_TABLE = '''
        INSERT INTO dimRegion (region_id, name, super_region, country)
            SELECT DISTINCT md5(county || state) as region_id,
                    file.county as name,
                    file.state as super_region,
                    'United States' as country
                FROM s3_schema.usa_source file
        '''

    DIM_TIME_REMOVE_DUPLICATES = '''
        DELETE FROM dimTime 
            WHERE unique_id IN (SELECT unique_id 
                                    FROM (SELECT unique_id, 
                                                 ROW_NUMBER() OVER (PARTITION BY date_id) counter
                                             FROM dimTime) dates 
                                    WHERE dates.counter > 1)
        '''

    DIM_REGION_REMOVE_DUPLICATES = '''
        DELETE FROM dimRegion 
            WHERE unique_id IN (SELECT unique_id 
                                    FROM (SELECT unique_id, 
                                                 ROW_NUMBER() OVER (PARTITION BY region_id) counter
                                             FROM dimRegion) regions 
                                    WHERE regions.counter > 1)
        '''
