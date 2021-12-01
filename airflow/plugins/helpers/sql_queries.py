
class SQLQueries:
    UK_STAGING_TABLE = '''
            BEGIN; END;
            SET json_serialization_enable TO TRUE;
            CREATE EXTERNAL TABLE s3_schema.uk_source (
                date_time   date,
                area_code   varchar,
                area_name   varchar,
                new_cases   int)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                'separatorChar' = ',',
                'quoteChar' = '\"',
                'escapeChar' = '\\\\')
            STORED AS TEXTFILE
            LOCATION 's3://dev-udacity-capstone-project/raw_data/uk_source/'
            TABLE PROPERTIES ('skip.header.line.count'='1');
         '''
    UK_LOAD_DATA_TO_FACT_TABLE = '''
        SELECT file.date_time as date_id,
                file.area_code as region_id,
                file.new_cases as new_cases 
            FROM s3_schema.uk_source file
        '''
    UK_LOAD_DATA_TO_DIM_TIME_TABLE = '''
        SELECT DISTINCT 
                file.date_time as date_id,
                EXTRACT(day FROM date_id) as day,
                EXTRACT(week FROM date_id) as week,
                EXTRACT(month FROM date_id) as month,
                EXTRACT(year FROM date_id) as year,
                EXTRACT(dayofweek FROM date_id) as weekday
            FROM s3_schema.uk_source file
        '''
    UK_LOAD_DATA_TO_DIM_REGION_TABLE = '''
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

    CANADA_STAGING_TABLE = '''
            BEGIN; END;
            CREATE EXTERNAL TABLE s3_schema.canada_source (
                province     varchar,
                last_updated varchar,
                results      array<struct<"date":VARCHAR, change_cases:int, total_cases:int>>,
                hr_uid       int,
                engname      varchar,
                frename      varchar)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
                'mapping.results' = 'data')
            STORED AS TEXTFILE
            LOCATION 's3://dev-udacity-capstone-project/raw_data/canada_source/';
        '''
    CANADA_LOAD_DATA_TO_FACT_TABLE = '''
        SELECT json_extract_path_text(json_extract_array_element_text(results, 0), 'date')::date as date_id,
                md5(file.province||file.engname) as region_id,
                json_extract_path_text(json_extract_array_element_text(results, 0), 'change_cases')::int as new_cases 
            FROM s3_schema.canada_source file
        '''
    CANADA_LOAD_DATA_TO_DIM_TIME_TABLE = '''
        SELECT DISTINCT 
                json_extract_path_text(json_extract_array_element_text(results, 0), 'date')::date as date_id,
                EXTRACT(day FROM date_id) as day,
                EXTRACT(week FROM date_id) as week,
                EXTRACT(month FROM date_id) as month,
                EXTRACT(year FROM date_id) as year,
                EXTRACT(dayofweek FROM date_id) as weekday
            FROM s3_schema.canada_source file
        '''
    CANADA_LOAD_DATA_TO_DIM_REGION_TABLE = '''
        SELECT DISTINCT 
                md5(file.province) as region_id,
                file.engname as name,
                file.province as super_region,
                'Canada' as country
            FROM s3_schema.canada_source file
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
            LOCATION 's3://dev-udacity-capstone-project/raw_data/usa_source/'
            TABLE PROPERTIES ('skip.header.line.count'='1');
        '''
    USA_LOAD_DATA_TO_FACT_TABLE = '''
        SELECT file."date" as date_id,
                md5(county || state) as region_id,
                file.cases - coalesce(LAG(file.cases) OVER (PARTITION BY region_id order by date_id), 0) as new_cases
            FROM s3_schema.usa_source file
        '''
    USA_LOAD_DATA_TO_DIM_TIME_TABLE = '''
        SELECT DISTINCT file."date" as date_id,
                EXTRACT(day FROM date_id) as day,
                EXTRACT(week FROM date_id) as week,
                EXTRACT(month FROM date_id) as month,
                EXTRACT(year FROM date_id) as year,
                EXTRACT(dayofweek FROM date_id) as weekday
            FROM s3_schema.usa_source file
        '''
    USA_LOAD_DATA_TO_DIM_REGION_TABLE = '''
        SELECT DISTINCT md5(county || state) as region_id,
                file.county as name,
                file.state as super_region,
                'United States' as country
            FROM s3_schema.usa_source file
        '''
