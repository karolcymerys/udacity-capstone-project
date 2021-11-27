
class SQLQueries:
    UK_LOAD_DATA_TO_FACT_TABLE = '''
        SELECT file.date_time as date_id,
                file.area_code as area_id,
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
    UK_LOAD_DATA_TO_DIM_AREA_TABLE = '''
        SELECT DISTINCT 
                file.area_code as area_id,
                file.area_name as name,
                case when SUBSTRING(file.area_code, 1, 1) = 'E' THEN 'England'
                     when SUBSTRING(file.area_code, 1, 1) = 'N' THEN 'Northern Ireland'
                     when SUBSTRING(file.area_code, 1, 1) = 'S' THEN 'Scotland'
                     when SUBSTRING(file.area_code, 1, 1) = 'W' THEN 'Wales'
                end as country
            FROM s3_schema.uk_source file
        '''
    CANADA_LOAD_DATA_TO_FACT_TABLE = '''
        SELECT json_extract_path_text(json_extract_array_element_text(results, 0), 'date')::date as date_id,
                'CANADA_' || file.province as area_id,
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
    CANADA_LOAD_DATA_TO_DIM_AREA_TABLE = '''
        SELECT DISTINCT 
                'CANADA_' || file.province as area_id,
                file.province as name,
                'Canada' as country
            FROM s3_schema.canada_source file
        '''
