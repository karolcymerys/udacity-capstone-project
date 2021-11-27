
class SQLQueries:
    UK_LOAD_DATA_TO_FACT_TABLE = '''
        SELECT extract(epoch from file.date_time) as date_id,
                file.area_code as area_id,
                file.new_cases as new_cases 
            FROM s3_schema.source_uk file
        '''
    UK_LOAD_DATA_TO_DIM_TIME_TABLE = '''
        SELECT DISTINCT 
                extract(epoch from file.date_time) as date_id,
                EXTRACT(day FROM file.date_time) as day,
                EXTRACT(week FROM file.date_time) as week,
                EXTRACT(month FROM file.date_time) as month,
                EXTRACT(year FROM file.date_time) as year,
                EXTRACT(dayofweek FROM file.date_time) as weekday
            FROM s3_schema.source_uk file
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
            FROM s3_schema.source_uk file
        '''
