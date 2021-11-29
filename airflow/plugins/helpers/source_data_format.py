

class SourceDataDetails:
    UK_DATA_FORMAT = {
        'date_time': 'date',
        'area_code': 'varchar',
        'area_name': 'varchar',
        'new_cases': 'int'
       }
    UK_FORMAT_PROPERTIES = """
        'separatorChar' = ',',
        'quoteChar' = '\"',
        'escapeChar' = '\\\\'
        """
    CANADA_DATA_FORMAT = {
        'province': 'varchar',
        'last_updated': 'varchar',
        'results': 'array<struct<"date":VARCHAR, change_cases:int, total_cases:int>>'
    }
    CANADA_FORMAT_PROPERTIES = """
        'mapping.results' = 'data'
        """

    USA_DATA_FORMAT = {
        '"date"': 'date',
        'county': 'varchar',
        'state': 'varchar',
        'fips': 'varchar',
        'cases': 'int',
        'death': 'int'
    }
    USA_FORMAT_PROPERTIES = """
        'separatorChar' = ',',
        'quoteChar' = '\"',
        'escapeChar' = '\\\\'
        """

    CSV_FORMAT = 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    JSON_FORMAT = 'org.openx.data.jsonserde.JsonSerDe'
