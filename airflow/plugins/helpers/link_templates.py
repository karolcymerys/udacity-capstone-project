
class LinkTemplates:

    UK_CASES_SOURCE = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=ltla;date={date}&structure={{"date_time":"date", "area_code": "areaCode", "area_name": "areaName", "new_cases":"newCasesByPublishDate"}}&format=csv'

    CANADA_CASES_SOURCE = 'https://api.covid19tracker.ca/reports/province/{province}?date={date}&stat=cases'
    CANADA_CASES_SOURCE_URL_PARAMS = {
        'province': ['ON', 'QC', 'NS', 'NB', 'MB', 'BC', 'PE', 'SK', 'AB', 'NL', 'NT', 'YT', 'NU']
    }

    USA_CASES_SOURCE = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
