
class LinkTemplates:

    UK_CASES_SOURCE = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=ltla;date={date}&structure={{"date_time":"date", "area_code": "areaCode", "area_name": "areaName", "new_cases":"newCasesByPublishDate"}}&format=json'
    USA_CASES_SOURCE = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
