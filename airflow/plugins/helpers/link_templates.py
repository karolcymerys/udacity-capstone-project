
class LinkTemplates:
    UK_SOURCE = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=ltla;date=%s&structure={"date_time":"date", "area_code": "areaCode", "area_name": "areaName", "new_cases":"newCasesByPublishDate"}&format=csv'
