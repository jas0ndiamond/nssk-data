import pprint
import re


# DatasetName
# MonitoringLocationID
# MonitoringLocationName
# MonitoringLocationLatitude
# MonitoringLocationLongitude
# MonitoringLocationHorizontalCoordinateReferenceSystem
# MonitoringLocationHorizontalAccuracyMeasure
# MonitoringLocationHorizontalAccuracyUnit
# MonitoringLocationVerticalMeasure
# MonitoringLocationVerticalUnit
# MonitoringLocationType
# ActivityType
# ActivityMediaName
# ActivityStartDate
# ActivityStartTime
# ActivityEndDate
# ActivityEndTime
# ActivityDepthHeightMeasure
# ActivityDepthHeightUnit
# SampleCollectionEquipmentName
# CharacteristicName
# MethodSpeciation
# ResultSampleFraction
# ResultValue
# ResultUnit
# ResultValueType
# ResultDetectionCondition
# ResultDetectionQuantitationLimitMeasure
# ResultDetectionQuantitationLimitUnit
# ResultDetectionQuantitationLimitType
# ResultStatusID
# ResultComment
# ResultAnalyticalMethodID
# ResultAnalyticalMethodContext
# ResultAnalyticalMethodName
# AnalysisStartDate
# AnalysisStartTime
# AnalysisStartTimeZone
# LaboratoryName
# LaboratorySampleID

def _is_valid(fields):
    # no narrowing of dataset done here
    # just check for data validity

    # max length on field data

    for field in fields:
        if False:
            return False

    return True


class DataEntry:

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, row_obj):

        if _is_valid(row_obj) is False:
            raise "Invalid data. Cannot build data entry"

        # TODO: type constraints
            # is indexable
            # is iterable

        # at this point we have a valid entry, but still want to clean it up
        # remove alphanumeric+ chars used in sql syntax [ ] { } | " ' ;

        self.monitoringLocationID = row_obj["MonitoringLocationID"]

        # TODO: make generic like "destinationTable"
        # set fields that will be used in destination and uniqueness checks
        if self.monitoringLocationID is None:
            pass

        # date

        # time

        # CharacteristicName
        # temperature
        # Conductivity
        # Specific conductance

        ##################
        # scrub invalid characters
        # [ #$%[]{},"'| ]

        scrub_pattern = re.compile(r'[\[\]\'\"\$\#\@\!\{\}\,\|]')

        for field in row_obj:
            re.sub(scrub_pattern, '',  row_obj[field])

        # TODO: move to subclass
        ##################
        # value buckets
        if row_obj['ActivityEndDate'] == '':
            row_obj['ActivityEndDate'] = None

        if row_obj['ActivityEndTime'] == '':
            row_obj['ActivityEndTime'] = None

        if row_obj['AnalysisStartDate'] == '':
            row_obj['AnalysisStartDate'] = None

        if row_obj['AnalysisStartTime'] == '':
            row_obj['AnalysisStartTime'] = None

        ##################

        self.row_data = row_obj

    def get(self, field_name):
        return self.row_data[field_name]

    def get_monitoring_location_id(self):
        return self.monitoringLocationID

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass

    def to_s(self):
        pprint.pprint(self.fields)
