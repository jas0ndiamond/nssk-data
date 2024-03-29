from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException


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


class CosmoDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if theres a problem
        super().__init__(entry_obj)

        # value buckets - db requires these as DATEs/TIMEs, but they may be empty
        # convert to None, which will eventually become nullable DATEs and TIMEs
        # ActivityStartDate' and 'ActivityStartTime' are used in timestamping the entry and are checked in _is_valid
        if self.get('ActivityEndDate') == '' or self.get('ActivityEndDate') is None:
            self.set('ActivityEndDate', None)

        if self.get('ActivityEndTime') == '' or self.get('ActivityEndTime') is None:
            self.set('ActivityEndTime', None)

        if self.get('AnalysisStartDate') == '' or self.get('AnalysisStartDate') is None:
            self.set('AnalysisStartDate', None)

        if self.get('AnalysisStartTime') == '' or self.get('AnalysisStartTime') is None:
            self.set('AnalysisStartTime', None)

        # for convenience
        self.monitoringLocationID = self.get("MonitoringLocationID")

        # set fields that will be used in destination and uniqueness checks
        if self.monitoringLocationID is None:
            raise Exception("Failed to determine MonitoringLocationID for row data %s" % self.to_s())

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        ##################
        # fields comprising the timestamp
        if fields['ActivityStartDate'] == '' or fields['ActivityStartDate'] is None:
            raise DataValidationException("found invalid ActivityStartDate [%s]" % fields['ActivityStartDate'])

        if fields['ActivityStartTime'] == '' or fields['ActivityStartTime'] is None:
            raise DataValidationException("Found invalid ActivityStartTime [%s]" % fields['ActivityStartTime'])

        # measurement name/type
        if fields['CharacteristicName'] == '' or fields['CharacteristicName'] is None:
            raise DataValidationException("Found invalid CharacteristicName [%s]" % fields['CharacteristicName'])

        # check that conductance measurements are greater than 0. stored as string
        if ((fields['CharacteristicName'] == 'Conductivity' or fields['CharacteristicName'] == 'Specific conductance')
                and float(fields['ResultValue']) < 0):
            raise DataValidationException("Found invalid Conductivity/Conductance value [%s]" % fields['ResultValue'])

        # water levels outside the range of 10 to 11.5
        # ^^ values may not be accurate

        # TODO: type constraints. enforce an alphabet on fields where applicable
        # MonitoringLocationID
        # CharacteristicName

    def get_db_destination(self):
        return self.monitoringLocationID

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
