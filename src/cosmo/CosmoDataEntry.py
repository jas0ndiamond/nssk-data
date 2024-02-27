from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry

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

        self.monitoringLocationID = self.get("MonitoringLocationID")

        # set fields that will be used in destination and uniqueness checks
        if self.monitoringLocationID is None:
            raise Exception("Failed to determine MonitoringLocationID for row data %s" % self.to_s())

        ##################
        # value buckets - db requires these for entry uniqueness
        if self.get('ActivityEndDate') == '':
            self.set('ActivityEndDate', None)

        if self.get('ActivityEndTime') == '':
            self.set('ActivityEndTime', None)

        if self.get('AnalysisStartDate') == '':
            self.set('AnalysisStartDate', None)

        if self.get('AnalysisStartTime') == '':
            self.set('AnalysisStartTime', None)

    def _is_valid(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        # max length on field data

        # for field in fields:
        #     if False:
        #         return False

        return True

    def get_db_destination(self):
        return self.monitoringLocationID

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass
