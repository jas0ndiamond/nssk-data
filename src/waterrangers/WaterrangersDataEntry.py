from pathlib import Path
import sys

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

# depends on adding src to sys.path
from src.data.DataEntry import DataEntry
from src.exception.DataValidationException import DataValidationException



class WaterrangersDataEntry(DataEntry):

    # row_obj is any structure that can be indexed and is iterable
    # csv, json, raw array
    def __init__(self, entry_obj):

        # raise exception if there's a problem
        super().__init__(entry_obj)

        # for convenience
        self.site = None

        # large availability of measurement possibilities, but relatively few measurement data recorded
        for field in WaterrangersDataEntry.schema:
            if self.get(field) == '':
                self.set(field, None)

    def _validate_data(self, fields):
        # no narrowing of dataset done here
        # just check for data validity

        ##################
        # fields comprising the timestamp
        if fields['observed_on'] == '' or fields['observed_on'] is None:
            raise DataValidationException("found invalid observed_on [%s]" % fields['observed_on'])

        # TODO: type constraints. enforce an alphabet on fields where applicable

    def get_db_destination(self):
        return self.site

    def set_db_destination(self, db_site):
        self.site = db_site

    def get_entry_date(self):
        pass

    def get_entry_time(self):
        pass

    schema_mapping = {
        "observed_on": "ObservedOn",
        "observed_by": "ObservedBy",
        "sample_id": "SampleId",
        "testers": "Testers",
        "hilsenhoff_biotic_index": "HilsenhoffBioticIndex",
        "microfibers": "Microfibers",
        "microbeads": "Microbeads",
        "fragments": "Fragments",
        "film": "Film",
        "ph": "Ph",
        "clarity": "Clarity",
        "nitrites": "Nitrites",
        "conductivity": "Conductivity",
        "alkalinity": "Alkalinity",
        "turbidity": "Turbidity",
        "hardness": "Hardness",
        "total_kjeldahl_nitrogen": "TotalKjeldahlNitrogen",
        "total_phosphorus": "TotalPhosphorus",
        "other_coliform": "OtherColiform",
        "total_coliform": "TotalColiform",
        "chlorophyll_a": "ChlorophyllA",
        "total_phosphorus_bottom": "TotalPhosphorusBottom",
        "turbidity_ntu": "TurbidityNtu",
        "calcium": "Calcium",
        "biochemical_oxygen_demand": "BiochemicalOxygenDemand",
        "total_suspended_solids": "TotalSuspendedSolids",
        "incubation_time": "IncubationTime",
        "incubation_temperature": "IncubationTemperature",
        "oxygen_percent": "OxygenPercent",
        "enterococci": "Enterococci",
        "water_flow": "WaterFlow",
        "chloride": "Chloride",
        "river_flow": "RiverFlow",
        "river_stage": "RiverStage",
        "fluorometer": "Fluorometer",
        "oxygen": "Oxygen",
        "water_depth": "WaterDepth",
        "chlorine": "Chlorine",
        "nitrates": "Nitrates",
        "phosphates_threshold": "PhosphatesThreshold",
        "test_strips": "TestStrips",
        "water_temperature": "WaterTemperature",
        "air_temperature": "AirTemperature",
        "salinity": "Salinity",
        "taxa": "Taxa",
        "civ": "Civ",
        "total_dissolved_solids": "TotalDissolvedSolids",
        "ammonia": "Ammonia",
        "ecoli": "Ecoli"
    }

    schema = [
        "observed_on",
        "observed_by",
        "sample_id",
        "testers",
        "hilsenhoff_biotic_index",
        "microfibers",
        "microbeads",
        "fragments",
        "film",
        "ph",
        "clarity",
        "nitrites",
        "conductivity",
        "alkalinity",
        "turbidity",
        "hardness",
        "total_kjeldahl_nitrogen",
        "total_phosphorus",
        "other_coliform",
        "total_coliform",
        "chlorophyll_a",
        "total_phosphorus_bottom",
        "turbidity_ntu",
        "calcium",
        "biochemical_oxygen_demand",
        "total_suspended_solids",
        "incubation_time",
        "incubation_temperature",
        "oxygen_percent",
        "enterococci",
        "water_flow",
        "chloride",
        "river_flow",
        "river_stage",
        "fluorometer",
        "oxygen",
        "water_depth",
        "chlorine",
        "nitrates",
        "phosphates_threshold",
        "test_strips",
        "water_temperature",
        "air_temperature",
        "salinity",
        "taxa",
        "civ",
        "total_dissolved_solids",
        "ammonia",
        "ecoli"
    ]