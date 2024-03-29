from typing import Dict, List, Literal, Tuple, get_args

# Type annotations
Project = Literal["E3SM Native", "E3SM CMIP6"]
PROJECTS: Tuple[Project, ...] = get_args(Project)


# E3SM Facets that are available in file/dataset id and directory format
# TODO: Get these facets from ESGF directly
AVAILABLE_FACETS: Dict[Project, Dict[str, List[str]]] = {
    "E3SM Native": {
        "realm": ["ocean", "atmos", "land", "sea-ice"],
        "data_type": ["time-series", "climo", "model-output", "mapping", "restart"],
        "time_frequency": [
            "3hr",
            "3hr_snap",
            "5day_snap",
            "6hr",
            "6hr_ave",
            "6hr_snap",
            "day",
            "day_cosp",
            "fixed",
            "mon",
            "monClim",
        ],
    },
    "E3SM CMIP6": {
        "realm": ["ocean", "atmos", "land", "sea-ice"],
        "data_type": ["time-series", "climo", "model-output", "mapping", "restart"],
        "activity": ["C4MIP", "CMIP", "DAMIP", "ScenarioMIP"],
        "variable_id": [
            "abs550aer",
            "albisccp",
            "areacella",
            "areacello",
            "cLitter",
            "cProduct",
            "cSoilFast",
            "cSoilMedium",
            "cSoilSlow",
            "cVeg",
            "cl",
            "clcalipso",
            "clhcalipso",
            "cli",
            "clisccp",
            "clivi",
            "cllcalipso",
            "clmcalipso",
            "clt",
            "cltcalipso",
            "cltisccp",
            "clw",
            "clwvi",
            "evspsbl",
            "evspsblsoi",
            "evspsblveg",
            "fFire",
            "fHarvest",
            "fsitherm",
            "gpp",
            "hfds",
            "hfls",
            "hfsifrazil",
            "hfss",
            "hur",
            "hus",
            "huss",
            "lai",
            "masscello",
            "masso",
            "mlotst",
            "mrfso",
            "mrro",
            "mrros",
            "mrso",
            "mrsos",
            "msftmz",
            "nbp",
            "o3",
            "od550aer",
            "orog",
            "pbo",
            "pctisccp",
            "pfull",
            "phalf",
            "pr",
            "prc",
            "prsn",
            "prveg",
            "prw",
            "ps",
            "psl",
            "pso",
            "ra",
            "rh",
            "rlds",
            "rldscs",
            "rlus",
            "rlut",
            "rlutcs",
            "rsds",
            "rsdscs",
            "rsdt",
            "rsus",
            "rsuscs",
            "rsut",
            "rsutcs",
            "rtmt",
            "sfcWind",
            "sfdsi",
            "sftlf",
            "siconc",
            "simass",
            "sisnmass",
            "sisnthick",
            "sitemptop",
            "sithick",
            "sitimefrac",
            "siu",
            "siv",
            "so",
            "sob",
            "soga",
            "sos",
            "sosga",
            "ta",
            "tas",
            "tauu",
            "tauuo",
            "tauv",
            "tauvo",
            "thetao",
            "thetaoga",
            "thkcello",
            "tob",
            "tos",
            "tosga",
            "tran",
            "ts",
            "tsl",
            "ua",
            "uo",
            "va",
            "vo",
            "volcello",
            "volo",
            "wap",
            "wfo",
            "wo",
            "zg",
            "zhalfo",
            "zos",
        ],
    },
}

UNAVAILABLE_FACETS = {  # Unavailable in templates
    "science_driver": ["Biogeochemical Cycle", "Cryosphere", "Water Cycle"],
    "campaign": ["BGC-v1", "Cryosphere-v1", "DECK-v1", "HighResMIP-v1"],
}
