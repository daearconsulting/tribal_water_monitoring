"""
sovereignty.py data governance acknowledgment for tribal_water_monitoring.

Companion to the data sovereignty module in tribal_fire_science and
tribal_ag_sd. Prints governance framework acknowledgment at the start
of each notebook and generates citations for outputs.

Frameworks implemented:
    OCAP®         : Ownership, Control, Access, Possession
    CARE          : Collective Benefit, Authority to Control, Responsibility, Ethics
    FAIR          : Findable, Accessible, Interoperable, Reusable
    IEEE 2890-2025 : Recommended Practice for Provenance of Indigenous Peoples' Data

Reference: https://standards.ieee.org/ieee/2890/10318/
"""

from __future__ import annotations
from src.constants import GOVERNANCE_REFS

# Governance framework preamble
_PREAMBLE = """
TRIBAL WATER MONITORING DATA GOVERNANCE ACKNOWLEDGMENT
This analysis uses data that describes the lands, waters, and communities
of the Oceti Sakowin (the Seven Council Fires of the Lakota, Dakota, and
Nakota peoples). This data is governed by the following frameworks:

OCAP®  : The Oceti Sakowin and all Tribal Nations have the right to
  Ownership, Control, Access, and Possession of data about their lands
  and communities. Federal monitoring data covering Tribal territories
  does not transfer that authority to federal agencies or researchers.
  Reference: https://fnigc.ca/ocap-training/

CARE   : Data use must deliver Collective Benefit to Indigenous peoples,
  respect their Authority to Control, uphold Responsibility to communities,
  and center Ethics across the full data lifecycle.
  Reference: https://www.gida-global.org/care

FAIR   : Data is Findable, Accessible, Interoperable, and Reusable.
  FAIR governs technical standards; CARE and OCAP® govern the ethical
  obligations to Tribal Nations that FAIR alone does not address.
  Reference: https://www.go-fair.org/fair-principles/

IEEE 2890-2025 : Recommended Practice for Provenance of Indigenous Peoples' Data.
  The first international standard for Indigenous data provenance. Establishes
  common parameters for describing and recording how data about Indigenous
  Peoples should be disclosed, connected to people and place, and governed
  across its lifecycle, including AI/ML and biodiversity contexts.
  Reference: https://standards.ieee.org/ieee/2890/10318/

CRITICAL DISTINCTION IN THIS SERIES:

  PUBLIC DATA   : federal/public datasets (USGS NWIS, NOAA, Census, NHD)
                  used in analysis notebooks. Freely available but still
                  describing Tribal lands. Results should be shared with
                  the relevant Tribal Nation before publication.

  TRIBAL DATA   : Tribal-collected operational data (well levels, water
                  quality samples) stored in data/raw/. Governed by OCAP®.
                  Never committed to version control. Stays under Tribal
                  control.
"""

# Data source registry 
_DATA_SOURCES = {
    "census_aiannh": {
        "name":       "US Census Bureau TIGER/Line AIANNH Boundaries",
        "url":        "https://www.census.gov/cgi-bin/geo/shapefiles/index.php",
        "steward":    "US Census Bureau",
        "license":    "Public domain",
        "note":       (
            "Census-defined boundaries are for statistical purposes only. "
            "They do not represent legal jurisdiction or Tribal self-definition."
        ),
    },
    "usgs_nwis_streamflow": {
        "name":    "USGS National Water Information System Streamflow",
        "url":     "https://waterdata.usgs.gov/nwis",
        "steward": "US Geological Survey",
        "license": "Public domain (USGS)",
        "note":    (
            "USGS streamflow monitoring is sparse on many Tribal lands. "
            "Monitoring gaps are documented as a policy finding, not a data artifact."
        ),
    },
    "usgs_nwis_groundwater": {
        "name":    "USGS National Water Information System Groundwater Levels",
        "url":     "https://waterdata.usgs.gov/nwis",
        "steward": "US Geological Survey",
        "license": "Public domain (USGS)",
        "note":    (
            "USGS groundwater monitoring well coverage is systematically sparse "
            "on Tribal lands. This is a federal infrastructure equity gap, not "
            "evidence of less groundwater. Tribal-collected well data fills this gap."
        ),
    },
    "usgs_nwis_water_quality": {
        "name":    "USGS/EPA Water Quality Portal",
        "url":     "https://www.waterqualitydata.us/",
        "steward": "USGS / EPA",
        "license": "Public domain",
        "note":    (
            "Public water quality monitoring coverage on Tribal lands is sparse. "
            "Tribal water quality sampling programs provide the ground-level data "
            "that federal monitoring misses."
        ),
    },
    "nhd_flowlines": {
        "name":    "USGS National Hydrography Dataset (NHD) Plus HR",
        "url":     "https://www.usgs.gov/national-hydrography/nhdplus-high-resolution",
        "steward": "US Geological Survey",
        "license": "Public domain (USGS)",
        "note":    None,
    },
    "wbd_huc": {
        "name":    "USGS Watershed Boundary Dataset (WBD)",
        "url":     "https://www.usgs.gov/national-hydrography/watershed-boundary-dataset",
        "steward": "US Geological Survey",
        "license": "Public domain (USGS)",
        "note":    (
            "HUC boundaries are hydrologically defined and do not align with "
            "Tribal territories. Overlay with Tribal boundaries is essential "
            "for Tribal water analysis."
        ),
    },
    "noaa_pdsi": {
        "name":    "NOAA Climate Division Palmer Drought Severity Index (PDSI)",
        "url":     "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/",
        "steward": "NOAA National Centers for Environmental Information",
        "license": "Public domain (NOAA)",
        "note":    None,
    },
    "maca_climate": {
        "name":    "MACAv2-METDATA Downscaled Climate Projections",
        "url":     "https://www.climatologylab.org/maca.html",
        "steward": "Northwest Knowledge Network, University of Idaho",
        "license": "Creative Commons CC0",
        "citation": (
            "Abatzoglou, J.T. and Brown, T.J. (2012). A comparison of statistical "
            "downscaling methods suited for wildfire applications. "
            "Int. J. Climatology. doi:10.1002/joc.2312"
        ),
    },
    "tribal_groundwater": {
        "name":    "Tribal-collected groundwater level data",
        "url":     None,
        "steward": "Tribal Nation Water Resources Program",
        "license": "Tribal governance principles apply",
        "note":    (
            "This data is owned and controlled by the Tribal Nation that "
            "collected it. It is stored locally in data/raw/ and is never "
            "committed to version control. Analysis results should be shared "
            "with the Nation before any external distribution."
        ),
    },
    "tribal_water_quality": {
        "name":    "Tribal-collected water quality data",
        "url":     None,
        "steward": "Tribal Nation Water Resources Program",
        "license": "Tribal governance principles apply",
        "note":    (
            "This data is owned and controlled by the Tribal Nation that "
            "collected it. Governed by OCAP®. See docs/data_sovereignty.md."
        ),
    },
}


def print_data_acknowledgment(source_keys: list[str] | None = None) -> None:
    """
    Print the governance preamble and data source acknowledgments.
    Call at the top of each notebook after imports.

    Parameters
    source_keys : List of keys from the _DATA_SOURCES registry.
                  If None, prints only the preamble.
    """
    print(_PREAMBLE)

    if source_keys:
        print("DATA SOURCES FOR THIS NOTEBOOK")
        for key in source_keys:
            src = _DATA_SOURCES.get(key)
            if not src:
                print(f"  [Unknown source key: {key}]")
                continue
            print(f"\n  {src['name']}")
            if src.get("url"):
                print(f"  URL     : {src['url']}")
            print(f"  Steward : {src['steward']}")
            print(f"  License : {src['license']}")
            if src.get("note"):
                note_words = src["note"].split()
                line = "  Note    : "
                for word in note_words:
                    if len(line) + len(word) + 1 > 72:
                        print(line)
                        line = "            " + word
                    else:
                        line = f"{line} {word}".lstrip()
                        if line == word:
                            line = "  Note    : " + word
                if line.strip():
                    print(line)


def generate_citations(source_keys: list[str]) -> str:
    """
    Generate a plain-text citation block for notebook outputs.
    Parameters
    source_keys : List of data source keys to cite

    Returns
    Formatted citation string
    """
    lines = ["DATA CITATIONS", "="]
    for key in source_keys:
        src = _DATA_SOURCES.get(key)
        if not src:
            continue
        lines.append(f"\n{src['name']}")
        if src.get("url"):
            lines.append(f"  {src['url']}")
        if src.get("citation"):
            lines.append(f"  {src['citation']}")
        lines.append(f"  Steward: {src['steward']} | License: {src['license']}")
    lines.append(
        "\nData governance: OCAP® | CARE | FAIR | IEEE 2890-2025"
    )
    for name, url in GOVERNANCE_REFS.items():
        lines.append(f"  {name.upper()}: {url}")
    return "\n".join(lines)
