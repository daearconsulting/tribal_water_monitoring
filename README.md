# Tribal Water Monitoring
**Author:** Lilly Jones, PhD, Daear Consulting, LLC 
**Primary Demonstration Sites:** Pine Ridge (Oglala Lakota) and Rosebud (Sicangu Lakota), Oceti Sakowin  
**License:**  AGPL-3.0 license

## Overview
Water is the foundation of life and sovereignty for Indigenous communities.
This repository provides a modular, reproducible water monitoring system
designed specifically for Tribal Nations, built to be deployed locally,
adapted to any Nation's data and priorities, and grounded in Indigenous
data sovereignty frameworks.

The system has two parallel tracks:

**Analysis Track** (notebooks) provide reproducible analysis of publicly available
water data for education, research, and baseline characterization. Demonstrated
at Pine Ridge and Rosebud Reservations.

**Operations Track** (pipeline and dashboard) a deployable decision support
tool that combines publicly available data with Tribal-collected field
observations (well levels, water quality samples) to support day-to-day
water management decisions.

## Design Principles
**Decision support, not research output.** The system is built to help someone
make a real decision, which well to check first, whether to issue a water
advisory, when drought conditions warrant restricting use, not to produce
academic findings.

**Simple, explainable, auditable.** Every indicator can be traced back to a
specific value, a specific threshold, and a specific line of code. No black
boxes. When someone asks "why is this well flagged critical?", you can
point to the exact number and the exact rule.

**Offline-first.** Public data is cached locally on first download. The
pipeline and dashboard run without internet connectivity after that.
Designed for Tribal offices with variable bandwidth.

**Config-driven thresholds.** All alert levels live in `config/config.yaml`.
Sit with Tribal water staff, ask what counts as an emergency for their wells,
and change one number so the entire system updates.

**Data sovereignty first.** Tribal-collected data (well levels, water quality)
stays local and is never committed to version control. Public federal data
is clearly distinguished from Tribal operational data throughout.

## Repository Structure

```
tribal_water_monitoring/
├── notebooks/
│   ├── 01_watershed_territorial_context.ipynb
│   ├── 02_groundwater_monitoring.ipynb
│   ├── 03_surface_water_reliability.ipynb
│   ├── 04_water_quality_context.ipynb
│   ├── 05_drought_water_stress.ipynb
│   ├── 06_compound_water_stress_index.ipynb
│   └── 07_climate_projections_water.ipynb
├── pipeline/
│   ├── ingest.py            # Pull USGS public data automatically
│   ├── groundwater.py       # Process well CSVs for trend detection and risk flags
│   ├── water_quality.py     # Process WQ CSVs and threshold alerts
│   ├── indicators.py        # Compound drought stage and action triggers
│   └── run_pipeline.bat     # Windows: double-click to run everything
├── app/
│   └── app.py               # Streamlit decision dashboard
├── src/
│   ├── loaders.py           # Public data loaders (USGS NWIS, NHD, Census)
│   ├── indicators.py        # Shared indicator computation functions
│   ├── constants.py         # Site IDs, bounding boxes, CRS, thresholds
│   └── sovereignty.py       # Data governance acknowledgment (OCAP®, CARE, FAIR, IEEE 2890)
├── data/
│   ├── raw/                 # GITIGNORED: Tribal operational data
│   ├── templates/           # COMMITTED: blank Excel templates for field staff
│   └── cache/               # GITIGNORED: downloaded public data
├── config/
│   └── config.yaml          
├── docs/
│   ├── data_sovereignty.md
│   ├── glossary.md
│   ├── data_intake_guide.md
│   └── adaptation_guide.md
├── environment.yml
└── README.md
```

## Quick Start
```bash
# Clone the repository
git clone https://github.com/your-org/tribal_water_monitoring
cd tribal_water_monitoring

# Create and activate the conda environment
conda env create -f environment.yml
conda activate tribal-water

# Register the kernel for Jupyter
python -m ipykernel install --user --name tribal-water \
    --display-name "Python (tribal-water)"

# Launch the analysis notebooks
jupyter lab notebooks/
```

**First time running the pipeline:**
```bash
python pipeline/ingest.py      # download public data
python pipeline/groundwater.py # process well data
python pipeline/indicators.py  # compute drought stages
streamlit run app/app.py       # launch dashboard
```

## Data

### Public data (downloaded automatically)
All public data is downloaded at first run and cached to `data/cache/`.
No data is committed to this repository.

| Source | What | Used in |
|---|---|---|
| USGS NWIS | Groundwater levels, streamflow, water quality | Notebooks 02–05, Pipeline |
| Census TIGER AIANNH | Tribal boundaries | All notebooks |
| USGS NHD | Stream network, watershed boundaries | Notebook 01, 03 |
| NOAA PDSI | Drought index | Notebook 05 |
| MACAv2 | Climate projections | Notebook 07 |

### Tribal operational data (your data)
Tribal-collected data lives in `data/raw/` and is never committed.
See `data/templates/` for blank Excel templates:

- `groundwater_template.xlsx` — well level measurements
- `water_quality_template.xlsx` — water quality sampling data

See `docs/data_intake_guide.md` for field staff instructions.

## Adapting for Your Nation
This repository was built and demonstrated at Pine Ridge and Rosebud
Reservations. To adapt it for another Tribal Nation:

1. Update `config/config.yaml` USGS site IDs, bounding box, thresholds
2. Update `src/constants.py` Tribal boundary names, centroid coordinates
3. Replace `data/raw/` files your Nation's well and water quality data
4. Run the pipeline everything else updates automatically

See `docs/adaptation_guide.md` for step-by-step instructions.

## Data Sovereignty
This repository implements the following data governance frameworks:

- **OCAP®** Ownership, Control, Access, Possession
  https://fnigc.ca/ocap-training/
- **CARE Principles** Collective Benefit, Authority to Control,
  Responsibility, Ethics
  https://www.gida-global.org/care
- **FAIR Principles** Findable, Accessible, Interoperable, Reusable
  https://www.go-fair.org/fair-principles/
- **IEEE 2890-2025** Recommended Practice for Provenance of
  Indigenous Peoples' Data
  https://standards.ieee.org/ieee/2890/10318/

Tribal-collected data (well levels, water quality samples) is governed
by the collecting Nation under OCAP®. Public federal data (USGS, NOAA,
Census) is distinguished from Tribal operational data throughout.
Analysis results should be shared with the relevant Tribal Nation before
publication or external distribution.

See `docs/data_sovereignty.md` for full discussion.

## Oceti Sakowin Context
The analysis notebooks in this repository focus on Pine Ridge
(Oglala Lakota) and Rosebud (Sicangu Lakota) as primary demonstration
sites, with broader context for all Oceti Sakowin Nations in
South Dakota. The Oceti Sakowin (the Seven Council Fires) are the
collective Lakota, Dakota, and Nakota peoples. The term is used
throughout the notebooks because it reflects the Nations' own name for
themselves and their relationships to one another.

## Citation
Jones, L. (2025) and Sanovia, J., Tribal Water Monitoring: A reproducible water
monitoring system for Tribal Nations. Daear Consulting, LLC.
https://github.com/your-org/tribal_water_monitoring

