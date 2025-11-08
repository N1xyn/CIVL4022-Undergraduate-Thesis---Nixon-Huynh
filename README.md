# CIVL4022-Undergraduate-Thesis---Nixon-Huynh
Repository for “Network Evaluation of a Bus Rapid Transit (BRT) Network for Sydney,” a CIVL4022 thesis at the University of Sydney. Compares Sydney FAST2030 and Plan B networks using accessibility-based analysis to identify optimal routes and propose a Top 15 BRT network for Greater Sydney.

***
Network Evaluation of a Bus Rapid Transit (BRT) Network for Sydney
CIVL4022 Undergraduate Thesis – The University of Sydney

Author: Nixon Lok Sang Huynh
Supervisor: Professor David Levinson
Date: November 2025

Overview

This repository accompanies the thesis "Network Evaluation of a Bus Rapid Transit (BRT) Network for Sydney", submitted for the unit CIVL4022 – Thesis at the University of Sydney.

The project evaluates and compares two proposed Bus Rapid Transit (BRT) network concepts for Sydney: Sydney FAST2030 (Levinson, 2022) and Plan B: Better Buses for Sydney (Committee for Sydney, 2024), using accessibility-based network analysis.

By integrating spatial datasets and open-source routing tools, the study identifies the most effective routes for improving Person-Weighted Accessibility (PWA) and proposes an optimised Top 15 BRT network for Greater Sydney.

Contents

This ZIP archive contains all data and code files required to reproduce the analyses and visualisations presented in the thesis. This includes:

GTFS files for Sydney – Sydney FAST2030 and Plan B: Better Buses for Sydney

PBF files containing the OSM network for Greater Sydney

R scripts used for accessibility calculations using r5r

Employment and travel zone data from:
• TZP24 Employment by Industry and Travel Zone 2021–2066
(Transport for NSW Data Portal: https://data.nsw.gov.au/data/dataset/2-employment-projections/resource/e7b8d4c6-161d-4cbf-b7b1-00850251bfa7?inner_span=True
)
• Census Mesh Block Counts (2021)
(Australian Bureau of Statistics: https://www.abs.gov.au/census/guide-census-data/mesh-block-counts/latest-release
)

Note: Directory paths have been omitted for privacy reasons. Please input your own local paths as required.

Methodology Summary

All analyses were conducted in R using the following key packages:

r5r – for multimodal accessibility computation using GTFS and OSM data

sf, dplyr, readr – for spatial data manipulation

leaflet, ggplot2 – for mapping and visualisation

Accessibility was calculated as Person-Weighted Accessibility (PWA) within 30-, 45-, and 60-minute travel time thresholds.
Each BRT route was evaluated both independently and as part of the following network scenarios:

Baseline Public Transport Network (2021)

Sydney FAST2030 Network

Plan B: Better Buses for Sydney Network

Synthesised Top 15 Route Network

Citation

If using any data or scripts from this repository, please cite as:

Huynh, N. (2025). Network Evaluation of a Bus Rapid Transit (BRT) Network for Sydney. Undergraduate Thesis, School of Civil Engineering, The University of Sydney.
