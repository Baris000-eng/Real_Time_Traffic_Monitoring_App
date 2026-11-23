# Flask Dashboard (Web UI)

**Part of the BD26_Project_T1_B Real-Time Taxi Fleet Monitor** [⬅ Back to Main Project README](../README.md)

## Overview
The `dashboard` directory contains the frontend and web server of the project. Built with Flask, it provides a real-time, interactive user interface to visualize the processed big data streams.

## Key Features

* **Live Map Visualization:** Integrates with Leaflet.js to plot active taxis on a map of Beijing, dynamically updating their positions.
* **Real-Time Metrics:** Fetches high-speed state data directly from Redis to display the number of currently driving taxis and the total fleet distance.
* **Incident Alerts:** Highlights area violations (taxis leaving the designated Beijing boundary) and speeding incidents instantly on the UI.
* **Optimized Rendering:** Built to handle thousands of concurrent taxi markers on the map without browser freezing, utilizing Canvas rendering and efficient data fetching.

## Access
Once the Docker containers are running, the dashboard is accessible via your web browser at:
`http://localhost:5050`

For full project architecture details and troubleshooting, please refer to the [Main README](../README.md).