# Fitbit QT

A desktop app that converts **Google Takeout Fitbit ZIP files** into a **Fitbit Dashboard CSV** for activity and sleep data.

This app is built with **PySide6 / Qt** and is available for both **macOS** and **Windows**.

## Download

Go to the repo's **Releases** page and download the version for your system:

- **macOS:** `v1.0.0_macos`
- **Windows:** `v1.0.0_windows`

Each release contains a zip file.

## Installation

### macOS
1. Download the zip file from `v1.0.0_macos`
2. Unzip it
3. Open the `.app`

If macOS blocks the app the first time:
- right-click the app
- choose **Open**
- then confirm

### Windows
1. Download the zip file from `v1.0.0_windows`
2. Unzip it
3. Open the `.exe` to install or launch the app

## What this app does

The app takes a **Google Takeout ZIP** that contains Fitbit export data and converts it into a single CSV output that can be used for dashboarding or downstream analysis.

The app currently supports:

- activity data
- sleep data

## How to use

1. Open the app
2. Enter the **Participant ID**
3. Select the **Start Date**
4. Select the **Returned Date**
5. Choose whether to keep only the **intersection date range across domains**
6. Click **Choose Takeout ZIP**
7. Select your Google Takeout ZIP file
8. Click **Process**
9. Review the preview tables
10. Click **Save CSV** to export the final file

## Input

- A **Google Takeout ZIP** containing Fitbit data

## Output

- A combined CSV file with Fitbit activity and sleep data

## Notes

- The app runs locally on your computer
- No Streamlit server is used
- The UI is a native desktop app built with Qt
- macOS and Windows builds are released separately

## Repository structure

```text
core/                 # conversion logic
ui/                   # Qt UI helpers
main.py               # desktop app entry point
requirements.txt      # Python dependencies
pysidedeploy.spec     # deployment config
