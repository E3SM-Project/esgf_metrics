# usage_plotter

This software parses ESGF Apache access logs for quarterly metrics specific to E3SM, including:

- Total Data Accessed
- Total Requests
- Both metrics above by facet (e.g., E3SM Time Frequency, CMIP6 Activity)

## Usage

1. Install Anaconda
2. Install and activate Conda environment
        conda env create -n conda-env/dev.yml`
        conda activate usage_plotter_dev
3. Run the software using
      `python -m usage_plotter.usage_plotter
4. View plot PNG files in root of repository

Tip: Add the flag `-h` or `--help` to view available command line arguments.

## Example Output

## How It Works

    1) Read in logs, here's an example line:
        "128.211.148.13 - - [22/Sep/2019:12:01:01 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc HTTP/1.1" 200 91564624 "-" "Wget/1.14 (linux-gnu)"\n"

    2) Split each log line into a list:
        ['128.211.148.13',
        '-',
        '-',
        '[22/Sep/2019:12:01:01',
        '-0700]',
        '"GET',
        '/thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc',
        'HTTP/1.1"',
        '200',
        '91564624',
        '"-"',
        '"Wget/1.14',
        '(linux-gnu)"']

    3) Parse each log line for the directory:
        "/thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc"
    4) Parse directory for the dataset id:
        "E3SM.1_0.historical.1deg_atm_60-30km_ocean.land.native.model-output.mon.ens1.v1"
    5) Parse directory for file id:
        "20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc"
    6) Parse for additional info (e.g., timestamp, facets)

## Templates for Parsing Logs

This list below includes an example log line from an Apache log and the project specific templates which can be used to parse log lines.

E3SM

- Example log line

      123.123.123.123 - - [22/Sep/2019:12:01:01 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc HTTP/1.1" 200 91564624 "-" "Wget/1.14 (linux-gnu)"\n

- File/Dataset ID Template

      %(root)s/%(source)s/%(model_version)s/%(experiment)s/%(grid_resolution)s/%(realm)s/%(regridding)s/%(data_type)s/%(time_frequency)s/%

- Directory Format Template

      %(source)s.%(model_version)s.%(experiment)s.%(grid_resolution)s.%(realm)s.%(regridding)s.%(data_type)s.%(time_frequency)s.%(ensemble_member)s

E3SM in CMIP6 Guidelines

- Example log

      123.123.123.123 - - [14/Jul/2019:06:58:07 -0700] "GET /thredds/fileServer/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Lmon/tran/gr/v20180608/tran_Lmon_E3SM-1-0_piControl_r1i1p1f1_gr_000101-050012.nc HTTP/1.1" 206 1573717 "-" "Wget/1.20.1 (linux-gnu)

- Directory format template

      %(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s

- File/Dateset ID template

      %(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s

E3SM CMIP6 Variables Guideline

- Example Log Line

      123.123.123.123 - - [18/Jul/2019:00:52:54 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/cmip6_variables/piControl/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/prc/gr/v20190206/prc_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_000101-050012.nc HTTP/1.0" 404 - "-" "Wget/1.12 (linux-gnu)"
