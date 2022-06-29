# ESGF Metrics

A Python package that parses ESGF Apache logs for E3SM data download metrics on a quarterly basis.

Metrics include:

- Cumulative number of requests
- Cumulative GB of data downloaded
- Both metrics above by facet (e.g., E3SM Time Frequency, CMIP6 Activity)

## Usage

1.  Install Miniconda and Docker with Docker-compose
2.  Install and activate the Conda environment

        conda env create -n conda-env/dev.yml
        conda activate esgf_metrics_dev

3.  Copy `.env.template` as `.env` and configure the environment variables
4.  Build the docker-compose services

        sudo docker-compose up

5.  The `esgf_metrics` service will now automatically run the `esgf_metrics` to parse new logs and adds them to the database on a weekly basis.

6.  View CSV and plots outputs in `/outputs`

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
        Before:
          "/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/"
        After:
          "E3SM.1_0.historical.1deg_atm_60-30km_ocean.land.native.model-output.mon.ens1.v1"
          (NOTE: Refer to the templates below for how to translate this)
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

- Search API URL

      https://esgf-node.llnl.gov/esg-search/search/?offset=0&limit=0&type=Dataset&replica=false&latest=true&project=e3sm&project=ACME&facets=experiment%2Cscience_driver%2Crealm%2Cmodel_version%2Cregridding%2Ctime_frequency%2Cdata_type%2Censemble_member%2Ctuning%2Ccampaign%2Cperiod%2Catmos_grid_resolution%2Cocean_grid_resolution%2Cland_grid_resolution%2Cseaice_grid_resolution%2Cdata_node&format=application%2Fsolr%2Bjson

E3SM CMIP6 Guidelines

- Example log

      123.123.123.123 - - [14/Jul/2019:06:58:07 -0700] "GET /thredds/fileServer/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Lmon/tran/gr/v20180608/tran_Lmon_E3SM-1-0_piControl_r1i1p1f1_gr_000101-050012.nc HTTP/1.1" 206 1573717 "-" "Wget/1.20.1 (linux-gnu)

- Directory format template

      %(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s

- File/Dateset ID template

      %(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s

- Search API URL:

      https://esgf-node.llnl.gov/esg-search/search/?offset=0&limit=0&type=Dataset&replica=false&latest=true&institution_id=E3SM-Project&project=CMIP6&facets=mip_era%2Cactivity_id%2Cmodel_cohort%2Cproduct%2Csource_id%2Cinstitution_id%2Csource_type%2Cnominal_resolution%2Cexperiment_id%2Csub_experiment_id%2Cvariant_label%2Cgrid_label%2Ctable_id%2Cfrequency%2Crealm%2Cvariable_id%2Ccf_standard_name%2Cdata_node&format=application%2Fsolr%2Bjson

E3SM CMIP6 Variables Guideline

- Example Log Line

      123.123.123.123 - - [18/Jul/2019:00:52:54 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/cmip6_variables/piControl/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/prc/gr/v20190206/prc_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_000101-050012.nc HTTP/1.0" 404 - "-" "Wget/1.12 (linux-gnu)"
