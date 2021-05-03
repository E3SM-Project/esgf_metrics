# usage_plotter

This Python software parses ESGF Apache access logs for metrics, especifically for E3SM.

How to read Apache logs:  https://www.keycdn.com/support/apache-access-log#reading-the-apache-access-logs

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
## E3SM Guidelines
### Example log line

`123.123.123.123 - - [22/Sep/2019:12:01:01 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc HTTP/1.1" 200 91564624 "-" "Wget/1.14 (linux-gnu)"\n`

### File/Dataset ID Template

`%(root)s/%(source)s/%(model_version)s/%(experiment)s/%(grid_resolution)s/%(realm)s/%(regridding)s/%(data_type)s/%(time_frequency)s/%`

### Directory Format Template

`%(source)s.%(model_version)s.%(experiment)s.%(grid_resolution)s.%(realm)s.%(regridding)s.%(data_type)s.%(time_frequency)s.%(ensemble_member)s`

## E3SM in CMIP6 Guidelines

### Example log

`123.123.123.123 - - [14/Jul/2019:06:58:07 -0700] "GET /thredds/fileServer/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Lmon/tran/gr/v20180608/tran_Lmon_E3SM-1-0_piControl_r1i1p1f1_gr_000101-050012.nc HTTP/1.1" 206 1573717 "-" "Wget/1.20.1 (linux-gnu)`
### Directory format template

`%(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s`

### File/Dateset ID template

`%(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s`

## E3SM CMIP6 Variables Guidelines

### Example Log Line:

`123.123.123.123 - - [18/Jul/2019:00:52:54 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/cmip6_variables/piControl/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/prc/gr/v20190206/prc_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_000101-050012.nc HTTP/1.0" 404 - "-" "Wget/1.12 (linux-gnu)"`
