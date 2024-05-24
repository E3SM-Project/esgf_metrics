#%%
"""A script to manually parse new logs and update plots.

This script is useful if the acme1 server is restarted and the docker-compose
containers are not restarted via supervisorctl.

To use this script:
1. First create and activate the mamba/conda dev environment, `dev.yml`.
2. Copy `.env.template` as `.env`
3. Uncomment the lines for the postgres config for local dev outside of docker.
4. Comment out lines for postgres config for docker.
"""
#%%
# flake8: noqa F401
from esgf_metrics.parse import LogParser
from esgf_metrics.plot import plot_cumsum_by_project

#%%
# Create log object.
parser = LogParser()

#%% Save object to SQL
if parser.df_log_file is not None and parser.df_log_request is not None:
    parser.to_sql()

# %%
plot_cumsum_by_project()
# %%
