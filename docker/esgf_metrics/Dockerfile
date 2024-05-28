FROM ubuntu:20.04

# Set the Miniconda paths for the `conda` commands to work.
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y wget cron \
    && rm -rf /var/lib/apt/lists/*

# Download Miniconda.
RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

# Configure conda.
RUN conda --version \
    && conda config --set channel_priority strict

# Create the conda environment and cache the packages.
COPY ./conda-env /conda-env
RUN . /root/.bashrc \
    && conda init bash \
    && conda env create -f /conda-env/dev.yml

# Copy the start script (runs the cron job after postgres service is up).
COPY ./docker/esgf_metrics/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start

# Copy the crontab script (the actual cron job config).
COPY ./docker/esgf_metrics/crontab  /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

WORKDIR /app
