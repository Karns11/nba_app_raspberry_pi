FROM apache/airflow:2.10.0-python3.10

ENV AIRFLOW_HOME=/opt/airflow
ENV R_LIBS_SITE=/usr/local/lib/R/site-library

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      r-base libxml2-dev libcurl4-openssl-dev libssl-dev libpq-dev \
      libicu-dev build-essential gfortran cmake libnlopt-dev libblas-dev liblapack-dev \
      libfreetype6-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev \
      libpng-dev libjpeg-dev libtiff5-dev libcairo2-dev pkg-config \
      git libgit2-dev \
  && mkdir -p ${R_LIBS_SITE} \
  && chown -R airflow:root ${R_LIBS_SITE} \
  && chmod -R g+ws ${R_LIBS_SITE} \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN R -q -e "install.packages('https://cran.r-project.org/src/contrib/Archive/Matrix/Matrix_1.6-5.tar.gz', repos=NULL, type='source')"

# Install remotes and devtools
RUN R -q -e "install.packages(c('remotes', 'devtools'), repos='https://cloud.r-project.org')"

# Install CRAN packages (including hoopR dependencies)
RUN R -q -e "install.packages(c('juicyjuice','MatrixModels','quantreg','nloptr','lme4','pbkrtest','car','pacman','bigrquery','dplyr','rvest','zoo','MASS','scales','stringr','RPostgres','DBI','forecast', 'purrr', 'tidyr', 'jsonlite', 'data.table', 'httr', 'progressr', 'cli', 'glue', 'rlang'), repos='https://cloud.r-project.org', Ncpus=2)"

# Install hoopR from GitHub with verbose output
RUN R -e "\
  options(repos = c(CRAN = 'https://cloud.r-project.org')); \
  remotes::install_github('sportsdataverse/hoopR', dependencies = TRUE, upgrade = 'never'); \
  if (!require('hoopR', quietly = TRUE)) { \
    stop('hoopR installation failed'); \
  } \
"

# Sanity checks
RUN R -q -e "library(bigrquery); library(rvest); library(car); library(RPostgres); library(hoopR); cat('R packages OK\n')"

# Python layer
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir streamlit pandas sqlalchemy nflreadpy psycopg2-binary tweepy dbt dbt-postgres 

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]
WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID