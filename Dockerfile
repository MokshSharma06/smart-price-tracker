FROM continuumio/miniconda3:latest

WORKDIR /app

# 1. Install Linux dependencies for Chrome and Virtual Screen
USER root
RUN apt-get update && apt-get install -y \
    wget gnupg unzip xvfb libxi6 libgconf-2-4 \
    libnss3 libgbm1 libasound2 libxrender1 libxtst6 \
    libfontconfig1 libx11-6 \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# setting up env 
COPY environment.yml /app/environment.yml
ENV CONDA_ENV_NAME=smart-price-tracker

SHELL ["bash", "-lc"]
RUN conda env create -f /app/environment.yml -n ${CONDA_ENV_NAME} \
    && conda clean -afy

# setting up paths
ENV PATH=/opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH
ENV JAVA_HOME="/opt/conda/envs/${CONDA_ENV_NAME}"
ENV SPARK_HOME="/opt/conda/envs/${CONDA_ENV_NAME}/lib/python3.10/site-packages/pyspark"
ENV DISPLAY=:99

# Permissions and Code
COPY . /app
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# 5. Launch with Xvfb
CMD ["bash", "-lc", "Xvfb :99 -screen 0 1920x1080x24 & conda run -n smart-price-tracker python main.py"]