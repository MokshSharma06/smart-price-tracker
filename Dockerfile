# ---- Conda-based Dockerfile (reliable, but larger images) ----
FROM continuumio/miniconda3:latest
# Workdir
WORKDIR /app

# Copy environment first so changes to code don't bust dependency layer
COPY environment.yml /app/environment.yml

ENV CONDA_ENV_NAME=spt-env

# Create conda env from environment.yml
SHELL ["bash", "-lc"]
RUN conda env create -f /app/environment.yml -n ${CONDA_ENV_NAME} \
    && conda clean -afy \
    && rm -rf /root/.cache/pip /root/.cache/conda

# Ensure conda env binaries are first on PATH
ENV PATH=/opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH
ENV JAVA_HOME="/opt/conda/envs/$CONDA_ENV_NAME"
ENV SPARK_MASTER_URL="local[*]"
# Set the Spark Home to the correct location for python execution
ENV SPARK_HOME="/opt/conda/envs/$CONDA_ENV_NAME/lib/python3.10/site-packages/pyspark"
# Copy application code
COPY . /app

# Create non-root user and give ownership
RUN useradd -m appuser \
    && chown -R appuser:appuser /app \
    && chown -R appuser:appuser /opt/conda/envs/${CONDA_ENV_NAME}

USER appuser
WORKDIR /app

# Make run script executable (if present)
RUN chmod +x /app/run.sh || true

# Default command: run your main.py using the conda env
CMD ["bash", "-lc", "conda run -n spt-env python main.py"]
