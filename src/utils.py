import yaml
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ---CONFIG LOADER ---
def load_config(config_path="conf/config.yaml"):
    """
    Standard YAML loader.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# ---ADLS PATH CONSTRUCTOR ---
def adls_path(layer_name: str) -> str:
    """
    Constructs the ABFSS path using the YAML structure.
    """
    config = load_config()
    azure_conf = config.get('azure', {})
    storage_account = azure_conf.get('account_name')
    container = azure_conf.get('container_name')
    
    if layer_name == "raw":
        base_folder = azure_conf.get('raw_path')
    elif layer_name == "processed":
        base_folder = azure_conf.get('processed_path')
    elif layer_name == "delta_path":
        base_folder = azure_conf.get('delta_path')
    else:
        raise ValueError(f"Unknown layer name: {layer_name}")
        
    if not base_folder:
        raise ValueError(f"Path for '{layer_name}' is not defined in config.yaml")

    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_folder.strip('/')}/"

# --- SPARK SESSION BUILDER ---
def get_spark_session(app_name: str = "smart-price-tracker"):
    config = load_config()
    spark_env = os.getenv("SPARK_ENV", "prod") #default env set to prod
    log4j_file = (
    "conf/log4j-ci.properties"
    if spark_env == "ci"
    else "conf/log4j.properties"
)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j.configuration=file:{log4j_file}"
               )
    )

    if spark_env == "prod":
        account_name = config["azure"]["account_name"]
        access_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

        if not access_key:
            raise RuntimeError(
                "AZURE_STORAGE_ACCOUNT_KEY environment variable not set"
            )

        azure_packages = [
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.microsoft.azure:azure-storage:8.6.6"
        ]

        builder = builder.config(
            f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
            access_key
        )

        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=azure_packages
        ).getOrCreate()
    else:
        # for ci / cd and tests to run without azure support
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark, config
