import configparser
import os

def get_spark_conf():
    config = configparser.ConfigParser()
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # points to project root
    config_path = os.path.join(BASE_DIR, "conf", "spark.conf")
    config.read(config_path)

    # Return dictionary of all configs from [SPARK]
    return dict(config.items("SPARK"))
