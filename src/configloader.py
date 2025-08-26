import configparser


def get_spark_conf():
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    # Return dictionary of all configs from [SPARK]
    return dict(config.items("SPARK"))
