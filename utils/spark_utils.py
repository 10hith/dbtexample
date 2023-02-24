import re
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# Connecting via jupyter
# %load_ext sql
# %config SqlMagic.autopandas=True
# %config SqlMagic.feedback = False
# %config SqlMagic.displaycon = False
#
# %sql duckdb:////home/basal/live_datasets/crimeapp_init.db
# # %sql duckdb:////home/basal/live_datasets/cens.db
# # %sql duckdb:////mnt/c/wsl_transfers/cens.db
# %sql pragma threads=16


JAR_PATHS = '/home/basal/excelanalysis'
DATA_PATH = str(JAR_PATHS) + '/resources/data'
DEUTILS_PATH = str(JAR_PATHS) + '/resources/DqProfiler-1.0-SNAPSHOT-jar-with-dependencies.jar'
POSTGRES_JDBC = str(JAR_PATHS) + '/resources/postgresql-42.3.3.jar'
GEOTOOLS_JAR = str(JAR_PATHS) + '/resources/geotools-wrapper-1.1.0-25.2.jar'
SEDONA_JAR = str(JAR_PATHS) + '/resources/sedona-python-adapter-3.0_2.12-1.1.1-incubating.jar'
AWS_JAVA = str(JAR_PATHS) + '/resources/aws-java-sdk-bundle-1.12.167.jar'
HADOOP_AWS = str(JAR_PATHS) + '/resources/hadoop-aws-3.2.0.jar'

SPARK_NUM_PARTITIONS = 8


def get_spatial_spark_session(app_name: str = "SparkTest"):
    """
    Creates a local spark session.
            # .config("spark.sql.shuffle.partitions", f"{SPARK_NUM_PARTITIONS}") \
            spark.debug.maxToStringFields=100
            spark.conf.set("spark.sql.debug.maxToStringFields", 100)
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory','32G') \
    Need to add more configuration
    :param app_name:
    :return:
    """
    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .appName(f"{app_name}") \
        .config("spark.sql.shuffle.partitions", f"{SPARK_NUM_PARTITIONS}") \
        .config('spark.jars',
                f"{DEUTILS_PATH}"
                ) \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.files.maxPartitionBytes", "536870912") \
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "2100") \
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "20000")\
        .config("spark.sql.files.openCostInBytes", "41943040") \
        .config("spark.sql.files.minPartitionNum", "4") \
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
        .config("spark.kryoserializer.buffer.max", "2047") \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.debug.maxToStringFields', 0) \
        .config("sedona.global.charset", "utf8") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.network.timeout", "600s")\
        .getOrCreate()

    #         .config('spark.sql.execution.arrow.pyspark.enabled', True) \
    #         .config("spark.sql.shuffle.partitions", "12") \
    SedonaRegistrator.registerAll(spark)
    return spark


def cleanup_col_name(col_name: str):
    str_no_special_chars = re.sub(r"[^a-zA-Z0-9]+", ' ', col_name)
    str_w_single_space = ' '.join(str_no_special_chars.split())
    str_w_underscore = str_w_single_space.replace(' ', '_' )
    return col_name, str_w_underscore.lower()


def build_select_expr(cols_map):
    select_expr = [" `{}` as {}".format(x[0],x[1]) for x in cols_map]
    return select_expr


def with_std_column_names():
    """
    Conform all the columns to be stripped of all whitespaces lowercase
    Args:
        cols: list of columns
    Returns:
        Dataframe with specified columns converted
    """

    def inner(df):
        cols_map = [cleanup_col_name(x) for x in df.columns]
        select_expr = build_select_expr(cols_map)
        return df.selectExpr(select_expr)

    return inner