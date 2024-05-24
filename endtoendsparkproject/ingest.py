import logging.config

logging.config.fileConfig('properties/configuration/logging.config')
logger = logging.getLogger('Ingest')


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load files method started...')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).options(header=header,inferSchema=inferSchema).load(file_dir)

    except Exception as e:
        logger.error('an error occured while loading files', str(e))
        raise
    else:
        logger.warning('dataframe created successfully which is of {}'.format(file_format))

    return df


def display_df(df,dfname):
    df_show=df.show()
    return df_show

def df_count(df,dfname):
    try:
        logger.warning('here to count the records in the {}'.format(dfname))
        df_c = df.count()

    except Exception as e:
        raise
    else:
        logger.warning('number of records present in the {} are:{}'.format(df,df_c))
    return df_c