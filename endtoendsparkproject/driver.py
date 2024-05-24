import os

import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date
from ingest import load_files,display_df,df_count
from data_processing import data_clean
import logging
import logging.config
import sys

logging.config.fileConfig('properties/configuration/logging.config')

def main():
    global file_format, file_dir, header, inferSchema
    try:
        logging.info('i am main method...')
        # print('hello')
        # print(gav.header)
        # print(gav.src_olap)
        logging.info('calling spark object...')
        spark = get_spark_object(gav.envn,gav.appName)
        logging.info('Validating Spark Object')
        get_current_date(spark)

        # print(os.listdir(gav.src_olap))       ---- for parquet files
        for file in os.listdir(gav.src_olap):
            # print('file is ', file)
            file_dir = gav.src_olap + '/'+file
            # print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format= 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which of = {}'.format(file_format))
        df_user = load_files(spark=spark,file_dir=file_dir,file_format=file_format,header=header,inferSchema=inferSchema)
        logging.info('display the dataframe{}'.format(df_user))
        # display_df(df_user,'df_user')
        logging.info('validating the dataframe.........')
        # df_count(df_user,'df_city')

        # for csv file
        for file in os.listdir(gav.src_oltp):
            # print('file is ', file)
            file_dir = gav.src_oltp + '/'+file
            # print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format= 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which of = {}'.format(file_format))
        df_movies = load_files(spark=spark,file_dir=file_dir,file_format=file_format,header=header,inferSchema=inferSchema)
        logging.info('display the dataframe{}'.format(df_movies))
        display_df(df_movies, 'df_movies')
        logging.info('validating the dataframe.........')
        df_count(df_movies, 'df_movies')

        logging.info('implementing data_processing methods ....')
        df_user_sel,df_movies_sel = data_clean(df_user,df_movies)

        display_df(df_user_sel,'df_user_sel')
        logging.info('schema of of table {}'.format(df_user_sel))
        df_user_sel.printSchema()

        display_df(df_movies_sel,'df_movies_sel')
        logging.info('schema of of table {}'.format(df_movies_sel))
        df_movies_sel.printSchema()

    except Exception as exp:
        logging.error('An Error occured when calling main(). please check the trace ===',str(exp))
        sys.exit(1)
if __name__=='__main__':
    main()
    logging.info('Application Done')