
import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('properties/configuration/logging.config')
loggers = logging.getLogger('Data_processing')

def data_clean(df1,df2):
    global df_user_sel, df_movies_sel
    try:
        loggers.warning('data clean method started ...')
        loggers.warning('selecting required columns from OLAP and converting them into uppercase if they required...')

        df_user_sel = df1.select(upper(col('first_name')).alias('fname'),upper(col('last_name')).alias('lname'),df1.email,df1.gender,upper(col('country')).alias('country'),df1.salary)

        loggers.warning('selecting required columns from OLTP and converting them into uppercase if they required...')
        df_movies_sel = df2.select(df2.name,df2.year,df2.score,df2.country,df2.runtime)

    except Exception as exp:
        loggers.error('An error occured at data clean() method===',str(exp))
    else:
        loggers.warning('data_clean() method executed done, for frwd... ')

    return df_user_sel,df_movies_sel
