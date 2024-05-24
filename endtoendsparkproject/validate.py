import logging.config

logging.config.fileConfig('properties/configuration/logging.config')
loggers = logging.getLogger('Validate')
def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method....')
        output=spark.sql("select current_date")
        logging.warning('validating spark object with current date-'+ str(output.collect()))
    except Exception as e:
        loggers.error('An Error occured in get_current_date',str(e))

        raise
    else:
        logging.warning('Validation done, go forward ...')


