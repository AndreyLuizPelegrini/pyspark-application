# -*- coding: utf-8 -*-

import logging 
from spark import Spark

spark = Spark.get_instance()

def load_data_from_csv(file_path, delimiter, header):
    logging.info('Importando arquivo {} para o Spark'.format(file_path))

    df = spark.read.option('delimiter', delimiter) \
                   .option('encoding', 'UTF-8') \
                   .csv(file_path, header=header)

    return df

def load_data_from_hive(table):
    logging.info('Importando tabela {} para o Spark'.format(table))

    df = spark.sql('SELECT * FROM {}'.format(table))

    return df