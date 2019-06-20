# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

class Spark():

    @staticmethod
    def get_instance():

        #enableHiveSupport() to integrate with Hive
        
        return SparkSession \
               .builder \
               .appName('Levantamento_Precos_Combustivel') \
               .getOrCreate()