# -*- coding: utf-8 -*-

import logging

from functions import load_data as source
from util import util

from pyspark.sql.functions import trim, when, lit, regexp_replace, max, col, sum, round
from pyspark.sql.types import FloatType

if __name__ == "__main__" :

    # Configurando biblioteca de logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    dataframe = source.load_data_from_csv('/home/andrey/Documentos/Estudos/pyspark-application/2019-04-gasolina-etanol.csv', '\\t', True)
    #dataframe = source.load_data_from_hive('adhoc.gasolina_etanol')

    # Renomeando colunas do dataframe
    dataframe = util.rename_columns(dataframe)

    # Tratando colunas nullas ou vazias nos campos de valor
    for column in ['valor_venda', 'valor_compra']:
        dataframe = dataframe.withColumn(column, when((trim(dataframe[column]) == '') | (dataframe[column].isNull()), 
                                                        lit('0').cast('double'))
                                                        .otherwise(regexp_replace(dataframe[column], ',', '.').cast('double')))

    dataframe.persist()

    # Qual estado tem mais registros na planilha ?
    estado_count_df = dataframe.groupBy('estado') \
                               .count() \
                               .orderBy('count', ascending=False).limit(1)

    logging.info('Estado com maior numero de aparicoes \n')
    logging.info(estado_count_df.show())

    # Qual o estado tem o maior valor de revenda?
    maior_revenda = dataframe.agg(max('valor_venda')) \
                             .select(col('max(valor_venda)').alias('valor_venda')) \
                             .join(dataframe, 'valor_venda', 'inner') \
                             .select('estado', 'valor_venda')                             

    logging.info('Estado com maior valor de revenda \n')
    logging.info(maior_revenda.show())

    # Qual municipio tem o maior valor de venda em cada estado?
    municipio_maior_venda_por_estado = dataframe.groupBy('estado') \
                                                .agg(max('valor_venda')) \
                                                .select('estado', col('max(valor_venda)').alias('valor_venda')) \
                                                .join(dataframe, ['estado', 'valor_venda'], 'inner') \
                                                .select('estado', 'municipio', 'valor_venda') \
                                                .distinct().orderBy('estado', 'municipio', 'valor_venda')

    logging.info(municipio_maior_venda_por_estado.show(29, False))

    # Qual municipio tem o maior valor de venda em todo o pais em uma determinada data?
    maior_valor_na_data_do_arquivo = municipio_maior_venda_por_estado.orderBy('valor_venda', ascending = False).limit(1)
    
    logging.info('Municipio com o maior valor de venda no pais \n')
    logging.info(maior_valor_na_data_do_arquivo.show())

    # Qual a media de diferenças entre valor de venda e valor de compra para uma determinada data?
    media_diferencas = dataframe.withColumn('media_diferenca', when(col('valor_compra') == '0.0', lit('0.0')) \
                                                              .otherwise((col('valor_venda') - col('valor_compra')).cast(FloatType())))

    media_diferencas_com_valor = media_diferencas.filter('valor_compra <> 0.0').count()
    media_diferencas = media_diferencas.agg(sum('media_diferenca')) \
                                       .select(round((col('sum(media_diferenca)') / media_diferencas_com_valor), 2) \
                                       .alias('media_diferenca'))

    logging.info('Media de diferenças \n')
    logging.info(media_diferencas.show())