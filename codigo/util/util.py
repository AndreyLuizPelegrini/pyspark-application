# -*- coding: utf-8 -*-

schema_definition = {
                        'Região - Sigla': 'regiao',
                        'Estado - Sigla': 'estado', 
                        'Município': 'municipio', 
                        'Revenda': 'revenda', 
                        'Instalação - Código': 'instalacao', 
                        'Produto': 'produto', 
                        'Data da coleta': 'data_coleta', 
                        'Valor de Venda': 'valor_venda', 
                        'Valor de Compra': 'valor_compra', 
                        'Unidade de Medida': 'unidade_medida', 
                        'Bandeira': 'bandeira'
                    }

def rename_columns(df_in):
    for key, value in schema_definition.items():
        df_in = df_in.withColumnRenamed(key, value)

    return df_in