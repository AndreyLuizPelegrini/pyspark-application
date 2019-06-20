echo 'Iniciando aplicação Spark...'

# Exportando Python encoding como UTF-8
export PYTHONIOENCODING=utf8

spark-submit --master local[*] \
             --deploy-mode client \
             ../main.py
