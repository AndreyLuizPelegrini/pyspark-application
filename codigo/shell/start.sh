echo 'Iniciando aplicação Spark...'

# Exportando Python encoding como UTF-8
export PYTHONIOENCODING=utf8

spark-submit --master local[*] \
             --driver-memory 4g \
             --num-executors 4 \
             --executor-memory 2g \
             --executor-cores 2 \
             --deploy-mode client \
             ../main.py
