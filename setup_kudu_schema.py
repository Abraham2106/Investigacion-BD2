import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    print("Inicio de PySpark con conector de Kudu")
    
    spark = SparkSession.builder \
        .appName("KuduSchemaSetup") \
        .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # Fijado a 127.0.0.1 para alinearse con el docker-compose corregido
    kudu_master = "127.0.0.1:7051"
    table_name = "orders"
    
    print(f"Conectando al Kudu Master en {kudu_master}...")

    sc = spark.sparkContext
    jvm = sc._jvm
    kudu_context = jvm.org.apache.kudu.spark.kudu.KuduContext(kudu_master, sc._jsc.sc())
    
    if kudu_context.tableExists(table_name):
        print("La tabla ya existe. Omitiendo creacion.")
        return
    
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    scala_schema = jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json())
    
    print(f"Configurando Hash Partitioning sobre 'order_id' en 3 buckets...")
    options = jvm.org.apache.kudu.client.CreateTableOptions()
    options.setNumReplicas(1)
    cols_list = jvm.java.util.ArrayList()
    cols_list.add("order_id")
    options.addHashPartitions(cols_list, 3)
    pk_seq = jvm.scala.collection.JavaConverters.asScalaBufferConverter(cols_list).asScala().toSeq()
    
    print(f"Creando tabla '{table_name}'...")
    try:
        kudu_context.createTable(table_name, scala_schema, pk_seq, options)
        print("¡Tabla creada exitosamente en Kudu!")
    except Exception as e:
        print(f"Error al crear la tabla en Kudu: {e}")
        sys.exit(1)
        
    print("Verificando existencia...")
    if kudu_context.tableExists(table_name):
        print("Esquema validado exitosamente. Todo listo.")
    else:
        print("Error: La tabla no se encontro tras la creacion.")
        
    spark.stop()

if __name__ == "__main__":
    main()