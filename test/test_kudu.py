import sys
import time
from pyspark.sql import SparkSession
import test_colors as tc

KUDU_MASTER = "localhost:7051"
TABLE_NAME = "default.test_kudu_table"

def run_kudu_test():
    tc.print_blue("Prueba de conexion Apache Kudu")
    total_steps = 4
    
    # Inicializar la barra en 0%
    tc.show_progress(0, total_steps)
    time.sleep(0.3)
    
    # Inicializar sesion de Spark
    try:
        spark = SparkSession.builder \
            .appName("KuduConnectionTest") \
            .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
    except Exception as e:
        tc.print_red(f"Error al iniciar la sesion de Spark: {e}")
        sys.exit(1)
        
    try:
        # Intentar conexion al puerto 7051
        gateway = spark.sparkContext._gateway
        kudu_client_builder = gateway.jvm.org.apache.kudu.client.KuduClient.KuduClientBuilder(KUDU_MASTER)
        kudu_client = kudu_client_builder.build()
        tc.print_green("Estad: Conectado al puerto 7051.")
        tc.show_progress(1, total_steps)
        time.sleep(0.3)
        
        # Limpiar si la tabla de prueba ya existia
        if kudu_client.tableExists(TABLE_NAME):
            kudu_client.deleteTable(TABLE_NAME)
            
        # Definir el esquema de la tabla
        ColumnSchemaBuilder = gateway.jvm.org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
        Type = gateway.jvm.org.apache.kudu.Type
        
        columns = gateway.jvm.java.util.ArrayList()
        columns.add(ColumnSchemaBuilder("id", Type.STRING).key(True).build())
        columns.add(ColumnSchemaBuilder("name", Type.STRING).build())
        schema = gateway.jvm.org.apache.kudu.Schema(columns)
        
        # Opciones de creacion
        CreateTableOptions = gateway.jvm.org.apache.kudu.client.CreateTableOptions
        options = CreateTableOptions()
        options.setNumReplicas(1)
        
        hash_cols = gateway.jvm.java.util.ArrayList()
        hash_cols.add("id")
        options.addHashPartitions(hash_cols, 3)
        
        # Crear tabla temp para prueba
        kudu_client.createTable(TABLE_NAME, schema, options)
        tc.print_green("Se creo tabla de prueba.")
        tc.show_progress(2, total_steps)
        time.sleep(0.3)
        
        # Escribir registros de prueba
        data = [("1", "Alice"), ("2", "Bob")]
        columns_spark = ["id", "name"]
        df = spark.createDataFrame(data, columns_spark)
        
        df.write \
            .format("org.apache.kudu.spark.kudu") \
            .option("kudu.master", KUDU_MASTER) \
            .option("kudu.table", TABLE_NAME) \
            .option("kudu.operation", "upsert") \
            .mode("append") \
            .save()
        
        # Leer registros de prueba
        df_read = spark.read \
            .format("org.apache.kudu.spark.kudu") \
            .option("kudu.master", KUDU_MASTER) \
            .option("kudu.table", TABLE_NAME) \
            .load()
            
        count = df_read.count()
        
        if count == 2:
            tc.print_green("Escritura y lectura exitosa.")
        else:
            tc.print_red("Error al recuperar el valor.")
            sys.exit(1)
        tc.show_progress(3, total_steps)
        time.sleep(0.3)
            
        # Eliminar tabla y cerrar conexion
        kudu_client.deleteTable(TABLE_NAME)
        kudu_client.close()
        tc.print_green("Tabla de prueba eliminada.")
        tc.show_progress(4, total_steps)
        time.sleep(0.3)
        tc.print_blue("Fin del test.")
        
    except Exception as e:
        tc.print_red(f"Error durante la prueba de Kudu: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_kudu_test()
