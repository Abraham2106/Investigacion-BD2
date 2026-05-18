import sys
import os

# Asegurar que el directorio actual de pruebas este en el path de importacion de Python
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_colors as tc
from test_ignite import run_ignite_test
from test_kafka import run_kafka_test
from test_kudu import run_kudu_test
from test_fastapi import run_fastapi_test

def main():
    tc.print_blue(" ========================== Inicio de pruebas ========================== ")
    print()
    
    # 1. Prueba de Apache Ignite
    run_ignite_test()
    print()
    
    # 2. Prueba de Apache Kafka
    run_kafka_test()
    print()
    
    # 3. Prueba de Apache Kudu
    run_kudu_test()
    print()
    
    # 4. Prueba de FastAPI Gateway
    run_fastapi_test()
    print()
    
    tc.print_green(" ========================== Fin de pruebas ========================== ")

if __name__ == "__main__":
    main()
