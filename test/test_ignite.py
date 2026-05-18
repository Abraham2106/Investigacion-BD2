import sys
import time
from pyignite import Client
import test_colors as tc

def run_ignite_test():
    tc.print_blue("Prueba de conexion Apache Ignite")
    total_steps = 4
    
    # Inicializar la barra en 0%
    tc.show_progress(0, total_steps)
    time.sleep(0.3)
    
    client = Client()
    try:
        # Intentar conexion al puerto 10800 
        client.connect('localhost', 10800)
        tc.print_green("Estad: Conectado al puerto 10800.")
        tc.show_progress(1, total_steps)
        time.sleep(0.3)
        
        # Crear cache temp para prueba 
        cache = client.get_or_create_cache('test_connection_cache')
        tc.print_green("Se creo cache de prueba.")
        tc.show_progress(2, total_steps)
        time.sleep(0.3)
        
        # Escribir y leer clave
        cache.put('test_key', 'test_value')
        value = cache.get('test_key')
        
        if value == 'test_value':
            tc.print_green("Read y Write exitoso.")
        else:
            tc.print_red("Error al recuperar el valor.")
            sys.exit(1)
        tc.show_progress(3, total_steps)
        time.sleep(0.3)
            
        # Destruir cache y cerrar conexion
        cache.destroy()
        client.close()
        tc.print_green("Cache destruida y conexion cerrada.")
        tc.show_progress(4, total_steps)
        time.sleep(0.3)
        tc.print_blue("Fin del test.")
        
    except Exception as e:
        tc.print_red(f"Error durante la prueba de Ignite: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_ignite_test()
