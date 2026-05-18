import sys
import time
from fastapi import FastAPI
from fastapi.testclient import TestClient
import test_colors as tc

app = FastAPI(
    title="Prueba de conectividad de FastAPI",
    description="API para verificar el estado del gateway",
    version="1.0.0"
)

@app.get("/health", tags=["Health"])
def health_check():
    """Retorna el estado de salud del gateway del servidor FastAPI."""
    return {
        "status": "healthy",
        "service": "FastAPI Gateway"
    }

def run_fastapi_test():
    tc.print_blue("Prueba de conexion FastAPI")
    total_steps = 2
    
    # Inicializar la barra en 0%
    tc.show_progress(0, total_steps)
    time.sleep(0.3)
    
    try:
        # Intentar conexion al cliente de pruebas
        client = TestClient(app)
        tc.print_green("Estad: TestClient de FastAPI inicializado.")
        tc.show_progress(1, total_steps)
        time.sleep(0.3)
        
        # Realizar peticion al endpoint /health
        response = client.get("/health")
        
        if response.status_code == 200 and response.json().get("status") == "healthy":
            tc.print_green("Escritura y lectura de endpoint exitosa.")
        else:
            tc.print_red(f"Error al recuperar el valor.")
            sys.exit(1)
        tc.show_progress(2, total_steps)
        time.sleep(0.3)
        tc.print_blue("Fin del test.")
        
    except Exception as e:
        tc.print_red(f"Error durante la prueba de FastAPI: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_fastapi_test()
