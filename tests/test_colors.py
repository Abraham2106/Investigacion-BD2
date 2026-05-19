# Modulo auxiliar de formato y progreso aniado para pruebas de conectividad

# Colores ANSI
GREEN = "\033[92m"
BLUE = "\033[94m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"

def print_blue(text):
    """Limpia la barra de progreso, imprime el log en azul y avanza de linea."""
    print(f"\r\033[K{BLUE}{text}{RESET}")

def print_green(text):
    """Limpia la barra de progreso, imprime el log en verde y avanza de linea."""
    print(f"\r\033[K{GREEN}{text}{RESET}")

def print_red(text):
    """Limpia la barra de progreso, imprime el log en rojo y avanza de linea."""
    print(f"\r\033[K{RED}{text}{RESET}")

def show_progress(step, total, prefix="Progreso:"):
    """Dibuja la barra de progreso en la ultima linea activa sin avanzar de linea (end="")."""
    length = 20
    filled = int(length * step // total)
    bar = "█" * filled + "-" * (length - filled)
    percent = f"{100 * step / total:.0f}%"
    
    # \r regresa el cursor al inicio, \033[K borra residuos, end="" mantiene el progreso en la ultima linea
    print(f"\r\033[K{prefix} |{bar}| {percent}", end="", flush=True)
