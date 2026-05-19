from pyignite import Client
import sys
import os
from tests import test_colors as tc

import time

# Instancia global del cliente de Ignite 
ignite_client = Client()

countries = ["CR","US","MX","AR","BR","CO","CL","PE","PA","GT","SV","HN","NI"]

try:
    ignite_client.connect('localhost', 10800)
    tc.print_green("Conectado a Apache Ignite")
except Exception as e:
    tc.print_red(f"Error de conexion a Apache Ignite: {e}")

def get_risk_countries_cache():
    """Obtiene o crea la cache de paises de riesgo.""" 
    
    cache = ignite_client.get_or_create_cache('risk_countries')
    for country in countries:
        if country in ['PA', 'SV', 'CL', 'NI']:
            cache.put(country, True)
        else:
            if cache.get(country) is None:
                cache.put(country, False)
    return cache

def get_user_velocity_cache():
    """Obtiene o crea la cache de velocidad de usuarios.""" 
    
    cache = ignite_client.get_or_create_cache('user_velocity')
    return cache

# Se exportan las caches
risk_countries_cache = get_risk_countries_cache()
user_velocity_cache = get_user_velocity_cache()
