import os
from collections import defaultdict
from datetime import datetime, timedelta
from sqlalchemy import inspect, text
from sources.db_connect import get_db_engine, get_db_connection

# Diccionario para almacenar la caché con control de tamaño y tiempo
cache = defaultdict(dict)
MAX_CACHE_SIZE = 100  # Límite de tablas en cache
CACHE_EXPIRY_HOURS = 1  # Tiempo de expiración del caché


def clear_cache_if_needed():
    """Limpia el cache si excede el tamaño máximo"""
    if len(cache) > MAX_CACHE_SIZE:
        # Eliminar la mitad de las entradas más antiguas
        items_to_remove = len(cache) // 2
        for i, key in enumerate(list(cache.keys())):
            if i >= items_to_remove:
                break
            del cache[key]
        print(f"🧹 Cache limpiado. Entradas removidas: {items_to_remove}")


def get_db_info():
    """
    Obtiene información sobre todas las tablas de la base de datos MySQL,
    incluyendo el nombre y tipo de cada columna para cada tabla.
    Utiliza caché con expiración de 1 hora.

    Returns:
        dict: Diccionario con la estructura {tabla: {'columns': [(nombre, tipo)]}}
    """
    now = datetime.now()
    
    # Verificar si el caché existe y no ha expirado
    if 'db_info' in cache and 'timestamp' in cache['db_info']:
        time_diff = now - cache['db_info']['timestamp']
        if time_diff < timedelta(hours=CACHE_EXPIRY_HOURS):
            print("💾 Usando información de BD desde caché")
            return {k: v for k, v in cache['db_info'].items() if k != 'timestamp'}
    
    print("🔄 Actualizando información de estructura de BD...")
    
    try:
        engine = get_db_engine()
        inspector = inspect(engine)
        
        # Obtener lista de todas las tablas
        tables = inspector.get_table_names()
        print(f"📊 Tablas encontradas: {len(tables)}")
        
        db_info = {}
        
        for table_name in tables:
            try:
                # Obtener columnas de la tabla
                columns = inspector.get_columns(table_name)
                columns_info = [(column['name'], str(column['type'])) for column in columns]
                db_info[table_name] = {'columns': columns_info}
                print(f"  ✅ {table_name}: {len(columns_info)} columnas")
            except Exception as e:
                print(f"  ❌ Error obteniendo info de tabla {table_name}: {e}")
                continue
        
        # Guardar en caché con timestamp
        db_info['timestamp'] = now
        cache['db_info'] = db_info
        
        print(f"✅ Información de BD actualizada ({len(db_info)-1} tablas)")
        
        # Retornar sin el timestamp
        return {k: v for k, v in db_info.items() if k != 'timestamp'}
        
    except Exception as e:
        print(f"❌ Error en get_db_info: {e}")
        return {}


def get_table_info(table_name: str):
    """
    Obtiene información sobre una tabla específica en la base de datos.
    Utiliza caché individual por tabla.

    Args:
        table_name (str): El nombre de la tabla para la cual se desea obtener información.

    Returns:
        dict: Diccionario con la estructura {'columns': [(nombre, tipo)]}
    """
    print(f"🔍 Obteniendo info para tabla: '{table_name}'")
    
    # Verificar caché individual de la tabla
    if table_name in cache and 'table_info' in cache[table_name]:
        if 'timestamp' in cache[table_name]:
            time_diff = datetime.now() - cache[table_name]['timestamp']
            if time_diff < timedelta(hours=CACHE_EXPIRY_HOURS):
                print(f"💾 Usando info de tabla '{table_name}' desde caché")
                return cache[table_name]['table_info']
    
    clear_cache_if_needed()
    
    try:
        engine = get_db_engine()
        inspector = inspect(engine)
        
        # Verificar si la tabla existe
        existing_tables = inspector.get_table_names()
        
        if table_name not in existing_tables:
            print(f"❌ Tabla '{table_name}' NO existe")
            print(f"📝 Tablas disponibles: {existing_tables}")
            raise Exception(f"Tabla '{table_name}' no existe. Tablas disponibles: {existing_tables}")
        
        # Obtener columnas
        columns = inspector.get_columns(table_name)
        columns_info = [(column['name'], str(column['type'])) for column in columns]
        
        table_info = {'columns': columns_info}
        
        # Guardar en caché
        cache[table_name]['table_info'] = table_info
        cache[table_name]['timestamp'] = datetime.now()
        
        print(f"✅ Info de tabla '{table_name}' cargada ({len(columns_info)} columnas)")
        
        return table_info
        
    except Exception as e:
        print(f"❌ Error obteniendo info de tabla '{table_name}': {e}")
        raise


def get_table_sample(table_name: str, limit: int = 100):
    """
    Obtiene una muestra de datos de una tabla específica con valores únicos por columna.

    Args:
        table_name (str): Nombre de la tabla
        limit (int): Número máximo de filas a consultar

    Returns:
        dict: Diccionario con estructura {tabla: {columna: [valores_unicos]}}
    """
    print(f"🎯 Obteniendo muestra para tabla: '{table_name}'")
    
    # Verificar caché
    if table_name in cache and 'table_sample' in cache[table_name]:
        if 'sample_timestamp' in cache[table_name]:
            time_diff = datetime.now() - cache[table_name]['sample_timestamp']
            if time_diff < timedelta(hours=CACHE_EXPIRY_HOURS):
                print(f"💾 Usando muestra de '{table_name}' desde caché")
                return cache[table_name]['table_sample']
    
    clear_cache_if_needed()
    
    connection = None
    try:
        engine = get_db_engine()
        inspector = inspect(engine)
        
        # Verificar existencia de tabla
        existing_tables = inspector.get_table_names()
        if table_name not in existing_tables:
            raise Exception(f"Tabla '{table_name}' no existe para obtener muestra")
        
        # Obtener columnas
        columns = inspector.get_columns(table_name)
        column_names = [column['name'] for column in columns]
        
        connection = get_db_connection()
        
        # Consultar muestra de datos
        query = text(f"SELECT * FROM `{table_name}` LIMIT :limit")
        result = connection.execute(query, {"limit": limit})
        rows = result.fetchall()
        
        print(f"📊 Obtenidas {len(rows)} filas de muestra")
        
        # Convertir a diccionarios
        rows_as_dicts = [dict(zip(column_names, row)) for row in rows]
        
        # Extraer valores únicos por columna (máximo 10 por columna)
        table_sample = {table_name: {}}
        for column_name in column_names:
            unique_values = set()
            for row in rows_as_dicts:
                value = row[column_name]
                # Convertir a string para facilitar el análisis
                if value is not None:
                    unique_values.add(str(value))
                if len(unique_values) >= 10:
                    break
            table_sample[table_name][column_name] = list(unique_values)[:10]
        
        # Guardar en caché
        cache[table_name]['table_sample'] = table_sample
        cache[table_name]['sample_timestamp'] = datetime.now()
        
        print(f"✅ Muestra de tabla '{table_name}' cargada en caché")
        
        return table_sample
        
    except Exception as e:
        print(f"❌ Error obteniendo muestra de tabla '{table_name}': {e}")
        raise
    finally:
        if connection:
            connection.close()


def extract_column_names(table_info: dict) -> list:
    """
    Extrae solo los nombres de las columnas de la información de una tabla.

    Args:
        table_info (dict): Diccionario con la estructura {'columns': [(nombre, tipo)]}

    Returns:
        list: Lista con los nombres de las columnas
    """
    columns = table_info.get('columns', [])
    return [column[0] for column in columns]


def format_table_structure(table_name: str, table_info: dict) -> str:
    """
    Formatea la estructura de una tabla de manera legible para el LLM.

    Args:
        table_name (str): Nombre de la tabla
        table_info (dict): Información de la tabla

    Returns:
        str: String formateado con la estructura de la tabla
    """
    columns = table_info.get('columns', [])
    
    lines = [f"Tabla: {table_name}"]
    lines.append(f"Columnas ({len(columns)}):")
    
    for col_name, col_type in columns:
        lines.append(f"  - {col_name}: {col_type}")
    
    return "\n".join(lines)


def get_all_tables_list() -> list:
    """
    Obtiene una lista simple de todas las tablas en la base de datos.

    Returns:
        list: Lista de nombres de tablas
    """
    try:
        engine = get_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    except Exception as e:
        print(f"❌ Error obteniendo lista de tablas: {e}")
        return []


def clear_cache():
    """Limpia todo el caché manualmente"""
    cache.clear()
    print("🧹 Caché completamente limpiado")


def get_cache_stats() -> dict:
    """
    Obtiene estadísticas del caché actual.

    Returns:
        dict: Estadísticas del caché
    """
    stats = {
        "total_entries": len(cache),
        "tables_cached": sum(1 for k in cache.keys() if k != 'db_info'),
        "has_db_info": 'db_info' in cache,
        "cache_size_limit": MAX_CACHE_SIZE,
        "expiry_hours": CACHE_EXPIRY_HOURS
    }
    
    if 'db_info' in cache and 'timestamp' in cache['db_info']:
        age = datetime.now() - cache['db_info']['timestamp']
        stats['db_info_age_minutes'] = int(age.total_seconds() / 60)
    
    return stats
