import json
from typing import Any, Dict

def truncate_json(json_data: str, max_tokens: int = 1200) -> Dict:
    """
    Trunca un JSON para que no exceda un número máximo de tokens.
    
    Args:
        json_data: String JSON a truncar
        max_tokens: Número máximo de tokens permitidos
        
    Returns:
        Diccionario truncado
    """
    try:
        data = json.loads(json_data) if isinstance(json_data, str) else json_data
    except json.JSONDecodeError:
        return {"error": "JSON inválido"}
    
    # Si es una lista, truncar elementos
    if isinstance(data, list):
        # Calcular cuántos elementos podemos mantener
        estimated_tokens = len(json.dumps(data)) // 4  # Aproximación
        if estimated_tokens > max_tokens:
            # Reducir la lista proporcionalmente
            ratio = max_tokens / estimated_tokens
            new_length = int(len(data) * ratio)
            data = data[:max(1, new_length)]
    
    return data


def format_table_structure(columns: list) -> str:
    """
    Formatea la estructura de una tabla de manera legible.
    
    Args:
        columns: Lista de tuplas (nombre_columna, tipo)
        
    Returns:
        String formateado con la estructura
    """
    lines = ["Columnas:"]
    for col_name, col_type in columns:
        lines.append(f"  - {col_name}: {col_type}")
    return "\n".join(lines)


def extract_column_names(table_info: dict) -> list:
    """
    Extrae solo los nombres de las columnas de la información de una tabla.
    
    Args:
        table_info: Diccionario con información de la tabla
        
    Returns:
        Lista de nombres de columnas
    """
    columns = table_info.get('columns', [])
    return [column[0] for column in columns]
