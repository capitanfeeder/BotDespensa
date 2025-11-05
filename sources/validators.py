import re
from typing import Any

class ValidationError(Exception):
    """Excepción personalizada para errores de validación"""
    pass

class SQLValidator:
    """Validador para consultas SQL y nombres de tablas"""
    
    # Patrones peligrosos en SQL
    DANGEROUS_PATTERNS = [
        r'\b(DELETE|DROP|ALTER|TRUNCATE|INSERT|UPDATE|CREATE|RENAME|REVOKE|GRANT)\b',
        r'--',           # Comentarios SQL
        r'/\*.*?\*/',    # Comentarios de bloque
        r'\b(EXEC|EXECUTE)\b',  # Ejecución de procedimientos
    ]
    
    @staticmethod
    def validate_table_name(table_name: str) -> bool:
        """
        Valida que el nombre de tabla sea seguro.
        
        Raises:
            ValidationError: Si el nombre no es válido
        """
        if not table_name or not isinstance(table_name, str):
            raise ValidationError("El nombre de tabla no puede estar vacío")
        
        # Longitud máxima razonable para MySQL
        if len(table_name) > 64:
            raise ValidationError("El nombre de tabla es demasiado largo")
        
        # Permitir caracteres alfanuméricos, guiones bajos y guiones
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_-]*$', table_name):
            raise ValidationError("El nombre de tabla contiene caracteres inválidos")
        
        # Verificar caracteres peligrosos
        dangerous_chars = ['--', '/*', '*/', ';', '\\', '\'\'']
        for char in dangerous_chars:
            if char in table_name:
                raise ValidationError(f"El nombre de tabla contiene caracteres peligrosos: {char}")
        
        return True
    
    @staticmethod
    def validate_sql_query(query: str, max_length: int = 10000) -> bool:
        """
        Valida que una consulta SQL sea segura.
        
        Raises:
            ValidationError: Si la consulta no es válida
        """
        if not query or not isinstance(query, str):
            raise ValidationError("La consulta SQL no puede estar vacía")
        
        if len(query) > max_length:
            raise ValidationError(f"La consulta SQL es demasiado larga (máximo {max_length} caracteres)")
        
        # Verificar patrones peligrosos
        for pattern in SQLValidator.DANGEROUS_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE | re.DOTALL):
                raise ValidationError(f"La consulta contiene patrones no permitidos")
        
        return True

class InputValidator:
    """Validador general para entradas de API"""
    
    @staticmethod
    def validate_text_input(text: str, max_length: int = 1000, min_length: int = 1) -> bool:
        """
        Valida entrada de texto general.
        
        Raises:
            ValidationError: Si el texto no es válido
        """
        if not isinstance(text, str):
            raise ValidationError("El texto debe ser una cadena")
        
        if len(text) < min_length:
            raise ValidationError(f"El texto debe tener al menos {min_length} caracteres")
        
        if len(text) > max_length:
            raise ValidationError(f"El texto no puede exceder {max_length} caracteres")
        
        # Verificar caracteres de control peligrosos
        control_chars = ['\x00', '\x08', '\x0b', '\x0c', '\x0e', '\x0f']
        for char in control_chars:
            if char in text:
                raise ValidationError("El texto contiene caracteres de control no permitidos")
        
        return True

def sanitize_log_data(data: Any) -> Any:
    """
    Sanitiza datos antes de escribir logs para evitar inyección de logs.
    """
    if isinstance(data, str):
        # Remover caracteres de control y saltos de línea
        sanitized = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', data)
        sanitized = sanitized.replace('\n', ' ').replace('\r', ' ')
        return sanitized
    elif isinstance(data, dict):
        return {k: sanitize_log_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_log_data(item) for item in data]
    else:
        return data
