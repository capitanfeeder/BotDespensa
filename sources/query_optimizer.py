import re
from typing import Tuple, List, Any, Dict

class QueryOptimizer:
    """Optimiza consultas SQL para MySQL"""
    
    @staticmethod
    def fix_markdown_artifacts(query: str) -> str:
        """
        Remueve artefactos de markdown de las consultas generadas por IA.
        """
        # Remover bloques de código markdown
        query = re.sub(r'```\w*\n?', '', query)
        query = re.sub(r'\n?```', '', query)
        
        # Remover comentarios de markdown
        query = re.sub(r'<!--.*?-->', '', query, flags=re.DOTALL)
        
        # Limpiar espacios extra
        query = ' '.join(query.split())
        
        return query.strip()
    
    @staticmethod
    def fix_sql_syntax_issues(query: str) -> str:
        """
        Corrige problemas comunes de sintaxis SQL para MySQL.
        """
        # Remover múltiples espacios
        query = re.sub(r'\s+', ' ', query).strip()
        
        # Corregir "; LIMIT" a " LIMIT;"
        query = re.sub(r';\s*LIMIT\s+(\d+)', r' LIMIT \1;', query, flags=re.IGNORECASE)
        
        # Corregir múltiples punto y coma
        query = re.sub(r';+', ';', query)
        
        # Remover punto y coma al final si existe (MySQL lo acepta con o sin él)
        query = query.rstrip(';')
        
        return query
    
    @staticmethod
    def add_error_resilience(query: str) -> str:
        """
        Agrega elementos de resistencia a errores en la consulta.
        """
        query = query.strip()
        
        # Si no tiene LIMIT, agregar uno razonable
        if not re.search(r'\bLIMIT\s+\d+', query, re.IGNORECASE):
            query += ' LIMIT 1000'
        
        return query
    
    @staticmethod
    def validate_and_enhance_query(query: str, table_name: str) -> Tuple[str, List[str]]:
        """
        Valida y mejora una consulta SQL para MySQL.
        
        Returns:
            Tupla con (consulta_mejorada, lista_de_warnings)
        """
        warnings = []
        enhanced_query = query
        
        # 1. Limpiar artefactos de markdown
        cleaned_query = QueryOptimizer.fix_markdown_artifacts(enhanced_query)
        if cleaned_query != enhanced_query:
            warnings.append("Se removieron artefactos de markdown de la consulta")
            enhanced_query = cleaned_query
        
        # 2. Agregar resistencia a errores
        resilient_query = QueryOptimizer.add_error_resilience(enhanced_query)
        if resilient_query != enhanced_query:
            warnings.append("Se agregaron elementos de resistencia a errores")
            enhanced_query = resilient_query
        
        # 3. Corregir problemas de sintaxis
        syntax_fixed_query = QueryOptimizer.fix_sql_syntax_issues(enhanced_query)
        if syntax_fixed_query != enhanced_query:
            warnings.append("Se corrigieron problemas de sintaxis SQL")
            enhanced_query = syntax_fixed_query
        
        return enhanced_query, warnings


def diagnose_query_error(error_message: str, query: str) -> Dict[str, Any]:
    """
    Diagnostica errores en consultas SQL de MySQL y proporciona sugerencias.
    """
    diagnosis = {
        "error_type": "unknown",
        "description": "",
        "suggestions": [],
        "auto_fix_available": False
    }
    
    error_lower = error_message.lower()
    
    # Error de sintaxis con markdown
    if "```sql" in error_message or "```" in error_message:
        diagnosis.update({
            "error_type": "markdown_artifacts",
            "description": "La consulta contiene artefactos de markdown",
            "suggestions": [
                "Remover los bloques de código markdown (```)",
                "Limpiar la consulta antes de ejecutar"
            ],
            "auto_fix_available": True
        })
    
    # Error de columna desconocida
    elif "unknown column" in error_lower:
        diagnosis.update({
            "error_type": "unknown_column",
            "description": "Columna no existe o nombre incorrecto",
            "suggestions": [
                "Verificar el nombre de la columna",
                "Revisar mayúsculas/minúsculas si es relevante",
                "Consultar la estructura de la tabla"
            ],
            "auto_fix_available": False
        })
    
    # Error de sintaxis general
    elif "syntax error" in error_lower or "you have an error in your sql syntax" in error_lower:
        diagnosis.update({
            "error_type": "syntax_error",
            "description": "Error de sintaxis SQL",
            "suggestions": [
                "Verificar paréntesis balanceados",
                "Revisar comillas y caracteres especiales",
                "Validar estructura de la consulta"
            ],
            "auto_fix_available": False
        })
    
    # Error de tabla no existe
    elif "table" in error_lower and ("doesn't exist" in error_lower or "not found" in error_lower):
        diagnosis.update({
            "error_type": "table_not_found",
            "description": "La tabla especificada no existe",
            "suggestions": [
                "Verificar el nombre de la tabla",
                "Confirmar que la tabla existe en la base de datos",
                "Revisar permisos de acceso"
            ],
            "auto_fix_available": False
        })
    
    return diagnosis
