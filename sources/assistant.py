import os
import re
import json
import decimal
import datetime
from dotenv import load_dotenv
from langchain_ollama import OllamaLLM
from langchain_community.utilities import SQLDatabase
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sources.db_connect import get_db_connection
from sources.validators import SQLValidator, InputValidator
from sources.query_optimizer import QueryOptimizer, diagnose_query_error
from sources.table_info import get_table_info, get_table_sample, extract_column_names, format_table_structure, get_all_tables_list

# Cargar las variables de entorno
load_dotenv('cred.env')

qwen = "qwen3:1.7b"
#qwen = "qwen3-coder:480b-cloud"

# Cliente LLM global
llm = OllamaLLM(model=qwen, temperature=0.5)

def generate_query(question: str, db_structure: dict) -> str:
    """
    Genera una consulta SQL que responde a una pregunta usando toda la estructura de la BD.
    El modelo decide qué tabla(s) usar basándose en el contexto completo.
    
    Args:
        question: Pregunta del usuario
        db_structure: Estructura completa de la base de datos con todas las tablas y columnas
        
    Returns:
        Consulta SQL generada
    """
    # Construir descripción completa de la BD
    db_description = []
    for table_name, table_info in db_structure.items():
        columns = table_info.get('columns', [])
        columns_str = ', '.join([f"`{col[0]}` ({col[1]})" for col in columns])
        db_description.append(f"Tabla `{table_name}`:\n  Columnas: {columns_str}")
    
    db_text = '\n\n'.join(db_description)
    
    # Crear el prompt para generar SQL
    prompt = f"""Genera UNA consulta SQL para MySQL que responda esta pregunta: {question}

ESTRUCTURA COMPLETA DE LA BASE DE DATOS:
{db_text}

REGLAS CRÍTICAS:
1. Escribe SOLO la consulta SQL, sin bloques markdown (sin ```sql o ```)
2. Elige automáticamente la tabla más relevante según la pregunta
3. NUNCA uses columnas ID con valores de texto
4. Para buscar por nombre/texto, usa columnas que contengan 'nombre' en su nombre
5. Los IDs (id_*) son SIEMPRE numéricos
6. Usa LIKE '%valor%' para búsquedas de texto
7. Usa backticks (`) para nombres de tablas y columnas en MySQL
8. NO incluyas LIMIT, se agregará automáticamente
9. Si necesitas JOIN entre tablas, hazlo usando las columnas ID apropiadas
10. Revisa las columnas disponibles antes de usarlas

Ejemplos correctos:
- "cuántos productos": SELECT COUNT(*) as total FROM `productos`
- "productos de Pepsi": SELECT * FROM `productos` WHERE `nombre_marca` LIKE '%Pepsi%'
- "clientes de Madrid": SELECT * FROM `clientes` WHERE `ciudad` LIKE '%Madrid%'

Genera SOLO la consulta SQL:"""

    try:
        response = llm.invoke(prompt)
        
        # Limpiar la respuesta de markdown, espacios y tags <think>
        query = response.strip()
        query = QueryOptimizer.remove_think_tags(query)
        query = QueryOptimizer.fix_markdown_artifacts(query)
        
        # Eliminar comillas simples alrededor del resultado si existen
        if query.startswith("'") and query.endswith("'"):
            query = query[1:-1]
        
        # Eliminar saltos de línea múltiples
        query = ' '.join(query.split())
        
        print(f"📝 Query generada: {query}")
        
        return query
        
    except Exception as e:
        raise Exception(f"Error generando consulta: {e}")


def execute_query(query: str) -> str:
    """
    Ejecuta una consulta SQL y devuelve el resultado en formato JSON.
    Incluye optimización automática y manejo inteligente de errores.
    
    Args:
        query: Consulta SQL a ejecutar
        
    Returns:
        Resultado de la consulta en formato JSON
    """
    # Validar que no haya comandos peligrosos
    forbidden_words = ["DELETE", "DROP", "ALTER", "TRUNCATE", "INSERT", "UPDATE", "CREATE", "RENAME", "REVOKE", "GRANT"]
    for word in forbidden_words:
        if re.search(word, query, re.IGNORECASE):
            return json.dumps({"error": "Disculpa, no tengo permisos para hacer eso."})

    # Optimizar consulta antes de ejecutar
    try:
        optimized_query, warnings = QueryOptimizer.validate_and_enhance_query(query, "")
        if warnings:
            print(f"⚠️ Advertencias: {warnings}")
    except Exception as e:
        print(f"⚠️ Error en optimización: {e}")
        optimized_query = query

    connection = None
    try:
        connection = get_db_connection()
        
        result = connection.execute(text(optimized_query))
        rows = result.fetchall()
        columns = result.keys()

        # Convertir las filas en una lista de diccionarios
        result_list = [dict(zip(columns, row)) for row in rows]

        # Convertir valores especiales a tipos serializables
        for row in result_list:
            for key, value in row.items():
                if isinstance(value, decimal.Decimal):
                    row[key] = float(value)
                elif isinstance(value, datetime.datetime):
                    row[key] = value.isoformat()
                elif isinstance(value, datetime.date):
                    row[key] = value.isoformat()

        result_json = json.dumps(result_list, separators=(',', ':'))
        return result_json

    except SQLAlchemyError as error:
        error_message = f"Error al ejecutar la consulta: {error}"
        
        # Diagnosticar el error
        diagnosis = diagnose_query_error(str(error), optimized_query)
        
        # Si hay auto-fix disponible, intentar una vez más
        if diagnosis["auto_fix_available"] and optimized_query == query:
            try:
                if diagnosis["error_type"] == "markdown_artifacts":
                    fixed_query = QueryOptimizer.fix_markdown_artifacts(query)
                    if fixed_query != query:
                        return execute_query(fixed_query)
            except Exception:
                pass
        
        # Incluir diagnóstico en la respuesta de error
        error_response = {
            "error": str(error),
            "diagnosis": diagnosis,
            "suggestions": diagnosis["suggestions"]
        }
        
        return json.dumps(error_response)
        
    except Exception as error:
        return json.dumps({"error": str(error)})
    finally:
        if connection:
            connection.close()


def transform_response(question: str, result_json: str) -> dict:
    """
    Transforma el resultado de una consulta SQL en una respuesta amigable y conversacional.
    
    Args:
        question: Pregunta del usuario
        result_json: Resultado de la consulta en formato JSON
        
    Returns:
        Respuesta amigable para el usuario
    """
    try:
        result_dict = json.loads(result_json)
    except json.JSONDecodeError:
        result_dict = {"error": "Error procesando resultado"}

    # Verificar si hay errores
    if "error" in result_dict:
        return {"answer": f"Lo siento, hubo un error: {result_dict['error']}"}
    
    # Si el resultado está vacío
    if not result_dict or (isinstance(result_dict, list) and len(result_dict) == 0):
        return {"answer": "No encontré resultados para tu consulta. ¿Quieres que busque algo diferente?"}
    
    # Limitar resultado si es muy largo
    if isinstance(result_dict, list) and len(result_dict) > 50:
        result_dict = result_dict[:50]
        truncated = True
    else:
        truncated = False
    
    # Crear respuesta con LLM
    prompt = f"""Eres un asistente de despensa amigable. Responde la pregunta del usuario basándote ÚNICAMENTE en los datos proporcionados.

Pregunta: {question}
Datos de la base de datos: {json.dumps(result_dict, ensure_ascii=False)}

REGLAS:
1. Responde de forma DIRECTA y NATURAL, como si fueras un humano conversando
2. NO digas cosas como "La respuesta que proporcionaste ya es correcta" o "Espero que esto te sea útil"
3. NO repitas la pregunta
4. Ve directo al grano con la información
5. Si hay números o listas, preséntelos de forma clara
6. Máximo 80 palabras
7. Sé específico con los datos, no inventes nada
{"8. IMPORTANTE: Menciona que solo se muestran los primeros 50 resultados de un total mayor" if truncated else ""}

Ejemplos de BUENAS respuestas:
- "Hay 25 productos en total."
- "Encontré 3 productos de Pepsi: Pepsi Cola, Pepsi Light y Pepsi Zero."
- "Las categorías disponibles son: Bebidas, Snacks, Lácteos y Panadería."

Responde SOLO con la información, sin frases de relleno:"""

    try:
        response = llm.invoke(prompt)
        
        # Limpiar respuesta de tags <think> y posibles frases innecesarias
        response = QueryOptimizer.remove_think_tags(response)
        response = response.strip()
        
        # Remover frases comunes no deseadas
        unwanted_phrases = [
            "La respuesta que proporcionaste ya es correcta.",
            "¡Espero que esto te sea útil!",
            "Espero que esto te ayude.",
            "¿Hay algo más en lo que pueda ayudarte?",
        ]
        
        for phrase in unwanted_phrases:
            response = response.replace(phrase, "").strip()
        
        return {"answer": response}
    except Exception as e:
        # Si falla el LLM, devolver resultado directo
        if isinstance(result_dict, list):
            count = len(result_dict)
            return {"answer": f"Encontré {count} resultado{'s' if count != 1 else ''}."}
        else:
            return {"answer": str(result_dict)}


def process_question(question: str) -> dict:
    """
    Procesa una pregunta del usuario y devuelve la respuesta.
    Pasa toda la estructura de la BD al modelo para que genere la query apropiada.
    
    Args:
        question: Pregunta del usuario
        
    Returns:
        Diccionario con la respuesta
    """
    try:
        # Validar entrada
        InputValidator.validate_text_input(question, max_length=500, min_length=5)
        
        print(f"❓ Procesando pregunta: {question}")
        
        # Obtener estructura COMPLETA de la base de datos
        from sources.table_info import get_db_info
        db_structure = get_db_info()
        
        if not db_structure:
            return {"answer": "No se pudo acceder a la estructura de la base de datos"}
        
        print(f"📊 Estructura BD obtenida: {len(db_structure)} tabla(s)")
        
        # Generar consulta SQL pasando toda la estructura
        query = generate_query(question, db_structure)
        
        # Ejecutar consulta
        result = execute_query(query)
        
        # Transformar respuesta
        response = transform_response(question, result)
        
        return response
        
    except Exception as e:
        print(f"❌ Error en process_question: {e}")
        return {"answer": f"Error procesando tu pregunta: {str(e)}"}
