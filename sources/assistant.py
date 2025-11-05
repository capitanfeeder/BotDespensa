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

# Cliente LLM global
llm = OllamaLLM(model="gemma3:1b", temperature=0.3)

def detect_relevant_table(question: str) -> str:
    """
    Detecta qué tabla es relevante para la pregunta del usuario analizando
    la estructura completa de la base de datos con IA.
    
    Args:
        question: Pregunta del usuario
        
    Returns:
        Nombre de la tabla relevante
    """
    from sources.table_info import get_db_info
    
    # Obtener estructura completa de la BD con columnas
    db_info = get_db_info()
    
    if not db_info:
        raise Exception("No se pudo obtener información de la base de datos")
    
    # Si solo hay una tabla, usarla directamente
    if len(db_info) == 1:
        table_name = list(db_info.keys())[0]
        print(f"✅ Solo hay una tabla: {table_name}")
        return table_name
    
    # Construir descripción de tablas con sus columnas
    tables_description = []
    for table_name, info in db_info.items():
        columns = info.get('columns', [])
        column_names = [col[0] for col in columns]
        tables_description.append(f"- {table_name}: columnas({', '.join(column_names)})")
    
    tables_text = '\n'.join(tables_description)
    
    print(f"📊 Analizando estructura completa de BD...")
    
    # Usar el LLM con contexto completo de la estructura
    prompt = f"""Pregunta del usuario: "{question}"

Estructura de la base de datos:
{tables_text}

Analiza la pregunta y determina qué tabla es la más relevante basándote en:
1. Las palabras en la pregunta (cliente, producto, marca, etc.)
2. Los nombres de las columnas en cada tabla
3. El contexto de lo que se pregunta

Responde SOLO con el nombre exacto de UNA tabla de la lista, sin puntos, comillas ni explicaciones.

Nombre de la tabla:"""

    try:
        response = llm.invoke(prompt).strip()
        
        # Limpiar la respuesta
        response = response.replace('"', '').replace("'", '').replace('.', '').replace(':', '').strip()
        
        # Buscar coincidencia exacta
        if response in db_info:
            print(f"🎯 Tabla detectada: {response}")
            return response
        
        # Buscar coincidencia parcial (case-insensitive)
        response_lower = response.lower()
        for table_name in db_info.keys():
            if table_name.lower() == response_lower:
                print(f"🎯 Tabla detectada: {table_name}")
                return table_name
        
        # Si no encuentra coincidencia, usar la primera tabla
        first_table = list(db_info.keys())[0]
        print(f"⚠️ No se detectó tabla clara, usando: {first_table}")
        return first_table
        
    except Exception as e:
        first_table = list(db_info.keys())[0]
        print(f"⚠️ Error detectando tabla: {e}, usando: {first_table}")
        return first_table


def generate_query(table_name: str, question: str, table_info: dict, table_sample: dict = None) -> str:
    """
    Genera una consulta SQL que responde a una pregunta relacionada con la tabla especificada.
    
    Args:
        table_name: Nombre de la tabla
        question: Pregunta del usuario
        table_info: Información de la estructura de la tabla
        table_sample: Muestra de datos de la tabla (opcional)
        
    Returns:
        Consulta SQL generada
    """
    # Obtener las columnas de la tabla
    columns = table_info.get('columns', [])
    columns_str = ', '.join([f"{col[0]} ({col[1]})" for col in columns])
    column_names = [col[0] for col in columns]
    
    # Crear contexto de ejemplo si hay muestra de datos
    sample_context = ""
    if table_sample and table_name in table_sample:
        sample_context = "\nEjemplos de valores en las columnas:\n"
        for col_name, values in table_sample[table_name].items():
            if values:
                sample_context += f"  - {col_name}: {', '.join(values[:5])}\n"
    
    # Crear el prompt para generar SQL
    prompt = f"""Genera UNA consulta SQL para MySQL que responda esta pregunta: {question}

Tabla: {table_name}
Columnas disponibles:
{columns_str}
{sample_context}

REGLAS CRÍTICAS:
1. Escribe SOLO la consulta SQL, sin bloques markdown (sin ```sql o ```)
2. NUNCA uses columnas ID con valores de texto
3. Para buscar por nombre/texto, usa columnas que contengan 'nombre' en su nombre
4. Los IDs (id_*) son SIEMPRE numéricos
5. Usa LIKE '%valor%' para búsquedas de texto
6. Usa backticks (`) para nombres de tablas y columnas en MySQL
7. NO incluyas LIMIT, se agregará automáticamente
8. Revisa las columnas disponibles antes de usarlas

Ejemplos correctos:
- "cuántos productos": SELECT COUNT(*) as total FROM `{table_name}`
- "productos de Pepsi": SELECT * FROM `{table_name}` WHERE `nombre_marca` LIKE '%Pepsi%'
- "categorías": SELECT DISTINCT `nombre_categoria` FROM `{table_name}`

Columnas disponibles: {', '.join([f'`{col}`' for col in column_names])}

Genera SOLO la consulta SQL:"""

    try:
        response = llm.invoke(prompt)
        
        # Limpiar la respuesta de markdown y espacios
        query = response.strip()
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


def execute_query(query: str, table_name: str = None) -> str:
    """
    Ejecuta una consulta SQL y devuelve el resultado en formato JSON.
    Incluye optimización automática y manejo inteligente de errores.
    
    Args:
        query: Consulta SQL a ejecutar
        table_name: Nombre de la tabla (opcional)
        
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
        optimized_query, warnings = QueryOptimizer.validate_and_enhance_query(query, table_name or "")
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
                        return execute_query(fixed_query, table_name)
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
        # Limpiar respuesta de posibles frases innecesarias
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
    Detecta automáticamente la tabla relevante y obtiene su estructura.
    
    Args:
        question: Pregunta del usuario
        
    Returns:
        Diccionario con la respuesta
    """
    try:
        # Validar entrada
        InputValidator.validate_text_input(question, max_length=500, min_length=5)
        
        print(f"❓ Procesando pregunta: {question}")
        
        # Detectar tabla relevante
        table_name = detect_relevant_table(question)
        print(f"📊 Tabla detectada: {table_name}")
        
        # Obtener estructura de la tabla desde la BD (con caché)
        table_info = get_table_info(table_name)
        print(f"✅ Estructura obtenida: {len(table_info.get('columns', []))} columnas")
        
        # Obtener muestra de datos (opcional, con caché)
        try:
            table_sample = get_table_sample(table_name, limit=50)
        except Exception as e:
            print(f"⚠️ No se pudo obtener muestra: {e}")
            table_sample = None
        
        # Generar consulta SQL con contexto completo
        query = generate_query(table_name, question, table_info, table_sample)
        
        # Ejecutar consulta
        result = execute_query(query, table_name)
        
        # Transformar respuesta
        response = transform_response(question, result)
        
        return response
        
    except Exception as e:
        print(f"❌ Error en process_question: {e}")
        return {"answer": f"Error procesando tu pregunta: {str(e)}"}
