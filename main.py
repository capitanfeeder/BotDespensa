import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sources.assistant import process_question
from sources.validators import InputValidator, ValidationError
from sources.table_info import get_all_tables_list, get_db_info, get_cache_stats, clear_cache

# Carga variables de entorno (credenciales MySQL, etc.)
load_dotenv('cred.env')

# Inicializa FastAPI
app = FastAPI(
    title="Bot Despensa API", 
    version="2.0.0",
    description="API inteligente para consultas sobre despensa usando IA"
)

# Modelo para la entrada del usuario
class ChatInput(BaseModel):
    message: str = Field(..., min_length=5, max_length=500, description="Pregunta del usuario")

@app.post("/chat")
async def chat(input: ChatInput):
    """
    Endpoint para procesar preguntas sobre la despensa.
    El sistema detecta automáticamente la tabla relevante y genera consultas SQL inteligentes.
    """
    try:
        # Validar entrada
        InputValidator.validate_text_input(input.message, max_length=500, min_length=5)
        
        # Procesar pregunta
        response = process_question(input.message)
        
        return {"response": response.get("answer", "No se pudo generar una respuesta")}
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Error de validación: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando el mensaje: {str(e)}")

@app.get("/health")
async def health():
    """
    Endpoint para verificar el estado del servicio.
    """
    return {"status": "ok", "service": "Bot Despensa", "version": "2.0.0"}

@app.get("/tables")
async def list_tables():
    """
    Endpoint para listar todas las tablas disponibles en la base de datos.
    """
    try:
        tables = get_all_tables_list()
        return {
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo tablas: {str(e)}")

@app.get("/tables/info")
async def get_database_structure():
    """
    Endpoint para obtener la estructura completa de todas las tablas.
    """
    try:
        db_info = get_db_info()
        return {
            "database_structure": db_info,
            "total_tables": len(db_info)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo estructura: {str(e)}")

@app.get("/cache/stats")
async def cache_statistics():
    """
    Endpoint para obtener estadísticas del caché.
    """
    try:
        stats = get_cache_stats()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo stats: {str(e)}")

@app.post("/cache/clear")
async def clear_cache_endpoint():
    """
    Endpoint para limpiar el caché manualmente.
    """
    try:
        clear_cache()
        return {"message": "Caché limpiado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error limpiando caché: {str(e)}")

@app.get("/debug/test-query")
async def test_query(table: str = "clientes", question: str = "¿Cuántos registros hay?"):
    """
    Endpoint de prueba para verificar consultas directas a una tabla.
    """
    from sources.assistant import generate_query, execute_query
    from sources.table_info import get_table_info, get_table_sample
    
    try:
        # Obtener info de la tabla
        table_info = get_table_info(table)
        table_sample = get_table_sample(table, limit=10)
        
        # Generar query
        query = generate_query(table, question, table_info, table_sample)
        
        # Ejecutar query
        result = execute_query(query, table)
        
        return {
            "table": table,
            "question": question,
            "query_generated": query,
            "result": result,
            "table_structure": table_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en test: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
