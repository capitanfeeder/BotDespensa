import os
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv('cred.env')

db_host = os.getenv("HOST")
db_port = os.getenv("PORT")
db_name = os.getenv("DATABASE")
db_user = os.getenv("USER")
db_password = os.getenv("PASSWORD")

# Validar que todas las variables estén presentes
required_vars = [db_host, db_port, db_name, db_user, db_password]
if not all(required_vars):
    missing_vars = [var for var, val in zip(['HOST', 'PORT', 'DATABASE', 'USER', 'PASSWORD'], required_vars) if not val]
    raise ValueError(f"Faltan variables de entorno de base de datos: {', '.join(missing_vars)}")

# Construir URL de conexión para MySQL
db_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Engine global con pool de conexiones
_engine = None

def get_db_engine():
    """
    Crea y devuelve un motor de base de datos con pool de conexiones.
    Utiliza patrón singleton para reutilizar el mismo engine.
    """
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=5,           # Número de conexiones permanentes
                max_overflow=10,       # Conexiones adicionales permitidas
                pool_pre_ping=True,    # Verifica conexiones antes de usar
                pool_recycle=3600,     # Recicla conexiones cada hora
                echo=False             # Cambiar a True para debug SQL
            )
            
            # Probar conexión inmediatamente
            test_conn = _engine.connect()
            test_conn.close()
            
        except Exception as e:
            raise Exception(f"Error al crear el engine de base de datos: {e}")
        
    return _engine

def get_db_connection():
    """
    Obtiene una conexión del pool de forma segura.
    
    Returns:
        Connection: Una conexión de base de datos del pool
    """
    try:
        engine = get_db_engine()
        connection = engine.connect()
        return connection
    except Exception as e:
        raise Exception(f"Error obteniendo conexión: {e}")

def close_db_engine():
    """
    Cierra el engine y todas las conexiones del pool.
    Útil para cleanup en shutdown de la aplicación.
    """
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
