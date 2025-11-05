# BotDespensa

Bot inteligente para gestión de despensa usando IA y MySQL.

## Características

- ✅ Generación automática de consultas SQL desde lenguaje natural
- ✅ Limpieza automática de artefactos markdown
- ✅ Validación de seguridad para prevenir inyecciones SQL
- ✅ Optimización automática de consultas
- ✅ Diagnóstico inteligente de errores
- ✅ Pool de conexiones a MySQL
- ✅ API REST con FastAPI

## Requisitos

- Python 3.11+
- MySQL Server
- Ollama con modelo `gemma3:1b`

## Instalación

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Configurar variables de entorno en `cred.env`:
```env
HOST=localhost
PORT=3306
DATABASE=nombre_bd
USER=usuario
PASSWORD=contraseña
```

3. Asegurarse de tener Ollama corriendo con el modelo:
```bash
ollama pull gemma3:1b
```

## Uso

Iniciar el servidor:
```bash
python main.py
```

El servidor estará disponible en `http://127.0.0.1:8000`

### Endpoints

**POST /chat**
```json
{
  "message": "¿Cuántos productos hay?"
}
```

**GET /health**
Verifica el estado del servicio

## Arquitectura

```
sources/
├── assistant.py        # Lógica principal del asistente
├── db_connect.py       # Pool de conexiones MySQL
├── query_optimizer.py  # Optimización de consultas
├── validators.py       # Validación de entrada
└── utils.py           # Utilidades generales
```

## Mejoras Implementadas

- **Query Optimizer**: Limpia markdown y optimiza consultas automáticamente
- **Validators**: Previene inyecciones SQL y valida entradas
- **Error Diagnosis**: Diagnóstico inteligente de errores con sugerencias
- **Connection Pool**: Pool de conexiones eficiente para MySQL
- **Clean Architecture**: Separación de responsabilidades inspirada en Litic-IA