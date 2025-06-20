from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
import psycopg2.extras
import jwt
import hashlib
from datetime import datetime, timedelta

# Configuration 
SECRET_KEY = "accident-api-secret-key"
ALGORITHM = "HS256"

# DB Config
DB_CONFIG = {
    "host": "localhost",
    "database": "accident_viz", 
    "user": "postgres",
    "password": "password",
    "port": 5432
}

# FastAPI avec Swagger auto
app = FastAPI(
    title="🚗 Accident Analysis API",
    description="API sécurisée avec pagination pour l'analyse des accidents",
    version="1.0.0",
    docs_url="/docs", 
    redoc_url="/redoc"
)

security = HTTPBearer()

# Models 
class UserLogin(BaseModel):
    username: str
    password: str
    
    class Config:
        schema_extra = {
            "example": {
                "username": "admin",
                "password": "admin"
            }
        }

class Token(BaseModel):
    access_token: str
    token_type: str

class StateStats(BaseModel):
    state: str
    total_accidents: int
    avg_severity: float
    cities_count: int

class PaginatedResponse(BaseModel):
    data: List[dict]
    page: int
    size: int
    total_pages: int
    total_items: int

# Utilisateurs
USERS = {
    "admin": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",  # admin
    "user": "04f8996da763b7a969b1028ee3007569eaf3a635486ddab211d512c85b9df8fb"   # user123
}

def verify_user(username: str, password: str) -> bool:
    """Vérifie utilisateur"""
    hashed = hashlib.sha256(password.encode()).hexdigest()
    return USERS.get(username) == hashed

def create_token(username: str) -> str:
    """Crée token JWT"""
    payload = {
        "sub": username,
        "exp": datetime.utcnow() + timedelta(hours=1)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Vérifie token"""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Token invalide")
        return username
    except:
        raise HTTPException(status_code=401, detail="Token invalide")

def get_db():
    """Connexion DB simple"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur DB: {str(e)}")

def paginate_query(query: str, page: int, size: int, params=None):
    """Pagination simple"""
    offset = (page - 1) * size
    paginated_query = f"{query} LIMIT {size} OFFSET {offset}"
    
    conn = get_db()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Exécuter requête paginée
    if params:
        cursor.execute(paginated_query, params)
    else:
        cursor.execute(paginated_query)
    
    results = cursor.fetchall()
    
    # Compter total
    count_query = f"SELECT COUNT(*) FROM ({query.split('LIMIT')[0]}) as count_table"
    if params:
        cursor.execute(count_query, params)
    else:
        cursor.execute(count_query)
    
    total_items = cursor.fetchone()['count']
    
    cursor.close()
    conn.close()
    
    total_pages = (total_items + size - 1) // size
    
    return {
        "data": [dict(row) for row in results],
        "page": page,
        "size": size,
        "total_pages": total_pages,
        "total_items": total_items
    }

# Routes

@app.get("/", tags=["Info"])
async def root():
    """Page d'accueil de l'API"""
    return {
        "message": "🚗 Accident Analysis API",
        "version": "1.0.0",
        "swagger": "/docs",
        "login_endpoint": "/auth/login",
        "example_users": {
            "admin": "admin",
            "user": "user123"
        }
    }

@app.post("/auth/login", response_model=Token, tags=["Authentification"])
async def login(user: UserLogin):
    """
    Connexion utilisateur
    
    Utilisateurs de test:
    - admin / admin
    - user / user123
    """
    if not verify_user(user.username, user.password):
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    
    token = create_token(user.username)
    return {"access_token": token, "token_type": "bearer"}

@app.get("/api/states", response_model=PaginatedResponse, tags=["Données"])
async def get_states(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(10, ge=1, le=100, description="Taille de page"),
    current_user: str = Depends(verify_token)
):
    """
    Statistiques par état avec pagination
    
    - **page**: Numéro de page (défaut: 1)
    - **size**: Nombre d'éléments par page (défaut: 10, max: 100)
    """
    query = """
        SELECT state, total_accidents, avg_severity, cities_count
        FROM state_stats 
        ORDER BY total_accidents DESC
    """
    
    return paginate_query(query, page, size)

@app.get("/api/weather", response_model=PaginatedResponse, tags=["Données"])
async def get_weather(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(10, ge=1, le=50, description="Taille de page"),
    current_user: str = Depends(verify_token)
):
    """
    Analyse météorologique avec pagination
    """
    query = """
        SELECT weather_condition, total_accidents, avg_severity, avg_temperature
        FROM weather_analysis 
        ORDER BY total_accidents DESC
    """
    
    return paginate_query(query, page, size)

@app.get("/api/cities", response_model=PaginatedResponse, tags=["Données"])
async def get_cities(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(20, ge=1, le=100, description="Taille de page"),
    state: Optional[str] = Query(None, description="Filtrer par état (ex: CA)"),
    current_user: str = Depends(verify_token)
):
    """
    Hotspots des villes avec pagination et filtre
    
    - **state**: Code état pour filtrer (optionnel)
    """
    if state:
        query = """
            SELECT state, city, total_accidents, avg_severity, latitude, longitude
            FROM city_hotspots 
            WHERE state = %s
            ORDER BY total_accidents DESC
        """
        return paginate_query(query, page, size, (state.upper(),))
    else:
        query = """
            SELECT state, city, total_accidents, avg_severity, latitude, longitude
            FROM city_hotspots 
            ORDER BY total_accidents DESC
        """
        return paginate_query(query, page, size)

@app.get("/api/time", response_model=PaginatedResponse, tags=["Données"])
async def get_time_patterns(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(24, ge=1, le=50, description="Taille de page"),
    current_user: str = Depends(verify_token)
):
    """
    Patterns temporels avec pagination
    """
    query = """
        SELECT hour, day_of_week, total_accidents, avg_severity
        FROM time_patterns 
        ORDER BY total_accidents DESC
    """
    
    return paginate_query(query, page, size)

@app.get("/api/ml/severity-predictions", response_model=PaginatedResponse, tags=["Machine Learning"])
async def get_severity_predictions(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(10, ge=1, le=50, description="Taille de page"),
    current_user: str = Depends(verify_token)
):
    """
    Prédictions de sévérité ML avec pagination
    """
    query = """
        SELECT severity, prediction, start_lat, start_lng, model_type, accuracy, training_timestamp
        FROM ml_severity_predictions 
        ORDER BY training_timestamp DESC
    """
    
    return paginate_query(query, page, size)

@app.get("/api/ml/risk-zones", response_model=PaginatedResponse, tags=["Machine Learning"])
async def get_risk_zones(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(10, ge=1, le=50, description="Taille de page"),
    current_user: str = Depends(verify_token)
):
    """
    Zones à risque ML avec pagination
    """
    query = """
        SELECT risk_zone, accident_count, avg_severity, center_lat, center_lng, risk_level
        FROM ml_risk_zones 
        ORDER BY accident_count DESC
    """
    
    return paginate_query(query, page, size)

@app.get("/api/ml/hotspots", response_model=PaginatedResponse, tags=["Machine Learning"])
async def get_ml_hotspots(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(10, ge=1, le=50, description="Taille de page"),
    current_user: str = Depends(verify_token)
):
    """
    Hotspots géographiques ML avec pagination
    """
    query = """
        SELECT lat_grid, lng_grid, total_accidents, avg_severity, hotspot_score
        FROM ml_hotspots 
        ORDER BY hotspot_score DESC
    """
    
    return paginate_query(query, page, size)

@app.get("/api/summary", tags=["Statistiques"])
async def get_summary(current_user: str = Depends(verify_token)):
    """
    Résumé général des données
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("SELECT SUM(total_accidents) FROM state_stats")
        total_accidents = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM state_stats")
        total_states = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM city_hotspots")
        total_cities = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "total_accidents": total_accidents,
            "total_states": total_states,
            "total_cities": total_cities,
            "user": current_user,
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)