import os

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import PyMongoError


app = Flask(__name__)


# ============================================================
# CONFIGURACIÓN
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongo:27017"
)

DATABASE_NAME = "crime_db"

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000
)

db = client[DATABASE_NAME]

crimes_collection = db["crimes"]
hotspots_collection = db["crime_hotspots_grid"]
monthly_collection = db["crime_monthly_area_stats"]


# ============================================================
# UTILIDADES
# ============================================================

def serialize_document(document):
    """
    Convierte ObjectId y otros valores BSON a tipos serializables.
    """

    if "_id" in document:
        document["_id"] = str(document["_id"])

    return document


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
def health():
    try:
        client.admin.command("ping")

        return jsonify({
            "status": "ok",
            "database": "connected"
        }), 200

    except PyMongoError as error:
        return jsonify({
            "status": "error",
            "message": str(error)
        }), 500


# ============================================================
# CONSULTA $near
# ============================================================

@app.get("/near")
def near():

    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
        radius = float(request.args.get("radius", 1000))

    except (TypeError, ValueError):
        return jsonify({
            "error": (
                "Los parámetros lat, lon y radius "
                "deben ser numéricos."
            )
        }), 400

    if lat < -90 or lat > 90:
        return jsonify({
            "error": "Latitud fuera de rango."
        }), 400

    if lon < -180 or lon > 180:
        return jsonify({
            "error": "Longitud fuera de rango."
        }), 400

    if radius <= 0:
        return jsonify({
            "error": "El radio debe ser mayor que cero."
        }), 400

    query = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "$maxDistance": radius
            }
        }
    }

    try:

        results = list(
            crimes_collection
            .find(query)
            .limit(100)
        )

        results = [
            serialize_document(doc)
            for doc in results
        ]

        return jsonify({
            "query": {
                "latitude": lat,
                "longitude": lon,
                "radius_meters": radius
            },
            "count": len(results),
            "results": results
        }), 200

    except PyMongoError as error:

        return jsonify({
            "error": str(error)
        }), 500


# ============================================================
# CONSULTA $geoWithin
# ============================================================

@app.post("/polygon")
def polygon():

    data = request.get_json(silent=True)

    if not data:
        return jsonify({
            "error": "Se requiere un cuerpo JSON."
        }), 400

    polygon_data = data.get("polygon")

    if not polygon_data:
        return jsonify({
            "error": "Se requiere el campo polygon."
        }), 400

    if polygon_data.get("type") != "Polygon":
        return jsonify({
            "error": (
                "El objeto enviado debe ser "
                "GeoJSON de tipo Polygon."
            )
        }), 400

    coordinates = polygon_data.get("coordinates")

    if not coordinates:
        return jsonify({
            "error": (
                "El polígono debe contener "
                "coordenadas."
            )
        }), 400

    query = {
        "location": {
            "$geoWithin": {
                "$geometry": polygon_data
            }
        }
    }

    try:

        results = list(
            crimes_collection
            .find(query)
            .limit(100)
        )

        results = [
            serialize_document(doc)
            for doc in results
        ]

        return jsonify({
            "count": len(results),
            "results": results
        }), 200

    except PyMongoError as error:

        return jsonify({
            "error": str(error)
        }), 500


# ============================================================
# CONSULTA $geoNear
# ============================================================

@app.get("/geonear")
def geonear():

    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
        radius = float(request.args.get("radius", 1000))

    except (TypeError, ValueError):
        return jsonify({
            "error": (
                "Los parámetros lat, lon y radius "
                "deben ser numéricos."
            )
        }), 400

    pipeline = [
        {
            "$geoNear": {
                "near": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "distanceField": "distance_meters",
                "maxDistance": radius,
                "spherical": True
            }
        },
        {
            "$limit": 100
        },
        {
            "$project": {
                "_id": 0,
                "crime_description": 1,
                "area_name": 1,
                "address": 1,
                "location": 1,
                "distance_meters": 1
            }
        }
    ]

    try:

        results = list(
            crimes_collection.aggregate(pipeline)
        )

        return jsonify({
            "query": {
                "latitude": lat,
                "longitude": lon,
                "radius_meters": radius
            },
            "count": len(results),
            "results": results
        }), 200

    except PyMongoError as error:

        return jsonify({
            "error": str(error)
        }), 500


# ============================================================
# RESULTADOS SPARK - HOTSPOTS
# ============================================================

@app.get("/analytics/hotspots")
def analytics_hotspots():

    try:
        limit = int(request.args.get("limit", 20))

    except ValueError:
        return jsonify({
            "error": "limit debe ser un número entero."
        }), 400

    limit = max(
        1,
        min(limit, 100)
    )

    try:

        results = list(
            hotspots_collection
            .find(
                {},
                {
                    "_id": 0
                }
            )
            .sort(
                "total_crimes",
                -1
            )
            .limit(limit)
        )

        return jsonify({
            "count": len(results),
            "results": results
        }), 200

    except PyMongoError as error:

        return jsonify({
            "error": str(error)
        }), 500


# ============================================================
# RESULTADOS SPARK - TEMPORAL
# ============================================================

@app.get("/analytics/monthly")
def analytics_monthly():

    year = request.args.get("year")
    month = request.args.get("month")

    query = {}

    if year:

        try:
            query["occ_year"] = int(year)

        except ValueError:
            return jsonify({
                "error": "year debe ser un número entero."
            }), 400

    if month:
        query["occ_month"] = month

    try:

        results = list(
            monthly_collection
            .find(
                query,
                {
                    "_id": 0
                }
            )
            .sort(
                "total_crimes",
                -1
            )
            .limit(100)
        )

        return jsonify({
            "filters": {
                "year": year,
                "month": month
            },
            "count": len(results),
            "results": results
        }), 200

    except PyMongoError as error:

        return jsonify({
            "error": str(error)
        }), 500


# ============================================================
# INICIO LOCAL
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )