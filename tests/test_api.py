from api.app import app


def test_health():
    client = app.test_client()

    response = client.get("/health")

    assert response.status_code == 200

    data = response.get_json()

    assert data["status"] == "ok"
    assert data["database"] == "connected"


def test_near():
    client = app.test_client()

    response = client.get(
        "/near?lat=34.0522&lon=-118.2437&radius=1000"
    )

    assert response.status_code == 200

    data = response.get_json()

    assert "count" in data
    assert "results" in data
    assert data["count"] > 0


def test_geonear():
    client = app.test_client()

    response = client.get(
        "/geonear?lat=34.0522&lon=-118.2437&radius=1000"
    )

    assert response.status_code == 200

    data = response.get_json()

    assert "count" in data
    assert "results" in data
    assert data["count"] > 0

    first_result = data["results"][0]

    assert "distance_meters" in first_result


def test_hotspots():
    client = app.test_client()

    response = client.get(
        "/analytics/hotspots?limit=5"
    )

    assert response.status_code == 200

    data = response.get_json()

    assert data["count"] == 5
    assert len(data["results"]) == 5

    first_result = data["results"][0]

    assert "grid_lat" in first_result
    assert "grid_lon" in first_result
    assert "total_crimes" in first_result


def test_monthly():
    client = app.test_client()

    response = client.get(
        "/analytics/monthly?year=2024&month=Jan"
    )

    assert response.status_code == 200

    data = response.get_json()

    assert data["count"] > 0
    assert "results" in data