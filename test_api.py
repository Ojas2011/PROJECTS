import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_event_ingestion_and_trace():
    payload = {
        "advisor_id": "advisor-007",
        "agent_id": "agent-022",
        "task_id": "task-1012",
        "event_type": "tool_completed",
        "status": "completed",
        "timestamp": "2026-10-02T17:00:00+00:00",
        "tool_name": "get_portfolio",
        "duration_ms": 1200,
        "error_message": None,
        "metadata": {"client_id": "client-1001"}
    }
    
    # Send test event
    res = client.post("/events", json=payload)
    assert res.status_code == 201
    assert "event_id" in res.json()

    # Retrieve execution trace
    trace_res = client.get("/tasks/task-1012/events")
    assert trace_res.status_code == 200
    assert len(trace_res.json()) > 0
    assert trace_res.json()[0]["task_id"] == "task-1012"