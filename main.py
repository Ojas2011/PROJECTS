import os
import json
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="AgentWatch Backend API")

# Database connection URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/agentwatch")

# Contract definitions from specification
ALLOWED_STATUSES = {"waiting", "running", "completed", "failed"}
ALLOWED_EVENT_TYPES = {
    "agent_started", "task_started", "tool_started", 
    "tool_completed", "tool_failed", "task_completed", 
    "task_failed", "agent_finished"
}

# --- Request Data Models (Validation) ---
class EventIngestRequest(BaseModel):
    advisor_id: str = Field(..., min_length=1)
    agent_id: str = Field(..., min_length=1)
    task_id: str = Field(..., min_length=1)
    event_type: str
    status: str
    timestamp: str
    tool_name: Optional[str] = None
    duration_ms: Optional[int] = Field(None, ge=0)
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in ALLOWED_STATUSES:
            raise ValueError(f"Invalid status '{v}'. Allowed: {ALLOWED_STATUSES}")
        return v

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        if v not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"Invalid event_type '{v}'. Allowed: {ALLOWED_EVENT_TYPES}")
        return v


# --- API Endpoints ---

@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/events", status_code=status.HTTP_201_CREATED)
def ingest_event(payload: EventIngestRequest):
    try:
        # connect_timeout=3 prevents hanging if DB is down
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=3)
        cur = conn.cursor()

        # 1. Verify Ownership / Hierarchy Rules
        cur.execute("SELECT advisor_id FROM agents WHERE agent_id = %s", (payload.agent_id,))
        agent_row = cur.fetchone()
        if agent_row and agent_row[0] != payload.advisor_id:
            raise HTTPException(
                status_code=400, 
                detail={"error": "InvalidOwnership", "message": "Agent does not belong to specified advisor"}
            )
        
        cur.execute("SELECT advisor_id, agent_id FROM tasks WHERE task_id = %s", (payload.task_id,))
        task_row = cur.fetchone()
        if task_row:
            if task_row[0] != payload.advisor_id or task_row[1] != payload.agent_id:
                raise HTTPException(
                    status_code=400, 
                    detail={"error": "InvalidOwnership", "message": "Task does not match advisor/agent hierarchy"}
                )

        # 2. Upsert Entities
        cur.execute("INSERT INTO advisors (advisor_id) VALUES (%s) ON CONFLICT DO NOTHING", (payload.advisor_id,))
        cur.execute("INSERT INTO agents (agent_id, advisor_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (payload.agent_id, payload.advisor_id))
        cur.execute("""
            INSERT INTO tasks (task_id, advisor_id, agent_id, status, updated_at) 
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (task_id) DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
        """, (payload.task_id, payload.advisor_id, payload.agent_id, payload.status))

        # 3. Insert Event Record
        cur.execute("""
            INSERT INTO events (advisor_id, agent_id, task_id, event_type, status, timestamp, tool_name, duration_ms, error_message, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
        """, (
            payload.advisor_id, payload.agent_id, payload.task_id, payload.event_type,
            payload.status, payload.timestamp, payload.tool_name, payload.duration_ms,
            payload.error_message, json.dumps(payload.metadata)
        ))
        
        event_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        return {"status": "accepted", "event_id": f"event-{event_id}"}

    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500, 
            detail={"error": "DatabaseError", "message": str(e)}
        )


@app.get("/dashboard/summary")
def get_dashboard_summary():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT status, COUNT(*) as count FROM tasks GROUP BY status")
        rows = cur.fetchall()
        summary = {"waiting": 0, "running": 0, "completed": 0, "failed": 0}
        for row in rows:
            if row["status"] in summary:
                summary[row["status"]] = row["count"]
        cur.close()
        conn.close()
        return summary
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail={"error": "DatabaseError", "message": str(e)})


@app.get("/advisors")
def get_advisors():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT * FROM advisors")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail={"error": "DatabaseError", "message": str(e)})


@app.get("/agents")
def get_agents():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT * FROM agents")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail={"error": "DatabaseError", "message": str(e)})


@app.get("/tasks")
def get_tasks(status: Optional[str] = None):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=3)
        cur = conn.cursor()
        if status:
            cur.execute("SELECT * FROM tasks WHERE status = %s ORDER BY updated_at DESC", (status,))
        else:
            cur.execute("SELECT * FROM tasks ORDER BY updated_at DESC")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail={"error": "DatabaseError", "message": str(e)})


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE task_id = %s", (task_id,))
        task = cur.fetchone()
        cur.close()
        conn.close()
        if not task:
            raise HTTPException(status_code=404, detail={"error": "NotFound", "message": "Task not found"})
        return task
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail={"error": "DatabaseError", "message": str(e)})


@app.get("/tasks/{task_id}/events")
def get_task_trace(task_id: str):
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT * FROM events WHERE task_id = %s ORDER BY timestamp ASC", (task_id,))
        events = cur.fetchall()
        cur.close()
        conn.close()
        return events
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail={"error": "DatabaseError", "message": str(e)})