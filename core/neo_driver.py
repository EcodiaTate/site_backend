from __future__ import annotations
from typing import Generator
from contextlib import contextmanager
from neo4j import GraphDatabase, Driver
from fastapi import Request

def build_driver(uri: str, user: str, password: str) -> Driver:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    # quick connectivity test
    with driver.session() as s:
        s.run("RETURN 1").consume()
    return driver
# site_backend/core/neo_driver.py

def ensure_constraints(driver: Driver) -> None:
    stmts = [
        "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
        "CREATE CONSTRAINT user_email IF NOT EXISTS FOR (u:User) REQUIRE u.email IS UNIQUE",
        "CREATE CONSTRAINT youth_user_unique IF NOT EXISTS FOR (y:YouthProfile) REQUIRE y.user_id IS UNIQUE",
        "CREATE CONSTRAINT biz_user_unique IF NOT EXISTS FOR (b:BusinessProfile) REQUIRE b.user_id IS UNIQUE",
        # NEW: ensure BusinessProfile.id exists & is unique
        "CREATE CONSTRAINT business_id IF NOT EXISTS FOR (b:BusinessProfile) REQUIRE b.id IS UNIQUE",
    ]
    with driver.session() as s:
        for q in stmts:
            s.run(q).consume()

@contextmanager
def neo_session(driver: Driver):
    with driver.session() as s:
        yield s

# FastAPI dependency: yields a session using app.state.driver
def session_dep(request: Request):
    driver: Driver = request.app.state.driver  # type: ignore[attr-defined]
    with neo_session(driver) as s:
        yield s
