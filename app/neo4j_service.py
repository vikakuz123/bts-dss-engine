from __future__ import annotations

from typing import Any

from neo4j import GraphDatabase
from neo4j import Driver
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config import Settings
from app.db import _as_text
from app.models import DecisionRecommendation, OpportunityScore, OpportunityState, OpportunityUnit


def build_neo4j_driver(settings: Settings) -> Driver:
    return GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )


def check_neo4j(settings: Settings) -> dict[str, Any]:
    try:
        with build_neo4j_driver(settings) as driver:
            driver.verify_connectivity()
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) AS node_count").single()
                node_count = result["node_count"] if result else 0
        return {"status": "ok", "uri": settings.neo4j_uri, "node_count": node_count}
    except Exception as exc:
        return {"status": "error", "uri": settings.neo4j_uri, "error": str(exc)}


def _latest_score(session: Session, unit_id: int) -> OpportunityScore | None:
    return (
        session.query(OpportunityScore)
        .filter_by(opportunity_unit_id=unit_id)
        .order_by(OpportunityScore.id.desc())
        .first()
    )


def _active_states(session: Session, unit_id: int) -> list[OpportunityState]:
    return (
        session.query(OpportunityState)
        .filter_by(opportunity_unit_id=unit_id, active=True)
        .order_by(OpportunityState.id.desc())
        .all()
    )


def _recommendations(session: Session, unit_id: int) -> list[DecisionRecommendation]:
    return (
        session.query(DecisionRecommendation)
        .filter_by(opportunity_unit_id=unit_id)
        .order_by(DecisionRecommendation.id.desc())
        .limit(20)
        .all()
    )


def _node_id(prefix: str, value: object) -> str:
    text = _as_text(value)
    return f"{prefix}:{text}" if text else ""


def _unit_payload(session: Session, unit: OpportunityUnit) -> dict[str, Any]:
    score = _latest_score(session, unit.id)
    states = _active_states(session, unit.id)
    recommendations = _recommendations(session, unit.id)
    extracted = (unit.raw_context or {}).get("extracted") if isinstance(unit.raw_context, dict) else {}
    return {
        "opportunity": {
            "id": _node_id("opportunity", unit.id),
            "db_id": unit.id,
            "bitrix_deal_id": unit.bitrix_deal_id,
            "priority_score": score.priority_score if score else 0.0,
            "pclose": score.pclose if score else 0.0,
            "risk_level": unit.risk_level,
            "next_step": unit.next_step,
            "commercial_scenario": unit.commercial_scenario,
            "data_quality_score": unit.data_quality_score,
        },
        "company": {
            "id": unit.client_entity_id,
            "name": unit.client_name,
        }
        if unit.client_entity_id
        else None,
        "person": {
            "id": unit.contact_entity_id,
            "name": unit.contact_name,
        }
        if unit.contact_entity_id
        else None,
        "object": {
            "id": unit.object_entity_id,
            "name": unit.object_name,
            "address": unit.object_address,
        }
        if unit.object_entity_id
        else None,
        "equipment": {
            "id": _node_id("equipment_type", unit.equipment_type),
            "name": unit.equipment_type,
            "model": unit.equipment_model,
        }
        if unit.equipment_type
        else None,
        "competitor": {
            "id": _node_id("competitor", "mentioned"),
            "name": "competitor_mentioned",
        }
        if isinstance(extracted, dict) and extracted.get("competitor_mention")
        else None,
        "states": [
            {
                "id": _node_id("risk_flag", state.state_code),
                "code": state.state_code,
                "name": state.state_name,
                "confidence_score": state.confidence_score,
            }
            for state in states
        ],
        "recommendations": [
            {
                "id": _node_id("decision_action", recommendation.action_code),
                "code": recommendation.action_code,
                "target_role": recommendation.target_role,
                "requires_escalation": recommendation.requires_escalation,
                "status": recommendation.status,
            }
            for recommendation in recommendations
        ],
    }


def _merge_unit_graph(tx: Any, payload: dict[str, Any]) -> None:
    tx.run(
        """
        MERGE (o:Opportunity {id: $opportunity.id})
        SET o += $opportunity
        """,
        opportunity=payload["opportunity"],
    )
    if payload["company"]:
        tx.run(
            """
            MERGE (c:Company {id: $company.id})
            SET c += $company
            WITH c
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:FOR_COMPANY]->(c)
            """,
            company=payload["company"],
            opportunity_id=payload["opportunity"]["id"],
        )
    if payload["person"]:
        tx.run(
            """
            MERGE (p:Person {id: $person.id})
            SET p += $person
            WITH p
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:ASSIGNED_TO]->(p)
            """,
            person=payload["person"],
            opportunity_id=payload["opportunity"]["id"],
        )
        if payload["company"]:
            tx.run(
                """
                MATCH (p:Person {id: $person_id})
                MATCH (c:Company {id: $company_id})
                MERGE (c)-[:HAS_CONTACT]->(p)
                MERGE (p)-[:WORKS_FOR]->(c)
                """,
                person_id=payload["person"]["id"],
                company_id=payload["company"]["id"],
            )
    if payload["object"]:
        tx.run(
            """
            MERGE (po:ProjectObject {id: $object.id})
            SET po += $object
            WITH po
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:FOR_OBJECT]->(po)
            """,
            object=payload["object"],
            opportunity_id=payload["opportunity"]["id"],
        )
        if payload["company"]:
            tx.run(
                """
                MATCH (c:Company {id: $company_id})
                MATCH (po:ProjectObject {id: $object_id})
                MERGE (c)-[:OWNS_PROJECT]->(po)
                """,
                company_id=payload["company"]["id"],
                object_id=payload["object"]["id"],
            )
    if payload["equipment"]:
        tx.run(
            """
            MERGE (e:EquipmentType {id: $equipment.id})
            SET e += $equipment
            WITH e
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:NEEDS]->(e)
            """,
            equipment=payload["equipment"],
            opportunity_id=payload["opportunity"]["id"],
        )
        if payload["object"]:
            tx.run(
                """
                MATCH (po:ProjectObject {id: $object_id})
                MATCH (e:EquipmentType {id: $equipment_id})
                MERGE (po)-[:USES]->(e)
                """,
                object_id=payload["object"]["id"],
                equipment_id=payload["equipment"]["id"],
            )
    if payload["competitor"]:
        tx.run(
            """
            MERGE (comp:Competitor {id: $competitor.id})
            SET comp += $competitor
            WITH comp
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:HAS_COMPETITOR_SIGNAL]->(comp)
            """,
            competitor=payload["competitor"],
            opportunity_id=payload["opportunity"]["id"],
        )
        if payload["object"]:
            tx.run(
                """
                MATCH (po:ProjectObject {id: $object_id})
                MATCH (comp:Competitor {id: $competitor_id})
                MERGE (po)-[:HAS_COMPETITOR]->(comp)
                """,
                object_id=payload["object"]["id"],
                competitor_id=payload["competitor"]["id"],
            )
    for state in payload["states"]:
        tx.run(
            """
            MERGE (r:RiskFlag {id: $state.id})
            SET r += $state
            WITH r
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:HAS_RISK]->(r)
            """,
            state=state,
            opportunity_id=payload["opportunity"]["id"],
        )
    for recommendation in payload["recommendations"]:
        tx.run(
            """
            MERGE (a:DecisionAction {id: $recommendation.id})
            SET a += $recommendation
            WITH a
            MATCH (o:Opportunity {id: $opportunity_id})
            MERGE (o)-[:RECOMMENDS]->(a)
            """,
            recommendation=recommendation,
            opportunity_id=payload["opportunity"]["id"],
        )


def sync_opportunity_graph(engine: Engine, settings: Settings) -> dict[str, Any]:
    with Session(engine) as db_session:
        units = db_session.query(OpportunityUnit).order_by(OpportunityUnit.id.asc()).all()
        payloads = [_unit_payload(db_session, unit) for unit in units]

    with build_neo4j_driver(settings) as driver:
        with driver.session() as graph_session:
            graph_session.execute_write(
                lambda tx: tx.run(
                    "CREATE CONSTRAINT opportunity_id IF NOT EXISTS FOR (n:Opportunity) REQUIRE n.id IS UNIQUE"
                )
            )
            graph_session.execute_write(
                lambda tx: tx.run("CREATE CONSTRAINT company_id IF NOT EXISTS FOR (n:Company) REQUIRE n.id IS UNIQUE")
            )
            graph_session.execute_write(
                lambda tx: tx.run("CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE")
            )
            graph_session.execute_write(
                lambda tx: tx.run(
                    "CREATE CONSTRAINT object_id IF NOT EXISTS FOR (n:ProjectObject) REQUIRE n.id IS UNIQUE"
                )
            )
            graph_session.execute_write(
                lambda tx: tx.run(
                    "CREATE CONSTRAINT equipment_id IF NOT EXISTS FOR (n:EquipmentType) REQUIRE n.id IS UNIQUE"
                )
            )
            graph_session.execute_write(
                lambda tx: tx.run(
                    "CREATE CONSTRAINT competitor_id IF NOT EXISTS FOR (n:Competitor) REQUIRE n.id IS UNIQUE"
                )
            )
            for payload in payloads:
                graph_session.execute_write(_merge_unit_graph, payload)
            counts = graph_session.run(
                """
                MATCH (n)
                RETURN labels(n)[0] AS label, count(n) AS count
                ORDER BY label
                """
            ).data()
            edge_counts = graph_session.run(
                """
                MATCH ()-[r]->()
                RETURN type(r) AS type, count(r) AS count
                ORDER BY type
                """
            ).data()
    return {
        "synced_opportunities": len(payloads),
        "node_counts": counts,
        "edge_counts": edge_counts,
    }


def get_object_graph_from_neo4j(settings: Settings, object_id: str) -> dict[str, Any] | None:
    query = """
    MATCH (po:ProjectObject)
    WHERE po.id = $object_id OR po.name = $object_id
    MATCH path = (po)-[*1..2]-(n)
    WITH collect(path) AS paths
    UNWIND paths AS path
    UNWIND nodes(path) AS node
    WITH paths, collect(DISTINCT node) AS nodes
    UNWIND paths AS path
    UNWIND relationships(path) AS rel
    RETURN
      [node IN nodes | {id: node.id, type: labels(node)[0], label: coalesce(node.name, node.bitrix_deal_id, node.code, node.id), properties: properties(node)}] AS nodes,
      collect(DISTINCT {source: startNode(rel).id, target: endNode(rel).id, type: type(rel)}) AS edges
    """
    try:
        with build_neo4j_driver(settings) as driver:
            with driver.session() as session:
                record = session.run(query, object_id=object_id).single()
                if not record:
                    return None
                return {"nodes": record["nodes"], "edges": record["edges"], "source": "neo4j"}
    except Exception:
        return None
