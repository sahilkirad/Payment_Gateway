# sallma_pipelines/pipelines/neo4j_loader.py

import logging
import os
from neo4j import GraphDatabase
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class Neo4jLoader:
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info("Neo4jLoader initialized.")

    def __call__(self, batch: pd.DataFrame) -> Dict[str, int]:
        nodes_created_count = 0
        rels_created_count = 0

        with self._driver.session() as session:
            for graph_data in batch['graph_batch']:
                nodes = graph_data.get("nodes", {})
                rels = graph_data.get("relationships", [])

                # Create nodes
                for label, items in nodes.items():
                    # CHANGE: Use an explicit length check to avoid ambiguity.
                    if len(items) > 0:
                        session.run(f"UNWIND $items AS item MERGE (n:{label} {{id: item}})", items=items)
                        nodes_created_count += len(items)
                
                # Create relationships
                # CHANGE: Use an explicit length check here as well.
                if len(rels) > 0:
                    rels_by_type = {}
                    for rel in rels:
                        rel_type = rel['type']
                        if rel_type not in rels_by_type:
                            rels_by_type[rel_type] = []
                        rels_by_type[rel_type].append(rel)

                    for rel_type, rel_list in rels_by_type.items():
                        session.run(
                            f"""
                            UNWIND $rels AS r
                            MATCH (a {{id: r.source}})
                            MATCH (b {{id: r.target}})
                            MERGE (a)-[:{rel_type}]->(b)
                            """,
                            rels=rel_list
                        )
                        rels_created_count += len(rel_list)
        
        logger.info(f"Loaded batch: {nodes_created_count} nodes, {rels_created_count} relationships.")
        return {"nodes_created": [nodes_created_count], "relationships_created": [rels_created_count]}

    def close(self):
        if self._driver:
            self._driver.close()