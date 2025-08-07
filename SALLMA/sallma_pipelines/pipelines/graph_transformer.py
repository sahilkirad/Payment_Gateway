# sallma_pipelines/pipelines/graph_transformer.py

import logging
from typing import Dict, Any, List
import pyarrow as pa
import pandas as pd

logger = logging.getLogger(__name__)

def transform_to_graph(batch: pa.RecordBatch) -> Dict[str, List[Dict]]:
    """
    Transforms a batch of data into a dictionary of nodes and relationships,
    wrapped in a dictionary field to work with modern Ray versions.
    """
    logger.info(f"Transforming batch with {batch.num_rows} rows...")
    df = batch.to_pandas()
    
    nodes = {'Account': set(), 'Transaction': set(), 'Service': set(), 'Region': set()}
    relationships = []
    
    for _, row in df.iterrows():
        acc_no, txn_id, service, region = row.get('acc_no'), row.get('txn_id'), row.get('service'), row.get('region')
        
        if acc_no: nodes['Account'].add(acc_no)
        if txn_id: nodes['Transaction'].add(txn_id)
        if service: nodes['Service'].add(service)
        if region: nodes['Region'].add(region)
            
        if acc_no and txn_id:
            relationships.append({'source': acc_no, 'target': txn_id, 'type': 'PERFORMED'})
        if txn_id and service:
            relationships.append({'source': txn_id, 'target': service, 'type': 'USED_SERVICE'})
        if service and region:
            relationships.append({'source': service, 'target': region, 'type': 'LOCATED_IN'})
    
    graph_data = {
        "nodes": {label: list(items) for label, items in nodes.items()},
        "relationships": relationships
    }
    
    # CHANGE: Return a dictionary with a named field, as required by modern Ray.
    return {'graph_batch': [graph_data]}