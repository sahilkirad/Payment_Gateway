"""
Graph transformation pipeline for converting Parquet data to graph format.

This module contains functions to transform tabular data into graph structures
by extracting nodes and relationships based on the data schema.
"""

import logging
from typing import Dict, List, Any, Set
import pyarrow as pa


def transform_to_graph(batch: pa.RecordBatch) -> Dict[str, Any]:
    """
    Transform a batch of Parquet data into graph format with nodes and relationships.
    
    This function extracts nodes and relationships from the data based on the schema:
    - acc_no: Account nodes
    - txn_id: Transaction nodes  
    - service: Service nodes
    - region: Region nodes
    
    Args:
        batch (pa.RecordBatch): A batch of data from the Parquet file
        
    Returns:
        Dict[str, Any]: A dictionary containing nodes and relationships
    """
    logging.info(f"Processing batch with {batch.num_rows} rows")
    
    # Convert batch to pandas DataFrame for easier processing
    df = batch.to_pandas()
    
    # Initialize collections for nodes and relationships
    nodes = {
        'accounts': set(),
        'transactions': set(), 
        'services': set(),
        'regions': set()
    }
    
    relationships = []
    
    # Extract nodes from each row
    for _, row in df.iterrows():
        # Extract node identifiers (handle potential None values)
        acc_no = row.get('acc_no')
        txn_id = row.get('txn_id') 
        service = row.get('service')
        region = row.get('region')
        
        # Add nodes to collections (skip None values)
        if acc_no is not None:
            nodes['accounts'].add(acc_no)
        if txn_id is not None:
            nodes['transactions'].add(txn_id)
        if service is not None:
            nodes['services'].add(service)
        if region is not None:
            nodes['regions'].add(region)
            
        # Create relationships between entities
        if acc_no is not None and txn_id is not None:
            relationships.append({
                'source': acc_no,
                'target': txn_id,
                'type': 'ACCOUNT_TRANSACTION',
                'source_type': 'account',
                'target_type': 'transaction'
            })
            
        if txn_id is not None and service is not None:
            relationships.append({
                'source': txn_id,
                'target': service,
                'type': 'TRANSACTION_SERVICE',
                'source_type': 'transaction', 
                'target_type': 'service'
            })
            
        if service is not None and region is not None:
            relationships.append({
                'source': service,
                'target': region,
                'type': 'SERVICE_REGION',
                'source_type': 'service',
                'target_type': 'region'
            })
            
        if acc_no is not None and region is not None:
            relationships.append({
                'source': acc_no,
                'target': region, 
                'type': 'ACCOUNT_REGION',
                'source_type': 'account',
                'target_type': 'region'
            })
    
    # Convert sets to lists for JSON serialization
    graph_data = {
        'nodes': {
            'accounts': list(nodes['accounts']),
            'transactions': list(nodes['transactions']),
            'services': list(nodes['services']),
            'regions': list(nodes['regions'])
        },
        'relationships': relationships,
        'batch_stats': {
            'total_rows': batch.num_rows,
            'unique_accounts': len(nodes['accounts']),
            'unique_transactions': len(nodes['transactions']),
            'unique_services': len(nodes['services']),
            'unique_regions': len(nodes['regions']),
            'total_relationships': len(relationships)
        }
    }
    
    logging.info(f"Extracted {len(relationships)} relationships from batch")
    logging.info(f"Node counts - Accounts: {len(nodes['accounts'])}, "
                f"Transactions: {len(nodes['transactions'])}, "
                f"Services: {len(nodes['services'])}, "
                f"Regions: {len(nodes['regions'])}")
    
    return graph_data


def extract_graph_statistics(graph_data: Dict[str, Any]) -> Dict[str, int]:
    """
    Extract statistics from the transformed graph data.
    
    Args:
        graph_data (Dict[str, Any]): The graph data returned by transform_to_graph
        
    Returns:
        Dict[str, int]: Statistics about the graph structure
    """
    return graph_data.get('batch_stats', {})