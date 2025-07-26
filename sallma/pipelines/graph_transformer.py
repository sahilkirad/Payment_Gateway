"""
Graph transformation module for converting Parquet data to graph format.

This module contains functions to transform Parquet data with schema
(acc_no, txn_id, service, region) into nodes and relationships for graph processing.
"""

import logging
from typing import Dict, List, Tuple, Any
import pandas as pd
import ray

logger = logging.getLogger(__name__)


def transform_to_graph(batch: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transform a batch of Parquet data into graph nodes and relationships.
    
    This function extracts nodes and relationships based on the schema:
    - acc_no: Account number (node)
    - txn_id: Transaction ID (node)
    - service: Service type (node)
    - region: Region (node)
    
    Args:
        batch: A pandas DataFrame containing the Parquet data batch
        
    Returns:
        Dictionary containing 'nodes' and 'relationships' lists
    """
    try:
        nodes = []
        relationships = []
        
        # Extract unique nodes from the data
        for _, row in batch.iterrows():
            # Create account node
            if pd.notna(row.get('acc_no')):
                account_node = {
                    'id': f"account_{row['acc_no']}",
                    'type': 'account',
                    'properties': {
                        'acc_no': str(row['acc_no']),
                        'label': f"Account {row['acc_no']}"
                    }
                }
                nodes.append(account_node)
            
            # Create transaction node
            if pd.notna(row.get('txn_id')):
                transaction_node = {
                    'id': f"transaction_{row['txn_id']}",
                    'type': 'transaction',
                    'properties': {
                        'txn_id': str(row['txn_id']),
                        'label': f"Transaction {row['txn_id']}"
                    }
                }
                nodes.append(transaction_node)
            
            # Create service node
            if pd.notna(row.get('service')):
                service_node = {
                    'id': f"service_{row['service']}",
                    'type': 'service',
                    'properties': {
                        'service': str(row['service']),
                        'label': f"Service {row['service']}"
                    }
                }
                nodes.append(service_node)
            
            # Create region node
            if pd.notna(row.get('region')):
                region_node = {
                    'id': f"region_{row['region']}",
                    'type': 'region',
                    'properties': {
                        'region': str(row['region']),
                        'label': f"Region {row['region']}"
                    }
                }
                nodes.append(region_node)
            
            # Create relationships
            if pd.notna(row.get('acc_no')) and pd.notna(row.get('txn_id')):
                # Account -> Transaction relationship
                account_txn_rel = {
                    'source': f"account_{row['acc_no']}",
                    'target': f"transaction_{row['txn_id']}",
                    'type': 'HAS_TRANSACTION',
                    'properties': {
                        'relationship_type': 'account_transaction'
                    }
                }
                relationships.append(account_txn_rel)
            
            if pd.notna(row.get('txn_id')) and pd.notna(row.get('service')):
                # Transaction -> Service relationship
                txn_service_rel = {
                    'source': f"transaction_{row['txn_id']}",
                    'target': f"service_{row['service']}",
                    'type': 'USES_SERVICE',
                    'properties': {
                        'relationship_type': 'transaction_service'
                    }
                }
                relationships.append(txn_service_rel)
            
            if pd.notna(row.get('service')) and pd.notna(row.get('region')):
                # Service -> Region relationship
                service_region_rel = {
                    'source': f"service_{row['service']}",
                    'target': f"region_{row['region']}",
                    'type': 'AVAILABLE_IN',
                    'properties': {
                        'relationship_type': 'service_region'
                    }
                }
                relationships.append(service_region_rel)
        
        # Remove duplicate nodes (keep only unique nodes by id)
        unique_nodes = {}
        for node in nodes:
            unique_nodes[node['id']] = node
        
        # Remove duplicate relationships
        unique_relationships = {}
        for rel in relationships:
            rel_key = f"{rel['source']}_{rel['type']}_{rel['target']}"
            unique_relationships[rel_key] = rel
        
        result = {
            'nodes': list(unique_nodes.values()),
            'relationships': list(unique_relationships.values())
        }
        
        logger.info(f"Transformed batch: {len(unique_nodes)} unique nodes, {len(unique_relationships)} unique relationships")
        return result
        
    except Exception as e:
        logger.error(f"Error transforming batch to graph: {e}")
        return {'nodes': [], 'relationships': []}


def process_dataset_to_graph(dataset: ray.data.Dataset) -> ray.data.Dataset:
    """
    Process an entire Ray dataset to transform it into graph format.
    
    Args:
        dataset: Ray dataset containing Parquet data
        
    Returns:
        Ray dataset containing graph data (nodes and relationships)
    """
    try:
        logger.info("Starting graph transformation of dataset...")
        
        # Apply the transform_to_graph function to each batch
        graph_dataset = dataset.map_batches(
            transform_to_graph,
            batch_format="pandas",
            batch_size=1000  # Adjust batch size as needed
        )
        
        logger.info("Graph transformation completed successfully")
        return graph_dataset
        
    except Exception as e:
        logger.error(f"Error processing dataset to graph: {e}")
        raise


def get_complete_graph_statistics(dataset: ray.data.Dataset) -> Dict[str, Any]:
    """
    Process the entire dataset and return comprehensive graph statistics.
    
    Args:
        dataset: Ray dataset containing Parquet data
        
    Returns:
        Dictionary containing complete graph statistics
    """
    try:
        logger.info("Processing entire dataset for complete graph statistics...")
        
        # Transform the dataset
        graph_dataset = process_dataset_to_graph(dataset)
        
        # Collect all nodes and relationships
        all_nodes = []
        all_relationships = []
        
        # Process all batches
        for batch_result in graph_dataset.iter_batches():
            if isinstance(batch_result, dict) and 'nodes' in batch_result and 'relationships' in batch_result:
                all_nodes.extend(batch_result['nodes'])
                all_relationships.extend(batch_result['relationships'])
        
        # Remove duplicates
        unique_nodes = {}
        for node in all_nodes:
            unique_nodes[node['id']] = node
        
        unique_relationships = {}
        for rel in all_relationships:
            rel_key = f"{rel['source']}_{rel['type']}_{rel['target']}"
            unique_relationships[rel_key] = rel
        
        # Calculate statistics
        node_types = {}
        for node in unique_nodes.values():
            node_type = node.get('type', 'unknown')
            node_types[node_type] = node_types.get(node_type, 0) + 1
        
        rel_types = {}
        for rel in unique_relationships.values():
            rel_type = rel.get('type', 'unknown')
            rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
        
        statistics = {
            'total_unique_nodes': len(unique_nodes),
            'total_unique_relationships': len(unique_relationships),
            'node_distribution': node_types,
            'relationship_distribution': rel_types,
            'all_nodes': list(unique_nodes.values()),
            'all_relationships': list(unique_relationships.values())
        }
        
        logger.info(f"Complete graph statistics calculated: {len(unique_nodes)} nodes, {len(unique_relationships)} relationships")
        return statistics
        
    except Exception as e:
        logger.error(f"Error calculating complete graph statistics: {e}")
        raise