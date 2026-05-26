#!/usr/bin/env python3
"""
Example: Send evaluation scores to zradar

This example demonstrates two ways to submit evaluation scores:
1. REST API - Direct HTTP POST
2. OTLP Logs - OpenTelemetry logs with score.* attributes
"""

import requests
import json
import time
from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource
import logging

# Configuration
ZVRADAR_URL = "http://localhost:8081"
ZVRADAR_OTLP = "http://localhost:4317"
API_KEY = "your_api_key_here"  # Replace with actual API key
PROJECT_ID = "your_project_id"  # Replace with actual project ID

def send_score_via_rest_api(trace_id: str, score_name: str, score_value: float):
    """Send an evaluation score via REST API"""
    print(f"\n📊 Sending score via REST API...")
    
    url = f"{ZVRADAR_URL}/api/v1/projects/{PROJECT_ID}/scores"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "trace_id": trace_id,
        "name": score_name,
        "value": score_value,
        "data_type": "NUMERIC",
        "source": "API",
        "comment": f"Score {score_name} = {score_value}"
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Score created successfully!")
        print(f"   ID: {result['id']}")
        print(f"   Trace: {result['trace_id']}")
        print(f"   Name: {result['name']}")
        print(f"   Value: {result['value']}")
        
        return result
    except requests.exceptions.RequestException as e:
        print(f"❌ Error sending score: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        return None

def send_score_via_otlp(trace_id: str, score_name: str, score_value: float):
    """Send an evaluation score via OTLP logs"""
    print(f"\n📊 Sending score via OTLP logs...")
    
    # Configure OTLP exporter
    resource = Resource.create({"service.name": "evaluation-service"})
    
    logger_provider = LoggerProvider(resource=resource)
    exporter = OTLPLogExporter(
        endpoint=ZVRADAR_OTLP,
        headers=(("authorization", f"Bearer {API_KEY}"),)
    )
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    
    # Create logger
    handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
    logger = logging.getLogger("evaluation")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    # Send score as log with score.* attributes
    logger.info(
        "evaluation_score",
        extra={
            "score.trace_id": trace_id,
            "score.name": score_name,
            "score.value": score_value,
            "score.data_type": "NUMERIC",
            "score.source": "EVAL",
            "score.comment": f"Automated evaluation: {score_name} = {score_value}",
        }
    )
    
    # Flush and wait
    logger_provider.force_flush()
    time.sleep(1)
    
    print(f"✅ Score sent via OTLP!")
    print(f"   Trace: {trace_id}")
    print(f"   Name: {score_name}")
    print(f"   Value: {score_value}")

def get_trace_scores(trace_id: str):
    """Get all scores for a trace"""
    print(f"\n📖 Getting scores for trace {trace_id}...")
    
    url = f"{ZVRADAR_URL}/api/v1/traces/{trace_id}/scores"
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        scores = response.json()
        print(f"✅ Found {len(scores)} score(s):")
        
        for score in scores:
            print(f"\n   📊 {score['name']}")
            print(f"      Value: {score['value']}")
            print(f"      Source: {score['source']}")
            print(f"      Created: {score['created_at']}")
            if score.get('comment'):
                print(f"      Comment: {score['comment']}")
        
        return scores
    except requests.exceptions.RequestException as e:
        print(f"❌ Error getting scores: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        return []

def get_trace_score_summary(trace_id: str):
    """Get score summary for a trace"""
    print(f"\n📈 Getting score summary for trace {trace_id}...")
    
    url = f"{ZVRADAR_URL}/api/v1/traces/{trace_id}/scores/summary"
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        summary = response.json()
        print(f"✅ Score Summary:")
        
        for item in summary:
            print(f"\n   📊 {item['name']}")
            print(f"      Avg: {item['avg_value']:.3f}")
            print(f"      Min: {item['min_value']:.3f}")
            print(f"      Max: {item['max_value']:.3f}")
            print(f"      Count: {item['count']}")
        
        return summary
    except requests.exceptions.RequestException as e:
        print(f"❌ Error getting summary: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        return []

def main():
    """Main example flow"""
    print("🎯 zradar Evaluation Scores Example")
    print("=" * 50)
    
    # Use a trace ID (in real usage, this comes from your traces)
    trace_id = "trace_example_123"
    
    # Example 1: Send score via REST API
    send_score_via_rest_api(trace_id, "accuracy", 0.95)
    send_score_via_rest_api(trace_id, "hallucination", 0.12)
    send_score_via_rest_api(trace_id, "toxicity", 0.03)
    
    # Example 2: Send score via OTLP (commented out by default)
    # Uncomment to test OTLP logs ingestion
    # send_score_via_otlp(trace_id, "relevance", 0.88)
    
    # Wait a moment for processing
    time.sleep(2)
    
    # Example 3: Get all scores for the trace
    scores = get_trace_scores(trace_id)
    
    # Example 4: Get score summary
    if scores:
        summary = get_trace_score_summary(trace_id)
    
    print("\n" + "=" * 50)
    print("✅ Example completed!")

if __name__ == "__main__":
    main()

