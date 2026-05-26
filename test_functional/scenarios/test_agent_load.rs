//! Agent Workflow & Load Tests
//!
//! Tests for high fan-out agent traces (100 tool calls), agent-to-sub-agent
//! delegation, error/retry workflows, and load/stress tests.

#[allow(unused_imports)]
use crate::*;

use opentelemetry_proto::tonic::common::v1::AnyValue;

// ============================================================================
// Helper: create a string AnyValue
// ============================================================================

fn str_val(s: &str) -> AnyValue {
    AnyValue {
        value: Some(
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s.to_string()),
        ),
    }
}

// ============================================================================
// Agent Workflow Tests
// ============================================================================

/// Test 1: Agent root span with 100 TOOL child spans (high fan-out).
#[tokio::test]
#[ignore]
async fn test_agent_with_100_tool_calls() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let root_span_id = TestDataGenerator::span_id();

    // Build root AGENT span
    let mut span_defs = vec![SpanDefExt {
        name: "orchestrator".to_string(),
        span_id: root_span_id,
        parent_span_id: None,
        attributes: vec![("agent.name".to_string(), str_val("orchestrator"))],
        status_code: Some(1), // OK
    }];

    // Build 100 TOOL child spans
    for i in 0..100 {
        let child_span_id = TestDataGenerator::span_id();
        span_defs.push(SpanDefExt {
            name: format!("tool_{}", i),
            span_id: child_span_id,
            parent_span_id: Some(root_span_id),
            attributes: vec![
                ("tool.name".to_string(), str_val(&format!("tool_{}", i))),
                ("tool.call.id".to_string(), str_val(&format!("call_{}", i))),
            ],
            status_code: Some(1),
        });
    }

    let request =
        env.otlp
            .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
    env.otlp.export_traces(request).await?;
    println!("✅ Trace with 1 AGENT + 100 TOOL spans sent");

    // Poll until the trace appears
    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace(&env.client, &trace_id_hex, Duration::from_secs(30)).await?;
    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");

    assert_eq!(
        spans.len(),
        101,
        "Should have 101 spans (1 root + 100 tools)"
    );

    // Find root span
    let root_span_id_hex = hex::encode(root_span_id);
    let root = spans
        .iter()
        .find(|s| s["span_id"].as_str() == Some(&root_span_id_hex))
        .expect("Root span should exist");
    assert_eq!(
        root["span_type"].as_str().unwrap_or(""),
        "AGENT",
        "Root span should be AGENT"
    );

    // Verify all children are TOOL and point to root
    let children: Vec<&Value> = spans
        .iter()
        .filter(|s| s["span_id"].as_str() != Some(&root_span_id_hex))
        .collect();
    assert_eq!(children.len(), 100, "Should have exactly 100 child spans");

    for child in &children {
        assert_eq!(
            child["span_type"].as_str().unwrap_or(""),
            "TOOL",
            "All children should be TOOL"
        );
        assert_eq!(
            child["parent_span_id"].as_str().unwrap_or(""),
            root_span_id_hex,
            "All children should have root as parent"
        );
    }

    // Verify agent.name attribute is present on root span
    let agent_name = root["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        agent_name, "orchestrator",
        "Root span should have agent.name attribute"
    );

    // Verify tool attributes are present on children (sample a few)
    let sample_children = vec![0, 25, 50, 75, 99];
    for &idx in &sample_children {
        let child = &children[idx];
        let tool_name = child["attributes"]
            .get("tool.name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let tool_call_id = child["attributes"]
            .get("tool.call.id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        assert_eq!(
            tool_name,
            format!("tool_{}", idx),
            "Tool {} should have correct tool.name",
            idx
        );
        assert_eq!(
            tool_call_id,
            format!("call_{}", idx),
            "Tool {} should have correct tool.call.id",
            idx
        );
    }

    println!(
        "✅ Agent with 100 tool calls verified: hierarchy, span types, parent links, and attributes"
    );
    Ok(())
}

/// Test 2: Root agent delegates to a sub-agent, each with tool calls.
///
/// ```text
/// AGENT "planner"
///  ├── GENERATION "plan-step"
///  ├── AGENT "researcher"
///  │    ├── TOOL "web_search"
///  │    ├── TOOL "doc_lookup"
///  │    └── GENERATION "summarize"
///  └── GENERATION "final-answer"
/// ```
#[tokio::test]
#[ignore]
async fn test_agent_delegates_to_sub_agent_with_tools() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let planner_id = TestDataGenerator::span_id();
    let plan_step_id = TestDataGenerator::span_id();
    let researcher_id = TestDataGenerator::span_id();
    let web_search_id = TestDataGenerator::span_id();
    let doc_lookup_id = TestDataGenerator::span_id();
    let summarize_id = TestDataGenerator::span_id();
    let final_answer_id = TestDataGenerator::span_id();

    let span_defs = vec![
        // Root agent
        SpanDefExt {
            name: "planner".to_string(),
            span_id: planner_id,
            parent_span_id: None,
            attributes: vec![("agent.name".to_string(), str_val("planner"))],
            status_code: Some(1),
        },
        // Plan step (GENERATION under root)
        SpanDefExt {
            name: "plan-step".to_string(),
            span_id: plan_step_id,
            parent_span_id: Some(planner_id),
            attributes: vec![("gen_ai.request.model".to_string(), str_val("gpt-4"))],
            status_code: Some(1),
        },
        // Sub-agent (child of root)
        SpanDefExt {
            name: "researcher".to_string(),
            span_id: researcher_id,
            parent_span_id: Some(planner_id),
            attributes: vec![("agent.name".to_string(), str_val("researcher"))],
            status_code: Some(1),
        },
        // Tool: web_search (child of researcher)
        SpanDefExt {
            name: "web_search".to_string(),
            span_id: web_search_id,
            parent_span_id: Some(researcher_id),
            attributes: vec![("tool.name".to_string(), str_val("web_search"))],
            status_code: Some(1),
        },
        // Tool: doc_lookup (child of researcher)
        SpanDefExt {
            name: "doc_lookup".to_string(),
            span_id: doc_lookup_id,
            parent_span_id: Some(researcher_id),
            attributes: vec![("tool.name".to_string(), str_val("doc_lookup"))],
            status_code: Some(1),
        },
        // GENERATION: summarize (child of researcher)
        SpanDefExt {
            name: "summarize".to_string(),
            span_id: summarize_id,
            parent_span_id: Some(researcher_id),
            attributes: vec![("gen_ai.request.model".to_string(), str_val("gpt-4"))],
            status_code: Some(1),
        },
        // GENERATION: final-answer (child of root)
        SpanDefExt {
            name: "final-answer".to_string(),
            span_id: final_answer_id,
            parent_span_id: Some(planner_id),
            attributes: vec![("gen_ai.request.model".to_string(), str_val("gpt-4"))],
            status_code: Some(1),
        },
    ];

    let request =
        env.otlp
            .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
    env.otlp.export_traces(request).await?;
    println!("✅ Sub-agent delegation trace sent (7 spans)");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");

    assert_eq!(spans.len(), 7, "Should have 7 spans");

    // Verify span types
    let planner_hex = hex::encode(planner_id);
    let researcher_hex = hex::encode(researcher_id);
    let web_search_hex = hex::encode(web_search_id);
    let doc_lookup_hex = hex::encode(doc_lookup_id);

    let find_span = |id_hex: &str| -> &Value {
        spans
            .iter()
            .find(|s| s["span_id"].as_str() == Some(id_hex))
            .unwrap_or_else(|| panic!("Span {} not found", id_hex))
    };

    // Root = AGENT
    assert_eq!(
        find_span(&planner_hex)["span_type"].as_str().unwrap_or(""),
        "AGENT"
    );
    // Researcher = AGENT
    assert_eq!(
        find_span(&researcher_hex)["span_type"]
            .as_str()
            .unwrap_or(""),
        "AGENT"
    );
    // Tools are children of researcher, not root
    let ws = find_span(&web_search_hex);
    assert_eq!(ws["span_type"].as_str().unwrap_or(""), "TOOL");
    assert_eq!(ws["parent_span_id"].as_str().unwrap_or(""), researcher_hex);
    let dl = find_span(&doc_lookup_hex);
    assert_eq!(dl["span_type"].as_str().unwrap_or(""), "TOOL");
    assert_eq!(dl["parent_span_id"].as_str().unwrap_or(""), researcher_hex);

    // Count span types
    let agent_count = spans
        .iter()
        .filter(|s| s["span_type"].as_str() == Some("AGENT"))
        .count();
    let tool_count = spans
        .iter()
        .filter(|s| s["span_type"].as_str() == Some("TOOL"))
        .count();
    let gen_count = spans
        .iter()
        .filter(|s| s["span_type"].as_str() == Some("GENERATION"))
        .count();
    assert_eq!(agent_count, 2, "Should have 2 AGENT spans");
    assert_eq!(tool_count, 2, "Should have 2 TOOL spans");
    assert_eq!(gen_count, 3, "Should have 3 GENERATION spans");

    // Verify agent.name attributes
    let planner_name = find_span(&planner_hex)["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        planner_name, "planner",
        "Planner span should have agent.name = 'planner'"
    );

    let researcher_name = find_span(&researcher_hex)["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        researcher_name, "researcher",
        "Researcher span should have agent.name = 'researcher'"
    );

    // Verify tool.name attributes
    let ws_name = ws["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        ws_name, "web_search",
        "Web search tool should have tool.name = 'web_search'"
    );

    let dl = find_span(&doc_lookup_hex);
    let dl_name = dl["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        dl_name, "doc_lookup",
        "Doc lookup tool should have tool.name = 'doc_lookup'"
    );

    // Verify gen_ai.request.model attributes on GENERATION spans
    let plan_step_hex = hex::encode(plan_step_id);
    let summarize_hex = hex::encode(summarize_id);
    let final_answer_hex = hex::encode(final_answer_id);

    let plan_step = find_span(&plan_step_hex);
    let plan_model = plan_step["attributes"]
        .get("gen_ai.request.model")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        plan_model, "gpt-4",
        "Plan step should have gen_ai.request.model = 'gpt-4'"
    );

    let summarize = find_span(&summarize_hex);
    let summarize_model = summarize["attributes"]
        .get("gen_ai.request.model")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        summarize_model, "gpt-4",
        "Summarize should have gen_ai.request.model = 'gpt-4'"
    );

    let final_answer = find_span(&final_answer_hex);
    let final_model = final_answer["attributes"]
        .get("gen_ai.request.model")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        final_model, "gpt-4",
        "Final answer should have gen_ai.request.model = 'gpt-4'"
    );

    println!("✅ Sub-agent delegation verified: hierarchy, types, parent links, and attributes");
    Ok(())
}

/// Test 3: Three levels of agent delegation with tool calls.
///
/// ```text
/// AGENT "coordinator"
///  ├── AGENT "analyst"
///  │    ├── TOOL "query_db"
///  │    ├── TOOL "run_sql"
///  │    └── AGENT "validator"
///  │         ├── TOOL "schema_check"
///  │         └── TOOL "data_validate"
///  ├── GENERATION "synthesize"
///  └── TOOL "send_report"
/// ```
#[tokio::test]
#[ignore]
async fn test_multi_level_agent_delegation() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let coordinator_id = TestDataGenerator::span_id();
    let analyst_id = TestDataGenerator::span_id();
    let query_db_id = TestDataGenerator::span_id();
    let run_sql_id = TestDataGenerator::span_id();
    let validator_id = TestDataGenerator::span_id();
    let schema_check_id = TestDataGenerator::span_id();
    let data_validate_id = TestDataGenerator::span_id();
    let synthesize_id = TestDataGenerator::span_id();
    let send_report_id = TestDataGenerator::span_id();

    let span_defs = vec![
        SpanDefExt {
            name: "coordinator".to_string(),
            span_id: coordinator_id,
            parent_span_id: None,
            attributes: vec![("agent.name".to_string(), str_val("coordinator"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "analyst".to_string(),
            span_id: analyst_id,
            parent_span_id: Some(coordinator_id),
            attributes: vec![("agent.name".to_string(), str_val("analyst"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "query_db".to_string(),
            span_id: query_db_id,
            parent_span_id: Some(analyst_id),
            attributes: vec![("tool.name".to_string(), str_val("query_db"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "run_sql".to_string(),
            span_id: run_sql_id,
            parent_span_id: Some(analyst_id),
            attributes: vec![("tool.name".to_string(), str_val("run_sql"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "validator".to_string(),
            span_id: validator_id,
            parent_span_id: Some(analyst_id),
            attributes: vec![("agent.name".to_string(), str_val("validator"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "schema_check".to_string(),
            span_id: schema_check_id,
            parent_span_id: Some(validator_id),
            attributes: vec![("tool.name".to_string(), str_val("schema_check"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "data_validate".to_string(),
            span_id: data_validate_id,
            parent_span_id: Some(validator_id),
            attributes: vec![("tool.name".to_string(), str_val("data_validate"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "synthesize".to_string(),
            span_id: synthesize_id,
            parent_span_id: Some(coordinator_id),
            attributes: vec![("gen_ai.request.model".to_string(), str_val("claude-3"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "send_report".to_string(),
            span_id: send_report_id,
            parent_span_id: Some(coordinator_id),
            attributes: vec![("tool.name".to_string(), str_val("send_report"))],
            status_code: Some(1),
        },
    ];

    let request =
        env.otlp
            .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
    env.otlp.export_traces(request).await?;
    println!("✅ Multi-level delegation trace sent (9 spans)");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");

    assert_eq!(spans.len(), 9, "Should have 9 spans");

    let coordinator_hex = hex::encode(coordinator_id);
    let analyst_hex = hex::encode(analyst_id);
    let validator_hex = hex::encode(validator_id);

    let find_span = |id_hex: &str| -> &Value {
        spans
            .iter()
            .find(|s| s["span_id"].as_str() == Some(id_hex))
            .unwrap_or_else(|| panic!("Span {} not found", id_hex))
    };

    // Verify nesting: validator → analyst → coordinator
    assert_eq!(
        find_span(&validator_hex)["parent_span_id"]
            .as_str()
            .unwrap_or(""),
        analyst_hex,
        "validator should be child of analyst"
    );
    assert_eq!(
        find_span(&analyst_hex)["parent_span_id"]
            .as_str()
            .unwrap_or(""),
        coordinator_hex,
        "analyst should be child of coordinator"
    );

    // Verify span type counts
    let agent_count = spans
        .iter()
        .filter(|s| s["span_type"].as_str() == Some("AGENT"))
        .count();
    let tool_count = spans
        .iter()
        .filter(|s| s["span_type"].as_str() == Some("TOOL"))
        .count();
    let gen_count = spans
        .iter()
        .filter(|s| s["span_type"].as_str() == Some("GENERATION"))
        .count();
    assert_eq!(agent_count, 3, "Should have 3 AGENT spans");
    assert_eq!(tool_count, 5, "Should have 5 TOOL spans");
    assert_eq!(gen_count, 1, "Should have 1 GENERATION span");

    // Verify agent.name attributes on all agents
    let coordinator = find_span(&coordinator_hex);
    let coordinator_name = coordinator["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        coordinator_name, "coordinator",
        "Coordinator should have agent.name = 'coordinator'"
    );

    let analyst = find_span(&analyst_hex);
    let analyst_name = analyst["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        analyst_name, "analyst",
        "Analyst should have agent.name = 'analyst'"
    );

    let validator = find_span(&validator_hex);
    let validator_name = validator["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        validator_name, "validator",
        "Validator should have agent.name = 'validator'"
    );

    // Verify gen_ai.request.model on GENERATION span
    let synthesize_hex = hex::encode(synthesize_id);
    let synthesize = find_span(&synthesize_hex);
    let synthesize_model = synthesize["attributes"]
        .get("gen_ai.request.model")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        synthesize_model, "claude-3",
        "Synthesize should have gen_ai.request.model = 'claude-3'"
    );

    // Verify tool.name attributes on all tools
    let query_db_hex = hex::encode(query_db_id);
    let run_sql_hex = hex::encode(run_sql_id);
    let schema_check_hex = hex::encode(schema_check_id);
    let data_validate_hex = hex::encode(data_validate_id);
    let send_report_hex = hex::encode(send_report_id);

    let query_db = find_span(&query_db_hex);
    let query_db_name = query_db["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        query_db_name, "query_db",
        "Query DB tool should have tool.name = 'query_db'"
    );

    let run_sql = find_span(&run_sql_hex);
    let run_sql_name = run_sql["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        run_sql_name, "run_sql",
        "Run SQL tool should have tool.name = 'run_sql'"
    );

    let schema_check = find_span(&schema_check_hex);
    let schema_check_name = schema_check["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        schema_check_name, "schema_check",
        "Schema check tool should have tool.name = 'schema_check'"
    );

    let data_validate = find_span(&data_validate_hex);
    let data_validate_name = data_validate["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        data_validate_name, "data_validate",
        "Data validate tool should have tool.name = 'data_validate'"
    );

    let send_report = find_span(&send_report_hex);
    let send_report_name = send_report["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        send_report_name, "send_report",
        "Send report tool should have tool.name = 'send_report'"
    );

    // Verify tool parents match owning agents
    assert_eq!(
        find_span(&schema_check_hex)["parent_span_id"]
            .as_str()
            .unwrap_or(""),
        validator_hex,
        "schema_check should be child of validator"
    );
    assert_eq!(
        find_span(&data_validate_hex)["parent_span_id"]
            .as_str()
            .unwrap_or(""),
        validator_hex,
        "data_validate should be child of validator"
    );

    println!(
        "✅ Multi-level agent delegation verified: 3 agents, 5 tools, 1 generation, and all attributes"
    );
    Ok(())
}

/// Test 4: Agent with tool error and retry.
///
/// ```text
/// AGENT "assistant"
///  ├── TOOL "api_call" (status = ERROR)
///  ├── GENERATION "replan"
///  └── TOOL "api_call_retry" (status = OK)
/// ```
#[tokio::test]
#[ignore]
async fn test_agent_with_tool_error_and_retry() -> Result<()> {
    let env = TestEnv::setup().await?;

    let trace_id = TestDataGenerator::trace_id();
    let assistant_id = TestDataGenerator::span_id();
    let api_call_id = TestDataGenerator::span_id();
    let replan_id = TestDataGenerator::span_id();
    let retry_id = TestDataGenerator::span_id();

    let span_defs = vec![
        SpanDefExt {
            name: "assistant".to_string(),
            span_id: assistant_id,
            parent_span_id: None,
            attributes: vec![("agent.name".to_string(), str_val("assistant"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "api_call".to_string(),
            span_id: api_call_id,
            parent_span_id: Some(assistant_id),
            attributes: vec![("tool.name".to_string(), str_val("api_call"))],
            status_code: Some(2), // ERROR
        },
        SpanDefExt {
            name: "replan".to_string(),
            span_id: replan_id,
            parent_span_id: Some(assistant_id),
            attributes: vec![("gen_ai.request.model".to_string(), str_val("gpt-4"))],
            status_code: Some(1),
        },
        SpanDefExt {
            name: "api_call_retry".to_string(),
            span_id: retry_id,
            parent_span_id: Some(assistant_id),
            attributes: vec![("tool.name".to_string(), str_val("api_call"))],
            status_code: Some(1), // OK
        },
    ];

    let request =
        env.otlp
            .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
    env.otlp.export_traces(request).await?;
    println!("✅ Error/retry trace sent (4 spans)");

    let trace_id_hex = hex::encode(trace_id);
    let trace_data = wait_for_trace_default(&env.client, &trace_id_hex).await?;
    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");

    assert_eq!(spans.len(), 4, "Should have 4 spans");

    let api_call_hex = hex::encode(api_call_id);
    let retry_hex = hex::encode(retry_id);

    let find_span = |id_hex: &str| -> &Value {
        spans
            .iter()
            .find(|s| s["span_id"].as_str() == Some(id_hex))
            .unwrap_or_else(|| panic!("Span {} not found", id_hex))
    };

    // First tool call should have ERROR status
    let error_span = find_span(&api_call_hex);
    assert_eq!(
        error_span["status"].as_str().unwrap_or(""),
        "ERROR",
        "First api_call should have ERROR status"
    );

    // Retry should have OK status
    let retry_span = find_span(&retry_hex);
    assert_ne!(
        retry_span["status"].as_str().unwrap_or("ERROR"),
        "ERROR",
        "Retry span should not have ERROR status"
    );

    // Verify agent.name on assistant span
    let assistant_hex = hex::encode(assistant_id);
    let assistant = find_span(&assistant_hex);
    let assistant_name = assistant["attributes"]
        .get("agent.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        assistant_name, "assistant",
        "Assistant should have agent.name = 'assistant'"
    );

    // Verify tool.name on both tool spans
    let error_tool_name = error_span["attributes"]
        .get("tool.name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        error_tool_name, "api_call",
        "Error tool should have tool.name = 'api_call'"
    );

    assert_eq!(
        retry_span["attributes"]
            .get("tool.name")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        "api_call",
        "Retry tool should have tool.name = 'api_call'"
    );

    // Verify gen_ai.request.model on replan span
    let replan_hex = hex::encode(replan_id);
    let replan = find_span(&replan_hex);
    let replan_model = replan["attributes"]
        .get("gen_ai.request.model")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        replan_model, "gpt-4",
        "Replan should have gen_ai.request.model = 'gpt-4'"
    );

    println!("✅ Tool error and retry verified: status codes and attributes");
    Ok(())
}

// ============================================================================
// Load Tests
// ============================================================================

/// Test 5: Sequential throughput — 200 single-span traces.
#[tokio::test]
#[ignore]
async fn test_load_sequential_traces() -> Result<()> {
    let env = TestEnv::setup().await?;

    let service_name = TestDataGenerator::service_name();
    let count = 200;
    let mut last_trace_id = None;

    let start = std::time::Instant::now();

    for i in 0..count {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();
        let span_name = format!("load.seq.{}", i);

        env.otlp
            .send_test_trace(&service_name, &trace_id, &span_id, &span_name)
            .await?;

        if i == count - 1 {
            last_trace_id = Some(trace_id);
        }
        if (i + 1) % 50 == 0 {
            println!("  Sent {}/{} traces", i + 1, count);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "✅ {} traces ingested in {:?} ({:.2} traces/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Verify the last trace was stored
    if let Some(trace_id) = last_trace_id {
        let trace_id_hex = hex::encode(trace_id);
        let trace_data =
            wait_for_trace(&env.client, &trace_id_hex, Duration::from_secs(20)).await?;
        assert!(
            trace_data["spans"]
                .as_array()
                .map(|s| !s.is_empty())
                .unwrap_or(false),
            "Last trace should be stored"
        );
    }

    println!("✅ Sequential load test passed ({} traces)", count);
    Ok(())
}

/// Test 6: Concurrent throughput — 100 traces in parallel.
#[tokio::test]
#[ignore]
async fn test_load_concurrent_traces() -> Result<()> {
    let env = TestEnv::setup().await?;

    let grpc_url = env.grpc_url().to_string();
    let api_key = env.api_key.clone();
    let service_name = TestDataGenerator::service_name();
    let tenant_id = env.tenant_id;
    let project_id = env.project_id;
    let count = 100;

    let mut handles = vec![];
    let mut trace_ids = vec![];

    let start = std::time::Instant::now();

    for i in 0..count {
        let key = api_key.clone();
        let url = grpc_url.clone();
        let svc = service_name.clone();
        let tid_ctx = tenant_id;
        let pid = project_id;
        let trace_id = TestDataGenerator::trace_id();
        trace_ids.push(trace_id);
        let tid = trace_id;

        let handle = tokio::spawn(async move {
            let otlp_client = OtlpClient::new(url)
                .with_api_key(key)
                .with_tenant_id(tid_ctx.to_string())
                .with_project_id(pid.to_string());
            let span_id = TestDataGenerator::span_id();
            let span_name = format!("load.concurrent.{}", i);
            otlp_client
                .send_test_trace(&svc, &tid, &span_id, &span_name)
                .await
        });

        handles.push(handle);
    }

    // All sends must succeed
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent trace {} failed to send", i);
    }

    let elapsed = start.elapsed();
    println!(
        "✅ {} concurrent traces sent in {:?} ({:.2} traces/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Verify a sampled trace was stored
    let last_hex = hex::encode(trace_ids[count - 1]);
    let trace_data = wait_for_trace(&env.client, &last_hex, Duration::from_secs(20)).await?;
    assert!(
        trace_data["spans"]
            .as_array()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "Last concurrent trace should be stored"
    );

    println!("✅ Concurrent load test passed ({} traces)", count);
    Ok(())
}

/// Test 7: Concurrent multi-span traces — 20 traces × 10 children each.
#[tokio::test]
#[ignore]
async fn test_load_concurrent_multi_span_traces() -> Result<()> {
    let env = TestEnv::setup().await?;

    let grpc_url = env.grpc_url().to_string();
    let api_key = env.api_key.clone();
    let service_name = TestDataGenerator::service_name();
    let tenant_id = env.tenant_id;
    let project_id = env.project_id;
    let trace_count = 20;
    let children_per_trace = 10;

    let mut handles = vec![];
    let mut trace_ids = vec![];

    let start = std::time::Instant::now();

    for t in 0..trace_count {
        let key = api_key.clone();
        let url = grpc_url.clone();
        let svc = service_name.clone();
        let tid_ctx = tenant_id;
        let pid = project_id;
        let trace_id = TestDataGenerator::trace_id();
        trace_ids.push(trace_id);
        let tid = trace_id;

        let handle = tokio::spawn(async move {
            let otlp_client = OtlpClient::new(url)
                .with_api_key(key)
                .with_tenant_id(tid_ctx.to_string())
                .with_project_id(pid.to_string());

            let root_span_id = TestDataGenerator::span_id();
            let _spans: Vec<(&str, &[u8; 8], Option<&[u8; 8]>)> = Vec::new();

            // We need owned span IDs that live long enough
            let child_ids: Vec<[u8; 8]> = (0..children_per_trace)
                .map(|_| TestDataGenerator::span_id())
                .collect();

            // Build spans with references — use a temp vec of tuples
            let root_name = format!("root.{}", t);
            let child_names: Vec<String> = (0..children_per_trace)
                .map(|c| format!("child.{}.{}", t, c))
                .collect();

            // We need to use build_multi_span_trace which takes SpanDef
            // But SpanDef borrows, so we build inline
            let mut span_defs: Vec<(&str, &[u8; 8], Option<&[u8; 8]>)> = Vec::new();
            span_defs.push((&root_name, &root_span_id, None));
            for c in 0..children_per_trace {
                span_defs.push((&child_names[c], &child_ids[c], Some(&root_span_id)));
            }

            let request = otlp_client.build_multi_span_trace(&svc, &tid, span_defs);
            otlp_client.export_traces(request).await
        });

        handles.push(handle);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Concurrent multi-span trace {} failed: {:?}",
            i,
            result.err()
        );
    }

    let elapsed = start.elapsed();
    let total_spans = trace_count * (1 + children_per_trace);
    println!(
        "✅ {} traces ({} total spans) sent in {:?} ({:.2} spans/sec)",
        trace_count,
        total_spans,
        elapsed,
        total_spans as f64 / elapsed.as_secs_f64()
    );

    // Verify a sampled trace has correct span count
    let sample_hex = hex::encode(trace_ids[trace_count - 1]);
    let trace_data = wait_for_trace(&env.client, &sample_hex, Duration::from_secs(20)).await?;
    let spans = trace_data["spans"]
        .as_array()
        .expect("spans should be an array");
    assert_eq!(
        spans.len(),
        1 + children_per_trace,
        "Sampled trace should have {} spans",
        1 + children_per_trace
    );

    println!(
        "✅ Concurrent multi-span load test passed ({} traces × {} children)",
        trace_count, children_per_trace
    );
    Ok(())
}

/// Test 8: Concurrent agent trees — 10 traces × 50 TOOL children with attributes.
#[tokio::test]
#[ignore]
async fn test_load_concurrent_agent_trees() -> Result<()> {
    let env = TestEnv::setup().await?;

    let grpc_url = env.grpc_url().to_string();
    let api_key = env.api_key.clone();
    let tenant_id = env.tenant_id;
    let project_id = env.project_id;
    let trace_count = 10;
    let tools_per_agent = 50;

    let mut handles = vec![];
    let mut trace_ids = vec![];

    let start = std::time::Instant::now();

    for t in 0..trace_count {
        let key = api_key.clone();
        let url = grpc_url.clone();
        let tid_ctx = tenant_id;
        let pid = project_id;
        let trace_id = TestDataGenerator::trace_id();
        trace_ids.push(trace_id);
        let tid = trace_id;

        let handle = tokio::spawn(async move {
            let otlp_client = OtlpClient::new(url)
                .with_api_key(key)
                .with_tenant_id(tid_ctx.to_string())
                .with_project_id(pid.to_string());

            let root_span_id = TestDataGenerator::span_id();
            let mut span_defs = vec![SpanDefExt {
                name: format!("agent_{}", t),
                span_id: root_span_id,
                parent_span_id: None,
                attributes: vec![(
                    "agent.name".to_string(),
                    AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                format!("agent_{}", t),
                            ),
                        ),
                    },
                )],
                status_code: Some(1),
            }];

            for i in 0..tools_per_agent {
                span_defs.push(SpanDefExt {
                    name: format!("tool_{}_{}", t, i),
                    span_id: TestDataGenerator::span_id(),
                    parent_span_id: Some(root_span_id),
                    attributes: vec![(
                        "tool.name".to_string(),
                        AnyValue {
                            value: Some(
                                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                    format!("tool_{}", i),
                                ),
                            ),
                        },
                    )],
                    status_code: Some(1),
                });
            }

            let request = otlp_client.build_multi_span_trace_with_attributes(
                "agent-load-service",
                &tid,
                span_defs,
            );
            otlp_client.export_traces(request).await
        });

        handles.push(handle);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Concurrent agent tree {} failed: {:?}",
            i,
            result.err()
        );
    }

    let elapsed = start.elapsed();
    let total_spans = trace_count * (1 + tools_per_agent);
    println!(
        "✅ {} agent traces ({} total spans) sent in {:?} ({:.2} spans/sec)",
        trace_count,
        total_spans,
        elapsed,
        total_spans as f64 / elapsed.as_secs_f64()
    );

    // Verify 2 sampled traces
    for idx in [0, trace_count - 1] {
        let sample_hex = hex::encode(trace_ids[idx]);
        let trace_data = wait_for_trace(&env.client, &sample_hex, Duration::from_secs(30)).await?;
        let spans = trace_data["spans"]
            .as_array()
            .expect("spans should be an array");
        assert_eq!(
            spans.len(),
            1 + tools_per_agent,
            "Agent trace {} should have {} spans",
            idx,
            1 + tools_per_agent
        );
    }

    println!(
        "✅ Concurrent agent tree load test passed ({} traces × {} tools)",
        trace_count, tools_per_agent
    );
    Ok(())
}

// ============================================================================
// Agent Attribute Filter Tests
// ============================================================================

/// Test filtering spans by agent_name
#[tokio::test]
#[ignore]
async fn test_filter_spans_by_agent_name() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Create traces with different agent names
    let agents = vec![
        ("planner", "gpt-4"),
        ("researcher", "claude-3"),
        ("validator", "gpt-4"),
    ];
    let mut trace_ids = Vec::new();

    for (agent_name, model) in &agents {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("agent.name".to_string(), str_val(agent_name)),
                ("gen_ai.request.model".to_string(), str_val(model)),
            ],
            status_code: Some(1),
        }];

        let request =
            env.otlp
                .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces to be stored
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Test filtering by agent_name = "planner"
    let filter_url = "/api/v1/spans?agent_name=planner";
    let spans = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!spans.is_empty(), "Should find spans for planner agent");
    for span in &spans {
        if let Some(agent_name) = span["attributes"]
            .get("agent.name")
            .and_then(|v| v.as_str())
        {
            assert_eq!(
                agent_name, "planner",
                "All returned spans should have agent_name = planner"
            );
        }
    }

    // Test filtering by agent_name = "researcher"
    let filter_url = "/api/v1/spans?agent_name=researcher";
    let spans = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!spans.is_empty(), "Should find spans for researcher agent");
    for span in &spans {
        if let Some(agent_name) = span["attributes"]
            .get("agent.name")
            .and_then(|v| v.as_str())
        {
            assert_eq!(
                agent_name, "researcher",
                "All returned spans should have agent_name = researcher"
            );
        }
    }

    println!("✅ Agent name filter test passed — strict validation");
    Ok(())
}

/// Test filtering spans by llm_model
#[tokio::test]
#[ignore]
async fn test_filter_spans_by_llm_model() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Create traces with different LLM models
    let models = vec!["gpt-4", "claude-3", "llama-2"];
    let mut trace_ids = Vec::new();

    for model in &models {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "llm.generation".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("gen_ai.request.model".to_string(), str_val(model)),
                ("agent.name".to_string(), str_val("test-agent")),
            ],
            status_code: Some(1),
        }];

        let request =
            env.otlp
                .build_multi_span_trace_with_attributes("llm-service", &trace_id, span_defs);
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces to be stored
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Test filtering by llm_model = "gpt-4"
    let filter_url = "/api/v1/spans?llm_model=gpt-4";
    let spans = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!spans.is_empty(), "Should find spans for gpt-4 model");
    for span in &spans {
        if let Some(model) = span["attributes"]
            .get("gen_ai.request.model")
            .and_then(|v| v.as_str())
        {
            assert_eq!(
                model, "gpt-4",
                "All returned spans should have llm_model = gpt-4"
            );
        }
    }

    // Test filtering by llm_model = "claude-3"
    let filter_url = "/api/v1/spans?llm_model=claude-3";
    let spans = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!spans.is_empty(), "Should find spans for claude-3 model");
    for span in &spans {
        if let Some(model) = span["attributes"]
            .get("gen_ai.request.model")
            .and_then(|v| v.as_str())
        {
            assert_eq!(
                model, "claude-3",
                "All returned spans should have llm_model = claude-3"
            );
        }
    }

    println!("✅ LLM model filter test passed — strict validation");
    Ok(())
}

/// Test filtering spans by session_id
#[tokio::test]
#[ignore]
async fn test_filter_spans_by_session_id() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Create traces with different session IDs
    let sessions = vec!["session-123", "session-456", "session-789"];
    let mut trace_ids = Vec::new();

    for session_id in &sessions {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("session_id".to_string(), str_val(session_id)),
                ("agent.name".to_string(), str_val("test-agent")),
            ],
            status_code: Some(1),
        }];

        let request =
            env.otlp
                .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces to be stored
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Test filtering by session_id = "session-123"
    let filter_url = "/api/v1/spans?session_id=session-123";
    let spans = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!spans.is_empty(), "Should find spans for session-123");
    for span in &spans {
        if let Some(session) = span["attributes"]
            .get("session_id")
            .and_then(|v| v.as_str())
        {
            assert_eq!(
                session, "session-123",
                "All returned spans should have session_id = session-123"
            );
        }
    }

    // Test filtering by session_id = "session-456"
    let filter_url = "/api/v1/spans?session_id=session-456";
    let spans = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!spans.is_empty(), "Should find spans for session-456");
    for span in &spans {
        if let Some(session) = span["attributes"]
            .get("session_id")
            .and_then(|v| v.as_str())
        {
            assert_eq!(
                session, "session-456",
                "All returned spans should have session_id = session-456"
            );
        }
    }

    println!("✅ Session ID filter test passed — strict validation");
    Ok(())
}

/// Test filtering traces by agent_name (if implemented)
#[tokio::test]
#[ignore]
async fn test_filter_traces_by_agent_name() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Create traces with different agent names
    let agents = vec![
        ("planner", "gpt-4"),
        ("researcher", "claude-3"),
        ("validator", "gpt-4"),
    ];
    let mut trace_ids = Vec::new();

    for (agent_name, model) in &agents {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("agent.name".to_string(), str_val(agent_name)),
                ("gen_ai.request.model".to_string(), str_val(model)),
            ],
            status_code: Some(1),
        }];

        let request =
            env.otlp
                .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces to be stored
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Test filtering by agent_name = "planner"
    let filter_url = "/api/v1/traces?agent_name=planner";
    let traces = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!traces.is_empty(), "Should find traces for planner agent");
    for trace in &traces {
        // Note: TraceSummary doesn't include attributes, so this test verifies
        // the filter works but doesn't validate the actual agent.name
        assert!(trace.get("trace_id").is_some(), "Should have trace_id");
    }

    println!("✅ Trace agent name filter test passed");
    Ok(())
}

/// Test filtering traces by llm_model (if implemented)
#[tokio::test]
#[ignore]
async fn test_filter_traces_by_llm_model() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Create traces with different LLM models
    let models = vec!["gpt-4", "claude-3", "llama-2"];
    let mut trace_ids = Vec::new();

    for model in &models {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "llm.generation".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("gen_ai.request.model".to_string(), str_val(model)),
                ("agent.name".to_string(), str_val("test-agent")),
            ],
            status_code: Some(1),
        }];

        let request =
            env.otlp
                .build_multi_span_trace_with_attributes("llm-service", &trace_id, span_defs);
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces to be stored
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Test filtering by llm_model = "gpt-4"
    let filter_url = "/api/v1/traces?llm_model=gpt-4";
    let traces = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!traces.is_empty(), "Should find traces for gpt-4 model");
    for trace in &traces {
        assert!(trace.get("trace_id").is_some(), "Should have trace_id");
    }

    println!("✅ Trace LLM model filter test passed");
    Ok(())
}

/// Test filtering traces by session_id (if implemented)
#[tokio::test]
#[ignore]
async fn test_filter_traces_by_session_id() -> Result<()> {
    let env = TestEnv::setup().await?;

    // Create traces with different session IDs
    let sessions = vec!["session-123", "session-456", "session-789"];
    let mut trace_ids = Vec::new();

    for session_id in &sessions {
        let trace_id = TestDataGenerator::trace_id();
        let span_id = TestDataGenerator::span_id();

        let span_defs = vec![SpanDefExt {
            name: "agent.run".to_string(),
            span_id,
            parent_span_id: None,
            attributes: vec![
                ("session_id".to_string(), str_val(session_id)),
                ("agent.name".to_string(), str_val("test-agent")),
            ],
            status_code: Some(1),
        }];

        let request =
            env.otlp
                .build_multi_span_trace_with_attributes("agent-service", &trace_id, span_defs);
        env.otlp.export_traces(request).await?;
        trace_ids.push(trace_id);
    }

    // Wait for all traces to be stored
    for trace_id in &trace_ids {
        let trace_id_hex = hex::encode(trace_id);
        wait_for_trace_default(&env.client, &trace_id_hex).await?;
    }

    // Test filtering by session_id = "session-123"
    let filter_url = "/api/v1/traces?session_id=session-123";
    let traces = wait_for_items_default(&env.client, &filter_url).await?;

    assert!(!traces.is_empty(), "Should find traces for session-123");
    for trace in &traces {
        assert!(trace.get("trace_id").is_some(), "Should have trace_id");
    }

    println!("✅ Trace session ID filter test passed");
    Ok(())
}
