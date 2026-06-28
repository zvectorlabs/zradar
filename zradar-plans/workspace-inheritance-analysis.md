# Analysis: Embeddable Workspaces (Workspace Inheritance)

The user asked to evaluate the complexity of implementing "workspace inside workspace" (inheritance hierarchy: Tenant -> Project -> Workspace). 

## 1. Concept Overview

A hierarchical workspace structure allows an organization (Tenant) to have multiple Projects, and each Project to have multiple Workspaces. 

- **Tenant**: Top-level billing and organization boundary.
- **Project**: Logical grouping of related workspaces (e.g., "Production", "Staging").
- **Workspace**: The lowest-level unit of isolation where data is written and queried (e.g., "K8s Cluster A", "Microservice B").

## 2. Management Side (Complexity: Medium)

### Implications
- **Policy Enforcement**: Policies (retention, quotas, ingestion limits) would need to be inheritable. If a Workspace doesn't have a specific policy, it falls back to the Project policy, and then to the Tenant policy.
- **Settings Propagation**: We'd need to modify `WorkspaceSettings` to allow `NULL` or "inherited" states for fields. The `SettingsRepository` would have to recursively resolve settings.
- **Auth & Capabilities**: The authorization layer (`AdminAuth`, `ApiKeyConfig`) would need to understand "Role-Based Access Control (RBAC) inheritance" — i.e., having "Admin" access on a Tenant implies "Admin" on all its Projects and Workspaces.

### Doability
- This is very doable. It requires adding a `parent_id` (or `tenant_id` and `project_id`) back to the workspace configuration tables (e.g., `workspace_settings`).
- We can handle inheritance strictly at the API layer so the runtime doesn't have to perform recursive lookups for every request.

## 3. Write Side (Complexity: Low to Medium)

### Implications
- **Ingestion Guard**: The write path (`api-optel`) currently resolves the `workspace_id` from the API key. 
- **WAL & Parquet**: The storage engine uses `workspace_id` as the physical partitioning key.

### Doability
- **Low Complexity**: If data is always written directly to a specific leaf `workspace_id`, the ingestion path is completely unaffected. The `workspace_id` acts as the physical partition. The inheritance hierarchy only matters for validating the API key (which belongs to a workspace).

## 4. Query Side (Complexity: High)

### Implications
- **Cross-Workspace Queries**: Users will expect to query across a Tenant (all projects) or across a Project (all workspaces).
- **Physical Partitioning**: Currently, Parquet files are partitioned by `workspace_id`. A cross-workspace query would require the query engine (`DataFusion` or the `QueryService`) to scan multiple `workspace_id` partitions concurrently.
- **Query Enforcer**: The `QueryEnforcer` currently enforces retention policies per `workspace_id`. It would need to apply different retention bounds for different files within the same cross-workspace query.

### Doability
- **High Complexity**: 
  - To support querying at the Tenant level, the query engine needs a new API to specify *multiple* `workspace_id`s, or we need an API endpoint like `GET /api/v1/tenant/{tenant_id}/query` that expands into all underlying `workspace_id`s.
  - The `FileMover` and `FileReclaimer` operate on individual workspaces.
  - The query layer needs to merge `RecordBatches` from multiple workspaces, which might have differing schemas if they evolve independently.

## Conclusion

Implementing a **Tenant -> Project -> Workspace** hierarchy is highly beneficial for enterprise adoption but introduces specific complexities:

1. **Physical vs Logical**: Keep `workspace_id` as the **only physical partitioning key** in storage (`zradar-parquet`, `zradar-wal`).
2. **Management Layer**: Re-introduce `tenant_id` and `project_id` purely as **logical grouping constructs** in the database (`workspace_settings` table would have `tenant_id` and `project_id` foreign keys).
3. **Query Expansion**: Implement cross-workspace queries by having the API expand a `tenant_id` into a list of `workspace_id`s and feeding that list into the query engine.

**Recommendation**: Delay this until the base `workspace_id` refactoring is fully stabilized. When we do implement it, it should be entirely a metadata/API-layer concern, leaving the core data plane (`zradar-parquet`, `zradar-wal`) ignorant of the hierarchy and strictly bound to `workspace_id`.
