-- Seed permission definitions

-- ============================================================================
-- Seed Permission Definitions
-- ============================================================================
INSERT INTO permission_definitions (id, category, action, name, description, applicable_scopes, risk_level) VALUES
-- Traces
('traces:read', 'traces', 'read', 'View Traces', 'View trace data and search traces', ARRAY['project'], 'low'),
('traces:write', 'traces', 'write', 'Write Traces', 'Send trace data via API', ARRAY['project'], 'low'),
('traces:delete', 'traces', 'delete', 'Delete Traces', 'Delete trace data (use with caution)', ARRAY['project'], 'high'),
('traces:export', 'traces', 'export', 'Export Traces', 'Export trace data to external systems', ARRAY['project'], 'medium'),
('traces:*', 'traces', 'all', 'All Trace Permissions', 'Full access to traces', ARRAY['project'], 'high'),

-- Metrics
('metrics:read', 'metrics', 'read', 'View Metrics', 'View metrics and dashboards', ARRAY['project'], 'low'),
('metrics:write', 'metrics', 'write', 'Write Metrics', 'Send metrics data via API', ARRAY['project'], 'low'),
('metrics:*', 'metrics', 'all', 'All Metrics Permissions', 'Full access to metrics', ARRAY['project'], 'high'),

-- API Keys
('api_keys:read', 'api_keys', 'read', 'View API Keys', 'View API keys (not the secret)', ARRAY['project'], 'low'),
('api_keys:create', 'api_keys', 'create', 'Create API Keys', 'Create new API keys', ARRAY['project'], 'medium'),
('api_keys:revoke', 'api_keys', 'revoke', 'Revoke API Keys', 'Revoke/disable API keys', ARRAY['project'], 'medium'),
('api_keys:delete', 'api_keys', 'delete', 'Delete API Keys', 'Permanently delete API keys', ARRAY['project'], 'high'),
('api_keys:*', 'api_keys', 'all', 'All API Key Permissions', 'Full API key management', ARRAY['project'], 'high'),

-- Project Settings
('project:read', 'project', 'read', 'View Project', 'View project information', ARRAY['project'], 'low'),
('project:settings', 'project', 'settings', 'Modify Settings', 'Change project settings', ARRAY['project'], 'medium'),
('project:members', 'project', 'members', 'Manage Members', 'Add/remove project members', ARRAY['project'], 'high'),
('project:delete', 'project', 'delete', 'Delete Project', 'Permanently delete project', ARRAY['project'], 'critical'),
('project:*', 'project', 'all', 'All Project Permissions', 'Full project control', ARRAY['project'], 'critical'),

-- Organization (only for org-level roles)
('org:settings', 'organization', 'settings', 'Org Settings', 'Modify organization settings', ARRAY['organization'], 'medium'),
('org:billing', 'organization', 'billing', 'Manage Billing', 'Access billing and payment info', ARRAY['organization'], 'high'),
('org:members', 'organization', 'members', 'Manage Members', 'Add/remove organization members', ARRAY['organization'], 'high'),
('org:roles', 'organization', 'roles', 'Manage Roles', 'Create and modify custom roles', ARRAY['organization'], 'high'),
('org:delete', 'organization', 'delete', 'Delete Organization', 'Permanently delete organization', ARRAY['organization'], 'critical'),

-- Wildcards
('*', 'all', 'all', 'Super Admin', 'Full access to everything', ARRAY['organization', 'project'], 'critical')
ON CONFLICT (id) DO NOTHING;

