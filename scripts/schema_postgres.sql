-- PostgreSQL Schema for zradar Control Plane
-- 
-- NOTE: This is a consolidated schema file for reference.
-- For production, use sqlx migrations in the migrations/ directory
-- Run migrations with: sqlx migrate run
--
-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    full_name VARCHAR(255),
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    email_verified BOOLEAN DEFAULT false NOT NULL,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    last_login_at TIMESTAMPTZ,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_active ON users(is_active) WHERE is_active = true;

-- Organizations table
CREATE TABLE IF NOT EXISTS organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Owner
    owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    
    -- Billing/Limits
    plan VARCHAR(50) DEFAULT 'free' NOT NULL,
    monthly_span_limit BIGINT DEFAULT 1000000 NOT NULL,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    -- Metadata
    settings JSONB DEFAULT '{}'::jsonb NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL
);

CREATE INDEX idx_organizations_slug ON organizations(slug);
CREATE INDEX idx_organizations_owner ON organizations(owner_id);
CREATE INDEX idx_organizations_active ON organizations(is_active) WHERE is_active = true;

-- Organization members table
CREATE TABLE IF NOT EXISTS organization_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Role (predefined or custom)
    role VARCHAR(50),  -- 'owner', 'admin', 'member'
    custom_role_id UUID,  -- References custom_roles, added later
    
    -- Permissions (optional granular control)
    permissions TEXT[] DEFAULT ARRAY[]::TEXT[] NOT NULL,
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    invited_by UUID REFERENCES users(id),
    joined_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    UNIQUE(organization_id, user_id),
    CHECK (role IS NOT NULL OR custom_role_id IS NOT NULL)
);

CREATE INDEX idx_org_members_org ON organization_members(organization_id);
CREATE INDEX idx_org_members_user ON organization_members(user_id);
CREATE INDEX idx_org_members_active ON organization_members(is_active) WHERE is_active = true;

-- Projects table
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    
    slug VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    environment VARCHAR(50) DEFAULT 'production' NOT NULL,
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    
    -- Settings
    retention_days INTEGER DEFAULT 90 NOT NULL,
    sampling_rate FLOAT DEFAULT 1.0 NOT NULL,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    -- Metadata
    settings JSONB DEFAULT '{}'::jsonb NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    
    UNIQUE(organization_id, slug)
);

CREATE INDEX idx_projects_org ON projects(organization_id);
CREATE INDEX idx_projects_active ON projects(is_active) WHERE is_active = true;

-- Project members table
CREATE TABLE IF NOT EXISTS project_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Role (predefined or custom)
    role VARCHAR(50),  -- 'maintainer', 'developer', 'viewer'
    custom_role_id UUID,  -- References custom_roles
    
    -- Granular permissions (optional)
    permissions TEXT[] DEFAULT ARRAY[]::TEXT[] NOT NULL,
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    added_by UUID REFERENCES users(id),
    joined_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    UNIQUE(project_id, user_id),
    CHECK (role IS NOT NULL OR custom_role_id IS NOT NULL)
);

CREATE INDEX idx_project_members_project ON project_members(project_id);
CREATE INDEX idx_project_members_user ON project_members(user_id);
CREATE INDEX idx_project_members_active ON project_members(is_active) WHERE is_active = true;

-- Permission definitions table
CREATE TABLE IF NOT EXISTS permission_definitions (
    id VARCHAR(100) PRIMARY KEY,
    
    -- Categorization
    category VARCHAR(50) NOT NULL,
    action VARCHAR(50) NOT NULL,
    
    -- Display
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    
    -- Scope
    applicable_scopes TEXT[] NOT NULL,
    
    -- Risk level
    risk_level VARCHAR(20) DEFAULT 'low' NOT NULL,
    
    -- Dependencies
    requires TEXT[],
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_permission_defs_category ON permission_definitions(category);
CREATE INDEX idx_permission_defs_active ON permission_definitions(is_active) WHERE is_active = true;

-- Custom roles table
CREATE TABLE IF NOT EXISTS custom_roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    
    -- Role definition
    name VARCHAR(100) NOT NULL,
    description TEXT,
    scope VARCHAR(20) DEFAULT 'project' NOT NULL,  -- 'organization' or 'project'
    
    -- Permissions
    permissions TEXT[] NOT NULL,
    
    -- Metadata
    is_system BOOLEAN DEFAULT false NOT NULL,
    color VARCHAR(20),
    
    -- Lifecycle
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    UNIQUE(organization_id, name, scope)
);

CREATE INDEX idx_custom_roles_org ON custom_roles(organization_id);
CREATE INDEX idx_custom_roles_scope ON custom_roles(organization_id, scope);

-- Add foreign key constraints for custom_role_id now that custom_roles exists
ALTER TABLE organization_members 
    ADD CONSTRAINT fk_org_members_custom_role 
    FOREIGN KEY (custom_role_id) REFERENCES custom_roles(id) ON DELETE SET NULL;

ALTER TABLE project_members 
    ADD CONSTRAINT fk_project_members_custom_role 
    FOREIGN KEY (custom_role_id) REFERENCES custom_roles(id) ON DELETE SET NULL;

-- API keys table
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    -- Key (hashed)
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix VARCHAR(20) NOT NULL,
    
    -- Metadata
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Permissions
    permissions TEXT[] DEFAULT ARRAY['write:traces', 'write:metrics']::TEXT[] NOT NULL,
    
    -- Security
    ip_whitelist INET[],
    rate_limit_per_minute INTEGER,
    
    -- Status
    is_active BOOLEAN DEFAULT true NOT NULL,
    
    -- Lifecycle
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_org_project ON api_keys(organization_id, project_id);
CREATE INDEX idx_api_keys_active ON api_keys(is_active) WHERE is_active = true;

-- Audit logs table
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    
    -- Actor
    user_id UUID REFERENCES users(id),
    actor_type VARCHAR(50),  -- 'user', 'api_key', 'system'
    actor_id UUID,
    actor_ip INET,
    
    -- Action
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id UUID,
    
    -- Details
    status VARCHAR(20) NOT NULL,  -- 'success', 'failure', 'permission_denied'
    details JSONB,
    
    -- Timestamp
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_audit_logs_org ON audit_logs(organization_id, created_at DESC);
CREATE INDEX idx_audit_logs_user ON audit_logs(user_id, created_at DESC);
CREATE INDEX idx_audit_logs_action ON audit_logs(action, created_at DESC);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at DESC);

-- Role audit log table
CREATE TABLE IF NOT EXISTS role_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    role_id UUID NOT NULL,
    action VARCHAR(50) NOT NULL,  -- 'created', 'updated', 'deleted'
    changes JSONB,
    performed_by UUID REFERENCES users(id),
    performed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_role_audit_org ON role_audit_log(organization_id, performed_at DESC);
CREATE INDEX idx_role_audit_role ON role_audit_log(role_id, performed_at DESC);

-- Seed permission definitions
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

-- Update triggers for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_organizations_updated_at BEFORE UPDATE ON organizations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_projects_updated_at BEFORE UPDATE ON projects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_custom_roles_updated_at BEFORE UPDATE ON custom_roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_api_keys_updated_at BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

