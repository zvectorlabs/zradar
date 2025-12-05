-- Permission definitions and custom roles

-- ============================================================================
-- Permission Definitions
-- ============================================================================
CREATE TABLE IF NOT EXISTS permission_definitions (
    id VARCHAR(100) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    action VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    applicable_scopes TEXT[] NOT NULL,
    risk_level VARCHAR(20) DEFAULT 'low' NOT NULL,
    requires TEXT[],
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_permission_defs_category ON permission_definitions(category);
CREATE INDEX IF NOT EXISTS idx_permission_defs_active ON permission_definitions(is_active) WHERE is_active = true;

-- ============================================================================
-- Custom Roles
-- ============================================================================
CREATE TABLE IF NOT EXISTS custom_roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    scope VARCHAR(20) DEFAULT 'project' NOT NULL,
    permissions TEXT[] NOT NULL,
    is_system BOOLEAN DEFAULT false NOT NULL,
    color VARCHAR(20),
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE(organization_id, name, scope)
);

CREATE INDEX IF NOT EXISTS idx_custom_roles_org ON custom_roles(organization_id);
CREATE INDEX IF NOT EXISTS idx_custom_roles_scope ON custom_roles(organization_id, scope);

CREATE TRIGGER update_custom_roles_updated_at BEFORE UPDATE ON custom_roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add foreign key constraints now that custom_roles exists
ALTER TABLE organization_members 
    ADD CONSTRAINT fk_org_members_custom_role 
    FOREIGN KEY (custom_role_id) REFERENCES custom_roles(id) ON DELETE SET NULL;

ALTER TABLE project_members 
    ADD CONSTRAINT fk_project_members_custom_role 
    FOREIGN KEY (custom_role_id) REFERENCES custom_roles(id) ON DELETE SET NULL;

-- ============================================================================
-- Role Audit Log
-- ============================================================================
CREATE TABLE IF NOT EXISTS role_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    role_id UUID NOT NULL,
    action VARCHAR(50) NOT NULL,
    changes JSONB,
    performed_by UUID REFERENCES users(id),
    performed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_role_audit_org ON role_audit_log(organization_id, performed_at DESC);
CREATE INDEX IF NOT EXISTS idx_role_audit_role ON role_audit_log(role_id, performed_at DESC);

