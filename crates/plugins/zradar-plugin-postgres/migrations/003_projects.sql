-- Projects and project members

-- ============================================================================
-- Projects
-- ============================================================================
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    slug VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    environment VARCHAR(50) DEFAULT 'production' NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    retention_days INTEGER DEFAULT 90 NOT NULL,
    sampling_rate FLOAT DEFAULT 1.0 NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    settings JSONB DEFAULT '{}'::jsonb NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb NOT NULL,
    UNIQUE(organization_id, slug)
);

CREATE INDEX IF NOT EXISTS idx_projects_org ON projects(organization_id);
CREATE INDEX IF NOT EXISTS idx_projects_slug ON projects(organization_id, slug);
CREATE INDEX IF NOT EXISTS idx_projects_active ON projects(is_active) WHERE is_active = true;

CREATE TRIGGER update_projects_updated_at BEFORE UPDATE ON projects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Project Members
-- ============================================================================
CREATE TABLE IF NOT EXISTS project_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role VARCHAR(50),
    custom_role_id UUID,
    permissions TEXT[] DEFAULT ARRAY[]::TEXT[] NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    added_by UUID REFERENCES users(id),
    joined_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE(project_id, user_id),
    CHECK (role IS NOT NULL OR custom_role_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_project_members_project ON project_members(project_id);
CREATE INDEX IF NOT EXISTS idx_project_members_user ON project_members(user_id);
CREATE INDEX IF NOT EXISTS idx_project_members_active ON project_members(is_active) WHERE is_active = true;

