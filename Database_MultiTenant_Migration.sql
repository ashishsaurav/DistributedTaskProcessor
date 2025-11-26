-- Multi-Tenant Database Schema Migration Script
-- This script adds multi-tenant support to the DistributedTaskProcessor database

-- Step 1: Create Tenants table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Tenants')
BEGIN
    CREATE TABLE dbo.Tenants (
        TenantId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        TenantName NVARCHAR(255) NOT NULL UNIQUE,
        Status NVARCHAR(50) NOT NULL DEFAULT 'Active', -- Active, Suspended, Deleted
        MaxWorkers INT NOT NULL DEFAULT 10,
        MaxPartitionSize INT NOT NULL DEFAULT 10000,
        MaxConcurrentTasks INT NOT NULL DEFAULT 50,
        CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        IsDeleted BIT NOT NULL DEFAULT 0
    );
    
    CREATE UNIQUE INDEX UX_Tenants_Name ON dbo.Tenants(TenantName) WHERE IsDeleted = 0;
    PRINT 'Created Tenants table';
END
ELSE
BEGIN
    PRINT 'Tenants table already exists';
END;

-- Step 2: Add TenantId to Tasks table if not exists
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Tasks') AND name = 'TenantId')
BEGIN
    -- Create default tenant if needed
    IF NOT EXISTS (SELECT 1 FROM dbo.Tenants)
    BEGIN
        INSERT INTO dbo.Tenants (TenantId, TenantName, Status)
        VALUES (NEWID(), 'Default', 'Active');
        PRINT 'Created default tenant';
    END;
    
    -- Get the default tenant ID
    DECLARE @DefaultTenant UNIQUEIDENTIFIER = (SELECT TOP 1 TenantId FROM dbo.Tenants);
    
    -- Add column as nullable first
    ALTER TABLE dbo.Tasks ADD TenantId UNIQUEIDENTIFIER NULL;
    PRINT 'Added TenantId column to Tasks table (nullable)';
    
    -- Update all existing rows with the default tenant
    UPDATE dbo.Tasks SET TenantId = @DefaultTenant WHERE TenantId IS NULL;
    PRINT 'Updated existing Tasks with default tenant';
    
    -- Alter column to NOT NULL
    ALTER TABLE dbo.Tasks ALTER COLUMN TenantId UNIQUEIDENTIFIER NOT NULL;
    PRINT 'Changed TenantId to NOT NULL';
END;

-- Step 3: Add TenantId to WorkerHeartbeats if not exists
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.WorkerHeartbeats') AND name = 'TenantId')
BEGIN
    ALTER TABLE dbo.WorkerHeartbeats ADD TenantId UNIQUEIDENTIFIER NULL;
    PRINT 'Added TenantId to WorkerHeartbeats table';
END;

-- Step 4: Add health tracking columns to WorkerHeartbeats if not exist
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.WorkerHeartbeats') AND name = 'HealthScore')
BEGIN
    ALTER TABLE dbo.WorkerHeartbeats ADD 
        HealthScore INT DEFAULT 100,
        ErrorRate DECIMAL(5,2) DEFAULT 0,
        CpuUsage DECIMAL(5,2) DEFAULT 0,
        MemoryUsage DECIMAL(5,2) DEFAULT 0;
    PRINT 'Added health columns to WorkerHeartbeats table';
END;

-- Step 5: Create SourceData table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SourceData')
BEGIN
    CREATE TABLE dbo.SourceData (
        SourceDataId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        TenantId UNIQUEIDENTIFIER NOT NULL,
        Fund NVARCHAR(50),
        Symbol NVARCHAR(50),
        RunDate DATE,
        TotalRows BIGINT,
        ProcessedRows BIGINT DEFAULT 0,
        Status NVARCHAR(50) NOT NULL DEFAULT 'Pending',
        CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY (TenantId) REFERENCES dbo.Tenants(TenantId)
    );
    
    CREATE INDEX IX_SourceData_Tenant ON dbo.SourceData(TenantId);
    CREATE INDEX IX_SourceData_Status ON dbo.SourceData(Status);
    PRINT 'Created SourceData table';
END
ELSE
BEGIN
    PRINT 'SourceData table already exists';
END;

-- Step 6: Create TaskPartitions table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TaskPartitions')
BEGIN
    CREATE TABLE dbo.TaskPartitions (
        PartitionId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        SourceDataId UNIQUEIDENTIFIER NOT NULL,
        TenantId UNIQUEIDENTIFIER NOT NULL,
        StartRow BIGINT NOT NULL,
        EndRow BIGINT NOT NULL,
        AssignedWorker NVARCHAR(100),
        Status NVARCHAR(50) NOT NULL DEFAULT 'Pending',
        RetryCount INT NOT NULL DEFAULT 0,
        ProcessingStartTime DATETIME2,
        ProcessingEndTime DATETIME2,
        ErrorMessage NVARCHAR(MAX),
        CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY (TenantId) REFERENCES dbo.Tenants(TenantId),
        FOREIGN KEY (SourceDataId) REFERENCES dbo.SourceData(SourceDataId)
    );
    
    CREATE INDEX IX_TaskPartitions_Status ON dbo.TaskPartitions(Status);
    CREATE INDEX IX_TaskPartitions_Worker ON dbo.TaskPartitions(AssignedWorker);
    CREATE INDEX IX_TaskPartitions_SourceData ON dbo.TaskPartitions(SourceDataId);
    PRINT 'Created TaskPartitions table';
END
ELSE
BEGIN
    PRINT 'TaskPartitions table already exists';
END;

-- Step 7: Create WorkerMetrics table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'WorkerMetrics')
BEGIN
    CREATE TABLE dbo.WorkerMetrics (
        MetricId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        WorkerId NVARCHAR(100) NOT NULL,
        TenantId UNIQUEIDENTIFIER,
        TasksCompleted INT DEFAULT 0,
        TasksFailed INT DEFAULT 0,
        RowsProcessed BIGINT DEFAULT 0,
        AverageProcessingTimeMs INT DEFAULT 0,
        MaxMemoryUsageMb INT DEFAULT 0,
        MaxCpuUsage DECIMAL(5,2) DEFAULT 0,
        RecordedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
    
    CREATE INDEX IX_WorkerMetrics_Worker ON dbo.WorkerMetrics(WorkerId);
    CREATE INDEX IX_WorkerMetrics_Tenant ON dbo.WorkerMetrics(TenantId);
    PRINT 'Created WorkerMetrics table';
END
ELSE
BEGIN
    PRINT 'WorkerMetrics table already exists';
END;

-- Step 8: Create indexes for performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Tasks_TenantId' AND object_id = OBJECT_ID('dbo.Tasks'))
BEGIN
    CREATE INDEX IX_Tasks_TenantId ON dbo.Tasks(TenantId);
    PRINT 'Created index IX_Tasks_TenantId';
END;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Tasks_Status' AND object_id = OBJECT_ID('dbo.Tasks'))
BEGIN
    CREATE INDEX IX_Tasks_Status ON dbo.Tasks(Status);
    PRINT 'Created index IX_Tasks_Status';
END;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_WorkerHeartbeats_TenantId' AND object_id = OBJECT_ID('dbo.WorkerHeartbeats'))
BEGIN
    CREATE INDEX IX_WorkerHeartbeats_TenantId ON dbo.WorkerHeartbeats(TenantId);
    PRINT 'Created index IX_WorkerHeartbeats_TenantId';
END;

PRINT 'Database migration completed successfully!';
