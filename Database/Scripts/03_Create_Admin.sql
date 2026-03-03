-- =====================================================
-- PART 3: CREATE ADMIN USER
-- =====================================================

SET QUOTED_IDENTIFIER ON;
GO

-- Admin user (password: AdminPass123!)
-- BCrypt hash of 'AdminPass123!' is: $2b$12$YBL8LRPod7HvS/3YtcPDVORTjjBtN5te/EDhIP.gKUusoIs/P6zDW

IF NOT EXISTS (SELECT 1 FROM Users WHERE Email = 'admin@health.gov.sz')
BEGIN
    INSERT INTO Users (Id, Email, Name, Role, PasswordHash, IsActive, CreatedAt)
    VALUES (NEWID(), 'admin@health.gov.sz', 'System Administrator', 'admin', 
            '$2b$12$YBL8LRPod7HvS/3YtcPDVORTjjBtN5te/EDhIP.gKUusoIs/P6zDW', 
            1, GETUTCDATE());
    
    PRINT 'Admin user created successfully!';
END
ELSE
BEGIN
    PRINT 'Admin user already exists.';
END
GO

-- Test user (password: Test123!)
-- BCrypt hash of 'Test123!' is: $2a$12$T5ENiH9o57tHlxUi19buOO0JBGHQK/Zam83r.pj6Ed0vtSubmVRLy

IF NOT EXISTS (SELECT 1 FROM Users WHERE Email = 'test@health.gov.sz')
BEGIN
    INSERT INTO Users (Id, Email, Name, Role, PasswordHash, IsActive, CreatedAt)
    VALUES (NEWID(), 'test@health.gov.sz', 'Test User', 'viewer', 
            '$2a$12$T5ENiH9o57tHlxUi19buOO0JBGHQK/Zam83r.pj6Ed0vtSubmVRLy', 
            1, GETUTCDATE());
    
    PRINT 'Test user created successfully!';
END
GO

-- Verify users
SELECT Email, Name, Role, IsActive FROM Users;
GO

PRINT 'Admin setup completed successfully!';
PRINT 'Default admin credentials: Email: admin@health.gov.sz, Password: AdminPass123!';
PRINT 'Test user credentials: Email: test@health.gov.sz, Password: Test123!';
GO