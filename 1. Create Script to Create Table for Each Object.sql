-- Employee Table
CREATE TABLE Employee (
    Id INT PRIMARY KEY IDENTITY(1,1),
    EmployeeId VARCHAR(10) UNIQUE NOT NULL,
    FullName VARCHAR(100) NOT NULL,
    BirthDate DATE NOT NULL,
    Address VARCHAR(500)
);

-- PositionHistory Table
CREATE TABLE PositionHistory (
    Id INT PRIMARY KEY IDENTITY(1,1),
    PosId VARCHAR(10) NOT NULL,
    PosTitle VARCHAR(100) NOT NULL,
    EmployeeId VARCHAR(10) NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE
);
