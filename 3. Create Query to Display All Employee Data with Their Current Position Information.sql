SELECT 
    E.EmployeeId,
    E.FullName,
    E.BirthDate,
    E.Address,
    PH.PosId,
    PH.PosTitle,
    PH.StartDate,
    PH.EndDate
FROM 
    Employee E
JOIN 
    PositionHistory PH
ON 
    E.EmployeeId = PH.EmployeeId
WHERE 
    PH.EndDate IS NULL OR PH.EndDate >= GETDATE();
