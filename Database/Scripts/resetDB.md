# Drop the database (this removes everything)

/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'MyP@ssw0rd2026!' -C -Q "
DROP DATABASE EswatiniHealth_Staging;
"

# Recreate the database

/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'MyP@ssw0rd2026!' -C -Q "
CREATE DATABASE EswatiniHealth_Staging;
"

# Now run your scripts in order

/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'MyP@ssw0rd2026!' -C -d EswatiniHealth_Staging -i Database/Scripts/01_Create_Tables.sql
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'MyP@ssw0rd2026!' -C -d EswatiniHealth_Staging -i Database/Scripts/02_Create_Views.sql
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'MyP@ssw0rd2026!' -C -d EswatiniHealth_Staging -i Database/Scripts/03_Create_Admin.sql
