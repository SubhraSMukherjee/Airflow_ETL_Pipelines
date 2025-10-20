# PowerShell Script to create SQLite Database and Tables

$dataFolder = ".\shared_dbs"
if (-Not (Test-Path $dataFolder)) {
    New-Item -ItemType Directory -Path $dataFolder -Force
    Write-Host "Created folder: $dataFolder"
} else {
    Write-Host "Folder already exists: $dataFolder"
}


$dbPath = "$dataFolder\my_data.db"


$sqlCreateTable = @"
CREATE TABLE IF NOT EXISTS test (
    id INTEGER PRIMARY KEY,
    name TEXT
);
"@

# Execute table creation
sqlite3 $dbPath $sqlCreateTable
Write-Host "Database created and table 'test' initialized at $dbPath"


$sqlInsert = @"
INSERT INTO test (name) VALUES ('Alice2');
INSERT INTO test (name) VALUES ('Bob2');
INSERT INTO test (name) VALUES ('Charlie2');
"@

sqlite3 $dbPath $sqlInsert
Write-Host "Inserted example rows into 'test' table."


Write-Host "Current contents of 'test' table:"
sqlite3 $dbPath "SELECT * FROM test;"
