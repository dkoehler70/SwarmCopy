# SwarmCopy Usage Examples

## Basic File Operations

### Convert CSV to TSV
```bash
SwarmCopy -i data.csv -o data.tsv
```

### Convert TSV to CSV
```bash
SwarmCopy -i data.tsv -o data.csv
```

### Copy and Rename
```bash
SwarmCopy -i input.csv -o output.csv
```

## Batch File Operations

### Convert All CSV Files to TSV
```bash
SwarmCopy -i "*.csv" -o tsv_output_folder
```

### Concatenate Multiple Files
Combine all CSV files into one:
```bash
SwarmCopy -i "*.csv" -o combined.csv
```

### Process Files from Directory
```bash
SwarmCopy -i "C:\data\input" -o "C:\data\output"
```

## File to Database

### Single File to Table
```bash
SwarmCopy -i customers.csv -o "db:dbname=SalesDB&dbtable=Customers"
```

### Single File with Windows Auth
```bash
SwarmCopy -i products.csv -o "db:dbname=InventoryDB&dbtable=Products"
```

### Single File with SQL Auth
```bash
SwarmCopy -i orders.csv -o "db:dbname=OrderDB&dbhost=sqlserver.company.com&dbusername=dataloader&dbpassword=SecurePass123&dbtable=Orders"
```

### Multiple Files to Database
Each file becomes a separate table:
```bash
SwarmCopy -i "*.csv" -o "db:dbname=DataWarehouse"
```

Example:
- customers.csv → Customers table
- products.csv → Products table
- orders.csv → Orders table

### Import from Directory
```bash
SwarmCopy -i "C:\exports" -o "db:dbname=ImportDB"
```

## Database to File

### Export Single Table
```bash
SwarmCopy -i "db:dbname=SalesDB&dbtable=Customers" -o customers.csv
```

### Export to TSV
```bash
SwarmCopy -i "db:dbname=SalesDB&dbtable=Customers" -o customers.tsv
```

### Export All Tables
Each table exported to separate file:
```bash
SwarmCopy -i "db:dbname=SalesDB&dbtable=*" -o "C:\exports"
```

### Export from Remote Server
```bash
SwarmCopy -i "db:dbname=ProdDB&dbhost=prod-sql-01&dbusername=readonly&dbpassword=pass&dbtable=Orders" -o orders.csv
```

## Database to Database

### Copy Single Table (Local)
```bash
SwarmCopy -i "db:dbname=SourceDB&dbtable=BigTable" -o "db:dbname=DestDB&dbtable=BigTable"
```

### Copy Table with Rename
```bash
SwarmCopy -i "db:dbname=SourceDB&dbtable=OldName" -o "db:dbname=DestDB&dbtable=NewName"
```

### Copy All Tables (Same Server)
```bash
SwarmCopy -i "db:dbname=SourceDB&dbtable=*" -o "db:dbname=BackupDB"
```

### Migrate Database to Another Server
```bash
SwarmCopy -i "db:dbname=ProdDB&dbhost=prod-server&dbusername=sa&dbpassword=pass1&dbtable=*" -o "db:dbname=DevDB&dbhost=dev-server&dbusername=sa&dbpassword=pass2"
```

### Copy Between Development Environments
```bash
SwarmCopy -i "db:dbname=Staging&dbhost=staging-sql&dbtable=*" -o "db:dbname=QA&dbhost=qa-sql"
```

## Performance Scenarios

### Large Table Copy (10 Parallel Threads)
Automatically splits using BINARY_CHECKSUM:
```bash
SwarmCopy -i "db:dbname=BigData&dbtable=TransactionsLog" -o "db:dbname=Archive&dbtable=TransactionsLog"
```

### Bulk Import with Multiple Files
Each file processed in parallel:
```bash
SwarmCopy -i "C:\daily_exports\*.csv" -o "db:dbname=DataWarehouse"
```

### Cross-Server Replication
All tables copied in parallel with partitioning:
```bash
SwarmCopy -i "db:dbname=Master&dbhost=server1&dbtable=*" -o "db:dbname=Replica&dbhost=server2"
```

## Real-World Scenarios

### Daily ETL Process
```bash
# 1. Export from production
SwarmCopy -i "db:dbname=ProdDB&dbhost=prod-sql&dbtable=*" -o "C:\etl\exports"

# 2. Transform files (your custom processing)
# ... your transformation scripts ...

# 3. Load to data warehouse
SwarmCopy -i "C:\etl\transformed\*.csv" -o "db:dbname=DataWarehouse"
```

### Database Backup to Files
```bash
SwarmCopy -i "db:dbname=CriticalDB&dbtable=*" -o "C:\backups\2024-01-15"
```

### Database Restore from Files
```bash
SwarmCopy -i "C:\backups\2024-01-15" -o "db:dbname=RestoredDB"
```

### Log Archival
```bash
# Export old logs
SwarmCopy -i "db:dbname=AppDB&dbtable=ApplicationLogs" -o "archive_logs.csv"

# Import to archive database
SwarmCopy -i "archive_logs.csv" -o "db:dbname=ArchiveDB&dbtable=ApplicationLogs"
```

### Data Migration Project
```bash
# Migrate from legacy system
SwarmCopy -i "db:dbname=LegacyDB&dbhost=old-server&dbusername=admin&dbpassword=pass&dbtable=*" -o "db:dbname=ModernDB&dbhost=new-server"
```

### Multi-Environment Sync
```bash
# Refresh QA from Staging
SwarmCopy -i "db:dbname=Staging&dbhost=staging-sql&dbtable=Customers" -o "db:dbname=QA&dbhost=qa-sql&dbtable=Customers"
SwarmCopy -i "db:dbname=Staging&dbhost=staging-sql&dbtable=Products" -o "db:dbname=QA&dbhost=qa-sql&dbtable=Products"
SwarmCopy -i "db:dbname=Staging&dbhost=staging-sql&dbtable=Orders" -o "db:dbname=QA&dbhost=qa-sql&dbtable=Orders"
```

### Data Lake Ingestion
```bash
# Ingest CSV files into data lake tables
SwarmCopy -i "\\fileserver\data-lake\incoming\*.csv" -o "db:dbname=DataLake"
```

## Performance Tips

1. **Table-to-Table**: Automatically uses 10 parallel threads with BINARY_CHECKSUM partitioning
2. **Multiple Files**: Each file processes in parallel (up to CPU count)
3. **Large Files**: Consider splitting files before import for parallel processing
4. **Bulk Operations**: Uses SqlBulkCopy with 10,000 row batches
5. **Network**: For cross-server copies, ensure good network bandwidth
6. **Indexes**: Drop indexes before bulk insert, recreate after for best performance

## Troubleshooting

### Connection Issues
- Verify SQL Server allows remote connections
- Check firewall rules for port 1433
- Test connection with SQL Management Studio first

### Permission Issues
- Ensure user has CREATE TABLE permissions for new tables
- Ensure user has INSERT permissions for existing tables
- Windows auth requires proper domain/workstation setup

### Large Files
- Monitor memory usage
- Consider increasing SQL Server max memory
- For very large files (>1GB), split into smaller chunks

### Column Mismatches
- Tables are created with NVARCHAR(MAX) for all columns
- Missing columns are automatically added
- Extra columns in table are ignored during import
