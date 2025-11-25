# SwarmCopy Quick Reference

## Command Format
```bash
SwarmCopy -i <input> -o <output>
```

## Connection String Format
```
db:dbname=<name>&dbhost=<host>&dbport=<port>&dbusername=<user>&dbpassword=<pass>&dbtable=<table>
```

## Quick Examples

### Files
```bash
# CSV to TSV
SwarmCopy -i data.csv -o data.tsv

# Combine multiple files
SwarmCopy -i "*.csv" -o combined.csv

# Copy directory
SwarmCopy -i "C:\input" -o "C:\output"
```

### File → Database
```bash
# Single file
SwarmCopy -i data.csv -o "db:dbname=MyDB&dbtable=MyTable"

# Multiple files (each becomes a table)
SwarmCopy -i "*.csv" -o "db:dbname=MyDB"

# With authentication
SwarmCopy -i data.csv -o "db:dbname=MyDB&dbhost=server&dbusername=user&dbpassword=pass&dbtable=MyTable"
```

### Database → File
```bash
# Single table
SwarmCopy -i "db:dbname=MyDB&dbtable=MyTable" -o output.csv

# All tables
SwarmCopy -i "db:dbname=MyDB&dbtable=*" -o "C:\exports"
```

### Database → Database
```bash
# Single table (10 threads)
SwarmCopy -i "db:dbname=SourceDB&dbtable=BigTable" -o "db:dbname=DestDB&dbtable=BigTable"

# All tables
SwarmCopy -i "db:dbname=SourceDB&dbtable=*" -o "db:dbname=DestDB"

# Cross-server migration
SwarmCopy -i "db:dbname=DB1&dbhost=server1&dbtable=*" -o "db:dbname=DB2&dbhost=server2"
```

## Parameter Defaults
| Parameter | Default | Required |
|-----------|---------|----------|
| dbname | - | Yes |
| dbhost | localhost | No |
| dbport | 1433 | No |
| dbusername | (Windows Auth) | No |
| dbpassword | (Windows Auth) | No |
| dbtable | - | See notes |
| dbaction | - | No |

## Special Values
- `dbtable=*` - All tables
- `*.csv` - All CSV files
- Directory path - All supported files in directory

## File Extensions
| Extension | Delimiter |
|-----------|-----------|
| .csv | Comma (,) |
| .tsv, .tab | Tab (\t) |
| .psv | Pipe (\|) |
| Auto-detect | First line analysis |

## Performance Tips
✅ Table-to-table: 10 parallel threads automatically
✅ Multiple files: Parallel processing
✅ Bulk insert: 10,000 row batches
✅ Streaming: Low memory usage
✅ No timeout: Handles huge tables

## Common Patterns

### Daily ETL
```bash
# Export
SwarmCopy -i "db:dbname=ProdDB&dbhost=prod&dbtable=*" -o "C:\exports\$(date +%Y%m%d)"

# Import
SwarmCopy -i "C:\exports\transformed\*.csv" -o "db:dbname=DataWarehouse"
```

### Database Backup
```bash
SwarmCopy -i "db:dbname=MyDB&dbtable=*" -o "C:\backups\MyDB"
```

### Database Restore
```bash
SwarmCopy -i "C:\backups\MyDB" -o "db:dbname=MyDB_Restored"
```

### Environment Refresh
```bash
SwarmCopy -i "db:dbname=Prod&dbhost=prod-sql&dbtable=*" -o "db:dbname=QA&dbhost=qa-sql"
```

## Troubleshooting

### Can't connect to database
- Check SQL Server allows remote connections
- Verify firewall allows port 1433
- Test with SQL Management Studio first

### Permission denied
- Need CREATE TABLE for new tables
- Need INSERT for existing tables
- Use Windows Auth or appropriate SQL user

### Out of memory
- Files are streamed (shouldn't happen)
- Check available disk space
- Consider splitting very large files

### Slow performance
- Check network bandwidth for remote servers
- Drop indexes before bulk insert
- Verify SQL Server has adequate resources

## Exit Codes
- `0` - Success
- `1` - Error occurred

## Notes
- All database columns created as NVARCHAR(MAX)
- NULL values become empty strings
- Headers are required in all files
- Missing columns are added automatically
- Windows file paths only
- SQL Server only (not MySQL/PostgreSQL)
