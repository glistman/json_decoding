# Json Decoding Plugin

This is a plugin for the logical replication of postgres that translates the changes of the WAL in json format.

## Installation

```bash
make
make install
```
## Configuration Params
* include-xids: default true
* include-timestamp: default true
* skip-empty-xacts: default true
* only-local: default false
* include-rewrites: default false
* include-toast-datum: default true

## Output
For example for the following table the output will be
```sql
CREATE TABLE test_table
(
  id    INTEGER PRIMARY KEY,
  state BOOLEAN,
  date  TIMESTAMP
);
```
## Output Structure 
The json is composed of 2 sections informative fields (pg_change_table, pg_change_tnx_time, pg_change_tnx_id, pg_change_operation_type, old_primary_key) and data that represents the row of the table.
* pg_change_table: {schema}.{table_name}
* pg_change_tnx_time: timezone with timezone
* pg_change_type: INSERT, UPDATE, DELETE

## Output insert
```json
{
   "pg_change_table":"public.test_table",
   "pg_change_tnx_time":"2019-02-19 00:52:28.467626-05",
   "pg_change_tnx_id":4542284,
   "pg_change_operation_type":"INSERT",
   "id":6,
   "state":true,
   "date":"2019-02-14 14:36:20.308138"
}
```

## Output update
```json
{  
   "pg_change_table":"public.test_table",
   "pg_change_tnx_time":"2019-02-19 00:40:08.51564-05",
   "pg_change_tnx_id":4542272,
   "pg_change_operation_type":"UPDATE",
   "id":6,
   "state":true,
   "date":"2019-02-14 14:36:20.308138"
}
```

## Output update on the primary key
```json
{
   "pg_change_table":"public.test_table",
   "pg_change_tnx_time":"2019-02-21 12:15:05.360927-05",
   "pg_change_tnx_id":8568666,
   "pg_change_operation_type":"UPDATE",
   "old_primary_key":{
      "id":6
   },
   "id":5,
   "state":true,
   "date":"2019-02-14 14:36:20.308138"
}
```

## Output delete
```json
{
   "pg_change_table":"public.test_table",
   "pg_change_tnx_time":"2019-02-19 00:51:30.908506-05",
   "pg_change_tnx_id":4542283,
   "pg_change_operation_type":"DELETE",
   "id":6
}
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

# Initial version for postgres 11
* Postgres 11:   master branch
* Postgres 10:   pg10 branch (working on this)
* Postgres 9.6:  p96 branch  (working on this)

## License
[MIT](https://choosealicense.com/licenses/mit/)