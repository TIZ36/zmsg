# zmsg Benchmark Report

Generated: 2026-01-23 15:23:52

## SQL Builder

| Benchmark | Iterations | ns/op | B/op | allocs/op |
|-----------|------------|-------|------|----------|
| SQL_Builder/SQL_Basic | 6.6M | 182ns | 128 | 6 |
| SQL_Builder/SQL_OnConflict | 3.2M | 337ns | 384 | 12 |
| SQL_Builder/Counter_Inc | 3.8M | 296ns | 480 | 11 |
| SQL_Builder/Table_Column_Counter | 3.9M | 316ns | 481 | 11 |
| SQL_Builder/Slice_Add | 2.5M | 473ns | 760 | 17 |
| SQL_Builder/Map_Set | 2.4M | 473ns | 848 | 17 |

## JSON

| Benchmark | Iterations | ns/op | B/op | allocs/op |
|-----------|------------|-------|------|----------|
| JSON_Marshal | 1.9M | 605ns | 528 | 12 |
| JSON_Unmarshal | 900.6K | 1.39µs | 656 | 32 |

## Summary

### Fastest Operations

- **SQL_Builder/SQL_Basic**: 182ns
- **SQL_Builder/Counter_Inc**: 296ns
- **SQL_Builder/Table_Column_Counter**: 316ns
- **SQL_Builder/SQL_OnConflict**: 337ns
- **SQL_Builder/Slice_Add**: 473ns

### Slowest Operations

- **JSON_Unmarshal**: 1.39µs
- **JSON_Marshal**: 605ns
- **SQL_Builder/Map_Set**: 473ns
- **SQL_Builder/Slice_Add**: 473ns
- **SQL_Builder/SQL_OnConflict**: 337ns
