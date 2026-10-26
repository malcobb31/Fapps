# NTP12 User Migration Parser

This project is a Java-based CSV-to-database migration utility for processing user records and updating them in PostgreSQL. It reads CSV files from a source folder, validates and transforms the data, performs database writes, and then moves the processed CSV files into a processed folder.

The codebase contains several iterative versions under the `src` folder, including:

- `ntp12_process_v1`
- `ntp12_process_v2`
- `ntp12_process_v3`
- `ntp12_process_v4`
- `ntp12_process_v5`
- `ntp12_process_upd`
- `ntp12_process_upd_v2`

The most recent and likely active implementation appears to be in the `ntp12_process_upd_v2` package.

---

## Purpose

The process is designed to:

1. Read CSV input files from a configured folder.
2. Map CSV column values to database columns.
3. Validate user status, ranking, and migration rules.
4. Persist the data into PostgreSQL.
5. Log progress, warnings, and errors.
6. Move successfully processed CSV files into a processed directory.

This is effectively an ETL / migration pipeline for user data.

---

## Project structure

```text
NTP12/
├── .classpath
├── .project
├── .settings/
├── bin/
├── src/
│   ├── ntp12_process_v1/
│   ├── ntp12_process_v2/
│   ├── ntp12_process_v3/
│   ├── ntp12_process_v4/
│   ├── ntp12_process_v5/
│   ├── ntp12_process_upd/
│   └── ntp12_process_upd_v2/
├── README.md
└── ...
```

### Key classes

Inside the active version (`src/ntp12_process_upd_v2`):

- `ntp12_process_main_upd_v2.java`  -> entry point for the process
- `ntp12_process_methods.java`      -> core migration logic and CSV-to-DB transformation
- `DBUtil.java`                      -> database connection utilities
- `CSVUtil.java`                    -> CSV processing utilities

---

## Main execution flow

The main class accepts these arguments:

```text
pgUrl pgUser pgPass csvFolderPath processedFolderPath
```

Example:

```text
jdbc:postgresql://localhost:5432/mydb postgres user123 C:/input_csv C:/processed_csv
```

The program then:

1. Validates the input CSV folder.
2. Opens a PostgreSQL connection.
3. Loads the CSV records.
4. Converts the values to DB-safe types.
5. Applies the migration logic for each user row.
6. Commits transaction batches.
7. Moves processed files to the processed folder.

---

## Runtime behavior

### Input folder

The program expects the CSV files to be placed in a folder such as:

```text
C:/data/input_csv
```

### Output folder

After successful processing, CSV files are moved to a processed directory such as:

```text
C:/data/processed_csv
```

The code creates the processed folder automatically if it does not exist.

---

## Business logic summary

The active implementation applies protections before ranking and repeats critical protections in database `UPDATE` predicates. This keeps excluded users from being changed directly, indirectly by ranking, or by post-processing.

### 1. Users that must never be changed

The following protections apply:

- **Linked identity:** an existing database row with both `dsc_id IS NOT NULL` **and** `key_cloak_user_id IS NOT NULL` is skipped. It is also excluded from ranking and protected by the database-side write guard. If only one of those columns is non-null, this particular rule does not exclude the row.
- **Protected statuses:** rows whose current database `status` is `ACTIVE` or `SUSPENDED_BY_SYSTEM` are skipped and excluded from ranking. Status matching is case-insensitive. The write guard applies regardless of RE, including the 80-user and 400-plus-user RE groups.
- **Flagged REs:** rows for which the incoming RE or the user's currently stored RE has `re_flag = false` are skipped. These events cannot update the row or influence ranking. The database write guard requires the stored reporting entity to have `re_flag IS NULL`.
- **Fail-closed RE handling:** unexpected non-null flag values, unresolved REs, and ambiguous FI-code lookups are also skipped. Only `re_flag IS NULL` is eligible.

The same protected status and linked-identity conditions are applied to ranking-propagated writes and the duplicate email/mobile cleanup. A database-side predicate is used as a second line of protection if state changes after the initial read.

### 2. Per-event validation

Before ranking or writing, each incoming master row is checked in this order:

1. `user_id` must be present.
2. If the user is already known as a protected linked-identity/status record, skip the event.
3. The incoming RE and the stored RE must both be eligible (`re_flag IS NULL`).
4. `user_mode` must be supported: `IA`, `IU`, `IRA`, `IRU`, or `IBU`.
5. Region and branch requirements must pass.

`IRA` and `IRU` require a region code that resolves in the region lookup. `IBU` requires a valid region and a branch that resolves to that same region. `IA` and `IU` do not require region/branch validation. Invalid rows are skipped before they can change ranking membership.

### 3. Ranking scopes and limits

User counts are ranked independently by role and scope:

| Role | Maximum users | Ranking scope |
| --- | ---: | --- |
| `IA` | 2 | FI (`bc_organisation`) |
| `IU` | 3 | FI (`bc_organisation`) |
| `IRA` | 2 | FI + region (`bc_organisation` + `region_code`) |
| `IRU` | 3 | FI + region (`bc_organisation` + `region_code`) |
| `IBU` | 3 | FI + region + branch (`bc_organisation` + `region_code` + `user_branch_id`) |

Only eligible users with source `user_status` codes `1`, `2`, `4`, `6`, `7`, `12`, or `20` participate in ranking. Excluded linked-identity users, protected-status users, and ineligible RE users neither consume a slot nor affect the rank of eligible users.

### 4. Ranking decisions

For each affected role and scope, the parser rebuilds the full current ranking population from database-seeded state plus the incoming events already processed in this run. It sorts members by `login_time` descending, then `user_id` ascending as a deterministic tie-breaker.

- For `IA`, the first two users receive user types `IAU_1` and `IAU_2`, with status `TESTBED`. Remaining ranked users retain user type `IA` and receive status `MIGRATION_IN_ACTIVE`.
- For `IU`, `IRA`, `IRU`, and `IBU`, the first users up to that role's limit receive the role's user type and status `TESTBED`. Additional ranked users receive `MIGRATION_IN_ACTIVE`.
- A region/branch role must satisfy its region/branch prerequisite before its source row is accepted into ranking. This prevents an invalid location from qualifying for `TESTBED`.
- When a user moves between ranking scopes, both the previous and new scopes are recalculated so the old scope can fill the vacated slot.

### 5. Status handling

The ranked source status codes are `1`, `2`, `4`, `6`, `7`, `12`, and `20`. These are the users considered by the ranking rules; rank decisions may set their final status to `TESTBED` or `MIGRATION_IN_ACTIVE`.

For non-ranked source status codes, the parser leaves the database `status` and `active` columns unchanged by binding null values that the SQL update preserves with `COALESCE`. Existing statuses such as `ACTIVE` and `SUSPENDED_BY_SYSTEM` are protected entirely and are not eligible for direct or propagated updates.

There is no automatic final sweep converting `MIGRATION_IN_ACTIVE` to `DEACTIVATED` in the current run. The post-migration status normalization call is not part of the active processing flow.

### 6. Write ordering, transactions, and failures

Each valid source event is handled sequentially:

1. Update the incoming user's row.
2. Immediately apply any changed ranking decisions to other affected users.
3. Continue to the next source event.

Writes use chunks of 500 source events per transaction. Each event has a savepoint. If an event's SQL write fails, the parser rolls back that event to its savepoint while retaining earlier successful events in the current chunk. At a successful chunk commit, the transaction-local ranking state becomes the committed baseline. If the commit fails, the chunk is rolled back.

### 7. Data enrichment and duplicate contact cleanup

The parser loads address, region, branch, and reporting-entity lookup data from PostgreSQL. It uses address/details CSV data to enrich master rows, converts values according to the target column types, and records datatype warnings.

After the main migration succeeds, `correctDuplicateEmailMobile()` checks eligible `NO`, `IAU_1`, and `IAU_2` users and corrects duplicate email/mobile values. Protected linked-identity users, protected statuses, and ineligible REs are excluded, with RE eligibility checked again on writes.

### 8. File handling and process result

The launcher moves CSV files to the processed folder only when `processor.run(...)` returns success. A failed processor run exits with a non-zero status and does not move the input CSV files. File-move failures are logged by the launcher.

---

## Requirements

- Java 8 or newer
- PostgreSQL JDBC driver
- A PostgreSQL database with the required schema/tables
- CSV files matching the expected format and column structure

The project is currently configured as an Eclipse Java project and uses a Java 8 target.

---

## Build instructions

### Option 1: Eclipse

1. Open the project in Eclipse.
2. Import the folder as a Java project.
3. Add the JDBC jar to the classpath.
4. Run the main class:
   - `ntp12_process_upd_v2.ntp12_process_main_upd_v2`

### Option 2: Command line

Compile the active version:

```powershell
javac -cp "C:\path\to\postgresql-driver.jar;C:\path\to\mysql-connector-j-8.4.0.jar" -d bin src\ntp12_process_upd_v2\*.java
```

Run the program:

```powershell
java -cp "bin;C:\path\to\postgresql-driver.jar;C:\path\to\mysql-connector-j-8.4.0.jar" ntp12_process_upd_v2.ntp12_process_main_upd_v2 "jdbc:postgresql://localhost:5432/mydb" "db_user" "db_password" "C:\input_csv" "C:\processed_csv"
```

> Note: the exact driver jar path depends on your local environment. Ensure PostgreSQL JDBC is present on the classpath.

---

## Important notes

- This project contains multiple revision folders; it was developed iteratively.
- The active pattern is to keep several versions side by side rather than replacing older code cleanly.
- Some classes and packages appear to have been reused or copied between versions.
- This is not a small utility; it includes validation logic, status handling, batch processing, and reporting.

---

## Suggested usage

If you are starting fresh and want to work with the current implementation, begin with:

- `src/ntp12_process_upd_v2/ntp12_process_main_upd_v2.java`
- `src/ntp12_process_upd_v2/ntp12_process_methods.java`

These are the best entry points for understanding the current migration flow.

---

## Troubleshooting

Common issues usually come from:

- missing PostgreSQL JDBC driver
- wrong database URL format
- CSV column mismatch
- invalid directory path
- schema mismatch in the target database
- unexpected status or user-rule values in the input data

Check the console logs. The application logs INFO, WARN, and ERROR messages for each stage of the process.

---

## Summary

NTP12 is a CSV-driven user migration and update pipeline for PostgreSQL. It is a complex ETL program with multiple version layers, but the active execution path is clearly centered on the `ntp12_process_upd_v2` package.

This README should help you orient quickly before modifying or debugging the code.
