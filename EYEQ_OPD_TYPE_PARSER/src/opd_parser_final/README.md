# OPD Classification Parser — v2

A production-grade ETL pipeline that classifies patient visit records in
`Patient_Type_Age_wise_final_copy` by setting the `OPD_TYPE` column according
to 12 business rules. Built on the same architecture as the TCN Call Data
Processing system.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Architecture Overview](#architecture-overview)
3. [Code Flow — Step by Step](#code-flow--step-by-step)
4. [Prerequisites](#prerequisites)
5. [How to Build](#how-to-build)
6. [How to Run](#how-to-run)
7. [Command Line Arguments](#command-line-arguments)
8. [Business Rules — Execution Order](#business-rules--execution-order)
9. [How Binary Search Works](#how-binary-search-works)
10. [Database Requirements](#database-requirements)
11. [Logging](#logging)
12. [Error Handling](#error-handling)
13. [Files Unchanged from TCN System](#files-unchanged-from-tcn-system)

---

## Project Structure

```
opd_parser_v2/
│
├── OPD_Main.java               Entry point — args, date resolution, result banner
├── ProcessOPDData.java         Service layer — orchestrates 13 steps in order
├── OPD_Source.java             Data layer — all SQL, binary search, JDBC lifecycle
│
├── (reused from TCN system, no changes)
├── MySQLConnection.java        JDBC connection utility
├── PrintUtilMsg.java           Timestamped logger utility
└── SNORangeFinder.java         Binary search engine for SNO range resolution
```

---

## Architecture Overview

```
OPD_Main
    │
    └─► ProcessOPDData.processOPDData()
            │
            ├─ STEP 1   OPD_Source.deleteCurrentMonthData()
            ├─ STEP 2   OPD_Source.initNaturalOpd()          ← RULE-0
            ├─ STEP 3   OPD_Source.applyOldOpd()             ← RULE-1
            ├─ STEP 4   OPD_Source.applyAttendantActivity()  ← RULE-2
            ├─ STEP 5   OPD_Source.applyCampOutReach()       ← RULE-3
            ├─ STEP 6   OPD_Source.applyCampInHouse()        ← RULE-4
            ├─ STEP 7   OPD_Source.applySurgicalCoupon()     ← RULE-5
            ├─ STEP 8   OPD_Source.applyReferral()           ← RULE-6
            ├─ STEP 9   OPD_Source.applyCorporateOpd()       ← RULE-7
            ├─ STEP 10  OPD_Source.applySeniorCitizenActivity() ← RULE-8
            ├─ STEP 11  OPD_Source.applyNewOpdEchs()         ← RULE-9
            ├─ STEP 12  OPD_Source.applyNewOpdCghs()         ← RULE-10
            └─ STEP 13  OPD_Source.applyNewOpdDghs()         ← RULE-11
```

Each `OPD_Source` method:
1. Calls `binarySearch()` twice to resolve a tight `sno` range for the month
2. Builds a scoped `UPDATE` SQL string
3. Executes it via the private `executeUpdate()` helper
4. Returns rows affected (or -1 on error)

`ProcessOPDData` collects all return values and reports a single boolean status.

---

## Code Flow — Step by Step

This section walks through exactly what happens from the moment you run the
JAR to the moment the final banner is printed. Every class, every method, and
every decision point is covered in order.

---

### PHASE 1 — JVM Startup → `OPD_Main.main()`

```
java -cp "..." opd_parser_v2.OPD_Main  <args>
         │
         ▼
OPD_Main.main(String[] args)
```

**What happens:**

```
1. Print startup banner
   "========================================================"
   "     OPD CLASSIFICATION PROCESSING - STARTED"

2. Print all received arguments (masked password in next step)

3. Validate argument count
   args.length < 7  →  print usage instructions and EXIT
   args.length >= 7 →  continue

4. Resolve Process Date
   args[7] provided and non-empty  →  use args[7] as processDate
   args[7] missing or blank        →  call getYesterdayDate()
                                       Calendar.getInstance()
                                       calendar.add(DATE, -1)
                                       SimpleDateFormat("yyyy-MM-dd").format()
                                       →  processDate = "2025-01-30"  (example)

5. Validate date format
   isValidDateFormat(processDate)
   Checks: not null, length == 10, char[4] == '-', char[7] == '-'
   Then: SimpleDateFormat.setLenient(false).parse()
   Invalid  →  print error and EXIT
   Valid    →  continue

6. Derive processMonth
   processDate.substring(0, 7)
   "2025-01-31"  →  processMonth = "2025-01"

7. Parse remaining arguments into named variables
   dbURL, dbName, dbPass, copyTable, campTable,
   freeScreeningTable, destTable

8. Print resolved config (password masked)
   maskPassword("secret")  →  "s****t"

9. Call ProcessOPDData().processOPDData(...)
```

---

### PHASE 2 — `ProcessOPDData.processOPDData()`

This is the **conductor**. It holds no SQL. It calls `OPD_Source` methods in
strict order and collects a single `overallStatus` boolean.

```
ProcessOPDData.processOPDData(
    copyTable, campTable, freeScreeningTable,
    processDate, processMonth,
    dbURL, dbName, dbPass
)
         │
         ▼
OPD_Source src = new OPD_Source()
boolean overallStatus = true
```

Each of the 13 steps below follows this pattern inside `ProcessOPDData`:

```java
overallStatus &= step("RULE-X | Label",
                      src.applyXxx(copyTable, ..., dbURL, dbName, dbPass));
```

The `step()` helper:
```
rowsAffected >= 0  →  log "SUCCESS: RULE-X | rows=N"  →  return true
rowsAffected == -1 →  log "FAILED : RULE-X"           →  return false
```

`&=` means: if any step ever returns false, `overallStatus` permanently
becomes false — but execution continues through all remaining steps anyway
so that every rule is attempted and every failure is logged.

---

### PHASE 3 — STEP 1: Delete Current Month Data

```
src.deleteCurrentMonthData(copyTable, processMonth, dbURL, dbName, dbPass)
         │
         ▼
MySQLConnection.getConnection()  →  conn (autoCommit=false)
         │
         ▼
DELETE FROM <copyTable>
WHERE DATE_FORMAT(Visit_Date, '%Y-%m') = '2025-01'
         │
         ▼
conn.commit()
log "Rows deleted: N"
         │
         ▼
SELECT MAX(sno) AS maxSno FROM <copyTable>
         │
         ▼
ALTER TABLE <copyTable> AUTO_INCREMENT = (maxSno + 1)
conn.commit()
log "AUTO_INCREMENT reset to: N"
         │
         ▼
stmt.close()  →  conn.close()
return true
```

**Why this step exists:** Each run processes the current month fresh. Deleting
first ensures no duplicate rows from a previous run on the same month. Resetting
`AUTO_INCREMENT` prevents gaps in the `sno` sequence after the delete.

---

### PHASE 4 — STEP 2: RULE-0 — Init Natural OPD (baseline)

This is the most important step. Every rule after this **overrides** what this
step sets, so this must run first on every row.

```
src.initNaturalOpd(copyTable, processMonth, processDate, dbURL, dbName, dbPass)
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BINARY SEARCH — called TWICE to resolve the month's SNO window     │
│                                                                     │
│  Call 1: binarySearch("yes",                                        │
│              "Visit_Date~~2025-01-01~~monthly",                     │
│              copyTable, dbURL, dbName, dbPass)                      │
│                                                                     │
│    Inside binarySearch():                                           │
│      split "~~"  →  dateColumn="Visit_Date"                        │
│                      dateValue ="2025-01-01"                        │
│                      trendType ="monthly"                           │
│      from_date_final = "2025-01-01"  (first day of month)          │
│      to_date_final   = "2025-01-01"  (same, monthly seed)          │
│                                                                     │
│      SNORangeFinder.getStartEndIdOfTable(copyTable, ...)           │
│      →  globalMin=1, globalMax=800000  (example)                   │
│                                                                     │
│      SNORangeFinder.getStartAndEndId(                              │
│          "2025-01-01", 1, 800000, copyTable,                       │
│          "DATE_FORMAT(Visit_Date,'%Y-%m-%d')",                     │
│          "daily", "120", false, "sno", ...)                        │
│      →  snoRange[0] = 482000  (first SNO on/after 2025-01-01)      │
│                                                                     │
│  Call 2: binarySearch("yes",                                        │
│              "Visit_Date~~2025-01-31~~daily",                      │
│              copyTable, dbURL, dbName, dbPass)                      │
│      →  snoRange2[1] = 619500  (last SNO on/before 2025-01-31)     │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
Build SQL:
  UPDATE Patient_Type_Age_wise_final_copy
  SET    OPD_TYPE = 'Natural OPD'
  WHERE  sno >= 482000
    AND  sno <= 619500
    AND  DATE_FORMAT(Visit_Date, '%Y-%m') = '2025-01'
         │
         ▼
executeUpdate("initNaturalOpd", sql, dbURL, dbName, dbPass)
    │
    ▼
  MySQLConnection.getConnection()  →  conn
  conn.createStatement().executeUpdate(sql)
  conn.commit()
  log "Rows affected: 137500"
  stmt.close()  →  conn.close()
  return 137500
         │
         ▼
step() → "SUCCESS: RULE-0 | Init Natural OPD | Rows affected: 137500"
```

**After this step**, every row in January 2025 has `OPD_TYPE = 'Natural OPD'`.
All subsequent steps only touch a subset of those rows.

---

### PHASE 5 — STEPS 3–13: Override Rules (RULE-1 through RULE-11)

Every subsequent rule follows **the identical internal pattern** as RULE-0.
The only differences are the SQL `WHERE` clause additions and which table(s)
are involved. Here is the full walkthrough for each:

---

#### STEP 3 — RULE-1: Old OPD

```
Reads:   APPOINTMENT_TYPE, PATIENT_TYPE  (copyTable only)
Writes:  OPD_TYPE = 'Old OPD'

SNO window resolved same as RULE-0 (Visit_Date binary search)

Additional WHERE conditions:
    AND APPOINTMENT_TYPE = 'Reg'
    AND PATIENT_TYPE != 'New'

Who gets this:  Returning patients with a regular appointment type.
                A "New" patient who comes on 'Reg' is NOT affected —
                the != 'New' guard ensures only established patients
                are reclassified here.
```

---

#### STEP 4 — RULE-2: Attendant Activity

```
Reads:   MRD_NO from copyTable, MRD_NO from Free_Screening_Count
Writes:  OPD_TYPE = 'Attendant Activity'

SQL pattern: UPDATE copyTable p
             INNER JOIN Free_Screening_Count fsc ON p.MRD_NO = fsc.MRD_NO
             SET p.OPD_TYPE = 'Attendant Activity'
             WHERE p.sno >= X AND p.sno <= Y
               AND DATE_FORMAT(p.Visit_Date, '%Y-%m') = '2025-01'

SNO range applied to alias p (the copy table).
Free_Screening_Count has no sno — it is used only as a membership filter.

Who gets this:  Any patient whose MRD_NO appears in the free-screening
                register for this month. The JOIN means only matched
                rows are updated.
```

---

#### STEP 5 — RULE-3: Camp (Out Reach)

```
Reads:   MRD_NO, PATIENT_CATEGORY from copyTable
         MRD_NO, opd_visit from Camp_count_detail
Writes:  OPD_TYPE = 'Camp (Out Reach)'

SQL pattern:
    UPDATE copyTable p
    SET p.OPD_TYPE = 'Camp (Out Reach)'
    WHERE p.sno >= X AND p.sno <= Y
      AND DATE_FORMAT(p.Visit_Date, '%Y-%m') = '2025-01'
      AND (
            EXISTS (
                SELECT 1 FROM Camp_count_detail c
                WHERE c.MRD_NO = p.MRD_NO AND c.opd_visit = 'Y'
            )
            OR p.PATIENT_CATEGORY = 'OUT - REACH CAMP'
          )

Two independent triggers — either one qualifies the row:
  Path A: The patient has a camp record (opd_visit = 'Y')
  Path B: The patient's category is already flagged as out-reach
```

---

#### STEP 6 — RULE-4: Camp (In House)

```
Reads:   PATIENT_CATEGORY (copyTable)
Writes:  OPD_TYPE = 'Camp (In House)'

Condition: PATIENT_CATEGORY = 'Camp Patient'

Who gets this:  Patients who attended an in-house camp event, identified
                by their category. No external table join needed.
```

---

#### STEP 7 — RULE-5: Surgical Coupon

```
Reads:   PATIENT_CATEGORY, PATIENT_SUBCATEGORY (copyTable)
Writes:  OPD_TYPE = 'Surgical Coupon'

Conditions (both must be true):
    PATIENT_CATEGORY    = 'MARKETING ACTIVITY'
    PATIENT_SUBCATEGORY = 'SURGICAL COUPON PATIENTS'

Who gets this:  Marketing-activity patients who came in via a surgical
                coupon. The subcategory is the distinguishing key within
                the broad MARKETING ACTIVITY category.
```

---

#### STEP 8 — RULE-6: Referral

```
Reads:   PATIENT_CATEGORY, PATIENT_SUBCATEGORY (copyTable)
Writes:  OPD_TYPE = 'Referral'

Conditions:
    PATIENT_CATEGORY = 'MARKETING ACTIVITY'
    PATIENT_SUBCATEGORY IN (
        'REFERRAL ARMY CLINIC', 'MAX ATTENDENT ACTIVITIES',
        'Lasik Referral Program', 'REFERENCE FROM AGENT',
        'INVITATION CARD ACTIVITY', 'IN CENTER COUPON ACTIVITY',
        'REFERRAL THROUGH DOCTORS', 'REFERRAL MARKETING',
        'REFERRAL FROM OPTICAL'
    )

Who gets this:  Marketing patients who arrived through any referral
                channel. This rule shares the same PATIENT_CATEGORY
                as RULE-5 and RULE-8 — the PATIENT_SUBCATEGORY value
                is the differentiator between those three rules.
```

---

#### STEP 9 — RULE-7: Corporate OPD

```
Reads:   PATIENT_CATEGORY (copyTable)
Writes:  OPD_TYPE = 'Corporate OPD'

Condition: PATIENT_CATEGORY IN ('CORPORATE (CASH)', 'CORPORATE (CASHLESS)')

Who gets this:  Corporate-empanelled patients regardless of payment mode.
                Both cash and cashless corporate patients land in the
                same OPD_TYPE.
```

---

#### STEP 10 — RULE-8: Senior Citizen Activity

```
Reads:   PATIENT_CATEGORY, PATIENT_SUBCATEGORY (copyTable)
Writes:  OPD_TYPE = 'Senior Citizen Activity'

Conditions (both must be true):
    PATIENT_CATEGORY    = 'MARKETING ACTIVITY'
    PATIENT_SUBCATEGORY = 'SENIOR CITIZEN ACTIVITY'

Who gets this:  Marketing patients who came through a senior citizen
                outreach programme.
```

---

#### STEP 11 — RULE-9: New OPD ECHS

```
Reads:   PATIENT_CATEGORY (copyTable)
Writes:  OPD_TYPE = 'New OPD ECHS'

Condition: PATIENT_CATEGORY = 'EX SERVICEMAN'

Who gets this:  Ex-servicemen / ECHS-enrolled patients. ECHS stands for
                Ex-Servicemen Contributory Health Scheme.
```

---

#### STEP 12 — RULE-10: New OPD CGHS

```
Reads:   PATIENT_CATEGORY (copyTable)
Writes:  OPD_TYPE = 'New OPD CGHS'

Condition: PATIENT_CATEGORY = 'CGHS'

Who gets this:  Central Government Health Scheme patients.
```

---

#### STEP 13 — RULE-11: New OPD DGHS

```
Reads:   PATIENT_CATEGORY (copyTable)
Writes:  OPD_TYPE = 'New OPD DGHS'

Condition: PATIENT_CATEGORY = 'DGHS'

Who gets this:  Directorate General of Health Services patients.
                This is the last rule and has the highest effective
                priority because it runs last and its matches always
                overwrite any earlier classification.
```

---

### PHASE 6 — Result Collection and Final Banner

After all 13 steps complete, control returns to `ProcessOPDData`:

```
overallStatus = true  (all 13 steps returned >= 0)
            OR false  (one or more steps returned -1)
         │
         ▼
log "ALL STEPS COMPLETED SUCCESSFULLY"
   OR
log "ONE OR MORE STEPS FAILED — check logs above"
         │
         ▼
return overallStatus  →  back to OPD_Main
```

Back in `OPD_Main`:

```
status == true
    →  print "✓ OPD CLASSIFICATION COMPLETED SUCCESSFULLY"
    →  print Process Date, Process Month, Copy Table, Dest Table

status == false
    →  print "✗ OPD CLASSIFICATION FAILED"
    →  print "Check logs above for error details."
```

---

### Full End-to-End Data Flow Summary

```
INPUT:  Patient_Type_Age_wise_final_copy
        (rows for 2025-01, OPD_TYPE = NULL or stale value)

        Camp_count_detail        (reference — read only)
        Free_Screening_Count     (reference — read only)

        ┌─────────────────────────────────────────────────────┐
        │  DELETE existing 2025-01 rows from copy table       │
        │  RESET AUTO_INCREMENT                               │
        └─────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────────────┐
        │  All 2025-01 rows  →  OPD_TYPE = 'Natural OPD'     │ RULE-0
        └─────────────────────────────────────────────────────┘
                              │
                              ▼  (each rule touches only rows that match its condition)
        ┌──────────────────────────────────────────────────────────────────┐
        │  Reg + non-New patients         →  'Old OPD'                    │ RULE-1
        │  Free-screened patients         →  'Attendant Activity'         │ RULE-2
        │  Camp-visited / out-reach       →  'Camp (Out Reach)'           │ RULE-3
        │  Camp Patient category          →  'Camp (In House)'            │ RULE-4
        │  Marketing / Surgical Coupon    →  'Surgical Coupon'            │ RULE-5
        │  Marketing / Referral channels  →  'Referral'                   │ RULE-6
        │  Corporate (cash/cashless)      →  'Corporate OPD'              │ RULE-7
        │  Marketing / Senior Citizen     →  'Senior Citizen Activity'    │ RULE-8
        │  EX SERVICEMAN                  →  'New OPD ECHS'               │ RULE-9
        │  CGHS                           →  'New OPD CGHS'               │ RULE-10
        │  DGHS                           →  'New OPD DGHS'               │ RULE-11
        └──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
OUTPUT: Patient_Type_Age_wise_final_copy
        (every 2025-01 row now has a classified OPD_TYPE value)
```

**Key point about rule ordering:** Later rules win. A patient whose
`PATIENT_CATEGORY = 'DGHS'` will have their `OPD_TYPE` set to `'Natural OPD'`
by RULE-0, possibly to something else by rules 1–10, and finally locked to
`'New OPD DGHS'` by RULE-11 because it runs last. This is why the sequence
in `ProcessOPDData` is non-negotiable and must never be reordered.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java | 7 or higher |
| MySQL Connector/J | 5.1.x (bundled) or 9.x |
| MySQL Server | 5.6 or higher |

---

## How to Build

```bash
javac -cp "mysql-connector-j-9.4.0.jar" opd_parser_v2/*.java
jar cf OPDProcessing.jar opd_parser_v2/*.class
```

---

## How to Run

**With explicit process date:**
```bash
java -cp "OPDProcessing.jar;mysql-connector-j-9.4.0.jar" opd_parser_v2.OPD_Main \
     "jdbc:mysql://localhost:3306/mydb" root secret \
     Patient_Type_Age_wise_final_copy \
     Camp_count_detail \
     Free_Screening_Count \
     Patient_Type_Age_wise_final_copy \
     2025-01-31
```

**With auto D-1 date (omit the last argument):**
```bash
java -cp "OPDProcessing.jar;mysql-connector-j-9.4.0.jar" opd_parser_v2.OPD_Main \
     "jdbc:mysql://localhost:3306/mydb" root secret \
     Patient_Type_Age_wise_final_copy \
     Camp_count_detail \
     Free_Screening_Count \
     Patient_Type_Age_wise_final_copy
```

> On Linux/Mac, replace `;` in the classpath with `:`

---

## Command Line Arguments

| Index | Name | Required | Example | Notes |
|---|---|---|---|---|
| `args[0]` | DB URL | Yes | `jdbc:mysql://localhost:3306/mydb` | Full JDBC URL |
| `args[1]` | DB Username | Yes | `root` | |
| `args[2]` | DB Password | Yes | `secret` | Masked in logs |
| `args[3]` | Copy Table | Yes | `Patient_Type_Age_wise_final_copy` | Main working table |
| `args[4]` | Camp Table | Yes | `Camp_count_detail` | Used in RULE-3 |
| `args[5]` | Free Screening Table | Yes | `Free_Screening_Count` | Used in RULE-2 |
| `args[6]` | Destination Table | Yes | `Patient_Type_Age_wise_final_copy` | Usually same as copy table |
| `args[7]` | Process Date | No | `2025-01-31` | Defaults to yesterday (D-1) if omitted |

---

## Business Rules — Execution Order

Rules run **sequentially**. Each rule overrides the previous one for matching
rows. Rule-0 must always run first to set the baseline.

| Step | Rule | OPD_TYPE Set To | Condition |
|---|---|---|---|
| STEP 2 | RULE-0 | `Natural OPD` | All rows in the month (baseline) |
| STEP 3 | RULE-1 | `Old OPD` | `APPOINTMENT_TYPE = 'Reg'` AND `PATIENT_TYPE != 'New'` |
| STEP 4 | RULE-2 | `Attendant Activity` | `MRD_NO` exists in `Free_Screening_Count` |
| STEP 5 | RULE-3 | `Camp (Out Reach)` | Exists in `Camp_count_detail` where `opd_visit = 'Y'` OR `PATIENT_CATEGORY = 'OUT - REACH CAMP'` |
| STEP 6 | RULE-4 | `Camp (In House)` | `PATIENT_CATEGORY = 'Camp Patient'` |
| STEP 7 | RULE-5 | `Surgical Coupon` | `PATIENT_CATEGORY = 'MARKETING ACTIVITY'` AND `PATIENT_SUBCATEGORY = 'SURGICAL COUPON PATIENTS'` |
| STEP 8 | RULE-6 | `Referral` | `PATIENT_CATEGORY = 'MARKETING ACTIVITY'` AND `PATIENT_SUBCATEGORY` IN referral list |
| STEP 9 | RULE-7 | `Corporate OPD` | `PATIENT_CATEGORY IN ('CORPORATE (CASH)', 'CORPORATE (CASHLESS)')` |
| STEP 10 | RULE-8 | `Senior Citizen Activity` | `PATIENT_CATEGORY = 'MARKETING ACTIVITY'` AND `PATIENT_SUBCATEGORY = 'SENIOR CITIZEN ACTIVITY'` |
| STEP 11 | RULE-9 | `New OPD ECHS` | `PATIENT_CATEGORY = 'EX SERVICEMAN'` |
| STEP 12 | RULE-10 | `New OPD CGHS` | `PATIENT_CATEGORY = 'CGHS'` |
| STEP 13 | RULE-11 | `New OPD DGHS` | `PATIENT_CATEGORY = 'DGHS'` |

**Referral sub-categories (RULE-6):**
- `REFERRAL ARMY CLINIC`
- `MAX ATTENDENT ACTIVITIES`
- `Lasik Referral Program`
- `REFERENCE FROM AGENT`
- `INVITATION CARD ACTIVITY`
- `IN CENTER COUPON ACTIVITY`
- `REFERRAL THROUGH DOCTORS`
- `REFERRAL MARKETING`
- `REFERRAL FROM OPTICAL`

---

## How Binary Search Works

Every rule resolves a tight `sno` range before executing its UPDATE, avoiding
full table scans on large tables.

```
processMonth = "2025-01"
processDate  = "2025-01-31"

binarySearch("Visit_Date~~2025-01-01~~monthly")  →  sno >= 482000
binarySearch("Visit_Date~~2025-01-31~~daily")    →  sno <= 619500

UPDATE ... WHERE sno >= 482000 AND sno <= 619500
           AND DATE_FORMAT(Visit_Date, '%Y-%m') = '2025-01'
           AND <rule condition>
```

`SNORangeFinder` uses a binary search algorithm on the sorted `sno` column to
find the first and last SNO whose `Visit_Date` falls within the target range.
MySQL can then use the `sno` index to jump directly to the relevant rows.

**Required indexes for full benefit:**
```sql
ALTER TABLE Patient_Type_Age_wise_final_copy ADD INDEX idx_sno        (sno);
ALTER TABLE Patient_Type_Age_wise_final_copy ADD INDEX idx_visit_date (Visit_Date);
ALTER TABLE Patient_Type_Age_wise_final_copy ADD INDEX idx_mrd        (MRD_NO);
ALTER TABLE Free_Screening_Count            ADD INDEX idx_mrd        (MRD_NO);
ALTER TABLE Camp_count_detail               ADD INDEX idx_mrd        (MRD_NO);
```

---

## Database Requirements

### Copy Table — `Patient_Type_Age_wise_final_copy`

Must have an `OPD_TYPE` column that this pipeline writes to:

```sql
ALTER TABLE Patient_Type_Age_wise_final_copy
    ADD COLUMN OPD_TYPE VARCHAR(50) DEFAULT NULL;
```

Columns read by the pipeline:

| Column | Used By |
|---|---|
| `sno` | All rules — SNO range filtering |
| `Visit_Date` | All rules — month scoping + binary search |
| `MRD_NO` | RULE-2 (join), RULE-3 (EXISTS join) |
| `APPOINTMENT_TYPE` | RULE-1 |
| `PATIENT_TYPE` | RULE-1 |
| `PATIENT_CATEGORY` | RULE-3, 4, 5, 6, 7, 8, 9, 10, 11 |
| `PATIENT_SUBCATEGORY` | RULE-5, 6, 8 |
| `OPD_TYPE` | Written by all rules |

### Camp Table — `Camp_count_detail`

| Column | Used By |
|---|---|
| `MRD_NO` | RULE-3 JOIN key |
| `opd_visit` | RULE-3 filter (`= 'Y'`) |

### Free Screening Table — `Free_Screening_Count`

| Column | Used By |
|---|---|
| `MRD_NO` | RULE-2 JOIN key |

---

## Logging

All log lines are written by `PrintUtilMsg.sop()` in the format:

```
dd-MM-yyyy HH:mm:ss SSS    [TAG][ClassName]    [LineNumber]    message
```

Example output:
```
31-01-2025 09:12:04 123    [processOPDData][opd_parser_v2.ProcessOPDData]    [42]    ===== STEP 2: RULE-0  | Init Natural OPD =====
31-01-2025 09:12:04 456    [initNaturalOpd][opd_parser_v2.OPD_Source]        [88]    SNO Range: [482000 - 619500]
31-01-2025 09:12:04 789    [initNaturalOpd][opd_parser_v2.OPD_Source]        [95]    SQL: UPDATE Patient_Type_Age_wise_final_copy SET OPD_TYPE = 'Natural OPD' WHERE sno >= 482000 ...
31-01-2025 09:12:05 012    [initNaturalOpd][opd_parser_v2.OPD_Source]        [99]    Rows affected: 137500
31-01-2025 09:12:05 013    [step][opd_parser_v2.ProcessOPDData]              [55]    SUCCESS: RULE-0  | Init Natural OPD | Rows affected: 137500
```

---

## Error Handling

| Scenario | Behaviour |
|---|---|
| A single rule fails (returns -1) | Logged as `FAILED`, `overallStatus` set to false, remaining rules still execute |
| DB connection failure in any rule | Exception caught, rollback attempted, -1 returned, processing continues |
| Delete step fails | Logged as warning, processing continues (non-fatal) |
| Invalid date format in args | Hard stop with error message before any DB call |
| Insufficient arguments | Hard stop with usage instructions |
| Overall failure | `OPD_Main` prints `✗ OPD CLASSIFICATION FAILED` and directs to logs |

---

## Files Unchanged from TCN System

These three files are reused directly with no modifications:

| File | Purpose |
|---|---|
| `MySQLConnection.java` | Opens a JDBC connection with `autoCommit=false` |
| `PrintUtilMsg.java` | Timestamped console logger with class and line number |
| `SNORangeFinder.java` | Binary search engine — resolves date strings to SNO boundaries |

Only the **package declaration** needs to change from `call_processing_v2` to
`opd_parser_v2` if you are compiling them into the new package. If you are
sharing a single JAR with both systems, the existing files in
`call_processing_v2` can be referenced directly without any change.
