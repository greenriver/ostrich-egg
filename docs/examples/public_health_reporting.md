# Public Health Reporting Example: Virus A Cases in a US State

**Scenario**: A US state needs to report Virus A cases—a respiratory illness—for the 2024-2025 season across counties A, B, and C.

**Population Data** (from American Community Survey):

**Total Population**

| County | Total Population |
| ------ | ---------------- |
| A      | 50,000           |
| B      | 75,000           |
| C      | 100,000          |


**Age Band Populations**
| County | under_18 | 18_30  | 30_40  | 40_50  | 50_60  | 60_70  | 70_plus |
| ------ | -------- | ------ | ------ | ------ | ------ | ------ | ------- |
| A      | 10,000   | 7,500  | 7,500  | 7,500  | 7,500  | 6,000  | 4,000   |
| B      | 15,000   | 11,250 | 11,250 | 11,250 | 11,250 | 9,000  | 6,000   |
| C      | 20,000   | 15,000 | 15,000 | 15,000 | 15,000 | 12,000 | 8,000   |


## HIPAA Compliance and Redaction

See test data: [public_health_example.json](../../tests/data_inputs/public_health_example.json)

### Redaction Threshold: < 11 Cases

**County-level totals** (aggregated across all age bands): All county-month totals are ≥ 11 cases, so **no redaction is needed** at this level.

**County × Age Band breakdown**: Some cells contain < 11 cases and must be redacted. To prevent these values from being revealed through subtraction, additional "near neighbor" cells are also suppressed.

**Example**:
- County B, month 2024-11, age 70_plus: 6 cases → **redacted**
- To prevent revealing this value by subtracting other age bands in County B, the 18_30 age band for County B was also suppressed
- To prevent revealing the 18_30 value by comparing across counties, the 18_30 age band for County A was also suppressed

### Configuration Options

The engine minimizes cell suppression while checking all possible dimension combinations to prevent data disclosure.

**Configuration Examples based on reporting needs:**

- **`first_order_only: true`**: Only prevents direct revelation of redacted cells (not latent disclosure through subtraction). Use when multi-layer obfuscation is acceptable—useful, for example during public health emergencies when more demographic precision is needed while still protecting privacy.

- **`non_summable_dimensions: ["month"]`**: Excludes month from cross-dimensional summation checks. Use when seasonal totals aren't reported—only county × age_range combinations need protection within each month.
