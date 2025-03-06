# Ostrich Egg: Aggregation Engine for Small-Cell Suppression

A tool for producing public analytics while protecting data privacy.

## Small Cells

An ostrich egg is the largest cell in the world, and this tool is used to prevent revelation of small cells.

When a metric or fact is a count of individual persons, data privacy considerations and requirements (e.g., HIPAA) necessitate the need to protect identifying information such as location and demography.

A `cell` refers to a data point, typically a metric or fact in a dataset, that is at the most dimensional it will ever be exposed to data consumers.

Consider a dataset a library in County A wants to publish for its friends donor participation:

| Age | Sex | Library Friend | Zip Code | Number of Citizens |
| --- | --- | -------------- | -------- | ------------------ |
| 35  | M   | Yes            | 00000    | 3                  |
| 25  | F   | No             | 00000    | 20                 |
| 15  | M   | Yes            | 00001    | 12                 |
| 55  | F   | No             | 00001    | 13                 |

In this example, the cell is `Number of Citizens` by `Age`, `Sex`, `Library Friend` status, `Zip Code`.

At face value, it is easy to reveal private information about citizens in County A. It might be easy to actually identity 3 35-year old males who live in zip 00000, and the more you know about them, such as their library involvement, the more likely it is we are not protecting their privacy. That record represents a `small cell` that this tool will help to suppress, or mark redacted in some meaningful way to support analytics workflows.

The Ostrich Egg engine uses `thresholds` to identify what counts as a small cell. For example, for [mathematical reasons](https://greenriver.com/wp-content/uploads/2023/09/GreenRiverwhitepaper-Protectingprivacyintheneighborhood-levelreleaseofhealthinformation-Knappetal-2022-05.pdf), a common threshold is 11.

## Latent Revelation through Subtraction

If we reported that 23 citizens were surveyed in zip code 00000, and 20 were females, but the number of males is redacted to protect privacy, we have latently revealed through subtraction a small cell — 23-20 = 3.

Thus, a methodology is needed to redact adjacent values to prevent this revelation problem.

## Pre-Work

Ostrich Egg is a tool that encourages users to think critically about what dimensionality a given dataset to be reported at.

1. identify what dimensions from a given dataset will interact. If reporting on a dashboard, this includes filters and intersections.
2. establish thresholds for each metric or fact you present. This might be requirement or regulation-driven. If you're not sure what's too small, we suggest using 11 as the default. If you are dealing with protected data like health data, you might be more cautious for certain population sizes and suppress any value where a population is sufficiently small, e.g., 2,500. Populations consider the location (e.g., a zip code) and the demography. When a demographic or semantic population is unknown (for example, the public won't know the number of friends of the library until you tell them), you might need to use discretion but generally want to be cautions with any of the data elements protected by common data privacy regulations.
3. decide if you can re-categorize the values in you dimensions to be less granular. For example, using age ranges like `18-24`, `25-34`.
4. consider your strategy for redaction. You might want to mark the value of small cells with a value like `Redacted`, or possibly just flag for a given reporting dimension, mark redacted cells that either below the threshold or need to be suppressed to prevent latent revelation.
