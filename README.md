# Energy and Utilities Billing

This project implements an end-to-end data pipeline that ingests Nigerian energy/utilities billing and payment data, transforms it, and loads it into Google Snowflake for analytics and reporting. It is built in Python and aims to help stakeholders (utilities companies, regulators, analysts) gain insights into billing efficiencies, revenue collection performance, customer payment behaviors, and billing anomalies in the Nigerian energy sector.

## Purpose & Motivation

- The Nigerian electricity and utilities sector faces challenges such as low collection efficiency, estimated billing instead of metered billing, disparities between billed vs collected amounts, and revenue losses. Public reports indicate large shortfalls between energy billed and what’s collected. (See e.g. NERC reports, ~ 70-80% billing efficiency in many quarters, rising estimated billing, etc.) [Nairametrics](https://nairametrics.com/2022/09/25/discos-collected-n210-billion-out-of-n303-billion-billed-to-customers-in-q4-2021)

- Accurate, timely data about billing vs payments is essential to identify inefficiencies, make data-driven policy/regulatory decisions, monitor performance of distribution companies (DisCos), and help improve revenue flows.

- Stakeholders (regulatory agencies, utilities, consumer advocacy groups) benefit from dashboards and reports that show where money is lost, how billing efficiency changes over time, which customer segments are paying, etc.

## Project Objective

Succesfully move data from Source, transform, and load into data warehouse.

Key Deliverables:

1. Create a data pipeline that moves data by batching into warehouse.
2. Design a pipeline architecture showing workflow.
3. A looker studio dashboard showing insights from the data.
