# Data Dictionary for GOLD Layer

## Overview
The GOLD layer in the data warehouse provides curated, business-ready views that combine cleaned and integrated data from the SILVER layer. These views are optimized for analytical queries, reporting, and dashboarding. Below are the primary entities in the GOLD layer:

---

## 1. `gold.dim_customer`

**Script Purpose**  
To provide a unified and enriched customer profile by combining CRM and ERP data sources. This view resolves inconsistencies and fills missing data to deliver a complete picture of each customer.

**Columns**

| Column Name     | Data Type | Description                                                                 |
|------------------|------------|-----------------------------------------------------------------------------|
| customer_key      | INTEGER   | Surrogate key, unique identifier for each customer                         |
| customer_id       | INTEGER   | CRM Customer ID                                                             |
| customer_number   | VARCHAR   | Customer key from CRM                                                       |
| first_name        | VARCHAR   | Customer first name                                                         |
| last_name         | VARCHAR   | Customer last name                                                          |
| country           | VARCHAR   | Country from ERP location table (e.g., United States)                      |
| marital_status    | VARCHAR   | Cleaned marital status from CRM (e.g., Married, Single)                    |
| gender            | VARCHAR   | Cleaned gender from CRM or ERP (e.g., Male, Female, n/a)                   |
| birthdate         | DATE      | Customer birth date from ERP (e.g., 1988-06-15)                            |
| create_date       | DATE      | CRM customer creation date (e.g., 2020-03-01)                              |

---

## 2. `gold.dim_products`

**Script Purpose**  
To deliver a refined list of products, with enriched and consistent details including product category, line, and valid start and end dates.

**Columns**

| Column Name   | Data Type | Description                                                                 |
|----------------|------------|-----------------------------------------------------------------------------|
| product_key     | INTEGER   | Surrogate key for each product                                              |
| prd_id          | INTEGER   | Product ID from CRM                                                         |
| cat_id          | VARCHAR   | Derived product category ID                                                 |
| prd_key         | VARCHAR   | Business product key                                                        |
| prd_name        | VARCHAR   | Name of the product (e.g., Road Bike)                                      |
| prd_cost        | INTEGER   | Cost of the product                                                         |
| prd_line        | VARCHAR   | Product line category (e.g., Mountain, City)                               |
| prd_start_dt    | DATE      | Product active start date (e.g., 2018-01-01)                               |
| prd_end_dt      | DATE      | Product active end date (e.g., 2022-12-31)                                 |

---

## 3. `gold.fact_sales`

**Script Purpose**  
To store transactional sales facts, linking customers and products with measures like sales amount, quantity, and pricing. This is the central fact table used for sales analytics.

**Columns**

| Column Name   | Data Type | Description                                                                 |
|----------------|------------|-----------------------------------------------------------------------------|
| sales_key       | INTEGER   | Surrogate key for each sale record                                          |
| sls_ord_num     | VARCHAR   | Sales order number (e.g., ORD-789)                                          |
| customer_key    | INTEGER   | Foreign key to `dim_customer`                                               |
| product_key     | INTEGER   | Foreign key to `dim_products`                                               |
| order_date      | DATE      | Date when order was placed (e.g., 2021-05-10)                              |
| ship_date       | DATE      | Date when order was shipped (e.g., 2021-05-12)                             |
| due_date        | DATE      | Due date for delivery (e.g., 2021-05-15)                                   |
| quantity        | INTEGER   | Number of units sold                                                       |
| price           | INTEGER   | Price per unit                                                             |
| sales_amount    | INTEGER   | Total sales amount                                                         |
                                                   

