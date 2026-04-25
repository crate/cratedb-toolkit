(nlsql-example-product)=

# NLSQL with product orders

Let's use a basic products / orders / customers database.

```sql
CREATE TABLE customers (customer_id INTEGER, name VARCHAR, city VARCHAR, email_address VARCHAR, gender_code VARCHAR);
CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, amount INTEGER);
CREATE TABLE products (product_id INTEGER, name VARCHAR, price NUMERIC(2), size VARCHAR);
CREATE TABLE order_items (order_id INTEGER, product_id INTEGER);
```

## Basic JOINs and filtering

:::{rubric} Provision
:::

Create table and insert data.
Populate the table using a few records worth of example data.

```sql
-- customers
INSERT INTO customers (customer_id, name, city) VALUES
(1, 'Alice', 'Berlin'),
(2, 'Bob', 'Munich'),
(3, 'Charlie', 'Hamburg');

-- products
INSERT INTO products (product_id, name) VALUES
(1, 'Laptop'),
(2, 'Phone'),
(3, 'Headphones');

-- orders
INSERT INTO orders (order_id, customer_id, amount) VALUES
(101, 1, 1200),
(102, 2, 800),
(103, 1, 200),
(104, 3, 150);

-- order_items
-- Alice bought Laptop, Bob bought Phone, Alice bought Headphones,
-- Charlie bought Headphones, Charlie also bought Phone.
INSERT INTO order_items (order_id, product_id) VALUES
(101, 1),  
(102, 2),
(103, 3),
(104, 3),
(104, 2);
```

:::{rubric} Query
:::

Submit a typical query in human language.

```shell
ctk query nlsql "List all customers with orders over €500."
```

:::{rubric} Response
:::

The model figures out the SQL statement, the engine runs it, and
uses the model again to come back with an answer in human language:

> The query results show that the customers 'Alice' from Berlin
> and 'Bob' from Munich have placed orders over €500.

The SQL statement was:
```sql
SELECT customers.name, customers.city
FROM customers JOIN orders ON customers.customer_id = orders.customer_id
WHERE orders.amount > 500;
```

## Advanced JOINs and filtering

:::{rubric} Provision
:::

Create table and insert data.
Add a few customers in New York and others elsewhere.
Synthesize orders with amounts both above and below the average.

```sql
INSERT INTO customers (customer_id, name, city) VALUES
(1, 'Alice Johnson', 'New York'),
(2, 'Bob Smith', 'Los Angeles'),
(3, 'Carol Lee', 'New York'),
(4, 'David Brown', 'Chicago');

INSERT INTO orders (order_id, customer_id, amount) VALUES
(101, 1, 500),   -- NY, high
(102, 1, 150),   -- NY, low
(103, 2, 300),   -- non-NY
(104, 3, 700),   -- NY, high
(105, 4, 200);   -- non-NY

INSERT INTO products (product_id, name) VALUES
(1001, 'Laptop'),
(1002, 'Phone'),
(1003, 'Tablet'),
(1004, 'Headphones');

INSERT INTO order_items (order_id, product_id) VALUES
(101, 1001),
(101, 1004),
(102, 1002),
(103, 1003),
(104, 1001),
(104, 1002),
(105, 1004);
```

:::{rubric} Query
:::

Submit a typical query in human language.

```shell
ctk query nlsql "Get the names of products that were ordered by customers in New York who spent more than the average amount."
```

:::{rubric} Response
:::

The model figures out the SQL statement, the engine runs it, and
uses the model again to come back with a synthesized response
based on the provided SQL query and its result:

> The query identifies the top 10 product names ordered by customers in New York
> who spent more than the average order amount.
> The results show that "Laptop", "Phone", and "Headphones" were among the most
> popular products purchased by New York customers with high spending.

The SQL statement was:
```sql
SELECT
    p.name FROM products AS p
    JOIN order_items AS oi ON p.product_id = oi.product_id
    JOIN orders AS o ON oi.order_id = o.order_id
    JOIN customers AS c ON o.customer_id = c.customer_id
WHERE
    c.city = 'New York'
ORDER BY
    o.amount DESC LIMIT 10;
```

## JOINs and grouping

:::{rubric} Provision
:::

```sql
INSERT INTO customers (customer_id, name, city, email_address, gender_code) VALUES
(1, 'Alice Johnson', 'New York', 'alice@example.com', 'F'),
(2, 'Bob Smith', 'Los Angeles', 'bob@example.com', 'M'),
(3, 'Carol Lee', 'Chicago', 'carol@example.com', 'F'),
(4, 'David Brown', 'Houston', 'david@example.com', 'M'),
(5, 'Eva Green', 'Phoenix', 'eva@example.com', 'F'),
(6, 'Frank Miller', 'Miami', 'frank@example.com', 'M'),
(7, 'Grace Kim', 'Seattle', 'grace@example.com', 'F'),
(8, 'Henry Davis', 'Boston', 'henry@example.com', 'O'); -- least common gender

INSERT INTO orders (order_id, customer_id, amount) VALUES
(101, 1, 120),
(102, 2, 200),
(103, 3, 150),
(104, 4, 300),
(105, 6, 80);

INSERT INTO products (product_id, name, price, size) VALUES
(1001, 'T-Shirt', 20, 'M'),
(1002, 'Jeans', 50, 'L'),
(1003, 'Jacket', 80, 'XL'),
(1004, 'Sneakers', 60, '42'),
(1005, 'Hat', 15, 'S');

INSERT INTO order_items (order_id, product_id) VALUES
(101, 1001),
(101, 1005),
(102, 1002),
(103, 1003),
(104, 1004),
(105, 1001);
```

:::{rubric} Q & A
:::

- Q: What are the email address and town of the customers who are of the least common gender?
  SQL: `SELECT email_address, city FROM customers GROUP BY gender_code ORDER BY count(*) ASC LIMIT 1`
- Q: What are the product price and the product size of the products whose price is above average?
  SQL: `SELECT products.price, products.size FROM products WHERE products.price > (SELECT AVG(price) FROM products)`
- Q: Which customers did not make any orders?
  SQL: `SELECT c.name FROM customers AS c LEFT JOIN orders AS o ON c.customer_id = o.customer_id WHERE o.order_id IS NULL;`
