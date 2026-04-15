(nlsql-example-employee)=

# NLSQL with employee data

Let's use a single `employees` database table
and populate it with a few records worth of data.

:::{rubric} Provision
:::

Create table and insert data.

```sql
CREATE TABLE employees (id INT, name TEXT, department TEXT, hire_date TIMESTAMP); 

INSERT INTO employees (id, name, department, hire_date) VALUES
(1, 'Alice Johnson', 'Engineering', '2022-03-15'),
(2, 'Bob Smith', 'Marketing', '2021-07-01'),
(3, 'Carol Lee', 'Human Resources', '2020-11-23'),
(4, 'David Brown', 'Finance', '2019-05-30'),
(5, 'Eva Green', 'Engineering', '2023-01-10'),
(6, 'Frank Miller', 'Sales', '2019-08-12'),
(7, 'Grace Kim', 'Sales', '2021-02-18'),
(8, 'Henry Davis', 'Sales', '2022-06-25'),
(9, 'Isabella Martinez', 'Sales', '2020-12-05'),
(10, 'Jack Wilson', 'Sales', '2023-09-14');
```

:::{rubric} Query
:::

Submit a typical query in human language.

```shell
ctk query nlsql "List all employees in the 'Sales' department hired after 2022."
```

:::{rubric} Response
:::

The model figures out the SQL statement, the engine runs it, and
uses the model again to come back with an answer in human language:
```text
The employees in the Sales department hired after 2022 are Henry Davis and Jack Wilson.
```

The SQL statement was:
```sql
SELECT
    name FROM employees
WHERE
    department = 'Sales' AND
    hire_date > '2022-01-01';
```
