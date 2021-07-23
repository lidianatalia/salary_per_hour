CREATE SCHEMA
  employees;
CREATE TABLE
  employee ( employee_id INT PRIMARY KEY,
    branch_id INT NOT NULL,
    salary DECIMAL NOT NULL,
    join_date DATE NOT NULL,
    resign_date DATE);
CREATE TABLE
  timesheet ( timesheet_id BIGINT NOT NULL PRIMARY KEY,
    employee_id INT NOT NULL,
    date DATE,
    checkin TIME,
    checkout TIME);
CREATE TABLE
  branch_hourly_salary (year year NOT NULL,
    month int NOT NULL,
    branch_id INT NOT NULL,
    salary_per_hour decimal NOT NULL );