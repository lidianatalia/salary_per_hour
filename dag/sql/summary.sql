SELECT
  year,
  month,
  branch_id,
  COUNT(employee_id) AS total_employee,
--   SUM(salary) AS total_salary,
--   SUM(total_work_hour) AS total_work_hour,
  SUM(salary)/SUM(total_work_hour) AS salary_per_hour
FROM (
  SELECT
		EXTRACT(year
		FROM
		  date) AS year,
		EXTRACT(month
		FROM
		  date) AS month,
    branch_id,
    employee_id,
    salary,
    SUM(CASE
        WHEN work_hour_duration_imputed IS NULL THEN ( SELECT ROUND(AVG(work_duration)) FROM v_emp_work_duration v
        where date <= ts_final.date 
        )
      ELSE
      work_hour_duration_imputed
    END
      ) AS total_work_hour
  FROM (
    SELECT
      *
      ,CASE
        WHEN work_hour_duration IS NULL OR work_hour_duration = 0 OR work_hour_duration > 14 #anomaly handler
      THEN ( SELECT ROUND(AVG(work_duration)) FROM v_emp_work_duration as v 
      WHERE employee_id = ts_imputed.employee_id
      ) -- get employee behavior and ensure the work hour duration not change by add param date
      ELSE
      work_hour_duration
    END
      AS work_hour_duration_imputed
    FROM (
      SELECT
		checkin,checkout,
        join_date,
        date,
        emp.branch_id,
        emp.employee_id,
        emp.salary,
        CASE
          WHEN ts.checkin <= ts.checkout THEN timestampdiff(hour, ts.checkin, ts.checkout)
          WHEN ts.checkin > ts.checkout THEN timestampdiff (hour,
          TIMESTAMP(ts.date,ts.checkin),
          TIMESTAMP(DATE_ADD(ts.date, INTERVAL 1 day), ts.checkout))
      END
        AS work_hour_duration
      FROM
        timesheet AS ts
      INNER JOIN
        employee AS emp
      ON
        ts.employee_id = emp.employee_id
      WHERE
        ts.date <= IFNULL(resign_date,
          CURRENT_DATE()) ) AS ts_imputed 
          ) AS ts_final
  GROUP BY
    1,
    2,
    3,
    4,
    5 
    ) AS summary
GROUP BY
  1,
  2,
  3;