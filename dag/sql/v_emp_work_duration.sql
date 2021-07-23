CREATE TABLE v_emp_work_duration AS 
SELECT
    employee_id,
    date,
    CASE
      WHEN work_hour_duration > 14 THEN 14 #handling anomaly by set max working hour based on dicrit government
      WHEN work_hour_duration < 7 THEN NULL #some of our employee get half day and this duration include to a special case
    ELSE
    work_hour_duration
  END
    AS work_duration,
    COUNT(1) AS total_days
  FROM (
    SELECT
      ts.employee_id,
      ts.date,
      CASE
        WHEN ts.checkin <= ts.checkout THEN timestampdiff(hour, ts.checkin, ts.checkout)
      ELSE
      timestampdiff (hour,
        TIMESTAMP(ts.date,ts.checkin),
        TIMESTAMP(DATE_ADD(ts.date, INTERVAL 1 day), ts.checkout))
    END
      AS work_hour_duration
    FROM
      timesheet AS ts
        ) AS cleaned
having work_duration is not null        
        
        