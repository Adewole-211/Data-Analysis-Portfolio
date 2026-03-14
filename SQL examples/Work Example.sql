WITH
    downtime_events
    AS
    (
        SELECT
            facility_id,
            region_name,
            facility_type,
            process_area,
            process_group,
            EXTRACT(YEAR FROM DATE(start_time_local)) AS year,
            DATE(start_time_local) AS event_date,
            start_time_local AS start_time,
            end_time_local AS end_time,
            DATE_DIFF('minute', start_time_local, end_time_local) AS total_downtime_minutes,
            EXTRACT(WEEK FROM DATE(start_time_local)) AS week_number,
            AVG(total_hours) AS total_hours
        FROM "database_name"."schema_name"."fact_downtime_summary"
        WHERE DATE(start_time_local) > '2024-12-31'
            AND facility_type IS NOT NULL
        GROUP BY
        facility_id,
        region_name,
        facility_type,
        process_area,
        process_group,
        start_time_local,
        end_time_local,
        EXTRACT(YEAR FROM DATE(start_time_local)),
        DATE(start_time_local),
        DATE(end_time_local),
        EXTRACT(WEEK FROM DATE(start_time_local))
    ),

    quality_incidents
    AS
    (
        SELECT
            b.incident_status,
            a.facility_id,
            DATE(a.incident_date) AS incident_date,
            a.shift,
            a.affected_units,
            a.category,
            a.root_cause,
            a.incident_id
        FROM "database_name"."schema_name"."fact_quality_incidents" AS a
            INNER JOIN "database_name"."schema_name"."fact_incident_status" AS b
            ON b.incident_id = a.incident_id
        WHERE b.incident_status = 'approved'
            AND a.incident_date >= '2024-12-31 00:00:00'
        GROUP BY
        a.incident_id,
        a.incident_date,
        a.facility_id,
        b.incident_status,
        a.category,
        a.root_cause,
        a.shift,
        a.affected_units
    ),

    forecast_accuracy
    AS
    (
        SELECT
            region,
            facility_id,
            DATE(forecast_timestamp) AS forecast_date,
            AVG(forecast_value_a) AS forecast_value_a,
            AVG(forecast_value_b) AS forecast_value_b,
            AVG(actual_value_a) AS actual_value_a,
            AVG(actual_value_b) AS actual_value_b,
            ROUND(
            (
                (AVG(actual_value_b) - AVG(forecast_value_b))
                / NULLIF(AVG(forecast_value_b), 0)
            ) * 100,
            2
        ) AS forecast_b_pct_diff,
            ROUND(
            (
                (AVG(actual_value_a) - AVG(forecast_value_a))
                / NULLIF(AVG(forecast_value_a), 0)
            ) * 100,
            2
        ) AS forecast_a_pct_diff,
            CASE
            WHEN ABS(
                (
                    (AVG(actual_value_b) - AVG(forecast_value_b))
                    / NULLIF(AVG(forecast_value_b), 0)
                ) * 100
            ) > 5 THEN 'Not Aligned'
            ELSE 'Aligned'
        END AS forecast_b_alignment_status,
            CASE
            WHEN ABS(
                (
                    (AVG(actual_value_a) - AVG(forecast_value_a))
                    / NULLIF(AVG(forecast_value_a), 0)
                ) * 100
            ) > 5 THEN 'Not Aligned'
            ELSE 'Aligned'
        END AS forecast_a_alignment_status,
            MAX(last_update_date) AS last_update_date
        FROM "database_name"."schema_name"."forecast_accuracy_metrics"
        WHERE forecast_timestamp > '2024-12-31 23:59:59'
            AND forecast_value_a > 0
            AND forecast_value_b > 0
            AND actual_value_a > 0
            AND actual_value_b > 0
        GROUP BY
        region,
        facility_id,
        DATE(forecast_timestamp)
    ),

    combined_data
    AS
    (
        SELECT
            fa.forecast_date,
            fa.region,
            fa.facility_id,
            fa.forecast_b_pct_diff,
            fa.forecast_a_pct_diff,
            fa.forecast_b_alignment_status,
            fa.forecast_a_alignment_status,
            dt.total_downtime_minutes,
            dt.total_hours AS downtime_total_hours,
            qi.affected_units
        FROM forecast_accuracy AS fa
            LEFT JOIN downtime_events AS dt
            ON fa.facility_id = dt.facility_id
                AND fa.forecast_date = dt.event_date
            LEFT JOIN quality_incidents AS qi
            ON fa.facility_id = qi.facility_id
                AND fa.forecast_date = qi.incident_date
    )