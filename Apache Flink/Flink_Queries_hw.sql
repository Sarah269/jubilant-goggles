-- DataExpert.io Academy Data Engineering Bootcamp
-- Apache Flink
-- Web Events

-- Execute queries after starting start_job.py and aggregation2_job.py Apache Flink jobs
-- micro data is contained in table processed_events
-- session_events_aggregated_ip is populated from data streamed from Apache Flink
-- session_events_aggregated_ip is aggregated on IP and host from processed_events using a 5 minute session window

-- Create table to receive Apache Flink aggregated session data

CREATE TABLE session_events_aggregated_ip (
            start_session TIMESTAMP(3),
            end_session TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            web_events BIGINT
           );


-- Q2. What is the average number of web events of a session from a user on Tech Creator?


SELECT host, ROUND(SUM(web_events)/COUNT(ip),0) as avg_web_events
FROM session_events_aggregated_ip
WHERE host = 'www.techcreator.io'
GROUP BY host


-- Q3. Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

SELECT host, ROUND(SUM(web_events)/COUNT(ip),0) as avg_web_events
FROM session_events_aggregated_ip
GROUP BY host

