# Apache Flink Job - Aggregate web events by IP and Host

## Overview
Using Apache Flink stream website activity in real-time into Postgres tables.

## Jobs
Directory: /src/job
- start_job.py: collect website activity
- aggregation2_job.py: aggregate website activitiy on IP and host using a session window with a 5 minute gap

## Tools
- Apache Flink Docker Container
- Postgres Docker Container
- Docker Desktop
- Apache Flink UI (localhost: 8081)

## Windowing
- Session with 5 minute gap

## Aggregate
- IP and Host

## Environment Variables
- KAFKA environment variables: TRAFFIC SECRET, TRAFFIC KEY
- POSTGRES environment variables: URL, USER, PASSWORD, DB
- JDBC: URL
- IP LOCATION: CODING KEY
  
## Build Docker Containers
- Postgress Docker Container: docker compose up -d
- Apache Flink Docker Container: docker compose --env-file flink-env.env up --build --remove-orphans -d

## Create Postgres Tables
- web event activity
<pre>CREATE TABLE processed_events (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
); </pre>

-- web event activity aggregated on IP and host
<pre>CREATE TABLE session_events_aggregated_ip (
            start_session TIMESTAMP(3),
            end_session TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            web_events BIGINT
           ); </pre>



## Job Excecution
- Start Postgres container
- Start Apache Flink container
- Execute start_job.py in jobmanager terminal: flink run -py /opt/src/job/start_job.py --pyFiles /opt/src -d

- Execute aggregation2_job.py: docker compose exec jobmanager flink run -py /opt/src/aggregation2_job.py --pyFiles /opt/src -d

## View Collected Data
- To view data collected, wait about 10 minutes then go into Postgres.
  - select * from processed_events;

  - select * from session_events_aggregated_ip;

