import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

# Table for web_events aggregated on IP and host

def create_agg_session_events_ip_sink_postgres(t_env):
    table_name = 'session_events_aggregated_ip'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            start_session TIMESTAMP(3),
            end_session TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            web_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

# Source table used to aggregate web events on IP and host
.
.
.

# Aggregated data on IP and host using session windowing with a 5 minute gap

def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_processed_events_source_kafka(t_env)

        aggregated_sink_ip_table = create_agg_session_events_ip_sink_postgres(t_env)

              
        t_env.from_path(source_table).window(
                    Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
                ).group_by(
                    col("w"),
                    col("ip"),
                    col("host"),
                   
                ) \
                    .select(
                    col("w").start.alias("start_session"),
                    col("w").end.alias("end_session"),
                    col("ip"),
                    col("host"),
                    col("ip").count.alias("web_events")
                ) \
                    .execute_insert(aggregated_sink_ip_table) \
                    .wait()



    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))
        print("Is Postgres started? Check values for environment variables.")

# Execute streaming

if __name__ == '__main__':
    log_aggregation()