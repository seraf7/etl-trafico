from pyspark.sql.functions import col, year, month, dayofmonth

# Construccion de la tabla de hechos
def build_fact_table(stations_df, traffic_df, weather_df, dateDim):
    df_fact = traffic_df.join(
        stations_df.alias("b"),
        (traffic_df.station_id == stations_df.station_id)
        #& (traffic_df.state_cd == stations_df.State_FIPS)
        ,how="left"
    ).select("id_dim_date", "state_cd", 
             "b.station_id", "sum_veh_count", 
             "functional_class", "latitude",
             "longitude", "state"
    )

    df_fact = df_fact.join(
        weather_df, 
        on="id_dim_date", 
        how="left"
    )

    df_fact = df_fact.join(
        dateDim, 
        (df_fact.id_dim_date == dateDim.skeyDate), 
        how="left"
    )

    df_fact = df_fact.drop("sha2", "skeyDate", "tstamp")

    return df_fact