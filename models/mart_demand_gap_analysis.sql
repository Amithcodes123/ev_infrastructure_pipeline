{{ config(materialized='table') }}

with live_occupancy_staged as (
    select
        cast(ingestion_timestamp as timestamp) as observation_time,
        upper(trim(city)) as clean_city,
        lpad(trim(postal_code), 5, '0') as clean_postal_code,
        tomtom_station_id,
        station_name,
        connector_type,
        cast(capacity_total_plugs as int64) as total_plugs,
        cast(live_occupied_plugs as int64) as occupied_plugs,
        cast(live_available_plugs as int64) as available_plugs
    from {{ source('warehouse_bronze', 'raw_live_occupancy') }}
),

bnetza_registry_staged as (
    select
        upper(trim(betreiber)) as commercial_operator,
        safe_cast(inbetriebnahmedatum as date) as commissioning_date,
        cast(nennleistung_kw as float64) as power_capacity_kw,
        lpad(trim(postleitzahl), 5, '0') as clean_postal_code,
        upper(trim(ort)) as clean_city
    from {{ source('warehouse_bronze', 'raw_bnetza_registry') }}
),

kba_demand_staged as (
    select
        upper(trim(Zulassungsbezirk)) as clean_district,
        cast(Pkw_insgesamt as int64) as total_registered_vehicles,
        cast(Pkw_BEV_Anteil as float64) as ev_adoption_percentage
    from {{ source('warehouse_bronze', 'raw_kba_demand') }}
)

select
    telemetry.observation_time,
    telemetry.tomtom_station_id,
    telemetry.station_name,
    grid.commercial_operator,
    grid.commissioning_date,
    grid.power_capacity_kw,
    telemetry.clean_postal_code as geographic_postal_zone,
    telemetry.clean_city as diagnostic_allocation_city,
    telemetry.connector_type,
    telemetry.total_plugs,
    telemetry.occupied_plugs,
    telemetry.available_plugs,
    coalesce(demand.ev_adoption_percentage, 0.0) as ev_adoption_percentage,
    
    case
        when telemetry.total_plugs > 0
        then round((cast(telemetry.occupied_plugs as float64) / cast(telemetry.total_plugs as float64)) * 100, 2)
        else 0.0
    end as current_utilization_rate,
    
    case
        when telemetry.total_plugs > 0
        then round(((cast(telemetry.occupied_plugs as float64) / cast(telemetry.total_plugs as float64)) * 100) * coalesce(demand.ev_adoption_percentage, 0.0), 2)
        else 0.0
    end as allocation_demand_gap_score

from live_occupancy_staged telemetry
left join bnetza_registry_staged grid
    on telemetry.clean_postal_code = grid.clean_postal_code 
    and telemetry.clean_city = grid.clean_city
left join kba_demand_staged demand
    on telemetry.clean_city = demand.clean_district
    or strpos(demand.clean_district, telemetry.clean_city) > 0