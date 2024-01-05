
{% macro array_length(arr) %}
  select array_length('{{ arr }}') as length
{% endmacro %}

-- haversine
{% macro haversine_distance(lat1, lon1, lat2, lon2) %}
    6371 * 10^3 * 2 * ATAN2(
        SQRT(
            POWER(SIN((RADIANS({{ lat2 }}) - RADIANS({{ lat1 }})) / 2), 2) +
            COS(RADIANS({{ lat1 }})) * COS(RADIANS({{ lat2 }})) * POWER(SIN((RADIANS({{ lon2 }}) - RADIANS({{ lon1 }})) / 2), 2)
        ),
        SQRT(1 - POWER(SIN((RADIANS({{ lat2 }}) - RADIANS({{ lat1 }})) / 2), 2) +
            COS(RADIANS({{ lat1 }})) * COS(RADIANS({{ lat2 }})) * POWER(SIN((RADIANS({{ lon2 }}) - RADIANS({{ lon1 }})) / 2), 2))
    )
{% endmacro %}