CREATE TABLE IF NOT EXISTS flights (
    id             SERIAL PRIMARY KEY,
    icao24         VARCHAR(10),
    callsign       VARCHAR(20),
    origin_country VARCHAR(50),
    longitude      FLOAT,
    latitude       FLOAT,
    altitude       FLOAT,
    velocity       FLOAT,
    velocity_kmh   FLOAT,
    heading        FLOAT,
    on_ground      BOOLEAN,
    timestamp      BIGINT,
    category       INTEGER,
    created_at     TIMESTAMP DEFAULT NOW()
);