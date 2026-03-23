-- GEOMAP
SELECT
  callsign AS `Flight`,
  restricted_zone AS `Restricted Zone`,
  severity AS `Severity`,
  risk_label AS `Risk Level`,
  risk_score AS `Risk Score`,
  zone_status AS `Zone Status`,
  miles_from_zone AS `Miles From Zone`,
  altitude AS `Altitude ft`,
  heading AS `Heading`,
  velocity AS `Speed Knots`,
  flight_phase AS `Flight Phase`,
  icao24 AS `ICAO24`,
  latitude,
  longitude
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`)
ORDER BY risk_score DESC;

-- LIVE ALERT FEED
SELECT
  callsign AS `Flight`,
  restricted_zone AS `Restricted Zone`,
  severity AS `Severity`,
  risk_label AS `Risk Level`,
  zone_status AS `Zone Status`,
  CAST(miles_from_zone AS INT64) AS `Miles From Zone`,
  CAST(altitude AS INT64) AS `Altitude ft`,
  CAST(velocity AS INT64) AS `Speed Knots`,
  flight_phase AS `Flight Phase`,
  snapshot_time AS `Last Seen`
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`)
ORDER BY risk_score DESC, miles_from_zone ASC;

-- TOTAL FLIGHTS MONITORED
SELECT COUNT(DISTINCT icao24) as value
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`);

-- RESTRICTED ZONES ACTIVE
SELECT COUNT(DISTINCT country) as value
FROM `flights-490708.flight_data.restricted_airspace`;

-- ACTIVE CONFLICT ZONES
SELECT COUNT(DISTINCT restricted_zone) as value
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE zone_status = 'INSIDE'
AND snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`);

-- INSIDE CLOSED AIRSPACE
SELECT COUNT(DISTINCT icao24) as value
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE risk_score = 10
AND snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`);

-- NEAR CLOSED OR INSIDE HIGH RISK
SELECT COUNT(DISTINCT icao24) as value
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE risk_score = 7
AND snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`);

-- NEAR HIGH RISK OR INSIDE RESTRICTED
SELECT COUNT(DISTINCT icao24) as value
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE risk_score = 5
AND snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`);

-- RESTRICTED AIRSPACE REFERENCE TABLE
SELECT
  country AS `Country`,
  severity AS `Severity`,
  reason AS `Reason`,
  since AS `Since`,
  updated_at AS `Last Updated`
FROM `flights-490708.flight_data.restricted_airspace`
ORDER BY CASE severity WHEN 'CLOSED' THEN 1 WHEN 'HIGH RISK' THEN 2 WHEN 'RESTRICTED' THEN 3 END;

-- TOP RESTRICTED ZONES
SELECT
  restricted_zone AS `Zone`,
  COUNT(DISTINCT icao24) AS `Flights`
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE zone_status = 'INSIDE'
AND snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`)
GROUP BY restricted_zone
ORDER BY Flights DESC
LIMIT 10;

-- FLIGHT PHASE BREAKDOWN
SELECT
  flight_phase AS `Phase`,
  COUNT(DISTINCT icao24) AS `Flights`
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`)
GROUP BY flight_phase;

-- ZONE STATUS BREAKDOWN
SELECT
  zone_status AS `Status`,
  COUNT(DISTINCT icao24) AS `Flights`
FROM `flights-490708.flight_data.flight_risk_snapshot`
WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM `flights-490708.flight_data.flight_risk_snapshot`)
GROUP BY zone_status
ORDER BY Flights DESC;
