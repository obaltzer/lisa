-- Query 4: PostGIS Query
SELECT states.id, counties.id, COUNT(geonames.*) FROM states
  LEFT JOIN counties 
    ON CONTAINS(states.geom, counties.geom)
  LEFT JOIN geonames
    ON CONTAINS(counties.geom, geonames.location)
  WHERE 
    CONTAINS(
      MakeBox2D(
	MakePoint(-93.88, 49.81),
	MakePoint(-65.39, 24.22)
      ),
      geonames.location
    )
  GROUP BY states.id, counties.id;

SELECT states.id, COUNT(*) FROM states
  LEFT JOIN counties 
    ON CONTAINS(states.geom, counties.geom)
  LEFT JOIN geonames
    ON CONTAINS(counties.geom, geonames.location)
  WHERE 
    CONTAINS(
      MakeBox2D(
	MakePoint(-93.88, 49.81),
	MakePoint(-65.39, 24.22)
      ),
      geonames.location
    )
  GROUP BY states.id;
