-- PostGIS Query
SELECT counties.id, COUNT(geonames.*) FROM count
    LEFT JOIN geonames ON CONTAINS(counties.geom, geonames.location)
    WHERE 
        CONTAINS(
            MakeBox2D(
                MakePoint(-93.88, 49.81),
                MakePoint(-65.39, 24.22)
            ),
            geonames.location
        )
    GROUP BY counties.id;

