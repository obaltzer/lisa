DROP FUNCTION IF EXISTS compute();
DROP TYPE IF EXISTS result_type;

CREATE TYPE result_type AS (states_id INTEGER, counties_id INTEGER, zip5_id INTEGER, area FLOAT);
CREATE OR REPLACE FUNCTION compute() RETURNS SETOF result_type AS
$BODY$
DECLARE
    query geometry;
    state RECORD;
    state_total FLOAT;
    county RECORD;
    county_total FLOAT;
    zip RECORD;
    cover FLOAT;
    result result_type;
BEGIN
    query := CAST(makebox2d(makepoint(-93.88, 24.22), makepoint(-65.39, 49.81)) AS geometry);
    FOR state IN SELECT states.id as id, INTERSECTION(query, states.geom) as geom FROM states WHERE INTERSECTS(states.geom, query) LOOP
        state_total := 0.0;
        IF (GeometryType(state.geom) = 'MULTIPOLYGON' OR GeometryType(state.geom) = 'POLYGON') AND isValid(state.geom) AND isSimple(state.geom) AND NOT isEmpty(state.geom) THEN
            FOR county IN SELECT counties.id AS id, INTERSECTION(state.geom, counties.geom) AS geom FROM counties WHERE INTERSECTS(counties.geom, state.geom) LOOP
                county_total := 0.0;
                IF (GeometryType(county.geom) = 'MULTIPOLYGON' OR GeometryType(county.geom) = 'POLYGON') AND isValid(county.geom) AND NOT isEmpty(county.geom) THEN
                    FOR zip IN SELECT zip5.id AS id, INTERSECTION(county.geom, zip5.geom) AS geom FROM zip5 WHERE INTERSECTS(zip5.geom, county.geom) LOOP
                        IF (GeometryType(zip.geom) = 'MULTIPOLYGON' OR GeometryType(zip.geom) = 'POLYGON') AND isValid(zip.geom) AND NOT isEmpty(zip.geom) THEN
                            SELECT SUM(AREA(INTERSECTION(zip.geom, lulc.geom)) / AREA(lulc.geom)) INTO cover FROM lulc WHERE INTERSECTS(lulc.geom, zip.geom);
                            -- SELECT CAST(COUNT(*) AS FLOAT) INTO cover FROM lulc WHERE INTERSECTS(lulc.geom, zip.geom);
                            IF cover IS NOT NULL THEN
                                result.states_id = state.id - 1;
                                result.counties_id = county.id - 1;
                                result.zip5_id = zip.id - 1;
                                result.area = cover;
                                county_total := county_total + cover;
                                RETURN NEXT result;
                            END IF;
                        END IF;
                    END LOOP;
                END IF;
		IF county_total != 0 THEN
	            result.states_id = state.id - 1;
        	    result.counties_id = county.id - 1;
                    result.zip5_id = NULL;
                    result.area = county_total;
                    state_total := state_total + county_total;
                    RETURN NEXT result;
		END IF;
            END LOOP;
        END IF;
	IF state_total != 0 THEN
            result.states_id = state.id - 1;
            result.counties_id = NULL;
            result.zip5_id = NULL;
            result.area = state_total;
	END IF;
        RETURN NEXT result;
    END LOOP;
    RETURN;
END
$BODY$
LANGUAGE 'plpgsql';
SELECT * FROM compute();
