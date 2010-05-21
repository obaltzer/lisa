-- SQLite 3 Query

.separator ,
SELECT species.id, MAX(plants.height) 
FROM species 
LEFT JOIN plants ON  plants.species_id = species.id 
WHERE plants.age >= 10 AND plants.age <= 50 
GROUP BY species.id;
