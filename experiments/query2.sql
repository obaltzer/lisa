-- SQLite 3 Query
.separator ,

SELECT family.id, genus.id, species.id, MAX(plants.height) 
FROM family
LEFT JOIN genus ON genus.family_id = family.id 
LEFT JOIN species ON species.genus_id = genus.id 
LEFT JOIN plants ON plants.species_id =  species.id 
WHERE plants.age >= 10 AND plants.age <= 50 
GROUP BY family.id, genus.id, species.id;

SELECT family.id, genus.id, MAX(plants.height) 
FROM family
LEFT JOIN genus ON genus.family_id = family.id 
LEFT JOIN species ON species.genus_id = genus.id 
LEFT JOIN plants ON plants.species_id =  species.id 
WHERE plants.age >= 10 AND plants.age <= 50 
GROUP BY family.id, genus.id;

SELECT family.id, MAX(plants.height) 
FROM family
LEFT JOIN genus ON genus.family_id = family.id 
LEFT JOIN species ON species.genus_id = genus.id 
LEFT JOIN plants ON plants.species_id =  species.id 
WHERE plants.age >= 10 AND plants.age <= 50 
GROUP BY family.id;

SELECT MAX(plants.height) 
FROM family
LEFT JOIN genus ON genus.family_id = family.id 
LEFT JOIN species ON species.genus_id = genus.id 
LEFT JOIN plants ON plants.species_id =  species.id 
WHERE plants.age >= 10 AND plants.age <= 50; 

-- GROUP BY ROLLUP(family.id, genus.id, species.id)
