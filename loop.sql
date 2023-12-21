DO $$ 
DECLARE
  i INT := 1;
  test_id INT;
BEGIN
  SELECT COALESCE(MAX(id), 0) INTO test_id FROM prize;

  LOOP
    EXIT WHEN i > 10;
    INSERT INTO prize (year, category, id)
    VALUES
      (2023,
       CASE i
         WHEN 1 THEN 'medicine'
         WHEN 2 THEN 'economics'
         ELSE 'chemistry'
       END,
	  test_id + i);
    i := i + 1;
  END LOOP;

END $$;