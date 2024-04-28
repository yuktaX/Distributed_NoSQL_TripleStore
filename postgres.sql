DROP Table if exists sample_yago;
 
CREATE TABLE sample_yago (
    id SERIAL PRIMARY KEY,
    subject TEXT,
    predicate TEXT,
    object TEXT
);


----Query on subject---
select * from sample_yago where subject = '<Elizabeth_II>';


---Update(subject,predicate)---

WITH upsert AS (
  -- Select the subject, predicate combination and the new object value
  SELECT 'your_subject' AS subject, 'your_predicate' AS predicate, 'new_object_value' AS new_object
),
ins AS (
  -- Insert the new values if the combination doesn't exist
  INSERT INTO sample_yago (subject, predicate, object)
  SELECT subject, predicate, new_object
  FROM upsert
  ON CONFLICT (id) DO NOTHING
  RETURNING *
)
-- Update the object value if the combination already exists
UPDATE sample_yago
SET object = upsert.new_object
FROM upsert
WHERE sample_yago.subject = upsert.subject
AND sample_yago.predicate = upsert.predicate;
