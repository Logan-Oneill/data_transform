START TRANSACTION;

INSERT INTO incident_unnormalized (
  id,
  creationTime,
  updateTime,
  sourceReference,
  type,
  subtype,
  description,
  street,
  direction,
  startTime,
  endTime
)
SELECT 
  (i.value->>'id')::INT AS id,
  (i.value->>'creationtime')::TIMESTAMPTZ,
  (i.value->>'updatetime')::TIMESTAMPTZ,
  i.value->'source'->>'reference',
  i.value->>'type',
  i.value->>'subtype',
  i.value->>'description',
  i.value->'location'->>'street',
  i.value->'location'->>'direction',
  (i.value->>'starttime')::TIMESTAMPTZ,
  (i.value->>'endtime')::TIMESTAMPTZ
FROM data_ingest r,
LATERAL jsonb_array_elements(r.raw_json->'incident') AS i(value);

	-- add to sub tables if not present
INSERT INTO api.type (typeName)
SELECT DISTINCT type
FROM incident_unnormalized
WHERE type IS NOT NULL
ON CONFLICT (typeName) DO NOTHING;

INSERT INTO api.subType (subTypeName, parentTypeId)
SELECT DISTINCT current.subtype, types.typeId
FROM incident_unnormalized current
JOIN testing.type types ON current.type = types.typeName
ON CONFLICT (subTypeName, parentTypeId) DO NOTHING;

INSERT INTO api.sourceReference (sourceReference)
SELECT DISTINCT sourceReference
FROM incident_unnormalized
WHERE sourceReference IS NOT NULL
ON CONFLICT (sourceReference) DO NOTHING;

INSERT INTO api.location (street, direction)
SELECT DISTINCT street, direction
FROM incident_unnormalized
WHERE street IS NOT NULL AND direction IS NOT NULL
ON CONFLICT (street, direction) DO NOTHING;

INSERT INTO api.description (description)
SELECT DISTINCT description
FROM incident_unnormalized
WHERE description IS NOT NULL
ON CONFLICT (description) DO NOTHING;

INSERT INTO api.incident (
  id,
  creationTime,
  updateTime,
  sourceReferenceId,
  subTypeId,
  descriptionId,
  locationId,
  startTime,
  endTime
)
SELECT DISTINCT ON (current.id)
  current.id,
  current.creationTime,
  current.updateTime,
  sr.id AS sourceReferenceId,
  st.id AS subTypeId,
  d.id AS descriptionId,
  l.id AS locationId,
  current.startTime,
  current.endTime
FROM incident_unnormalized current
JOIN api.sourceReference sr ON current.sourceReference = sr.sourceReference
JOIN api.type t ON current.type = t.typeName
JOIN api.subType st ON st.subTypeName IS NOT DISTINCT FROM current.subtype AND st.parentTypeId = t.typeId
JOIN api.description d ON current.description = d.description
JOIN api.location l ON current.street = l.street AND current.direction = l.direction
ON CONFLICT (id) DO NOTHING;

-- clean up data_ingest

DELETE FROM data_ingest;
DELETE FROM incident_unnormalized;

COMMIT;
