-- create the table
CREATE TABLE kiwi(key VARCHAR(255) PRIMARY KEY, val BYTEA);

-- create a function which pushes out notifications for changes
CREATE OR REPLACE FUNCTION notify_kiwi_changes()
RETURNS trigger AS $$
DECLARE
  current_row RECORD;
BEGIN
  IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
    current_row := NEW;
  ELSE
    current_row := OLD;
  END IF;
PERFORM pg_notify(
    'kiwi_update',
    -- TODO: Change this to send I or U or D for insert, update, delete
    TG_OP || current_row.key
  );
RETURN current_row;
END;
$$ LANGUAGE plpgsql;


-- wireup a trigger
CREATE TRIGGER notify_kiwi_changes_trg
AFTER INSERT OR UPDATE OR DELETE
ON kiwi
-- TODO: Is there a way we can do this in a batch update?
FOR EACH ROW
EXECUTE PROCEDURE notify_kiwi_changes();

