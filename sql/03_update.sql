-- updating multiple items
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname IN ('homer', 'marge', 'bart');

-- updating one item
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname = 'lisa';
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname = 'maggie';

-- updating single items in a transaction
BEGIN;
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname = 'ned';
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname = 'maude';
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname = 'todd';
UPDATE table_with_pk SET description = 'foo bar' WHERE itemname = 'rodd';
COMMIT;


-- updating multiple items
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname IN ('homer', 'marge', 'bart');

-- updating one item
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname = 'lisa';
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname = 'maggie';

-- updating single items in a transaction
BEGIN;
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname = 'ned';
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname = 'maude';
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname = 'todd';
UPDATE table_without_pk SET description = 'foo bar' WHERE itemname = 'rodd';
COMMIT;
