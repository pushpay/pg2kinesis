-- deleting multiple items
DELETE FROM table_with_pk WHERE itemname IN ('homer', 'marge', 'bart');

-- deleting one item
DELETE FROM table_with_pk WHERE itemname = 'lisa';
DELETE FROM table_with_pk WHERE itemname = 'maggie';

-- deleting single items in a transaction
BEGIN;
DELETE FROM table_with_pk WHERE itemname = 'ned';
DELETE FROM table_with_pk WHERE itemname = 'maude';
DELETE FROM table_with_pk WHERE itemname = 'todd';
DELETE FROM table_with_pk WHERE itemname = 'rodd';
COMMIT;


-- deleting multiple items
DELETE FROM table_without_pk WHERE itemname IN ('homer', 'marge', 'bart');

-- deleting one item
DELETE FROM table_without_pk WHERE itemname = 'lisa';
DELETE FROM table_without_pk WHERE itemname = 'maggie';

-- deleting single items in a transaction
BEGIN;
DELETE FROM table_without_pk WHERE itemname = 'ned';
DELETE FROM table_without_pk WHERE itemname = 'maude';
DELETE FROM table_without_pk WHERE itemname = 'todd';
DELETE FROM table_without_pk WHERE itemname = 'rodd';
COMMIT;
