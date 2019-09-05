-- inserting multiple items
INSERT INTO table_with_pk (itemname) VALUES ('homer'), ('marge'), ('bart');

-- inserting one item
INSERT INTO table_with_pk (itemname) VALUES ('lisa');
INSERT INTO table_with_pk (itemname) VALUES ('maggie');

-- inserting single items in a transaction
BEGIN;
INSERT INTO table_with_pk (itemname) VALUES ('ned');
INSERT INTO table_with_pk (itemname) VALUES ('maude');
INSERT INTO table_with_pk (itemname) VALUES ('todd');
INSERT INTO table_with_pk (itemname) VALUES ('rodd');
COMMIT;


-- inserting multiple items
INSERT INTO table_without_pk (itemname) VALUES ('homer'), ('marge'), ('bart');

-- inserting one item
INSERT INTO table_without_pk (itemname) VALUES ('lisa');
INSERT INTO table_without_pk (itemname) VALUES ('maggie');

-- inserting single items in a transaction
BEGIN;
INSERT INTO table_without_pk (itemname) VALUES ('ned');
INSERT INTO table_without_pk (itemname) VALUES ('maude');
INSERT INTO table_without_pk (itemname) VALUES ('todd');
INSERT INTO table_without_pk (itemname) VALUES ('rodd');
COMMIT;
