-- Author: Nicolas Ribot
-- 15 sept 2016
--
-- PL/PGSQL Function to create a table from a query that should run with parallel plan enabled,
-- as data creation with SQL is not possible with parallel plans (or is it ?)
-- (see: https://wiki.postgresql.org/wiki/Parallel_Query)
--
-- Function takes program to run (like psql for instance), number of workers to set (see pg doc for parallel features)
-- table name to create, and query to execute
--
-- Example:
-- select * from create_table_parallel(
--     'intersection_result',
--     'select p.id as idparc, c.gid as idcarreau, p.year,
--         clock_timestamp() AS creation_time,
--         st_intersection(p.geom, c.geom) as geom
--       from parcelle_ssample p
--       join carreau_ssample c on st_intersects(p.geom, c.geom)',
--     '/usr/local/pgsql-9.6/bin/psql -A -t -p 5439 -d nicolas -c',
--     8,
--     true);


-- params:
-- table_name: name (qualified or not) of the table to create.
-- query: text of the query to run. MUST NOT contain a LIMIT clause, as a LIMIT 0 clause will be added. TODO: change this:
-- detect limit clause
-- program: the program to run the query, ex: /usr/local/pgsql-9.6/bin/psql -A -t -p 5439 -d nicolas -c.
--          should produce an output compatible with COPY command with '|' delimiter (default delimiter for non-aligned psql mode)
--          TODO: custom delimiter
--          default to 'psql -A -t -c'
-- num_workers: number of workers to set before the query (see max_parallel_workers_per_gather config parameter)
--              default to 0
-- drop_table: true to drop the table prior to its creation
--             default to false
create or replace function create_table_parallel(
  table_name text,
  query text,
  program text DEFAULT 'psql -A -t -c',
  num_workers int DEFAULT 0,
  drop_table boolean DEFAULT false) returns text as $$

DECLARE
  v_set_workers text:=format('set max_parallel_workers_per_gather = %s;', num_workers);
  v_tmp text := '';
BEGIN

  if drop_table then
    execute format('drop table if exists %I', table_name);
  END IF;

--   v_tmp := format('create table %I as %s LIMIT 0', table_name, query);
--   raise notice 'Q: %', v_tmp;

  execute format('create unlogged table %I as %s LIMIT 0', table_name, query);

  raise notice 'table % created', table_name;

  -- escapes '$ $' string delimiter for shell:
  query := replace(query, '''', '''''');
  raise notice 'cmd: %', format('copy %I FROM PROGRAM ''%s "%s %s"'' with (DELIMITER ''|'')', table_name, program, v_set_workers, query);

  execute format('copy %I FROM PROGRAM ''%s "%s %s"'' with (DELIMITER ''|'')', table_name, program, v_set_workers, query);
  return format('%I created', table_name);

END;
$$ LANGUAGE plpgsql parallel safe;
-- TODO: other FN params needed ?

-- 24s vs 1m24s without // plan...
DROP TABLE toto;
