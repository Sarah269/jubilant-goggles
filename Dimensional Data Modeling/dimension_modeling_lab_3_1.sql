-- Graph Modeling

--create type vertex_type
--    as enum('player', 'team', 'game');
    
--create table vertices (
--	identifier text,
--	type vertex_type,
--	properties json,
--	primary key(identifier, type)
--	);
--	
--drop type edge_type;

--create type edge_type 
--as enum ('plays against', 
--		'shares_team',
--		'plays_in',
--		'plays_on');

--create table edges (
--	subject_identifier text,
--	subject_type vertex_type,
--	object_identifier text,
--	object_type vertex_type,
--	edge_type edge_type,
--	properties json,
--	primary key (subject_identifier,
--				 subject_type,
--				 object_identifier,
--				 object_type,
--				 edge_type)
--				 
--);

--
--drop table edges;
--list enum values

 SELECT unnest(enum_range(NULL::edge_type)) 
 -- add a value to enum 
 ALTER TYPE edge_type ADD VALUE 'plays against';
	
