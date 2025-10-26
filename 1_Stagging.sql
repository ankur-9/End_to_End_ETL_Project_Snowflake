create file format json_format
Type = JSON

---------------------------------------------STORAGE INTERGATIONS---------------------------------------------------
create or replace storage integration s3_int_cricket
type = external_stage
storage_provider = s3
enabled = True
storage_aws_role_arn = '*******'
storage_allowed_locations = ('s3://***')

describe storage integration s3_int_cricket

---------------------------------------------EXTERANL STAGES---------------------------------------------------
create or replace stage s3_ext_stage_cricket
storage_integration=s3_int_cricket
file_format=json_format
url = 's3://***'

list @s3_ext_stage_cricket


---------------------------------------------SNOW PIPE---------------------------------------------------
create or replace table raw_json_data 
  (
  filepath varchar, 
  filename varchar,
  data variant
  );

create or replace pipe s3_cricket_pipe auto_ingest = True as
copy into raw_json_data
from (
select METADATA$FILENAME,REPLACE(SPLIT_PART(METADATA$FILENAME, '/', -1), '.json', '')
,$1 
from @s3_ext_stage_cricket
)
on_error = continue;
FORCE = TRUE;


---------------------------------------------STREAM---------------------------------------------------
create or replace stream str_cricket_etl on table raw_json_data
append_only = True;


--------------------------create backup table for stream
create or replace table cricket_json (filename varchar, data variant, loaded_at timestamp);
--Truncate and Load table from stream
create or replace task load_json_cricket
schedule = '1 MINUTE'
when system$stream_has_data('str_cricket_etl')
AS
begin
truncate table cricket_json;
insert into cricket_json
select filename,data,current_timestamp from str_cricket_etl;
end;


---------------------------------------------STAGGING DDLs---------------------------------------------------
create or replace TABLE STG_TEAMS (
	TEAM_TYPE VARCHAR(16777216),
	TEAM_NAME VARCHAR(16777216)
);


create or replace TABLE STG_PLAYERS (
	TEAM_NAME VARCHAR(16777216),
	PLAYER_NAME VARCHAR(16777216)
);


create or replace TABLE STG_MATCH_INFO (
	MATCH_ID VARCHAR(16777216),
	MATCH_TYPE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	SEASON VARCHAR(16777216),
	CITY VARCHAR(16777216),
	VENUE VARCHAR(16777216),
	MATCH_DATE DATE,
	TEAM1 VARCHAR(16777216),
	TEAM2 VARCHAR(16777216),
	TOSS_WINNER VARCHAR(16777216),
	TOSS_DECISION VARCHAR(16777216),
	WINNER VARCHAR(16777216),
	WIN_MARGIN VARCHAR(16777216),
	EVENT_NAME VARCHAR(16777216),
	PLAYER_OF_MATCH VARCHAR(16777216)
);


create or replace TABLE STG_MATCH_INNINGS (
	MATCH_ID VARCHAR(16777216),
	INNINGS_ID NUMBER(38,0),
	BATTING_TEAM VARCHAR(16777216),
	BOWLING_TEAM VARCHAR(16777216),
	OVER NUMBER(38,0),
	BALL_IN_OVER NUMBER(38,0),
	BATTER VARCHAR(16777216),
	BOWLER VARCHAR(16777216),
	NON_STRIKER VARCHAR(16777216),
	RUNS_BATTER NUMBER(38,0),
	EXTRAS_TYPE VARCHAR(16777216),
	RUNS_EXTRA NUMBER(38,0),
	PLAYER_OUT VARCHAR(16777216),
	DISMISSAL_KIND VARCHAR(16777216),
	IS_POWERPLAY NUMBER(1,0)
);


---------------------------------------------TASK : team stagging---------------------------------------------------

create or replace task t_stg_teams
after load_json_cricket
as
begin
truncate table stg_teams;
insert into stg_teams
select 
t.data:info.team_type::varchar as team_type,
team.key as team_name
from cricket_json t,
lateral flatten (input => t.data:info:players) team;
end;


---------------------------------------------TASK : player stagging---------------------------------------------------

create or replace task t_stg_players
after load_json_cricket
as
begin
truncate table stg_players;
insert into stg_players
select team.key as team_name,
player.value::varchar as player_name
from cricket_json t,
lateral flatten (input => t.data:info:players) team,
lateral flatten (input => team.value) player;
end;

---------------------------------------------TASK : match info stagging---------------------------------------------------


create or replace task t_stg_match_info
after load_json_cricket
as
begin
truncate table stg_match_info;
insert into stg_match_info
select t.filename as match_id,
t.data:info.match_type::varchar as match_type,
t.data:info.gender::varchar as gender,
t.data:info.season::varchar as season,
t.data:info.city::varchar as city,
t.data:info.venue::varchar as venue,
t.data:info:dates[0]::date as match_date,
t.data:info:teams[0]::varchar as team1,
t.data:info:teams[1]::varchar as team2,
t.data:info:toss:winner::varchar as toss_winner,
t.data:info:toss:decision::varchar as toss_decision,
t.data:info.outcome.winner::varchar as winner,
concat(TRY_PARSE_JSON(t.data:info.outcome.by)[OBJECT_KEYS(t.data:info.outcome.by)[0]]::varchar,' ',OBJECT_KEYS(TRY_PARSE_JSON(t.data:info.outcome.by))[0]::varchar) as win_margin,
t.data:info.event:name::varchar as event_name, 
t.data:info:player_of_match[0]::varchar as player_of_match
from cricket_json t;
end;


---------------------------------------------TASK : match innings stagging---------------------------------------------------


create or replace task t_stg_match_innings
after load_json_cricket
as
begin
truncate table stg_match_innings;
insert into stg_match_innings
select t.filename as match_id,
innings.index+1 as innings_id,
innings.value:team::varchar as batting_team,
case when innings.value:team::varchar = t.data:info:teams[0]::varchar then t.data:info:teams[1]::varchar else t.data:info:teams[0]::varchar end as bowling_team,
overs.value:over::int as over,
deliveries.index+1 as ball_in_over,
deliveries.value:batter::varchar as batter,
deliveries.value:bowler::varchar as bowler,
deliveries.value:non_striker::varchar as non_striker,
deliveries.value:runs:batter::int as runs_batter,
OBJECT_KEYS(TRY_PARSE_JSON(deliveries.value:extras))[0]::varchar as extras_type,
deliveries.value:runs:extras::int as runs_extra,
deliveries.value:wickets[0]:player_out::varchar as player_out,
deliveries.value:wickets[0]:kind::varchar as dismissal_kind,
case when (overs.value:over::int + ((deliveries.index+1) / 10.0)) 
            between innings.value:powerplays[0]:from::FLOAT and innings.value:powerplays[0]:to::FLOAT 
            then 1 else 0 
            end as is_powerplay
from cricket_json t,
lateral flatten (input => t.data:innings) innings,
lateral flatten (input => innings.value:overs) overs,
lateral flatten (input => overs.value:deliveries) deliveries;
end;
