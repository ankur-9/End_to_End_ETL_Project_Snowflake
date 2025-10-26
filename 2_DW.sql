
---------------------------------------------------------DW DDLs----------------------------------------------------
CREATE or replace TABLE dim_teams (
    team_id INT IDENTITY(1,1) PRIMARY KEY,
    team_name STRING,
    team_type STRING,
    UNIQUE(team_name)  -- ensures deduplication
);


CREATE or replace TABLE dim_players (
    player_id   INT IDENTITY(1,1) PRIMARY KEY,
    player_name STRING,
    team_id     INT,
    CONSTRAINT fk_team FOREIGN KEY (team_id) REFERENCES dim_teams(team_id),
    UNIQUE(player_name, team_id)  -- avoids duplicates
);


CREATE or replace TABLE fact_match_info (
    match_id varchar PRIMARY KEY,
	MATCH_TYPE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	SEASON VARCHAR(16777216),
	CITY VARCHAR(16777216),
	VENUE VARCHAR(16777216),
	MATCH_DATE DATE,
	TEAM1_ID INT REFERENCES dim_teams(team_id),
	TEAM2_ID INT REFERENCES dim_teams(team_id),
	TOSS_WINNER VARCHAR(16777216),
	TOSS_DECISION VARCHAR(16777216),
	WINNER_ID INT REFERENCES dim_teams(team_id),
	WIN_MARGIN VARCHAR(16777216),
	EVENT_NAME VARCHAR(16777216),
	PLAYER_OF_MATCH_ID INT REFERENCES dim_players(player_id)
);


create or replace TABLE fact_MATCH_INNINGS (
    delivery_id INT IDENTITY(1,1) PRIMARY KEY,
    innings_id INT,
    match_id VARCHAR REFERENCES fact_match_info(match_id),
	BATTING_TEAM INT REFERENCES dim_teams(team_id),
    BOWLING_TEAM INT REFERENCES dim_teams(team_id),
	OVER NUMBER(38,0),
	BALL_IN_OVER NUMBER(38,0),
	BATTER INT REFERENCES dim_players(player_id),
	BOWLER INT REFERENCES dim_players(player_id),
	NON_STRIKER INT REFERENCES dim_players(player_id),
	RUNS_BATTER NUMBER(38,0),
	EXTRAS_TYPE VARCHAR(16777216),
	RUNS_EXTRA NUMBER(38,0),
	PLAYER_OUT INT REFERENCES dim_players(player_id),
	DISMISSAL_KIND VARCHAR(16777216),
	IS_POWERPLAY NUMBER(1,0)
);




---------------------------------------------------------Task : DIM_TEAMS----------------------------------------------------
create or replace task t_dw_teams
after load_json_cricket,t_stg_teams
as
MERGE INTO dim_teams t
USING (SELECT DISTINCT team_name, team_type FROM stg_teams) s
ON t.team_name = s.team_name
WHEN NOT MATCHED THEN
  INSERT (team_name, team_type)
  VALUES (s.team_name, s.team_type);

---------------------------------------------------------Task : DIM_PLAYERS----------------------------------------------------

create or replace task t_dw_players
after t_dw_teams,t_stg_players
as
Merge into dim_players p
USING (
SELECT DISTINCT 
sp.player_name,
dt.team_id
FROM stg_players sp
JOIN dim_teams dt 
ON sp.team_name = dt.team_name
) s
on p.player_name = s.player_name
and p.team_id = s.team_id
when not matched then
    insert (player_name,team_id)
    values (s.player_name,s.team_id);


---------------------------------------------------------Task : FACT_MATCH_INFO--------------------------------------------------------------------------

create or replace task t_dw_match_info
after t_dw_teams,t_dw_players,t_stg_match_info
as
insert into fact_match_info (match_id,match_type,gender,season,city,venue,match_date,team1_id,team2_id,toss_winner,toss_decision
,winner_id,win_margin,event_name,player_of_match_id)

WITH match_enriched AS (
select s.match_id
,s.match_type,s.gender,s.season,s.city,s.venue,s.match_date,t1.team_id as team1_id,t2.team_id as team2_id
,s.toss_winner,s.toss_decision,w.team_id AS winner_team_id,s.win_margin,s.event_name
,s.player_of_match
from stg_match_info s
left join dim_teams t1 on t1.team_name = s.team1
left join dim_teams t2 on t2.team_name = s.team2
left join dim_teams w on w.team_name = s.winner
),
player_match AS (
SELECT m.*,
p.player_id,
p.team_id AS potm_team_id,
ROW_NUMBER() OVER (PARTITION BY m.match_id ORDER BY 
                CASE 
                    WHEN p.team_id = m.winner_team_id THEN 1  -- prefer winner’s team
                    WHEN p.team_id IN (m.team1_id, m.team2_id) THEN 2  -- else opponent team
                    ELSE 3
                END
        ) AS rn
FROM match_enriched m
LEFT JOIN dim_players p 
ON p.player_name = m.player_of_match
)

SELECT match_id,match_type,gender,season,city,venue,match_date,team1_id,team2_id,toss_winner,toss_decision,winner_team_id,
win_margin,event_name,player_id AS player_of_match_id
FROM player_match
QUALIFY rn = 1;


---------------------------------------------------------Task : FACT MATCH INNINGS---------------------------------------------------------------------

create or replace task t_dw_match_innings
after t_dw_teams,t_dw_players,t_stg_match_innings
as
insert into fact_match_innings (innings_id,match_id,batting_team,bowling_team,over,ball_in_over,batter,bowler,non_striker,runs_batter
,extras_type,runs_extra,player_out,dismissal_kind,is_powerplay)
select s.innings_id,s.match_id,bt.team_id,bw.team_id,s.over,s.ball_in_over,batter.player_id,bo.player_id,nonstriker.player_id,s.runs_batter
,s.extras_type,s.runs_extra,
case when s.player_out = s.batter then batter.player_id 
     when s.player_out = s.non_striker then nonstriker.player_id 
     else null 
end as player_out_id
,s.dismissal_kind,s.is_powerplay
from stg_match_innings s
join dim_teams bt on bt.team_name = s.batting_team
join dim_teams bw on bw.team_name = s.bowling_team
left join dim_players bo on bo.player_name = s.bowler and bo.team_id = bw.team_id
left join dim_players batter on batter.player_name=s.batter and batter.team_id = bt.team_id
left join dim_players nonstriker on nonstriker.player_name=s.non_striker and nonstriker.team_id = bt.team_id;


alter task t_dw_match_innings resume;
alter task t_dw_match_info resume;
alter task t_dw_players resume;
alter task t_dw_teams resume;
alter task t_stg_match_innings resume;
alter task t_stg_match_info resume;
alter task t_stg_players resume;
alter task t_stg_teams resume;
alter task load_json_cricket resume;


select * from dim_teams;
select * from dim_players;
select * from fact_match_info;
select * from fact_match_innings;
