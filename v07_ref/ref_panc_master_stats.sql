drop table if exists a21_pos_stats_t;

create table a21_pos_stats_t (
m_well_id	TEXT 
,macsr_id	TEXT
,pkid	INT not null primary key
,meas_str_id	INT
,pixval_mean	NUMeric
,well_aw_mean	NUMeric
,well_uw_mean	NUMeric
,Area	NUMeric
,well_area_all	NUMeric
,well_aw_stdev NUMeric	
,pos_aw_zscore NUMeric	
,well_uw_stdev NUMeric	
,pos_uw_zscore NUMeric
,well_aw_mean_no NUMeric
);

drop view if exists a21_pos_stats_v;

create view a21_pos_stats_v as 
	select *,
	sum(pixval_mean * Area) FILTER (where abs(pos_aw_zscore) < 3) OVER (partition by m_well_id)
	/ 
	sum(Area) FILTER (where abs(pos_aw_zscore) < 3) OVER (partition by m_well_id)
as well_aw_mean_no
		from
		(
		select m_well_id, macsr_id, pkid, meas_str_id, pixval_mean, well_aw_mean, well_uw_mean, Area, well_area_all
		--, stddev_samp(pixval_mean)
		,SQRT(
			SUM(Area * power((pixval_mean - well_aw_mean),2)
			) over w2 
			/(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    ) AS well_aw_stdev
		    
		,(pixval_mean - well_aw_mean) /
		SQRT(
			SUM(Area * power((pixval_mean - well_aw_mean),2) 
			) over w2 
			/
			(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    )
		    as pos_aw_zscore

		, sqrt(sum(sq_diff_uw) over w2 / (count(*)  over w2 - 1 ) ) as well_uw_stdev

		,(pixval_mean - well_uw_mean) / sqrt(sum(sq_diff_uw) over w2 / (count(*)  over w2 - 1 ) ) 

		    as pos_uw_zscore
			from
			(

			select m_well_id, macsr_id, pkid, meas_str_id, pixval_mean, Area
			,avg(pixval_mean) over ww as well_uw_mean
			--,stddev_samp(pixval_mean) over ww as well_uw_stdev
			,sum(Area) over ww as well_area_all
			,sum(pixval_mean * Area) over ww / sum(Area) over ww as well_aw_mean
			,avg(pixval_mean) over ww as well_uw_mean
			,
				power((pixval_mean - avg(pixval_mean) over ww),2)

			 as sq_diff_uw
			 ,
			 power((pixval_mean - SUM(pixval_mean * Area) OVER ww / SUM(Area) OVER ww),2)
			 as sq_diff_aw
			 

				from fast_supertable
				where targ_channel in ('ratio','quot')
			window ww as (partition by m_well_id)
			)
		window w2 as (partition by m_well_id)
			--group by full_well_id
		)
;


insert into a21_pos_stats_t select * from a21_pos_stats_v;


drop table if exists a23_ln_pos_stats_t;

create table a23_ln_pos_stats_t (
m_well_id	TEXT 
,macsr_id	TEXT
,pkid	INT not null primary key
,meas_str_id	INT
,ln_pixval_mean	NUMeric
,well_lnaw_mean	NUMeric
,well_lnuw_mean	NUMeric
,Area	NUMeric
,well_area_all	NUMeric
,well_lnaw_stdev NUMeric	
,pos_lnaw_zscore NUMeric	
,well_lnuw_stdev NUMeric	
,pos_lnuw_zscore NUMeric
,well_lnaw_mean_no NUMeric
);

drop view if exists a23_ln_pos_stats_v;

create view a23_ln_pos_stats_v as 
	select *,
	sum(ln_pixval_mean * Area) FILTER (where abs(pos_lnaw_zscore) < 3) OVER (partition by m_well_id)
	/ 
	sum(Area) FILTER (where abs(pos_lnaw_zscore) < 3) OVER (partition by m_well_id)
as well_lnaw_mean_no
		from
		(
		select m_well_id, macsr_id, pkid, meas_str_id, ln_pixval_mean, well_lnaw_mean, well_lnuw_mean, Area, well_area_all
		--, stddev_samp(pixval_mean)
		,SQRT(
			SUM(Area * power((ln_pixval_mean - well_lnaw_mean),2)
			) over w2 
			/(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    ) AS well_lnaw_stdev
		    
		,(ln_pixval_mean - well_lnaw_mean) /
		SQRT(
			SUM(Area * power((ln_pixval_mean - well_lnaw_mean),2) 
			) over w2 
			/
			(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    )
		    as pos_lnaw_zscore

		, sqrt(sum(sq_diff_lnuw) over w2 / (count(*)  over w2 - 1 ) ) as well_lnuw_stdev

		,(ln_pixval_mean - well_lnuw_mean) / sqrt(sum(sq_diff_lnuw) over w2 / (count(*)  over w2 - 1 ) ) 

		    as pos_lnuw_zscore
			from
			(

			select m_well_id, macsr_id, pkid, meas_str_id, ln_pixval_mean, Area
			,avg(ln_pixval_mean) over ww as well_lnuw_mean
			--,stddev_samp(pixval_mean) over ww as well_uw_stdev
			,sum(Area) over ww as well_area_all
			,sum(ln_pixval_mean * Area) over ww / sum(Area) over ww as well_lnaw_mean
			,avg(ln_pixval_mean) over ww as well_lnuw_mean
			,
				power((ln_pixval_mean - avg(ln_pixval_mean) over ww),2)

			 as sq_diff_lnuw
			 ,
			 power((ln_pixval_mean - SUM(ln_pixval_mean * Area) OVER ww / SUM(Area) OVER ww),2)
			 as sq_diff_lnaw
			 

				from fast_supertable
				where targ_channel in ('ratio','quot')
			window ww as (partition by m_well_id)
			)
		window w2 as (partition by m_well_id)
			--group by full_well_id
		)
;

insert into a23_ln_pos_stats_t select * from a23_ln_pos_stats_v;



select m_well_id
,min(macsr_id) as macsr_id
,min(meas_str_id) as meas_str_id
,count(*) as well_pos_count_all
,min(well_lnaw_mean_no) as well_lnaw_mean_no

,SQRT(
        SUM(case when abs(pos_lnaw_zscore) < 3 then
        (Area * power((ln_pixval_mean - well_lnaw_mean_no), 2))
        else null end
        ) 
        	/
        (
            (
            CAST(COUNT(case when abs(pos_lnaw_zscore) < 3 then 1 else null end) - 1 AS REAL) /
            CAST(COUNT(case when abs(pos_lnaw_zscore) < 3 then 1 else null end) AS REAL)
            ) *
            SUM(case when abs(pos_lnaw_zscore) < 3 then Area else null end) 
        )
    ) AS well_lnaw_stdev_no
   
,sum(case when abs(pos_lnaw_zscore) < 3 then Area else null end) as well_lnaw_area_no
,count(case when abs(pos_lnaw_zscore) < 3 then 1 else null end) as well_lnaw_pos_no_count
,count(case when not abs(pos_lnaw_zscore) < 3 then 1 else null end) as well_lnaw_outlier_count
,group_concat(case when not abs(pos_lnaw_zscore) < 3 then pkid else null end, '|') as lnaw_outliers
,min(well_lnaw_mean) as well_lnaw_mean_all

,min(well_lnaw_stdev) as well_lnaw_stdev_all
,sum(Area) as well_area_all
,avg(case when abs(pos_lnuw_zscore) < 3 then ln_pixval_mean else null end ) as well_lnuw_mean_no
,stddev_samp(case when abs(pos_lnuw_zscore) < 3 then ln_pixval_mean else null end) as well_lnuw_stdev_no
,sum(case when abs(pos_lnuw_zscore) < 3 then Area else null end) as well_lnuw_area_no
,group_concat(case when not abs(pos_lnuw_zscore) < 3 then pkid else null end, '|') as lnuw_outliers
,min(well_lnuw_mean) as well_lnuw_mean_all
,min(well_lnuw_stdev) as well_lnuw_stdev_all

from a23_ln_pos_stats_t

group by m_well_id
order by m_well_id
;


drop view if exists a31_well_stats_v;

create view a31_well_stats_v as 
select m_well_id
,macsr_id
,count(*) as well_pos_count_all
,well_aw_mean_no

,SQRT(
        SUM(case when abs(pos_aw_zscore) < 3 then
        (Area * power((pixval_mean - well_aw_mean_no), 2))
        else null end
        ) 
        	/
        (
            (
            CAST(COUNT(case when abs(pos_aw_zscore) < 3 then 1 else null end) - 1 AS REAL) /
            CAST(COUNT(case when abs(pos_aw_zscore) < 3 then 1 else null end) AS REAL)
            ) *
            SUM(case when abs(pos_aw_zscore) < 3 then Area else null end) 
        )
    ) AS well_aw_stdev_no
    
    
   
,sum(case when abs(pos_aw_zscore) < 3 then Area else null end) as well_aw_area_no
,count(case when abs(pos_aw_zscore) < 3 then 1 else null end) as well_aw_pos_no_count
,count(case when not abs(pos_aw_zscore) < 3 then 1 else null end) as well_aw_outlier_count

,group_concat(case when not abs(pos_aw_zscore) < 3 then pkid else null end, '|') as aw_outliers
,well_aw_mean as well_aw_mean_all

,well_aw_stdev as well_aw_stdev_all
,sum(Area) as well_area_all
,avg(case when abs(pos_uw_zscore) < 3 then pixval_mean else null end ) as well_uw_mean_no
,stddev_samp(case when abs(pos_uw_zscore) < 3 then pixval_mean else null end) as well_uw_stdev_no
,sum(case when abs(pos_uw_zscore) < 3 then Area else null end) as well_uw_area_no
,group_concat(case when not abs(pos_uw_zscore) < 3 then pkid else null end, '|') as uw_outliers
,well_uw_mean as well_uw_mean_all
,well_uw_stdev as well_uw_stdev_all


	from
	(
	select *,
	sum(pixval_mean * Area) FILTER (where abs(pos_aw_zscore) < 3) OVER (partition by m_well_id)
	/ 
	sum(Area) FILTER (where abs(pos_aw_zscore) < 3) OVER (partition by m_well_id)
as well_aw_mean_no
		from
		(
		select m_well_id, macsr_id, pkid, pixval_mean, well_aw_mean, well_uw_mean, Area, well_area_all
		--, stddev_samp(pixval_mean)
		,SQRT(
			SUM(Area * (pixval_mean - well_aw_mean) * (pixval_mean - well_aw_mean) ) over w2 /
			(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    ) AS well_aw_stdev
		    
		,(pixval_mean - well_aw_mean) /
		SQRT(
			SUM(Area * (pixval_mean - well_aw_mean) * (pixval_mean - well_aw_mean)) over w2 /
			(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    )
		    as pos_aw_zscore

		, sqrt(sum(sq_diff_uw) over w2 / (count(*)  over w2 - 1 ) ) as well_uw_stdev

		,(pixval_mean - well_uw_mean) / sqrt(sum(sq_diff_uw) over w2 / (count(*)  over w2 - 1 ) ) 

		    as pos_uw_zscore
			from
			(

			select m_well_id, macsr_id, pkid, pixval_mean, Area
			,avg(pixval_mean) over ww as well_uw_mean
			--,stddev_samp(pixval_mean) over ww as well_uw_stdev
			,sum(Area) over ww as well_area_all
			,sum(pixval_mean * Area) over ww / sum(Area) over ww as well_aw_mean
			,avg(pixval_mean) over ww as well_uw_mean
			, SUM(pixval_mean * Area) OVER ww / SUM(Area) OVER ww AS well_aw_mean
			,
				(pixval_mean - avg(pixval_mean) over ww)
				*
				(pixval_mean - avg(pixval_mean) over ww)
			 as sq_diff_uw
			 ,
			 (pixval_mean - SUM(pixval_mean * Area) OVER ww / SUM(Area) OVER ww)
			 *
			 (pixval_mean - SUM(pixval_mean * Area) OVER ww / SUM(Area) OVER ww)
			 as sq_diff_aw
			 

				from fast_supertable
				where targ_channel in ('ratio','quot')
			window ww as (partition by m_well_id)
			)
		window w2 as (partition by m_well_id)
			--group by full_well_id
		)

	)

group by m_well_id
order by m_well_id
;



drop table if exists a31_well_stats_t;

CREATE TABLE a31_well_stats_t (
    m_well_id         TEXT    PRIMARY KEY,
    macsr_id	TEXT,
    well_pos_count_all NUMERIC,
    well_aw_mean_no   NUMERIC,
    well_aw_stdev_no  NUMERIC,
    well_aw_area_no NUMERIC,
 well_aw_pos_no_count int,
well_aw_outlier_count int,
    aw_outliers text,
    well_aw_mean_all  NUMERIC,
    well_aw_stdev_all NUMERIC,
    well_area_all NUMERIC,
    well_uw_mean_no   NUMERIC,
    well_uw_stdev_no  NUMERIC,
    well_uw_area_no NUMERIC,
    uw_outliers,
    well_uw_mean_all  NUMERIC,
    well_uw_stdev_all NUMERIC
);

insert into a31_well_stats_t select * from a31_well_stats_v ;


select m_well_id
,min(macsr_id) as macsr_id
,min(meas_str_id) as meas_str_id
,count(*) as well_pos_count_all
,min(well_lnaw_mean_no) as well_lnaw_mean_no

,SQRT(
        SUM(case when abs(pos_lnaw_zscore) < 3 then
        (Area * power((ln_pixval_mean - well_lnaw_mean_no), 2))
        else null end
        ) 
        	/
        (
            (
            CAST(COUNT(case when abs(pos_lnaw_zscore) < 3 then 1 else null end) - 1 AS REAL) /
            CAST(COUNT(case when abs(pos_lnaw_zscore) < 3 then 1 else null end) AS REAL)
            ) *
            SUM(case when abs(pos_lnaw_zscore) < 3 then Area else null end) 
        )
    ) AS well_lnaw_stdev_no
   
,sum(case when abs(pos_lnaw_zscore) < 3 then Area else null end) as well_lnaw_area_no
,count(case when abs(pos_lnaw_zscore) < 3 then 1 else null end) as well_lnaw_pos_no_count
,count(case when not abs(pos_lnaw_zscore) < 3 then 1 else null end) as well_lnaw_outlier_count
,group_concat(case when not abs(pos_lnaw_zscore) < 3 then pkid else null end, '|') as lnaw_outliers
,min(well_lnaw_mean) as well_lnaw_mean_all

,min(well_lnaw_stdev) as well_lnaw_stdev_all
,sum(Area) as well_area_all
,avg(case when abs(pos_lnuw_zscore) < 3 then ln_pixval_mean else null end ) as well_lnuw_mean_no
,stddev_samp(case when abs(pos_lnuw_zscore) < 3 then ln_pixval_mean else null end) as well_lnuw_stdev_no
,sum(case when abs(pos_lnuw_zscore) < 3 then Area else null end) as well_lnuw_area_no
,group_concat(case when not abs(pos_lnuw_zscore) < 3 then pkid else null end, '|') as lnuw_outliers
,min(well_lnuw_mean) as well_lnuw_mean_all
,min(well_lnuw_stdev) as well_lnuw_stdev_all

from a23_ln_pos_stats_t

group by m_well_id
order by m_well_id
;

drop table if exists a33_ln_well_stats_t;
CREATE TABLE a33_ln_well_stats_t (
    m_well_id               TEXT    PRIMARY KEY,
    macsr_id                TEXT,
    meas_str_id             NUMERIC,
    well_pos_count_all      NUMERIC,
    well_lnaw_mean_no       NUMERIC,
    well_lnaw_stdev_no      NUMERIC,
    well_lnaw_area_no       NUMERIC,
    well_lnaw_pos_no_count  INT,
    well_lnaw_outlier_count INT,
    lnaw_outliers,
    well_lnaw_mean_all      NUMERIC,
    well_lnaw_stdev_all     NUMERIC,
    well_area_all           NUMERIC,
    well_lnuw_mean_no       NUMERIC,
    well_lnuw_stdev_no      NUMERIC,
    well_lnuw_area_no       NUMERIC,
    lnuw_outliers,
    well_lnuw_mean_all      NUMERIC,
    well_lnuw_stdev_all     NUMERIC
);





--Looks like an autoindex gets made of the pk column

--This does each well individually. Below, we will make a copy of it with different partitions, which will aggregate all positions in duplicate wells of the same assay. It's the same as well_stats if there was only one well with the given parameters.


insert into a33_ln_well_stats_t select * from a33_ln_well_stats_v ;




drop view if exists a41_acsr_from_pos_stats_v;

create view a41_acsr_from_pos_stats_v as 

select macsr_id
,count(*) as acsr_pos_count_all
,acsr_aw_mean_no

,SQRT(
        SUM(case when abs(pos_aw_zscore) < 3 then
        (Area * power((pixval_mean - acsr_aw_mean_no), 2))
        else null end
        ) 
        	/
        (
            (
            CAST(COUNT(case when abs(pos_aw_zscore) < 3 then 1 else null end) - 1 AS REAL) /
            CAST(COUNT(case when abs(pos_aw_zscore) < 3 then 1 else null end) AS REAL)
            ) *
            SUM(case when abs(pos_aw_zscore) < 3 then Area else null end) 
        )
    ) AS acsr_aw_stdev_no
   
,sum(case when abs(pos_aw_zscore) < 3 then Area else null end) as acsr_aw_area_no
,group_concat(case when not abs(pos_aw_zscore) < 3 then pkid else null end, '|') as aw_outliers
,acsr_aw_mean as acsr_aw_mean_all

,acsr_aw_stdev as acsr_aw_stdev_all
,sum(Area) as acsr_area_all
,avg(case when abs(pos_uw_zscore) < 3 then pixval_mean else null end ) as acsr_uw_mean_no
,stddev_samp(case when abs(pos_uw_zscore) < 3 then pixval_mean else null end) as acsr_uw_stdev_no
,sum(case when abs(pos_uw_zscore) < 3 then Area else null end) as acsr_uw_area_no
,group_concat(case when not abs(pos_uw_zscore) < 3 then pkid else null end, '|') as uw_outliers
,acsr_uw_mean as acsr_uw_mean_all
,acsr_uw_stdev as acsr_uw_stdev_all


	from
	(
	select *,
	sum(pixval_mean * Area) FILTER (where abs(pos_aw_zscore) < 3) OVER (partition by macsr_id)
	/ 
	sum(Area) FILTER (where abs(pos_aw_zscore) < 3) OVER (partition by macsr_id)
as acsr_aw_mean_no
		from
		(
		select macsr_id, pkid, pixval_mean, acsr_aw_mean, acsr_uw_mean, Area, acsr_area_all
		--, stddev_samp(pixval_mean)
		,SQRT(
			SUM(Area * (pixval_mean - acsr_aw_mean) * (pixval_mean - acsr_aw_mean) ) over w2 /
			(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    ) AS acsr_aw_stdev
		    
		,(pixval_mean - acsr_aw_mean) /
		SQRT(
			SUM(Area * (pixval_mean - acsr_aw_mean) * (pixval_mean - acsr_aw_mean)) over w2 /
			(
			    (
			    CAST(COUNT(*) over w2 - 1 AS REAL) /
			    CAST(COUNT(*) over w2 AS REAL)
			    ) *
			    SUM(Area) over w2
			)
		    )
		    as pos_aw_zscore

		, sqrt(sum(sq_diff_uw) over w2 / (count(*)  over w2 - 1 ) ) as acsr_uw_stdev

		,(pixval_mean - acsr_uw_mean) / sqrt(sum(sq_diff_uw) over w2 / (count(*)  over w2 - 1 ) ) 

		    as pos_uw_zscore
			from
			(

			select macsr_id, pkid, pixval_mean, Area
			,avg(pixval_mean) over ww as acsr_uw_mean
			--,stddev_samp(pixval_mean) over ww as acsr_uw_stdev
			,sum(Area) over ww as acsr_area_all
			,sum(pixval_mean * Area) over ww / sum(Area) over ww as acsr_aw_mean
			,avg(pixval_mean) over ww as acsr_uw_mean
			,
				(pixval_mean - avg(pixval_mean) over ww)
				*
				(pixval_mean - avg(pixval_mean) over ww)
			 as sq_diff_uw
			 ,
			 (pixval_mean - SUM(pixval_mean * Area) OVER ww / SUM(Area) OVER ww)
			 *
			 (pixval_mean - SUM(pixval_mean * Area) OVER ww / SUM(Area) OVER ww)
			 as sq_diff_aw
			 

				from fast_supertable
				where targ_channel in ('ratio','quot')
			window ww as (partition by macsr_id)
			)
		window w2 as (partition by macsr_id)
			--group by full_acsr_id
		)

	)

group by macsr_id
order by macsr_id

;

drop table if exists a41_acsr_from_pos_stats_t;

CREATE TABLE a41_acsr_from_pos_stats_t (
    macsr_id         TEXT    PRIMARY KEY,
    acsr_pos_count_all	NUMERIC,
    acsr_aw_mean_no   NUMERIC,
    acsr_aw_stdev_no  NUMERIC,
    acsr_aw_area_no NUMERIC,
    aw_outliers,
    acsr_aw_mean_all  NUMERIC,
    acsr_aw_stdev_all NUMERIC,
    acsr_area_all NUMERIC,
    acsr_uw_mean_no   NUMERIC,
    acsr_uw_stdev_no  NUMERIC,
    acsr_uw_area_no NUMERIC,
    uw_outliers,
    acsr_uw_mean_all  NUMERIC,
    acsr_uw_stdev_all NUMERIC
);


insert into a41_acsr_from_pos_stats_t select * from a41_acsr_from_pos_stats_v ;




drop view if exists a42_macsr_from_well_stats_v;
create view a42_macsr_from_well_stats_v as
select
       min(macsr_id) as macsr_id,
    count(*) as macsr_well_count_all,
    
    case when count(*) > 1 then count(*) 
    else min(well_aw_pos_no_count) end
    as macsr_well_or_pos_count_aw,
    avg(well_aw_mean_no) as macsr_mean_pixval_uw_w_awno,
    case when count(*) > 1 then stddev_samp(well_aw_mean_no)
    else min(well_aw_stdev_no) end
    as macsr_aw_well_or_pos_sds,
   (case when count(*) > 1 then stddev_samp(well_aw_mean_no)
    else min(well_aw_stdev_no) end)
   /
    sqrt(case when count(*) > 1 then cast(count(*) as real)
    else cast(min(well_aw_pos_no_count) as real) end)
    as macsr_aw_sde,
    power(case when count(*) > 1 then stddev_samp(well_aw_mean_no)
    else cast(min(well_aw_stdev_no) as real) end, 2)
    /
    (case when count(*) > 1 then cast(count(*) as real)
    else min(well_aw_pos_no_count) end)
    as macsr_aw_meanstest_term,
    avg(well_aw_stdev_no) as macsr_mean_of_sds_uw_w_awno,
	case when count(*) > 1 then
    stddev_samp(well_aw_mean_no) 
	else null end as macsr_sds_of_means_uw_w_awno,
	case when count(*) > 1 then
    stddev_pop(well_aw_mean_no) 
    	else null end as macsr_sdp_of_means_uw_w_awno,
    sum(well_aw_area_no) as macsr_sum_area_uw_w_awno,
    group_concat(aw_outliers, '|') as macsr_outliers_w_aw,
    avg(well_aw_mean_all) as macsr_mean_pixval_uw_w_awal,
    avg(well_aw_stdev_all) as macsr_mean_of_sds_uw_w_awal,
	case when count(*) > 1 then
    stddev_samp(well_aw_mean_all) 
    	else null end as macsr_sds_of_means_uw_w_awal,
    	case when count(*) > 1 then
    stddev_pop(well_aw_mean_all)
    	else null end  as macsr_sdp_of_means_uw_w_awal,
    sum(well_area_all) as macsr_sum_area_w_al,
    avg(well_uw_mean_no) as macsr_mean_pixval_uw_w_uwno,
    avg(well_uw_stdev_no) as macsr_mean_of_sds_uw_w_uwno,
    	case when count(*) > 1 then
    stddev_samp(well_uw_mean_no) 
	else null end as macsr_sds_of_means_uw_w_uwno,
	case when count(*) > 1 then
    stddev_pop(well_uw_mean_no) 
    	else null end as macsr_sdp_of_means_uw_w_uwno,
    sum(well_uw_area_no) as macsr_sum_area_uw_w_uwno,
    group_concat(uw_outliers, '|') as macsr_outliers_w_uw,
    avg(well_uw_mean_all) as macsr_mean_pixval_uw_w_uwal,
    avg(well_uw_stdev_all) as macsr_mean_of_sds_uw_w_uwal,
        case when count(*) > 1 then
    stddev_samp(well_uw_mean_all) 
    	else null end as macsr_sds_of_means_uw_w_uwal,
        case when count(*) > 1 then
    stddev_pop(well_uw_mean_all) 
    	else null end as macsr_sdp_of_means_uw_w_uwal

from
a31_well_stats_t
group by macsr_id
;



drop table if exists a42_macsr_from_well_stats_t;
create table a42_macsr_from_well_stats_t (
  macsr_id text not null primary key,
    macsr_well_count_all numeric,
    macsr_well_or_pos_count_aw int,
macsr_mean_pixval_uw_w_awno numeric,
macsr_aw_well_or_pos_sds numeric,
macsr_aw_sde numeric,
macsr_aw_meanstest_term numeric,
    macsr_mean_of_sds_uw_w_awno numeric,
macsr_sds_of_means_uw_w_awno numeric,
macsr_sdp_of_means_uw_w_awno numeric,
    macsr_sum_area_uw_w_awno numeric,
    macsr_outliers_w_aw text,
    macsr_mean_pixval_uw_w_awal numeric,
    macsr_mean_of_sds_uw_w_awal numeric,
    macsr_sds_of_means_uw_w_awal numeric,
    macsr_sdp_of_means_uw_w_awal numeric,
    macsr_sum_area_w_al numeric,
    macsr_mean_pixval_uw_w_uwno numeric,
    macsr_mean_of_sds_uw_w_uwno numeric,
    macsr_sds_of_means_uw_w_uwno numeric,
    macsr_sdp_of_means_uw_w_uwno numeric,
    macsr_sum_area_uw_w_uwno numeric,
    macsr_outliers_w_uw text,
    macsr_mean_pixval_uw_w_uwal numeric,
    macsr_mean_of_sds_uw_w_uwal numeric,
    macsr_sds_of_means_uw_w_uwal numeric,
    macsr_sdp_of_means_uw_w_uwal numeric
);

insert into a42_macsr_from_well_stats_t
	select * from a42_macsr_from_well_stats_v
;




drop table if exists a43_macsr_from_well_ln_stats_t;
create table a43_macsr_from_well_ln_stats_t (
    macsr_id text not null primary key,
    macsr_well_count_all numeric,
    macsr_well_or_pos_count_lnaw numeric,
    macsr_mean_pixval_uw_w_lnawno numeric,
    macsr_lnaw_well_or_pos_sds numeric,
    macsr_lnaw_sde numeric,
    macsr_lnaw_meanstest_term numeric,
    macsr_mean_of_sds_uw_w_lnawno numeric,
    macsr_sds_of_means_uw_w_lnawno numeric,
    macsr_sdp_of_means_uw_w_lnawno numeric,
    macsr_sum_area_uw_w_lnawno numeric,
    macsr_lnaw_pos_count int,
    macsr_outlier_count_w_lnaw int,
    macsr_outliers_w_lnaw text,
    macsr_mean_pixval_uw_w_lnawal numeric,
    macsr_mean_of_sds_uw_w_lnawal numeric,
    macsr_sds_of_means_uw_w_lnawal numeric,
    macsr_sdp_of_means_uw_w_lnawal numeric,
    macsr_sum_area_w_al numeric,
    macsr_mean_pixval_uw_w_lnuwno numeric,
    macsr_mean_of_sds_uw_w_lnuwno numeric,
    macsr_sds_of_means_uw_w_lnuwno numeric,
    macsr_sdp_of_means_uw_w_lnuwno numeric,
    macsr_sum_area_uw_w_lnuwno numeric,
    macsr_outliers_w_lnuw text,
    macsr_mean_pixval_uw_w_lnuwal numeric,
    macsr_mean_of_sds_uw_w_lnuwal numeric,
    macsr_sds_of_means_uw_w_lnuwal numeric,
    macsr_sdp_of_means_uw_w_lnuwal numeric
);

drop view if exists a43_macsr_from_well_ln_stats_v;
create view a43_macsr_from_well_ln_stats_v as
select
    min(macsr_id) as macsr_id,
    count(*) as macsr_well_count_all,
    case when count(*) > 1 then count(*) 
    else min(well_lnaw_pos_no_count) end
    as macsr_well_or_pos_count_lnaw,
    avg(well_lnaw_mean_no) as macsr_mean_pixval_uw_w_lnawno,
    case when count(*) > 1 then stddev_samp(well_lnaw_mean_no)
    else min(well_lnaw_stdev_no) end
    as macsr_lnaw_well_or_pos_sds,
   (case when count(*) > 1 then stddev_samp(well_lnaw_mean_no)
    else min(well_lnaw_stdev_no) end)
   /
    sqrt(case when count(*) > 1 then cast(count(*) as real)
    else cast(min(well_lnaw_pos_no_count) as real) end)
    as macsr_lnaw_sde,
    power(case when count(*) > 1 then stddev_samp(well_lnaw_mean_no)
    else cast(min(well_lnaw_stdev_no) as real) end, 2)
    /
    (case when count(*) > 1 then cast(count(*) as real)
    else min(well_lnaw_pos_no_count) end)
    as macsr_lnaw_meanstest_term,
    avg(well_lnaw_stdev_no) as macsr_mean_of_sds_uw_w_lnawno,
	case when count(*) > 1 then
    stddev_samp(well_lnaw_mean_no) 
	else null end as macsr_sds_of_means_uw_w_lnawno,
	case when count(*) > 1 then
    stddev_pop(well_lnaw_mean_no) 
    	else null end as macsr_sdp_of_means_uw_w_lnawno,
    sum(well_lnaw_area_no) as macsr_sum_area_uw_w_lnawno,
    sum(well_lnaw_pos_no_count) as macsr_lnaw_pos_count,
    sum(well_lnaw_outlier_count) as macsr_lnaw_outlier_count,
    group_concat(lnaw_outliers, '|') as macsr_outliers_w_lnaw,
    avg(well_lnaw_mean_all) as macsr_mean_pixval_uw_w_lnawal,
    avg(well_lnaw_stdev_all) as macsr_mean_of_sds_uw_w_lnawal,
	case when count(*) > 1 then
    stddev_samp(well_lnaw_mean_all) 
    	else null end as macsr_sds_of_means_uw_w_lnawal,
    	case when count(*) > 1 then
    stddev_pop(well_lnaw_mean_all)
    	else null end  as macsr_sdp_of_means_uw_w_lnawal,
    sum(well_area_all) as macsr_sum_area_w_al,
    avg(well_lnuw_mean_no) as macsr_mean_pixval_uw_w_lnuwno,
    avg(well_lnuw_stdev_no) as macsr_mean_of_sds_uw_w_lnuwno,
    	case when count(*) > 1 then
    stddev_samp(well_lnuw_mean_no) 
	else null end as macsr_sds_of_means_uw_w_lnuwno,
	case when count(*) > 1 then
    stddev_pop(well_lnuw_mean_no) 
    	else null end as macsr_sdp_of_means_uw_w_lnuwno,
    sum(well_lnuw_area_no) as macsr_sum_area_uw_w_lnuwno,
    group_concat(lnuw_outliers, '|') as macsr_outliers_w_lnuw,
    avg(well_lnuw_mean_all) as macsr_mean_pixval_uw_w_lnuwal,
    avg(well_lnuw_stdev_all) as macsr_mean_of_sds_uw_w_lnuwal,
        case when count(*) > 1 then
    stddev_samp(well_lnuw_mean_all) 
    	else null end as macsr_sds_of_means_uw_w_lnuwal,
        case when count(*) > 1 then
    stddev_pop(well_lnuw_mean_all) 
    	else null end as macsr_sdp_of_means_uw_w_lnuwal
from
a33_ln_well_stats_t
group by macsr_id
;


insert into a43_macsr_from_well_ln_stats_t
	select * from a43_macsr_from_well_ln_stats_v
;

drop view if exists a44_macsr_from_pos_ln_stats_v;
create view a44_macsr_from_pos_ln_stats_v as
select macsr_id
,avg(case when pos_lnaw_zscore < 3 then ln_pixval_mean else null end) as macsr_lnawno_pos_mean
,stddev_samp(case when pos_lnaw_zscore < 3 then ln_pixval_mean else null end) as macsr_lnawno_pos_sds
,count(case when pos_lnaw_zscore < 3 then 1 else null end) as macsr_lnawno_pos_count
,count(case when pos_lnaw_zscore < 3 then null else 1 end) as macsr_lnawno_pos_outlier_ct
,group_concat(case when pos_lnaw_zscore < 3 then null else pkid end, '|') as macsr_lnawno_pos_outliers

,avg(case when pos_lnuw_zscore < 3 then ln_pixval_mean else null end) as macsr_lnuwno_pos_mean
,stddev_samp(case when pos_lnuw_zscore < 3 then ln_pixval_mean else null end) as macsr_lnuwno_pos_sds
,count(case when pos_lnuw_zscore < 3 then 1 else null end) as macsr_lnuwno_pos_count
,count(case when pos_lnuw_zscore < 3 then null else 1 end) as macsr_lnuwno_pos_outlier_ct
,group_concat(case when pos_lnuw_zscore < 3 then null else pkid end, '|') as macsr_lnuwno_pos_outliers

from a23_ln_pos_stats_t

group by macsr_id
;


drop view if exists a49_macsr_stats_slice_v;

create view a49_macsr_stats_slice_v as
select 
a43.macsr_id
,mrpcd.meas_str_id
,a43.macsr_well_count_all
,a43.macsr_well_or_pos_count_lnaw
,a43.macsr_mean_pixval_uw_w_lnawno as macsr_lnawno_mean
,a43.macsr_lnaw_meanstest_term
,a43.macsr_lnaw_well_or_pos_sds as macsr_lnawno_well_or_pos_sds
,a43.macsr_lnaw_sde
,a43.macsr_sds_of_means_uw_w_lnawno
,a44.macsr_lnawno_pos_mean
,a44.macsr_lnawno_pos_sds
,a44.macsr_lnawno_pos_count
,a44.macsr_lnuwno_pos_mean
,a44.macsr_lnuwno_pos_sds
,a44.macsr_lnuwno_pos_count
,a42.macsr_mean_pixval_uw_w_awno as wells_awno_mean
,a42.macsr_sds_of_means_uw_w_awno as wells_awno_stdev_of_means
,a42.macsr_mean_of_sds_uw_w_awno as wells_awno_mean_of_stdevs 
,a41.acsr_aw_mean_no as pos_awno_mean
,a41.acsr_aw_stdev_no as pos_awno_stdev
,a41.acsr_pos_count_all

 from a43_macsr_from_well_ln_stats_v a43
 
join a42_macsr_from_well_stats_t a42
 on
 a43.macsr_id = a42.macsr_id
 
join
a41_acsr_from_pos_stats_t a41
on
a43.macsr_id = a41.macsr_id

 join
a44_macsr_from_pos_ln_stats_v a44
on
a43.macsr_id = a44.macsr_id

 join
(
select macsr_id, group_concat(round(well_aw_mean_no, 4), '|') as list_awno_means
from
a31_well_stats_t
group by macsr_id
) a31
on
a43.macsr_id = a31.macsr_id

 join
(
select macsr_id, group_concat(round(well_lnaw_mean_no, 4), '|') as list_lnawno_means
from
a33_ln_well_stats_t
group by macsr_id
) a33
on
a43.macsr_id = a33.macsr_id

join
macsr_ref mr
on 
a43.macsr_id = mr.macsr_id
;






drop view if exists a50_macr_pan_vs_gfp_per_cond_v;
create view a50_macr_pan_vs_gfp_per_cond_v as
select 
mrpcd.macr_id as macr_id
,pcd.macsr_id as pcd_macsr_id
,gcd.macsr_id as gcd_macsr_id
,pcd.meas_str_id
,pcd.macsr_lnawno_mean as pcd_macsr_lnawno_mean
,gcd.macsr_lnawno_mean as gcd_macsr_lnawno_mean
,pcd.macsr_lnawno_mean - gcd.macsr_lnawno_mean as macr_pcd_minus_gcd_lnawno
,sqrt(pcd.macsr_lnaw_meanstest_term + gcd.macsr_lnaw_meanstest_term) as macr_pcd_gcd_lnawno_joint_se
,(pcd.macsr_lnawno_mean - gcd.macsr_lnawno_mean) 
	/ 
	sqrt(pcd.macsr_lnaw_meanstest_term + gcd.macsr_lnaw_meanstest_term)
as pmacr_cd_minus_gcd_lnawno_zscore

,pcd.wells_awno_mean / gcd.wells_awno_mean as pcd_over_cs
,ln(pcd.wells_awno_mean/gcd.wells_awno_mean) as ln_pcd_over_cs
,sqrt(
	(case when pcd.wells_awno_stdev_of_means is not null then 
	power(pcd.wells_awno_stdev_of_means,2)/ pcd.macsr_well_count_all 
	else 
	power(pcd.wells_awno_mean_of_stdevs,2) / pcd.acsr_pos_count_all
	end
	)
	+
	(case when gcd.wells_awno_stdev_of_means is not null then 
		power(gcd.wells_awno_stdev_of_means,2)/ gcd.macsr_well_count_all 
	else 
	power(gcd.wells_awno_mean_of_stdevs,2) / gcd.acsr_pos_count_all
	end
	)
) as macr_lnawno_pcd_over_gcd_test_sd

,pcd.macsr_lnawno_pos_mean as pcd_macsr_lnawno_pos_mean
,pcd.macsr_lnawno_pos_sds as  pcd_macsr_lnawno_pos_sds
,pcd.macsr_lnawno_pos_count as pcd_macsr_lnawno_pos_count
,pcd.macsr_lnuwno_pos_mean as pcd_macsr_lnuwno_pos_mean
,pcd.macsr_lnuwno_pos_sds as pcd_macsr_lnuwno_pos_sds
,pcd.macsr_lnuwno_pos_count as pcd_macsr_lnuwno_pos_count
,gcd.macsr_lnawno_pos_mean as gcd_macsr_lnawno_pos_mean
,gcd.macsr_lnawno_pos_sds as  gcd_macsr_lnawno_pos_sds
,gcd.macsr_lnawno_pos_count as gcd_macsr_lnawno_pos_count
,gcd.macsr_lnuwno_pos_mean as gcd_macsr_lnuwno_pos_mean
,gcd.macsr_lnuwno_pos_sds as gcd_macsr_lnuwno_pos_sds
,gcd.macsr_lnuwno_pos_count as gcd_macsr_lnuwno_pos_count

,(pcd.macsr_lnawno_pos_mean - gcd.macsr_lnawno_pos_mean) as macr_pcd_minus_gcd_lnawno_pos

,sqrt(
		(
		power(pcd.macsr_lnawno_pos_sds,2)
		/
		pcd.macsr_lnawno_pos_count
		)
		+
		(
		power(gcd.macsr_lnawno_pos_sds,2)
		/
		gcd.macsr_lnawno_pos_count
		)
	)as macr_pcd_minus_gcd_lnawno_pos_joint_se

,(pcd.macsr_lnawno_pos_mean - gcd.macsr_lnawno_pos_mean) 
	/
	sqrt(
		(
		power(pcd.macsr_lnawno_pos_sds,2)
		/
		pcd.macsr_lnawno_pos_count
		)
		+
		(
		power(gcd.macsr_lnawno_pos_sds,2)
		/
		gcd.macsr_lnawno_pos_count
		)
	)
		
as macr_pcd_minus_gcd_lnawno_pos_zscore


,(pcd.macsr_lnuwno_pos_mean - pcd.macsr_lnuwno_pos_mean) 
	/
	sqrt(
		(
		power(pcd.macsr_lnuwno_pos_sds,2)
		/
		pcd.macsr_lnuwno_pos_count
		)
		+
		(
		power(gcd.macsr_lnuwno_pos_sds,2)
		/
		gcd.macsr_lnuwno_pos_count
		)
	)
as macr_pcd_minus_gcd_lnuwno_pos_zscore


,pcd.wells_awno_mean as pcd_macsr_wells_awno_mean
,pcd.wells_awno_stdev_of_means as pcd_macsr_wells_awno_stdev_of_means
,pcd.pos_awno_mean as pcd_macsr_awno_pos_mean
,pcd.pos_awno_stdev as pcd_macsr_awno_pos_stdev

,gcd.wells_awno_mean as gcd_macsr_wells_awno_mean
,gcd.wells_awno_stdev_of_means as gcd_macsr_wells_awno_stdev_of_means
,gcd.pos_awno_mean as gcd_macsr_awno_pos_mean
,gcd.pos_awno_stdev as gcd_macsr_awno_pos_stdev

from 
a49_macsr_stats_slice_v pcd

join macsr_ref mrpcd
on
pcd.macsr_id = mrpcd.macsr_id

join macsr_ref mrgcd
on
mrpcd.gfp_partner_macsr_id = mrgcd.macsr_id

join
a49_macsr_stats_slice_v gcd
on
mrgcd.macsr_id = gcd.macsr_id
;








drop view if exists a53_ln_exp_v_ctl_per_assay_v;
create view a53_ln_exp_v_ctl_per_assay_v as
select 
es.macsr_id as es_macsr_id
,cs.macsr_id as cs_macsr_id
,es.meas_str_id
,es.macsr_lnawno_mean as es_macsr_lnawno_mean
,cs.macsr_lnawno_mean as cs_macsr_lnawno_mean
,es.macsr_lnawno_mean - cs.macsr_lnawno_mean as es_minus_cs_lnaw
,sqrt(es.macsr_lnaw_meanstest_term + cs.macsr_lnaw_meanstest_term) as es_cs_lnaw_joint_se
,(es.macsr_lnawno_mean - cs.macsr_lnawno_mean) 
	/ 
	sqrt(es.macsr_lnaw_meanstest_term + cs.macsr_lnaw_meanstest_term)
as es_minus_cs_lnaw_zscore

,es.wells_awno_mean / cs.wells_awno_mean as es_over_cs
,ln(es.wells_awno_mean/cs.wells_awno_mean) as ln_es_over_cs
,sqrt(
	(case when es.wells_awno_stdev_of_means is not null then 
	power(es.wells_awno_stdev_of_means,2)/ es.macsr_well_count_all 
	else 
	power(es.wells_awno_mean_of_stdevs,2) / es.acsr_pos_count_all
	end
	)
	+
	(case when cs.wells_awno_stdev_of_means is not null then 
		power(cs.wells_awno_stdev_of_means,2)/ cs.macsr_well_count_all 
	else 
	power(cs.wells_awno_mean_of_stdevs,2) / cs.acsr_pos_count_all
	end
	)
) as es_over_cs_test_sd

,es.macsr_lnawno_pos_mean as es_macsr_lnawno_pos_mean
,es.macsr_lnawno_pos_sds as  es_macsr_lnawno_pos_sds
,es.macsr_lnawno_pos_count as es_macsr_lnawno_pos_count
,es.macsr_lnuwno_pos_mean as es_macsr_lnuwno_pos_mean
,es.macsr_lnuwno_pos_sds as es_macsr_lnuwno_pos_sds
,es.macsr_lnuwno_pos_count as es_macsr_lnuwno_pos_count
,cs.macsr_lnawno_pos_mean as cs_macsr_lnawno_pos_mean
,cs.macsr_lnawno_pos_sds as  cs_macsr_lnawno_pos_sds
,cs.macsr_lnawno_pos_count as cs_macsr_lnawno_pos_count
,cs.macsr_lnuwno_pos_mean as cs_macsr_lnuwno_pos_mean
,cs.macsr_lnuwno_pos_sds as cs_macsr_lnuwno_pos_sds
,cs.macsr_lnuwno_pos_count as cs_macsr_lnuwno_pos_count

,(es.macsr_lnawno_pos_mean - cs.macsr_lnawno_pos_mean) as macsr_es_minus_cs_lnawno_pos

,(es.macsr_lnawno_pos_mean - cs.macsr_lnawno_pos_mean) 
	/
	sqrt(
		(
		power(es.macsr_lnawno_pos_sds,2)
		/
		es.macsr_lnawno_pos_count
		)
		+
		(
		power(cs.macsr_lnawno_pos_sds,2)
		/
		cs.macsr_lnawno_pos_count
		)
	)
		
as macsr_es_minus_cs_lnawno_pos_zscore

,(es.macsr_lnuwno_pos_mean - es.macsr_lnuwno_pos_mean) as macsr_es_minus_cs_lnuwno_pos

,(es.macsr_lnuwno_pos_mean - es.macsr_lnuwno_pos_mean) 
	/
	sqrt(
		(
		power(es.macsr_lnuwno_pos_sds,2)
		/
		es.macsr_lnuwno_pos_count
		)
		+
		(
		power(cs.macsr_lnuwno_pos_sds,2)
		/
		cs.macsr_lnuwno_pos_count
		)
	)
as macsr_es_minus_cs_lnuwno_pos_zscore


,es.wells_awno_mean as es_macsr_wells_awno_mean
,es.wells_awno_stdev_of_means as es_macsr_wells_awno_stdev_of_means
,es.pos_awno_mean as es_macsr_awno_pos_mean
,es.pos_awno_stdev as es_macsr_awno_pos_stdev

,cs.wells_awno_mean as cs_macsr_wells_awno_mean
,cs.wells_awno_stdev_of_means as cs_macsr_wells_awno_stdev_of_means
,cs.pos_awno_mean as cs_macsr_awno_pos_mean
,cs.pos_awno_stdev as cs_macsr_awno_pos_stdev

from 
a49_macsr_stats_slice_v es

join macsr_ref mres
on
es.macsr_id = mres.macsr_id

join macsr_ref mrcs
on
mres.ctl_cond_macsr_id = mrcs.macsr_id

join
a49_macsr_stats_slice_v cs
on
mrcs.macsr_id = cs.macsr_id
;

drop view if exists a60_exp_pvg_vs_ctl_pvg_per_assay_v;

create view a60_exp_pvg_vs_ctl_pvg_per_assay_v as

select 
epg.macr_id as macr_id
,mrepg.mcr_id as mcr_id
,epg.meas_str_id as meas_str_id
,epg.macr_id as epg_macr_id
,cpg.macr_id as cpg_macr_id
,epg.meas_str_id
,epg.macr_pcd_minus_gcd_lnawno as epg_macr_lnawno_mean
,cpg.macr_pcd_minus_gcd_lnawno as cpg_macr_lnawno_mean
,epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno as macr_epg_minus_cpg_lnawno

,sqrt(power(epg.macr_pcd_gcd_lnawno_joint_se, 2) + power(cpg.macr_pcd_gcd_lnawno_joint_se,2) ) as macr_epg_cpg_lnawno_joint_se
,(epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno) 
	/ 
	sqrt(power(epg.macr_pcd_gcd_lnawno_joint_se, 2) + power(cpg.macr_pcd_gcd_lnawno_joint_se,2) ) 
as macr_epg_minus_cpg_lnawno_zscore



,((epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno) - AVG((epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno)) OVER (PARTITION BY epg.macr_id)) 
/ 
 SQRT(SUM(power((epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno) - AVG((epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno)) OVER (PARTITION BY group_id), 2)) OVER (PARTITION BY epg.macr_id) / cast((COUNT((epg.macr_pcd_minus_gcd_lnawno - cpg.macr_pcd_minus_gcd_lnawno)) OVER (PARTITION BY epg.macr_id) - 1 as real)))
 as macr_epg_minus_cpg_lnawno_zscore

--,epg.wells_awno_mean / cpg.wells_awno_mean as epg_over_cs
--,ln(epg.wells_awno_mean/cpg.wells_awno_mean) as ln_epg_over_cs
--,sqrt(
--	(case when epg.wells_awno_stdev_of_means is not null then 
--	power(epg.wells_awno_stdev_of_means,2)/ epg.macsr_well_count_all 
--	else 
--	power(epg.wells_awno_mean_of_stdevs,2) / epg.acsr_pos_count_all
--	end
--	)
--	+
--	(case when cpg.wells_awno_stdev_of_means is not null then 
--		power(cpg.wells_awno_stdev_of_means,2)/ cpg.macsr_well_count_all 
--	else 
--	power(cpg.wells_awno_mean_of_stdevs,2) / cpg.acsr_pos_count_all
--	end
--	)
--) as epg_over_cpg_test_sd

,epg.macr_pcd_minus_gcd_lnawno_pos as epg_macr_pcd_minus_gcd_lnawno_pos
,epg.macr_pcd_minus_gcd_lnawno_pos_joint_se as  epg_macr_pcd_minus_gcd_lnawno_pos_joint_se
,epg.pcd_macsr_lnawno_pos_count as epg_pcd_macsr_lnawno_pos_count
,epg.gcd_macsr_lnawno_pos_count as epg_gcd_macsr_lnawno_pos_count
--,epg.macsr_lnuwno_pos_mean as epg_macsr_lnuwno_pos_mean
--,epg.macsr_lnuwno_pos_sds as epg_macsr_lnuwno_pos_sds
--,epg.macsr_lnuwno_pos_count as epg_macsr_lnuwno_pos_count
,cpg.macr_pcd_minus_gcd_lnawno_pos as cpg_macr_pcd_minus_gcd_lnawno_pos
,cpg.macr_pcd_minus_gcd_lnawno_pos_joint_se as  cpg_macr_pcd_minus_gcd_lnawno_pos_joint_se
,cpg.pcd_macsr_lnawno_pos_count as cpg_pcd_macsr_lnawno_pos_count
,cpg.gcd_macsr_lnawno_pos_count as cpg_gcd_macsr_lnawno_pos_count
--,cpg.macsr_lnuwno_pos_mean as cpg_macsr_lnuwno_pos_mean
--,cpg.macsr_lnuwno_pos_sds as cpg_macsr_lnuwno_pos_sds
--,cpg.macsr_lnuwno_pos_count as cpg_macsr_lnuwno_pos_count

,(epg.macr_pcd_minus_gcd_lnawno_pos - cpg.macr_pcd_minus_gcd_lnawno_pos) as macr_epg_minus_cpg_lnawno_pos

,	sqrt(
		(
		power(epg.macr_pcd_minus_gcd_lnawno_pos_joint_se,2)
--		/
--		(epg.pcd_macsr_lnawno_pos_count + epg.gcd_macsr_lnawno_pos_count)
--		)
		+
--		(
		power(cpg.macr_pcd_minus_gcd_lnawno_pos_joint_se,2)
--		/
--		cpg.pcd_macsr_lnawno_pos_count + cpg.gcd_macsr_lnawno_pos_count
		)
	)
	as macr_epg_minus_cpg_lnawno_pos_joint_sde
	
,(epg.macr_pcd_minus_gcd_lnawno_pos - cpg.macr_pcd_minus_gcd_lnawno_pos) 
	/
	sqrt(
		(
		power(epg.macr_pcd_minus_gcd_lnawno_pos_joint_se,2)
--		/
--		(epg.pcd_macsr_lnawno_pos_count + epg.gcd_macsr_lnawno_pos_count)
--		)
		+
--		(
		power(cpg.macr_pcd_minus_gcd_lnawno_pos_joint_se,2)
--		/
--		cpg.pcd_macsr_lnawno_pos_count + cpg.gcd_macsr_lnawno_pos_count
		)
	)
as macr_epg_minus_cpg_lnawno_pos_zscore



--,epg.wells_awno_mean as epg_macsr_wells_awno_mean
--,epg.wells_awno_stdev_of_means as epg_macsr_wells_awno_stdev_of_means
--,epg.pos_awno_mean as epg_macsr_awno_pos_mean
--,epg.pos_awno_stdev as epg_macsr_awno_pos_stdev

--,cpg.wells_awno_mean as cpg_macsr_wells_awno_mean
--,cpg.wells_awno_stdev_of_means as cpg_macsr_wells_awno_stdev_of_means
--,cpg.pos_awno_mean as cpg_macsr_awno_pos_mean
--,cpg.pos_awno_stdev as cpg_macsr_awno_pos_stdev

from 

a50_macr_pan_vs_gfp_per_cond_v epg
--a49_macsr_stats_slice_v es

 join macsr_ref mrepg
on
epg.pcd_macsr_id = mrepg.macsr_id

join macsr_ref mrcpg
on
mrepg.ctl_cond_macsr_id = mrcpg.macsr_id

join
a50_macr_pan_vs_gfp_per_cond_v cpg
on
mrcpg.macsr_id = cpg.pcd_macsr_id
;











drop view if exists a65_exp_v_ctrl_stats_same_sensor_v;
create view a65_exp_v_ctrl_stats_same_sensor_v as



--drop view if exists a60_exp_v_ctrl_stats_same_sensor_v;
--create view a60_exp_v_ctrl_stats_same_sensor_v as
select


es_mcsr_id as expr_mcsr_id
,mrc.meas_str_id
,cc.expect_ctrl as ctrl_cond
,cc.canon_cond as exp_cond
,region
,sensor
,mcsr_x_count
,mcsr_x_df
,mcsr_x_lnawno_mean_of_paired_es_cs_diffs
,mcsr_x_lnawno_t_of_paired_es_cs_diffs

,'2t: '||lnawno_pp.p_twotail 
--|| ' 1t: '||lnawno_pp.p_onetail 
as mcsr_x_lnawno_mean_of_paired_es_cs_diffs_pval

,mcsr_x_lnawno_geo_mean_of_paired_es_cs_diffs
,mcsr_x_lnawno_paired_es_cs_diffs
,spacer_1
,mcsr_x_lnawno_mean_of_paired_es_cs_diff_zscores
,mcsr_x_lnawno_t_of_paired_es_cs_diff_zscores
,'2t: '||lnawno_pzp.p_twotail 
--|| ' 1t: '||lnawno_pzp.p_onetail 
as mcsr_x_lnawno_mean_of_paired_es_cs_diff_zscores_pval

,mcsr_x_lnawno_paired_es_cs_diff_zscores
,spacer_2
,mcsr_x_lnawno_unpaired_es_mean_minus_cs_mean
,mcsr_x_lnawno_t_of_unpaired_es_mean_minus_cs_mean
,'2t: '||lnawno_upp.p_twotail 
--|| ' 1t: '||lnawno_upp.p_onetail 
as mcsr_x_lnawno_unpaired_es_mean_minus_cs_mean_pval
,mcsr_x_lnawno_geo_diff_es_mean_minus_cs_mean
,mcsr_es_lnawno_mean_of_macsrs
,mcsr_es_lnawno_geo_mean_of_macsrs
,mcsr_cs_lnawno_mean_of_macsrs
,mcsr_cs_lnawno_geo_mean_of_macsrs
,spacer_3
,mcsr_x_lnawno_pos_mean_of_paired_es_cs_diffs
,mcsr_x_lnawno_pos_t_of_paired_es_cs_diffs
,'2t: '||lnawno_pos_pp.p_twotail 
--|| ' 1t: '||lnawno_pos_pp.p_onetail 
as mcsr_x_lnawno_pos_mean_of_paired_es_cs_diffs_pval

,mcsr_x_lnawno_pos_geo_mean_of_paired_es_cs_diffs
,mcsr_x_lnawno_pos_paired_es_cs_diffs
,spacer_4
,mcsr_x_lnawno_pos_mean_of_paired_es_cs_diff_zscores
,mcsr_x_lnawno_pos_t_of_paired_es_cs_diff_zscores
,'2t: '||lnawno_pos_pzp.p_twotail 
--|| ' 1t: '||lnawno_pos_pzp.p_onetail 
as mcsr_x_lnawno_pos_mean_of_paired_es_cs_diff_zscores_pval
,mcsr_x_lnawno_pos_paired_es_cs_diff_zscores
,spacer_5
,mcsr_x_lnawno_pos_unpaired_es_mean_minus_cs_mean
,mcsr_x_lnawno_pos_t_of_unpaired_es_mean_minus_cs_mean
,'2t: '||lnawno_pos_upp.p_twotail 
--|| ' 1t: '||lnawno_pos_upp.p_onetail 
as mcsr_x_lnawno_unpaired_es_mean_minus_cs_mean_pval
,mcsr_x_lnawno_pos_geo_diff_es_mean_minus_cs_mean
,mcsr_es_lnawno_pos_mean_of_macsrs
,mcsr_es_lnawno_pos_geo_mean_of_macsrs
,mcsr_cs_lnawno_pos_mean_of_macsrs
,mcsr_cs_lnawno_pos_geo_mean_of_macsrs

from
(
select 
mres.mcsr_id as es_mcsr_id
,count(*) as mcsr_x_count
,count(*) -1.0 as mcsr_x_df
,null as spacer_0
,avg(es_minus_cs_lnaw) as mcsr_x_lnawno_mean_of_paired_es_cs_diffs
,avg(es_minus_cs_lnaw) 
    / 
    ( 
        stddev_samp(es_minus_cs_lnaw) 
        / 
        sqrt( count(*) 
        )
    ) as mcsr_x_lnawno_t_of_paired_es_cs_diffs
--lnawno_pp.p_val as mcsr_x_lnawno_mean_of_paired_es_cs_diffs_pval
,exp(avg(es_minus_cs_lnaw)) as mcsr_x_lnawno_geo_mean_of_paired_es_cs_diffs
,group_concat(es_minus_cs_lnaw, '|') as mcsr_x_lnawno_paired_es_cs_diffs

,null as spacer_1
,avg(es_minus_cs_lnaw_zscore)  as mcsr_x_lnawno_mean_of_paired_es_cs_diff_zscores
,avg(es_minus_cs_lnaw_zscore) / (stddev_samp(es_minus_cs_lnaw_zscore) / sqrt(count(*))) as mcsr_x_lnawno_t_of_paired_es_cs_diff_zscores
--lnawno_pzp.p_val as mcsr_x_lnawno_mean_of_paired_es_cs_diff_zscores_pval
,group_concat(es_minus_cs_lnaw_zscore, '|') as mcsr_x_lnawno_paired_es_cs_diff_zscores

,null as spacer_2
,avg(es_macsr_lnawno_mean) - avg(cs_macsr_lnawno_mean) as mcsr_x_lnawno_unpaired_es_mean_minus_cs_mean

,   avg(es_macsr_lnawno_mean) - avg(cs_macsr_lnawno_mean)
	/
		sqrt(
		(power(stddev_samp(es_macsr_lnawno_mean),2) 
		/
		count(*)
		)
		+
		--TO DO: replace this term with the cs_macsrs from all experiments, not just the ones with this experimental condition
		(power(stddev_samp(cs_macsr_lnawno_mean),2) 
		/
		count(*)
		)
        )
        as mcsr_x_lnawno_t_of_unpaired_es_mean_minus_cs_mean
--,lnawno_upp. as mcsr_x_lnawno_unpaired_es_mean_minus_cs_mean_pval
,exp(avg(es_macsr_lnawno_mean) - avg(cs_macsr_lnawno_mean)) as mcsr_x_lnawno_geo_diff_es_mean_minus_cs_mean

,avg(es_macsr_lnawno_mean) as mcsr_es_lnawno_mean_of_macsrs
,exp(avg(es_macsr_lnawno_mean)) as mcsr_es_lnawno_geo_mean_of_macsrs
,avg(cs_macsr_lnawno_mean) as mcsr_cs_lnawno_mean_of_macsrs
,exp(avg(cs_macsr_lnawno_mean)) as mcsr_cs_lnawno_geo_mean_of_macsrs


--__________________

,null as spacer_3
,avg(macsr_es_minus_cs_lnawno_pos) as mcsr_x_lnawno_pos_mean_of_paired_es_cs_diffs
,avg(macsr_es_minus_cs_lnawno_pos) 
    / 
    ( 
        stddev_samp(macsr_es_minus_cs_lnawno_pos) 
        / 
        sqrt( count(*) 
        )
    ) as mcsr_x_lnawno_pos_t_of_paired_es_cs_diffs
--lnawno_pos_pp.p_val as mcsr_x_lnawno_pos_mean_of_paired_es_cs_diffs_pval
,exp(avg(macsr_es_minus_cs_lnawno_pos)) as mcsr_x_lnawno_pos_geo_mean_of_paired_es_cs_diffs
,group_concat(es_minus_cs_lnaw, '|') as mcsr_x_lnawno_pos_paired_es_cs_diffs

,null as spacer_4
,avg(macsr_es_minus_cs_lnawno_pos_zscore)  as mcsr_x_lnawno_pos_mean_of_paired_es_cs_diff_zscores
,avg(macsr_es_minus_cs_lnawno_pos_zscore) / (stddev_samp(macsr_es_minus_cs_lnawno_pos_zscore) / sqrt(count(*))) as mcsr_x_lnawno_pos_t_of_paired_es_cs_diff_zscores
--lnawno_pos_pzp.p_val as mcsr_x_lnawno_pos_mean_of_paired_es_cs_diff_zscores_pval
,group_concat(macsr_es_minus_cs_lnawno_pos_zscore, '|') as mcsr_x_lnawno_pos_paired_es_cs_diff_zscores

,null as spacer_5
,avg(es_macsr_lnawno_pos_mean) - avg(cs_macsr_lnawno_pos_mean) as mcsr_x_lnawno_pos_unpaired_es_mean_minus_cs_mean

,   avg(es_macsr_lnawno_pos_mean) - avg(cs_macsr_lnawno_pos_mean)
	/
		sqrt(
		(power(stddev_samp(es_macsr_lnawno_pos_mean),2) 
		/
		count(*)
		)
		+
		--TO DO: replace this term with the cs_macsrs from all experiments, not just the ones with this experimental condition
		(power(stddev_samp(cs_macsr_lnawno_pos_mean),2) 
		/
		count(*)
		)
        )
        as mcsr_x_lnawno_pos_t_of_unpaired_es_mean_minus_cs_mean
--,lnawno_pos_upp. as mcsr_x_lnawno_unpaired_es_mean_minus_cs_mean_pval
,exp(avg(es_macsr_lnawno_pos_mean) - avg(cs_macsr_lnawno_pos_mean)) as mcsr_x_lnawno_pos_geo_diff_es_mean_minus_cs_mean

,avg(es_macsr_lnawno_pos_mean) as mcsr_es_lnawno_pos_mean_of_macsrs
,exp(avg(es_macsr_lnawno_pos_mean)) as mcsr_es_lnawno_pos_geo_mean_of_macsrs
,avg(cs_macsr_lnawno_pos_mean) as mcsr_cs_lnawno_pos_mean_of_macsrs
,exp(avg(cs_macsr_lnawno_pos_mean)) as mcsr_cs_lnawno_pos_geo_mean_of_macsrs






--_______________


--mcsr_x_awno_mean_of_paired es_cs_diffs
--awno_pp. as mcsr_x_awno_mean_of_paired_es_cs_diffs_pval
--mcsr_x_awno_geo_mean_of_paired_es_cs_diffs
--mcsr_x_awno_paired_es_cs_diffs
--
--mcsr_x_awno_mean_of_paired_es_cs_diff_zscores
--awno_pzp. as mcsr_x_awno_mean_of_paired_es_cs_diff_zscores_pval
--mcsr_x_awno_paired_es_cs_diff_zscores
--
--avg(es_macsr_wells_awno_mean) - avg(cs_macsr_wells_awno_mean) as mcsr_x_awno_unpaired_es_mean_minus_cs_mean
--awno_upp. as mcsr_x_awno_unpaired_es_mean_minus_cs_mean_pval
--mcsr_x_awno_geo_diff_es_mean_minus_cs_mean
--mcsr_es_awno_mean_of_macsrs
--mcsr_cs_awno_mean_of_macsrs
--
--mcsr_x_uwno_mean_of_paired es_cs_diffs
--uwno_pp. as mcsr_x_uwno_mean_of_paired_es_cs_diffs_pval
--mcsr_x_uwno_geo_mean_of_paired_es_cs_diffs
--mcsr_x_uwno_paired_es_cs_diffs
--
--mcsr_x_uwno_mean_of_paired_es_cs_diff_zscores
--uwno_pzp. as mcsr_x_uwno_mean_of_paired_es_cs_diff_zscores_pval
--mcsr_x_uwno_paired_es_cs_diff_zscores
--
--mcsr_x_uwno_unpaired_es_mean_minus_cs_mean
--uwno_upp. as mcsr_x_uwno_unpaired_es_mean_minus_cs_mean_pval
--mcsr_x_uwno_geo_diff_es_mean_minus_cs_mean
--mcsr_es_uwno_mean_of_macsrs
--mcsr_cs_uwno_mean_of_macsrs
--

    

from

a53_ln_exp_v_ctl_per_assay_v a53

join macsr_ref mres
on
mres.macsr_id = a53.es_macsr_id


group by es_mcsr_id


) sub

join (select distinct mcsr_id, meas_str_id, cond_id, sensor, region from macsr_ref) mrc
on
mrc.mcsr_id = sub.es_mcsr_id

join canon_conds cc
on
mrc.cond_id = cc.cond_id

join meas_str_shortcuts mss
on
mss.meas_str_id = mrc.meas_str_id

left join
t_table lnawno_pp
on
sub.mcsr_x_df = lnawno_pp.df
and
abs(sub.mcsr_x_lnawno_t_of_paired_es_cs_diffs) > lnawno_pp.lower_t
and 
abs(sub.mcsr_x_lnawno_t_of_paired_es_cs_diffs) < lnawno_pp.upper_t

left join
t_table lnawno_pzp
on
sub.mcsr_x_df = lnawno_pzp.df
and
abs(sub.mcsr_x_lnawno_t_of_paired_es_cs_diff_zscores) > lnawno_pzp.lower_t
and 
abs(sub.mcsr_x_lnawno_t_of_paired_es_cs_diff_zscores) < lnawno_pzp.upper_t

left join
t_table lnawno_upp
on
sub.mcsr_x_df = lnawno_upp.df
and
abs(sub.mcsr_x_lnawno_t_of_unpaired_es_mean_minus_cs_mean) > lnawno_upp.lower_t
and 
abs(sub.mcsr_x_lnawno_t_of_unpaired_es_mean_minus_cs_mean) < lnawno_upp.upper_t


left join
t_table lnawno_pos_pp
on
sub.mcsr_x_df = lnawno_pos_pp.df
and
abs(sub.mcsr_x_lnawno_pos_t_of_paired_es_cs_diffs) > lnawno_pos_pp.lower_t
and 
abs(sub.mcsr_x_lnawno_pos_t_of_paired_es_cs_diffs) < lnawno_pos_pp.upper_t

left join
t_table lnawno_pos_pzp
on
sub.mcsr_x_df = lnawno_pos_pzp.df
and
abs(sub.mcsr_x_lnawno_pos_t_of_paired_es_cs_diff_zscores) > lnawno_pos_pzp.lower_t
and 
abs(sub.mcsr_x_lnawno_pos_t_of_paired_es_cs_diff_zscores) < lnawno_pos_pzp.upper_t

left join
t_table lnawno_pos_upp
on
sub.mcsr_x_df = lnawno_pos_upp.df
and
abs(sub.mcsr_x_lnawno_pos_t_of_unpaired_es_mean_minus_cs_mean) > lnawno_pos_upp.lower_t
and 
abs(sub.mcsr_x_lnawno_pos_t_of_unpaired_es_mean_minus_cs_mean) < lnawno_pos_upp.upper_t




order by 
mrc.meas_str_id
,ctrl_cond
,exp_cond
,region
,sensor


--cc.expect_ctrl
--mrc.meas_str_id
--,cc.canon_cond as exp_cond
--,cc.expect_ctrl as ctrl_cond
--,mrc.sensor
--,mrc.region
--rder by sensor desc, region, canon_cond, expect_ctrl, meas_str_id
;

--select * from a60something_v;





drop view if exists a71_combine_pvg_exps_all_assays_v;
create view a71_combine_pvg_exps_all_assays_v as

select

sub.mcr_id
--,epg_mcsr_id as expr_mcsr_id
,mrc.meas_str_id
,cc.expect_ctrl as ctrl_cond
,cc.canon_cond as exp_cond
,region
,mcr_x_count
,mcr_x_df
--,macr_epg_cpg_lnawno_joint_se_agg

,spacer_000
,mcr_x_lnawno_weighted_mean_of_paired_epg_cpg_diffs
,mcr_x_lnawno_t_of_varweighted_mean_of_paired_epg_cpg_diffs
,'2t: '||lnawno_wpp.p_twotail 
--|| ' 1t: '||lnawno_wpp.p_onetail 
as mcr_x_lnawno_varweighted_mean_of_paired_epg_cpg_diffs_pval
,exp(mcr_x_lnawno_weighted_mean_of_paired_epg_cpg_diffs) as mcr_x_lnawno_geo_varweighted_mean_of_paired_epg_cpg_diffs
 ,spacer_00
,mcr_x_lnawno_weighted_sum_of_paired_epg_cpg_diff_zscores
 ,'2t: '||lnawno_wszp.p_twotail 
--|| ' 1t: '||lnawno_wpp.p_onetail 
 as mcr_x_lnawno_weighted_sum_of_paired_epg_cpg_diff_zscores_df30_pval
,spacer_0
,mcr_x_lnawno_mean_of_paired_epg_cpg_diffs
,mcr_x_lnawno_t_of_paired_epg_cpg_diffs
,'2t: '||lnawno_pp.p_twotail 
--|| ' 1t: '||lnawno_pp.p_onetail 
as mcr_x_lnawno_mean_of_paired_epg_cpg_diffs_pval
,mcr_x_lnawno_geo_mean_of_paired_epg_cpg_diffs
,mcr_x_lnawno_paired_epg_cpg_diffs
,spacer_1
,mcr_x_lnawno_mean_of_paired_epg_cpg_diff_zscores
,mcr_x_lnawno_t_of_paired_epg_cpg_diff_zscores
,'2t: '||lnawno_pzp.p_twotail 
--|| ' 1t: '||lnawno_pzp.p_onetail 
as mcr_x_lnawno_mean_of_paired_epg_cpg_diff_zscores_pval
,mcr_x_lnawno_paired_epg_cpg_diff_zscores
,spacer_2
,mcr_x_lnawno_unpaired_epg_mean_minus_cpg_mean
,mcr_x_lnawno_t_of_unpaired_epg_mean_minus_cpg_mean
,'2t: '||lnawno_upp.p_twotail 
--|| ' 1t: '||lnawno_upp.p_onetail 
as mcr_x_lnawno_unpaired_epg_mean_minus_cpg_mean_pval
,mcr_x_lnawno_geo_diff_epg_mean_minus_cpg_mean
,mcsr_epg_lnawno_mean_of_macrs
,mcsr_epg_lnawno_geo_mean_of_macrs
,mcsr_cpg_lnawno_mean_of_macrs
,mcsr_cpg_lnawno_geo_mean_of_macrs
,spacer_3
,mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diffs
,mcr_x_lnawno_pos_t_of_paired_epg_cpg_diffs
,'2t: '||lnawno_pos_pp.p_twotail 
--|| ' 1t: '||lnawno_pos_pp.p_onetail 
as mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diffs_pval

,mcr_x_lnawno_pos_geo_mean_of_paired_epg_cpg_diffs
,mcr_x_lnawno_pos_paired_epg_cpg_diffs
,spacer_4
,mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diff_zscores
,mcr_x_lnawno_pos_t_of_paired_epg_cpg_diff_zscores
,'2t: '||lnawno_pos_pzp.p_twotail 
--|| ' 1t: '||lnawno_pos_pzp.p_onetail 
as mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diff_zscores_pval
,mcr_x_lnawno_pos_paired_epg_cpg_diff_zscores
,spacer_5
,mcr_x_lnawno_pos_unpaired_epg_mean_minus_cpg_mean
,mcr_x_lnawno_pos_t_of_unpaired_epg_mean_minus_cpg_mean
,'2t: '||lnawno_pos_upp.p_twotail 
--|| ' 1t: '||lnawno_pos_upp.p_onetail 
as mcr_x_lnawno_unpaired_epg_mean_minus_cpg_mean_pval
,mcr_x_lnawno_pos_geo_diff_epg_mean_minus_cpg_mean
,mcsr_epg_lnawno_pos_mean_of_macrs
,mcsr_epg_lnawno_pos_geo_mean_of_macrs
,mcsr_cpg_lnawno_pos_mean_of_macrs
,mcsr_cpg_lnawno_pos_geo_mean_of_macrs

from
(
select 
a60.mcr_id
--,mres.mcsr_id as epg_mcsr_id
,count(*) as mcr_x_count
,count(*) -1.0 as mcr_x_df
--,group_concat(macr_epg_cpg_lnawno_joint_se, '|') as macr_epg_cpg_lnawno_joint_se_agg


,null as spacer_000

,sum(macr_epg_minus_cpg_lnawno / power(macr_epg_cpg_lnawno_joint_se, 2))
/
sum(1 / power(macr_epg_cpg_lnawno_joint_se, 2))
as mcr_x_lnawno_weighted_mean_of_paired_epg_cpg_diffs

,	(
	sum(macr_epg_minus_cpg_lnawno / power(macr_epg_cpg_lnawno_joint_se, 2))
	/
	sum(1 / power(macr_epg_cpg_lnawno_joint_se, 2))
	)
/
	sqrt(
		1
		/
		sum( 
			1 
			/ 
			power(macr_epg_cpg_lnawno_joint_se,2)
		)
	)

as mcr_x_lnawno_t_of_varweighted_mean_of_paired_epg_cpg_diffs
, null as mcr_x_lnawno_varweighted_mean_of_paired_epg_cpg_diffs_pval

,null as spacer_00
,sum(macr_epg_minus_cpg_lnawno_zscore * power(macr_epg_cpg_lnawno_joint_se,-1))
/
sqrt(sum(power(macr_epg_cpg_lnawno_joint_se,-2)))
as mcr_x_lnawno_weighted_sum_of_paired_epg_cpg_diff_zscores
,null as mcr_x_lnawno_weighted_sum_of_paired_epg_cpg_diff_zscores_df30_pval


,null as spacer_0
,avg(macr_epg_minus_cpg_lnawno) as mcr_x_lnawno_mean_of_paired_epg_cpg_diffs
,avg(macr_epg_minus_cpg_lnawno) 
    / 
    ( 
        stddev_samp(macr_epg_minus_cpg_lnawno) 
        / 
        sqrt( count(*) 
        )
    ) as mcr_x_lnawno_t_of_paired_epg_cpg_diffs
    
,null 
--lnawno_pp.p_val 
as mcr_x_lnawno_mean_of_paired_epg_cpg_diffs_pval
,exp(avg(macr_epg_minus_cpg_lnawno)) as mcr_x_lnawno_geo_mean_of_paired_epg_cpg_diffs
,group_concat(round(macr_epg_minus_cpg_lnawno,3) || ' (assay ' || mrsub.assay_id || ' [' || mrsub.assay_long || '])' ,' | ') as mcr_x_lnawno_paired_epg_cpg_diffs

,null as spacer_1
,avg(macr_epg_minus_cpg_lnawno_zscore)  as mcr_x_lnawno_mean_of_paired_epg_cpg_diff_zscores
,avg(macr_epg_minus_cpg_lnawno_zscore) / (stddev_samp(macr_epg_minus_cpg_lnawno_zscore) / sqrt(count(*))) as mcr_x_lnawno_t_of_paired_epg_cpg_diff_zscores
,null
--lnawno_pzp.p_val 
as mcr_x_lnawno_mean_of_paired_epg_cpg_diff_zscores_pval
,group_concat(round(macr_epg_minus_cpg_lnawno_zscore,3) || ' (assay ' || mrsub.assay_id || ' [' || mrsub.assay_long || '])' ,' | ') as mcr_x_lnawno_paired_epg_cpg_diff_zscores

,null as spacer_2
,avg(epg_macr_lnawno_mean) - avg(cpg_macr_lnawno_mean) as mcr_x_lnawno_unpaired_epg_mean_minus_cpg_mean

,   avg(epg_macr_lnawno_mean) - avg(cpg_macr_lnawno_mean)
	/
		sqrt(
		(power(stddev_samp(epg_macr_lnawno_mean),2) 
		/
		count(*)
		)
		+
		--TO DO: replace this term with the cpg_macrs from all experiments, not just the ones with this experimental condition
		(power(stddev_samp(cpg_macr_lnawno_mean),2) 
		/
		count(*)
		)
        )
        as mcr_x_lnawno_t_of_unpaired_epg_mean_minus_cpg_mean
,null
--,lnawno_upp. 
as mcr_x_lnawno_unpaired_epg_mean_minus_cpg_mean_pval
,exp(avg(epg_macr_lnawno_mean) - avg(cpg_macr_lnawno_mean)) as mcr_x_lnawno_geo_diff_epg_mean_minus_cpg_mean

,avg(epg_macr_lnawno_mean) as mcsr_epg_lnawno_mean_of_macrs
,exp(avg(epg_macr_lnawno_mean)) as mcsr_epg_lnawno_geo_mean_of_macrs
,avg(cpg_macr_lnawno_mean) as mcsr_cpg_lnawno_mean_of_macrs
,exp(avg(cpg_macr_lnawno_mean)) as mcsr_cpg_lnawno_geo_mean_of_macrs


--__________________

,null as spacer_3
,avg(macr_epg_minus_cpg_lnawno_pos) as mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diffs
,avg(macr_epg_minus_cpg_lnawno_pos) 
    / 
    ( 
        stddev_samp(macr_epg_minus_cpg_lnawno_pos) 
        / 
        sqrt( count(*) 
        )
    ) as mcr_x_lnawno_pos_t_of_paired_epg_cpg_diffs
,null
--lnawno_pos_pp.p_val 
as mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diffs_pval
,exp(avg(macr_epg_minus_cpg_lnawno_pos)) as mcr_x_lnawno_pos_geo_mean_of_paired_epg_cpg_diffs
,group_concat(round(macr_epg_minus_cpg_lnawno,3)  || ' (assay ' || mrsub.assay_id || ' [' || mrsub.assay_long || '])' ,' | ') as mcr_x_lnawno_pos_paired_epg_cpg_diffs

,null as spacer_4
,avg(macr_epg_minus_cpg_lnawno_pos_zscore)  as mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diff_zscores
,avg(macr_epg_minus_cpg_lnawno_pos_zscore) / (stddev_samp(macr_epg_minus_cpg_lnawno_pos_zscore) / sqrt(count(*))) as mcr_x_lnawno_pos_t_of_paired_epg_cpg_diff_zscores
,null
--lnawno_pos_pzp.p_val 
as mcr_x_lnawno_pos_mean_of_paired_epg_cpg_diff_zscores_pval
,group_concat(round(macr_epg_minus_cpg_lnawno_pos_zscore,3) || ' (assay ' || mrsub.assay_id || ' [' || mrsub.assay_long || '])' ,' | ') as mcr_x_lnawno_pos_paired_epg_cpg_diff_zscores

,null as spacer_5
,avg(epg_macr_pcd_minus_gcd_lnawno_pos) - avg(cpg_macr_pcd_minus_gcd_lnawno_pos) as mcr_x_lnawno_pos_unpaired_epg_mean_minus_cpg_mean

,   avg(epg_macr_pcd_minus_gcd_lnawno_pos) - avg(cpg_macr_pcd_minus_gcd_lnawno_pos)
	/
		sqrt(
		(power(stddev_samp(epg_macr_pcd_minus_gcd_lnawno_pos),2) 
		/
		count(*)
		)
		+
		--TO DO: replace this term with the cpg_macrs from all experiments, not just the ones with this experimental condition
		(power(stddev_samp(cpg_macr_pcd_minus_gcd_lnawno_pos),2) 
		/
		count(*)
		)
        )
        as mcr_x_lnawno_pos_t_of_unpaired_epg_mean_minus_cpg_mean
,null
--,lnawno_pos_upp. 
as mcr_x_lnawno_unpaired_epg_mean_minus_cpg_mean_pval
,exp(avg(epg_macr_pcd_minus_gcd_lnawno_pos) - avg(cpg_macr_pcd_minus_gcd_lnawno_pos)) as mcr_x_lnawno_pos_geo_diff_epg_mean_minus_cpg_mean

,avg(epg_macr_pcd_minus_gcd_lnawno_pos) as mcsr_epg_lnawno_pos_mean_of_macrs
,exp(avg(epg_macr_pcd_minus_gcd_lnawno_pos)) as mcsr_epg_lnawno_pos_geo_mean_of_macrs
,avg(cpg_macr_pcd_minus_gcd_lnawno_pos) as mcsr_cpg_lnawno_pos_mean_of_macrs
,exp(avg(cpg_macr_pcd_minus_gcd_lnawno_pos)) as mcsr_cpg_lnawno_pos_geo_mean_of_macrs






--_______________


--mcr_x_awno_mean_of_paired epg_cpg_diffs
--awno_pp. as mcr_x_awno_mean_of_paired_epg_cpg_diffs_pval
--mcr_x_awno_geo_mean_of_paired_epg_cpg_diffs
--mcr_x_awno_paired_epg_cpg_diffs
--
--mcr_x_awno_mean_of_paired_epg_cpg_diff_zscores
--awno_pzp. as mcr_x_awno_mean_of_paired_epg_cpg_diff_zscores_pval
--mcr_x_awno_paired_epg_cpg_diff_zscores
--
--avg(epg_macr_wells_awno_mean) - avg(cpg_macr_wells_awno_mean) as mcr_x_awno_unpaired_epg_mean_minus_cpg_mean
--awno_upp. as mcr_x_awno_unpaired_epg_mean_minus_cpg_mean_pval
--mcr_x_awno_geo_diff_epg_mean_minus_cpg_mean
--mcsr_epg_awno_mean_of_macrs
--mcsr_cpg_awno_mean_of_macrs
--
--mcr_x_uwno_mean_of_paired epg_cpg_diffs
--uwno_pp. as mcr_x_uwno_mean_of_paired_epg_cpg_diffs_pval
--mcr_x_uwno_geo_mean_of_paired_epg_cpg_diffs
--mcr_x_uwno_paired_epg_cpg_diffs
--
--mcr_x_uwno_mean_of_paired_epg_cpg_diff_zscores
--uwno_pzp. as mcr_x_uwno_mean_of_paired_epg_cpg_diff_zscores_pval
--mcr_x_uwno_paired_epg_cpg_diff_zscores
--
--mcr_x_uwno_unpaired_epg_mean_minus_cpg_mean
--uwno_upp. as mcr_x_uwno_unpaired_epg_mean_minus_cpg_mean_pval
--mcr_x_uwno_geo_diff_epg_mean_minus_cpg_mean
--mcsr_epg_uwno_mean_of_macrs
--mcsr_cpg_uwno_mean_of_macrs
--

    

from
a60_exp_pvg_vs_ctl_pvg_per_assay_v a60

left join (select distinct macr_id,  mcr_id, meas_str_id, assay_id, assay_long, region from macsr_ref) mrsub
on
mrsub.macr_id = a60.epg_macr_id


group by a60.mcr_id

) sub



join (select distinct meas_str_id, mcr_id, cond_id, region from macsr_ref) mrc
on
mrc.mcr_id = sub.mcr_id

join canon_conds cc
on
mrc.cond_id = cc.cond_id

join meas_str_shortcuts mss
on
mss.meas_str_id = mrc.meas_str_id

left join
t_table lnawno_wpp
on
sub.mcr_x_df = lnawno_wpp.df
and
abs(sub.mcr_x_lnawno_t_of_varweighted_mean_of_paired_epg_cpg_diffs) > lnawno_wpp.lower_t
and 
abs(sub.mcr_x_lnawno_t_of_varweighted_mean_of_paired_epg_cpg_diffs) < lnawno_wpp.upper_t


left join
t_table lnawno_wszp
on
--sub.mcr_x_df = lnawno_wszp.df
30 = lnawno_wszp.df
and
abs(sub.mcr_x_lnawno_weighted_sum_of_paired_epg_cpg_diff_zscores) > lnawno_wszp.lower_t
and 
abs(sub.mcr_x_lnawno_weighted_sum_of_paired_epg_cpg_diff_zscores) < lnawno_wszp.upper_t


left join
t_table lnawno_pp
on
sub.mcr_x_df = lnawno_pp.df
and
abs(sub.mcr_x_lnawno_t_of_paired_epg_cpg_diffs) > lnawno_pp.lower_t
and 
abs(sub.mcr_x_lnawno_t_of_paired_epg_cpg_diffs) < lnawno_pp.upper_t

left join
t_table lnawno_pzp
on
sub.mcr_x_df = lnawno_pzp.df
and
abs(sub.mcr_x_lnawno_t_of_paired_epg_cpg_diff_zscores) > lnawno_pzp.lower_t
and 
abs(sub.mcr_x_lnawno_t_of_paired_epg_cpg_diff_zscores) < lnawno_pzp.upper_t

left join
t_table lnawno_upp
on
sub.mcr_x_df = lnawno_upp.df
and
abs(sub.mcr_x_lnawno_t_of_unpaired_epg_mean_minus_cpg_mean) > lnawno_upp.lower_t
and 
abs(sub.mcr_x_lnawno_t_of_unpaired_epg_mean_minus_cpg_mean) < lnawno_upp.upper_t


left join
t_table lnawno_pos_pp
on
sub.mcr_x_df = lnawno_pos_pp.df
and
abs(sub.mcr_x_lnawno_pos_t_of_paired_epg_cpg_diffs) > lnawno_pos_pp.lower_t
and 
abs(sub.mcr_x_lnawno_pos_t_of_paired_epg_cpg_diffs) < lnawno_pos_pp.upper_t

left join
t_table lnawno_pos_pzp
on
sub.mcr_x_df = lnawno_pos_pzp.df
and
abs(sub.mcr_x_lnawno_pos_t_of_paired_epg_cpg_diff_zscores) > lnawno_pos_pzp.lower_t
and 
abs(sub.mcr_x_lnawno_pos_t_of_paired_epg_cpg_diff_zscores) < lnawno_pos_pzp.upper_t

left join
t_table lnawno_pos_upp
on
sub.mcr_x_df = lnawno_pos_upp.df
and
abs(sub.mcr_x_lnawno_pos_t_of_unpaired_epg_mean_minus_cpg_mean) > lnawno_pos_upp.lower_t
and 
abs(sub.mcr_x_lnawno_pos_t_of_unpaired_epg_mean_minus_cpg_mean) < lnawno_pos_upp.upper_t




order by 
mrc.meas_str_id
,ctrl_cond
,exp_cond
,region













______________________________________________________








drop table if exists a65_exp_v_ctrl_stats_same_sensor_t;
create table a65_exp_v_ctrl_stats_same_sensor_t as select * from a65_exp_v_ctrl_stats_same_sensor_t;


---___________________________________________



--Looks like an autoindex gets made of the pk column

--This does each well individually. Below, we will make a copy of it with different partitions, which will aggregate all positions in duplicate wells of the same assay. It's the same as well_stats if there was only one well with the given parameters.


-- This aggregate all positions in duplicate wells of the same assay.


drop view if exists a51_exp_vs_ctl_same_sens_awno;
--_____ 50 working
create view a51_exp_vs_ctl_same_sens_awno
as

select 
es_minus_cs / es_minus_cs_test_sd as es_minus_cs_zscore
,ln_es_over_cs / es_over_cs_test_sd as es_over_cs_zscore
,*
from
(
select 
es.macsr_id as es_macsr_id
,cs.macsr_id as cs_macsr_id
,es.meas_str_id

,es.wells_awno_mean - cs.wells_awno_mean as es_minus_cs
,sqrt(
	(
	power(case when es.wells_awno_stdev_of_means is not null then es.wells_awno_stdev_of_means else es.wells_awno_mean_of_stdevs end,2)
	/
	
	)
	+
	power(case when cs.wells_awno_stdev_of_means is not null then cs.wells_awno_stdev_of_means else cs.wells_awno_mean_of_stdevs end,2)
)as es_minus_cs_test_sd

,es.wells_awno_mean / cs.wells_awno_mean as es_over_cs
,ln(es.wells_awno_mean/cs.wells_awno_mean) as ln_es_over_cs
,sqrt(
	(case when es.wells_awno_stdev_of_means is not null then 
	power(es.wells_awno_stdev_of_means,2)/ es.acsr_well_count_all 
	else 
	power(es.wells_awno_mean_of_stdevs,2) / es.acsr_pos_count_all
	end
	)
	+
	(case when cs.wells_awno_stdev_of_means is not null then 
		power(cs.wells_awno_stdev_of_means,2)/ cs.acsr_well_count_all 
	else 
	power(cs.wells_awno_mean_of_stdevs,2) / cs.acsr_pos_count_all
	end
	)
) as es_over_cs_test_sd

from 
a49_acsr_stats_slice_v es

join macsr_ref mres
on
es.macsr_id = mres.macsr_id

join macsr_ref mrcs
on
mres.ctl_cond_macsr_id = mrcs.macsr_id

join
a49_acsr_stats_slice_v cs
on
mrcs.macsr_id = cs.macsr_id

;


drop view if exists a52_exp_cond_vs_ctl_cond_awno;
--_____ 50 working
create view a52_exp_cond_vs_ctl_cond_awno
as

select 
pcd_minus_gcd / pcd_minus_gcd_test_sd as pcd_minus_gcd_zscore
,ln_pcd_over_gcd / pcd_over_gcd_test_sd as pcd_over_gcd_zscore
,*
from
(
select 
pcd.macsr_id as pcd_macsr_id
,gcd.macsr_id as gcd_macsr_id
,pcd.meas_str_id

,pcd.wells_awno_mean - gcd.wells_awno_mean as pcd_minus_gcd
,sqrt(
	power(case when pcd.wells_awno_stdev_of_means is not null then pcd.wells_awno_stdev_of_means else pcd.wells_awno_mean_of_stdevs end,2) 
	+
	power(case when gcd.wells_awno_stdev_of_means is not null then gcd.wells_awno_stdev_of_means else gcd.wells_awno_mean_of_stdevs end,2)
)as pcd_minus_gcd_test_sd

,pcd.wells_awno_mean / gcd.wells_awno_mean as pcd_over_gcd
,ln(pcd.wells_awno_mean/gcd.wells_awno_mean) as ln_pcd_over_gcd
,sqrt(
	(case when pcd.wells_awno_stdev_of_means is not null then 
	power(pcd.wells_awno_stdev_of_means,2)/ pcd.acsr_well_count_all 
	else 
	power(pcd.wells_awno_mean_of_stdevs,2) / pcd.acsr_pos_count_all
	end
	)
	+
	(case when gcd.wells_awno_stdev_of_means is not null then 
		power(gcd.wells_awno_stdev_of_means,2)/ gcd.acsr_well_count_all 
	else 
	power(gcd.wells_awno_mean_of_stdevs,2) / gcd.acsr_pos_count_all
	end
	)
) as pcd_over_gcd_test_sd

from 
a49_acsr_stats_slice_v pcd

join macsr_ref mrpcd
on
pcd.macsr_id = mrpcd.macsr_id

join macsr_ref mrgcd
on
mrpcd.gfp_partner_macsr_id = mrgcd.macsr_id

join
a49_acsr_stats_slice_v gcd
on
mrgcd.macsr_id = gcd.macsr_id

)
;


drop view if exists a61_pan_ex_ct_vs_gfp_ex_ct_awno;

create view a61_pan_ex_ct_vs_gfp_ex_ct_awno as
select 
pan_evc.es_macsr_id as pan_ex_mascr_id
,pan_evc.cs_macsr_id as pan_ctl_macsr_id
,gfp_evc.es_macsr_id as gfp_ex_macsr_id
,gfp_evc.cs_macsr_id as gfp_ctl_macsr_id
,mrpan.cond_id as ex_cond_id
,mrpan.meas_str_id as meas_str_id

,pan_evc.es_minus_cs - gfp_evc.es_minus_cs as pan_diff_minus_gfp_diff

,pan_evc.es_minus_cs_zscore - gfp_evc.es_minus_cs_zscore as pan_diff_z_minus_gfp_diff_z

,pan_evc.es_over_cs - gfp_evc.es_over_cs as pan_rat_minus_gfp_rat

,pan_evc.ln_es_over_cs - gfp_evc.ln_es_over_cs as pan_ln_rat_minus_gfp_ln_rat

,pan_evc.es_over_cs_zscore - gfp_evc.es_over_cs_zscore as pan_rat_z_minus_gfp_rat_z

,pan_evc.es_minus_cs / gfp_evc.es_minus_cs as pan_diff_over_gfp_diff

,ln(pan_evc.es_minus_cs / gfp_evc.es_minus_cs) as ln_pan_diff_over_gfp_diff

,pan_evc.es_minus_cs_zscore / gfp_evc.es_minus_cs_zscore as pan_diff_z_over_gfp_diff_z

,ln(pan_evc.es_minus_cs_zscore / gfp_evc.es_minus_cs_zscore) as ln_pan_diff_z_over_gfp_diff_z

,pan_evc.es_over_cs / gfp_evc.es_over_cs as pan_rat_over_gfp_rat

,pan_evc.ln_es_over_cs / gfp_evc.ln_es_over_cs as pan_ln_rat_over_gfp_ln_rat

,ln(pan_evc.ln_es_over_cs / gfp_evc.ln_es_over_cs) as ln_pan_ln_rat_over_gfp_ln_rat

,pan_evc.es_over_cs_zscore / gfp_evc.es_over_cs_zscore as pan_rat_z_over_gfp_rat_z

,ln(pan_evc.es_over_cs_zscore / gfp_evc.es_over_cs_zscore) as ln_pan_rat_z_over_gfp_rat_z

from 

a51_exp_vs_ctl_same_sens_awno pan_evc

join
macsr_ref mrpan
on 
pan_evc.es_macsr_id = mrpan.macsr_id

join
a51_exp_vs_ctl_same_sens_awno gfp_evc
on
mrpan.gfp_partner_macsr_id = gfp_evc.es_macsr_id
;


drop view if exists a71_combine_exps_awno;
create view a71_combine_exps_awno
as 

select
a61.pan_ex_mascr_id 
,meas_str_id
,cc.canon_cond
,cc.expect_ctrl
,avg(pan_diff_z_minus_gfp_diff_z) as mean_pan_vs_gfp_zscore_diff
,stddev_samp(pan_diff_z_minus_gfp_diff_z) as sds_pan_vs_gfp_zscore_diff
,avg(pan_diff_z_minus_gfp_diff_z) / stddev_samp(pan_diff_z_minus_gfp_diff_z) as sds_z_of_diff_zs
,stddev_samp(pan_diff_z_minus_gfp_diff_z) /sqrt(count(*)) as paired_mean_sde_of_diff_zs
,avg(pan_diff_z_minus_gfp_diff_z) / ( stddev_samp(pan_diff_z_minus_gfp_diff_z) /sqrt(count(*)) ) as pse_z_of_diff_zs

,count(*) as exp_count
from

a61_pan_ex_ct_vs_gfp_ex_ct_awno a61

join
canon_conds cc
on
a61.ex_cond_id = cc.cond_id

group by a61.ex_cond_id, meas_str_id



Changing gears: I want to represent some of these results in a kind of point/bar graph hybrid that resembles something Prism does, but is open-source and python-friendly. (Would love to not have to use R as an intermediary and just pull data frames from my sqlite db, but I can ultimately do whatever. Basically, my x axis is all categorical. I have a number of different experimental conditions, and within those conditions a number of individual runs of the experiment. I want segments of the X axis to represent conditions, and within those I want individual points for the runs to be evenly spaced. For each condition, there are two points that have actual numeric values defining their Y position. So points look like Exp_cond_id, run_id, measure_a_or_b, result. Then, within each exp_cond, I have statistics for the mean and stdev of the a points and the mean and stdev of the b points, and I want the graph to superimpose some representation of the mean and the 95% confidence limits.



______________________not sure about this

select a49.macsr_id as exp_macsr, fsct.macsr_id as ct_macsr
from

a49_acsr_stats_slice_v a49

join

(select distinct meas_str_id, macsr_id, sensor, region
from
fast_supertable) fs
on
fs.macsr_id = a49.macsr_id

join

(select distinct meas_str_id, macsr_id, sensor, region 
from fast_supertable) fsct
on
fs.meas_str_id = fsct.meas_str_id
and
fs.assay_id = fsct.assay_id
and
fs.ctrl_cond_id = fsct.cond_id
and
fs.region = fsct.region









___________just a thing


select a49pan.* from a49_acsr_stats_slice_v a49pan

join
(select *, row_number() OVER (partition by macsr_id order by pkid) as rn

 from fast_supertable) fspan
on
fspan.rn = 1
and
a49pan.macsr_id = fspan.macsr_id

join
a49_acsr_stats_slice_v a49gfp
on



--OLD 71! REPLACED ABOVE IN THE MAIN TREE BY AN IN-APP MODIFICATION DDE
