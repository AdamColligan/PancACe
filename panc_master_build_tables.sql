--incomplete code here?

update table canon_conds
set expect_ctrl = null
where expect_ctrl = 'self';

update files_sheets_match
set kd_state = 'visible'
where kd_sheet = '6-21-23'
;
update files_sheets_match
set kd_state = 'hidden'
where kd_sheet = '7-26-23'
;




update kd_alldata_labels
set condition = 'exclude'
where kd_sheet = '6-21-23'
and well in ('F2','E2','C3','D3','C3','D3','E4','F4','C5','D5','C5','D5','C7','D7','C7','D7','C9','D9','C9','D9')
;

update kd_alldata_leabels
set condition = 'exclude'
where upper(region) = 'MITO'
and 
refeed_glc_3h mito
refeed_ac_3h mito



--The 3-17 split inserts are set up here and then actually dealt with in the 'join' clause of the fast_supertable creation query.
insert into files_sheets_match
select 
null as assay_id
, '3-17-a-placeholder' as image_set
,'3-17-23-a' as kd_sheet
,'visible' as kd_state
;

insert into files_sheets_match
select 
null as assay_id
, '3-17-b-placeholder' as image_set
,'3-17-23-b' as kd_sheet
,'visible' as kd_state
;

update files_sheets_match
set
image_set = 'split-Mar_17_see_a_and_b'
kd_sheet = null,
kd_state = null
where kd_sheet = '3-17-23'
;




update kd_alldata_labels 
set sensor = case 
	when upper(sensor) = 'PAN' then 'Pan'
	when upper(sensor) = 'GFP' then 'GFP' 
	else sensor end,
region = case
	when upper(region) = 'MITO' THEN 'mito'
	when upper(region) = 'CYTO' then 'cyto'
	when upper(region) = 'NUC' THEN 'nuc'
	else region end
;


--One of the assays was divided into two independent experiments / platings.
update kd_alldata_labels
set exp_sheet = 
	case when exp_sheet ='3-17-23'
	and well in ('B2','C2','D2','B3','C3','D3','B4','C4','D4','B5','C5','D5','B6','C6','D6','B7','C7','D7','B8','C8','D8','B9','C9','D9','E2','F2','G2','E3','F3','G3','E4','F4','G4','E5','F5','G5')
	then '3-17-23-a'
		else case when exp_sheet ='3-17-23'
		and well in ('E6','F6','G6','E7','F7','G7','E8','F8','G8','E9','F9','G9')
		then '3-17-23-b'
			else 
			exp_sheet
		end
	end
;

update kd_alldata_labels
set condition = 'dFBS/no glc'
where
	exp_sheet = '4-20-23'
	and
	well in ('E3','F3','G3', 'E5','F5','G5')
;


--Use this after importing new data/labels that might have as-yet-unseen condition formats.
--After the following, manually enter the appropriate canonical conditions
insert into cond_to_canon 
select distinct condition as kd_cond, null as canon_cond from kd_alldata_labels kdal 
where not
kdal.condition in (select kd_cond from cond_to_canon)
;


update kd_outs
set assay_id = (select assay_id from files_sheets_match where kd_sheet = assay_date)
;



CREATE TABLE meas_str_shortcuts (
    meas_str_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    meas_str                   TEXT,
    roisrc_str                 TEXT,
    targ_str                   TEXT,
    targ_channel               TEXT,
    targ_prep                  TEXT,
    targ_blur                  TEXT,
    targ_roi_type              TEXT,
    roisrc_channel             TEXT,
    roisrc_prep                TEXT,
    roisrc_blur                TEXT,
    roisrc_tmethod             TEXT,
    roisrc_particle            TEXT,
    meas_str_no_blurs          TEXT,
    meas_str_no_tmethod        TEXT,
    meas_str_no_targ_channel   TEXT,
    meas_str_no_roisrc_channel TEXT,
    meas_str_no_preps          TEXT
);

CREATE INDEX msid_tchan ON meas_str_shortcuts (
    meas_str_id,
    targ_channel,
    meas_str_no_targ_channel
);


insert into meas_str_shortcuts 

select distinct meas_str, roisrc_str, targ_str, targ_channel, targ_prep, targ_blur, targ_roi_type, roisrc_channel,roisrc_prep, roisrc_blur, roisrc_tmethod, roisrc_particle, meas_str_no_blurs, meas_str_no_tmethod, meas_str_no_targ_channel, meas_str_no_roisrc_channel, meas_str_no_preps

from batch_supertable
order by meas_str

;
--make quotient meas_strs


insert into meas_str_shortcuts

select 
null as pkid
,replace(meas_str, 'TARG_ch01','TARG_quot') as meas_str
,roisrc_str
,replace(targ_str, 'ch01','quot') as targ_str
,'quot' as targ_channel
,targ_prep
,targ_blur
,targ_roi_type
,roisrc_channel
,roisrc_prep
,roisrc_blur
,roisrc_tmethod
,roisrc_particle
,replace(meas_str_no_blurs, 'TARG_ch01','TARG_quot') as meas_str_no_blurs
,replace(meas_str_no_tmethod, 'TARG_ch01','TARG_quot') as
meas_str_no_tmethod
,meas_str_no_targ_channel
,replace(meas_str_no_roisrc_channel, 'TARG_ch01','TARG_quot') as
meas_str_no_roisrc_channel
,replace(meas_str_no_preps, 'TARG_ch01','TARG_quot') as
meas_str_no_preps
from meas_str_shortcuts mss_src
where targ_channel = 'ch01'
)
;


drop table if exists fast_supertable;

CREATE TABLE fast_supertable (
    pkid               INTEGER PRIMARY KEY AUTOINCREMENT,
    pixval_mean        NUMERIC,
    ln_pixval_mean	numeric,
    meas_str_id        INT,
	macsr_id		TEXT,
	mcsr_id text,
	macr_id	text,
	masr_id	text,
	mar_id		text,
	mcr_id text,
	acsr_id	TEXT,
    targ_channel	TEXT,
    gfp_partner_macsr_id	 TEXT,
    gfp_partner_masr_id text,
    ctl_cond_macsr_id	text,
    ctl_cond_macr_id	text,
    ctl_cond_id       INT,
    pos_id_no_m		TEXT,
    m_well_id		TEXT,
    cond_id            INT,
    cond_group         TEXT,
    sensor             TEXT,
    region             TEXT,
    assay_id           INT,
    assay_state        TEXT,
    well               TEXT,
    position           INT,
    Area               NUMERIC,
    StdDev             NUMERIC,
    Min                NUMERIC,
    Max                NUMERIC,
    area_fraction      NUMERIC,
    sample_flag        TEXT,
    process_level      TEXT,
    meas_count         INT
);

CREATE INDEX meas_pos ON fast_supertable (
    meas_str_id,
    pos_id_no_m
);

CREATE INDEX meas_macsr_well ON fast_supertable (
    meas_str_id,
    macsr_id,
    m_well_id
);


--IMPORTANT!!!
--For importing data with no conditions known, in theory this should work up to the a30s with all left joins apart from the initial fsm join for assay_id. I've done this as "fsv_left" for the first four image sets of 2024. This is because a20s and a30s just need m_well_id...right?

--EVEN MORE IMPORTANT!!! 
-- Getting null constraint violations with the use of fsv that will also be relevant to this. There are fst entries with pkids that are not used in the batch_supertable (I assume they are in the bst with different pkids but I can't actually be sure. So I'm doing NULL inserts with my fsv_left to just get something on the plate.
drop view if exists fast_supertable_v;


create view fast_supertable_v as

select pkid
, pixval_mean
, ln(pixval_mean)
, mss.meas_str_id
, 'm' || meas_str_id || 'a' || fsm.assay_id || 'c' || ccs.cond_id || 's' || kdal.sensor || 'r' || kdal.region  as macsr_id
, 'm' || meas_str_id || 'c' || ccs.cond_id || 's' || kdal.sensor || 'r' || kdal.region  as mcsr_id
, 'm' || meas_str_id || 'a' || fsm.assay_id || 'c' || ccs.cond_id || 'r' || kdal.region  as macr_id 
, 'm' || meas_str_id || 'a' || fsm.assay_id || 's' || kdal.sensor || 'r' || kdal.region  as masr_id
, 'm' || meas_str_id || 'a' || fsm.assay_id || 'r' || kdal.region as mar_id
, 'm' || meas_str_id || 'c' || ccs.cond_id ||  'r' || kdal.region  as mcr_id
,'a' || fsm.assay_id || 'c' || ccs.cond_id || 's' || kdal.sensor || 'r' || kdal.region  as acsr_id
, mss.targ_channel
, case when sensor = 'GFP' then null else 'm' || meas_str_id || 'a' || fsm.assay_id || 'c' || ccs.cond_id || 's' || 'GFP' || 'r' || kdal.region end as gfp_partner_macsr_id
, case when sensor = 'GFP' then null else 'm' || meas_str_id || 'a' || fsm.assay_id ||'s' || 'GFP' || 'r' || kdal.region end as gfp_partner_masr_id
, 'm' || meas_str_id || 'a' || fsm.assay_id || 'c' || ctrl_conds.cond_id || 's' || kdal.sensor || 'r' || kdal.region as ctl_cond_macsr_id
 , 'm' || meas_str_id || 'a' || fsm.assay_id || 'c' || ctrl_conds.cond_id || 'r' || kdal.region as ctl_cond_macr_id
 , ctrl_conds.cond_id as ctl_cond_id
, 'a' || fsm.assay_id || 'w' || bs.well || 'p' || bs.position as pos_id_no_m
, 'm' || meas_str_id || 'a' || fsm.assay_id || 'w' || bs.well as m_well_id
, ccs.cond_id
, ccs.cond_group
, kdal.sensor
, kdal.region
, fsm.assay_id
, fsm.kd_state as assay_state
, bs.well
, position
,  Area
, StdDEv
, Min
, Max
, area_fraction
, sample_flag
, process_level
, meas_count


from 

batch_supertable bs

join files_sheets_match fsm
on
bs.filename_before_well = fsm.image_set
or
fsm.kd_sheet = 
	case when 
		bs.filename_before_well = '2023-03-17-mito-glucdepriv-refeed'
		and
		bs.well in ('B2','C2','D2','B3','C3','D3','B4','C4','D4','B5','C5','D5','B6','C6','D6','B7','C7','D7','B8','C8','D8','B9','C9','D9','E2','F2','G2','E3','F3','G3','E4','F4','G4','E5','F5','G5')
		then
		'3-17-23-a'
		else case when
			bs.filename_before_well = '2023-03-17-mito-glucdepriv-refeed'
			and
			bs.well in ('E6','F6','G6','E7','F7','G7','E8','F8','G8','E9','F9','G9')
			then 
			'3-17-23-b'
		end
	end
	

join kd_alldata_labels kdal
on 
fsm.kd_sheet = kdal.exp_sheet
and
bs.well = kdal.well
and not 
assay_state = 'hidden'


join cond_to_canon ctc
on
kdal.condition = ctc.kd_cond

join canon_conds ccs
on
ctc.canon_cond = ccs.canon_cond
and not ctc.canon_cond = 'exclude'

join meas_str_shortcuts mss
on 
bs.meas_str = mss.meas_str

left join canon_conds ctrl_conds
on
ccs.expect_ctrl = ctrl_conds.canon_cond

;


insert into fast_supertable select * from fast_supertable_v
;

--______________________

--MAKE QUOTIENT MEASUREMENTS OUT OF CH00 AND CH01

insert into fast_supertable

select null as pkid
	,fs1.pixval_mean / fs2.pixval_mean as pixval_mean
	,ln(fs1.pixval_mean / fs2.pixval_mean) as ln_pixval_mean
	,mssquot.meas_str_id as meas_str_id
	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.macsr_id, LENGTH('m' || fs1.meas_str_id) + 1) as macsr_id
  	 
  	 ,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.mcsr_id, LENGTH('m' || fs1.meas_str_id) + 1)  as mcsr_id
  	
  	
  	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.macr_id, LENGTH('m' || fs1.meas_str_id) + 1)  as macr_id
  	
  	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.masr_id, LENGTH('m' || fs1.meas_str_id) + 1)  as masr_id
	
	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.mar_id, LENGTH('m' || fs1.meas_str_id) + 1) as mar_id
  	 
  	 ,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.mcr_id, LENGTH('m' || fs1.meas_str_id) + 1) as mcr_id
  	
  	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.acsr_id, LENGTH('m' || fs1.meas_str_id) + 1) as acsr_id
	
	,'quot' as targ_channel
	
	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.gfp_partner_macsr_id, LENGTH('m' || fs1.meas_str_id) + 1) as gfp_partner_macsr_id
  	
  	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.gfp_partner_masr_id, LENGTH('m' || fs1.meas_str_id) + 1) as gfp_partner_masr_id
  	
  	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.ctl_cond_macsr_id, LENGTH('m' || fs1.meas_str_id) + 1) as ctl_cond_macsr_id
  	
  	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.ctl_cond_macr_id, LENGTH('m' || fs1.meas_str_id) + 1) as ctl_cond_macr_id
  	 ,fs1.ctl_cond_id
	,fs1.pos_id_no_m
	,'m' || mssquot.meas_str_id || 
  	 SUBSTR(fs1.m_well_id, LENGTH('m' || fs1.meas_str_id) + 1) 				 
  	 as m_well_id
	,fs1.cond_id
	,fs1.cond_group
	,fs1.sensor
	,fs1.region
	,fs1.assay_id
	,fs1.assay_state
	,fs1.well
	,fs1.position
	,fs1.Area
	,null as "StdDev"
	,null as "Min"
	,null as "Max"
	,fs1.area_fraction
	,null as sample_flag
	,null as process_level
	,null as meas_count
	
from fast_supertable fs1 

join meas_str_shortcuts mss01 on
mss01.targ_channel = 'ch01' and
mss01.meas_str_id = fs1.meas_str_id

join
meas_str_shortcuts mss00 on
mss00.targ_channel = 'ch00'
and
mss00.meas_str_no_targ_channel = mss01.meas_str_no_targ_channel

join
meas_str_shortcuts mssquot on
mssquot.targ_channel = 'quot'
and
mssquot.meas_str_no_targ_channel = mss01.meas_str_no_targ_channel

join

fast_supertable fs2
on
mss00.meas_str_id = fs2.meas_str_id
and
fs2.pos_id_no_m = fs1.pos_id_no_m

;

drop table if exists macsr_ref;

create table macsr_ref (
macsr_id not null primary key
,meas_str_id
,macr_id
,mcsr_id
,masr_id
,mar_id
,mcr_id
,acsr_id
,targ_channel
,gfp_partner_macsr_id
,gfp_partner_masr_id
,cond_id
,cond_long
,cond_group
,ctl_cond_macsr_id
,ctl_cond_macr_id
,ctl_cond_id
,ctl_cond_long
,sensor
,region
,assay_id
,assay_long
,assay_state
);

insert into macsr_ref select distinct 
macsr_id
,meas_str_id
,macr_id
,mcsr_id
,masr_id
,mar_id
,mcr_id
,acsr_id
,targ_channel
,gfp_partner_macsr_id
,gfp_partner_masr_id
,fs.cond_id
,cc.canon_cond as cond_long
,fs.cond_group
,ctl_cond_macsr_id
,ctl_cond_macr_id
,ctl_cond_id
,cc.expect_ctrl as ctl_cond_long
,sensor
,region
,fs.assay_id
,fsm.kd_sheet as assay_long
,assay_state

from fast_supertable fs
join canon_conds cc on
fs.cond_id = cc.cond_id
join
files_sheets_match fsm
on
fsm.assay_id = fs.assay_id
order by macsr_id
;

CREATE INDEX macr_assay_idx ON macsr_ref (
    macr_id,
    assay_long
);


create table kd_prism_pval_export (
101 as meas_str_id,
ctrl_cond,
region,
exp_cond,
p_val float
);

insert into cond_to_canon
select distinct exp_cond as kd_cond, null as canon_cond from kd_prism_pval_export_101
where not exp_cond in (select kd_cond from cond_to_canon)
;
