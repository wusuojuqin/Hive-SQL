--车贷风险表现

select '' AS findex,
	   now() AS fmodify_time,
	   1 AS fversion,
       (case when u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)>14 then 4043
	        when (u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)<=14) or u.fsub_bu_type=4041 then 4041
            else u.fsub_bu_type
		end ) as fsub_bu_type,
       count(DISTINCT u.fuid) as fsx_cnt, --授信人数
       count(distinct case when r.fuid is not null then r.fuid end) as fjy_cnt,--交易人数
	   count(distinct case when r.frepay_capital>0 then r.fuid end) as  frepay_cnt,  --在贷人数
	   count(distinct case when r.frepay_date <= date_sub(now(),1) then r.fuid end) as frepay_dpd1_cnt,--达到1+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=1 then r.fuid end) as fdpd1, --逾期用户量
       count(distinct case when date_add(r.frepay_date,7)<=date_sub(now(),1) then r.fuid end) as frepay_dpd7_cnt,--达到7+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=7 and date_add(r.frepay_date,7)<=date_sub(now(),1) then r.fuid end)  as fdpd7, --7+逾期用户量
       count(distinct case when date_add(r.frepay_date,30)<=date_sub(now(),1) then r.fuid end) as  frepay_dpd30_cnt,--达到30+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=30 and date_add(r.frepay_date,30)<=date_sub(now(),1) then r.fuid end)  as fdpd30, --30+逾期用户量
       round(sum(r.fcapital),0) as fcapital, --总交易金额
	   round(sum(case when r.forder_type <> 250 then r.fcapital end),0) as fcapital_lk, -- 总交易金额（乐卡）
       round(sum(case when r.forder_type = 250 then r.fcapital end),0) as fcapital_zx, -- 总交易金额（专项）
       round(sum(r.frepay_capital),0) as frepay_capital_total, --总待还本金
	   round(sum(case when r.frepay_date <= date_sub(now(),1) then r.fcapital end ),0) as frepay_capital_total_act1, --达到1+的总交易金额
	   round(sum(case when r.frepay_date <= date_sub(now(),1) then r.frepay_capital end ),0) as fcapital_act1, --达到1+的总待还本金
	   round(sum(case when r.frepay_overdue>=1 then r.frepay_capital end),0) as frepay_capital_due1,  --1+逾期金额
	   round(sum(case when r.frepay_overdue>=7 then r.frepay_capital end),0) as frepay_capital_due7,  --7+逾期金额
	   round(sum(case when r.frepay_overdue>=30 then r.frepay_capital end),0) as frepay_capital_due30  --30+逾期金额
 from 
 (select fuid,max(fsub_bu_type) as fsub_bu_type,min(fcreate_time) as fcreate_time from dp_snap.rc_order_db_t_rc_order 
 where frc_order_state>=350 and fsub_bu_type in (4040,4041,4043,4046,4047) and fcredit_type in (10,20,130) 
 and fcreate_time >= '2017-11-20'  and (fuid < 3000000 OR fuid > 5000000)
  group by fuid) u
 inner join dp_fk_mart.fkfx_user_credit_limit c 
 on u.fuid=c.fuid
 left join	
(select fuid,
        max(forder_type) as forder_type,
        max(frepay_overdue) as frepay_overdue,
		sum(frepay_capital)/100 as frepay_capital,
		sum(forder_capital)/100 as fcapital,
        min(fnext_repay_day) as frepay_date 
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
group by fuid) r 
on u.fuid = r.fuid 
group by (case when u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)>14 then 4043
	        when (u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)<=14) or u.fsub_bu_type=4041 then 4041
            else u.fsub_bu_type
		end )


		
			
--提取车贷数据

create table dp_fk_tmp.ycj_car as 
select a.*,b.fcreate_time_mq,c.fmid,d.fname,d.fwork_flag,c.fuser_type,a2.*
from 
(select fuid,frc_order_id, fsub_bu_type,fcreate_time,frc_order_state from dp_snap.rc_order_db_t_rc_order 
 where  fsub_bu_type in (4046,4047) and fcredit_type in (10,20,130) 
 and fcreate_time >= '2018-05-01'  and (fuid < 3000000 OR fuid > 5000000)
) a 
left join 
(select fuid,frc_order_id, fsub_bu_type,fcreate_time,frc_order_state from dp_snap.rc_order_db_t_rc_order 
 where  fsub_bu_type =4010 and fcredit_type in (10,20,130) and frc_order_state>=350
 and fcreate_time >= '2017-06-01'  and (fuid < 3000000 OR fuid > 5000000)
) a2
on a.fuid=a2.fuid 
left join 
(select fuid, min(fcreate_time)fcreate_time_mq from dp_snap.work_agent_db_t_user_survey_code
 group by fuid)b 
on a.fuid=b.fuid 
left join 
(select b1.fuid,
        b1.fused_credit/100 as fused_credit ,
		b1.fcredit_limit/100 as fcredit_limit , 
        b1.fpocket_auth_date as fpocket_auth_date,
        b1.fbusiness_name,
	    b1.fdistrict_name,
	    b1.fcenter_name ,
	    b1.fbig_company_name,
	    b1.fcompany_name,
	    b1.farea_name,
	    (case when b1.Fmid=0 or b1.fmid is null then b2.fmid else b1.fmid end) as fmid,
		b1.fmin,
	    b1.fuser_type
 from dp_ph_mart.yhfx_user_base_info b1 
 left join dp_fk_work.fkfx_work_user_detail b2  
 on b1.fuid=b2.fuid 
 ) c 
on a.fuid=c.fuid 
left join dp_fk_work.fkfx_work_mid_detail d 
on c.fmid=d.fmid 



select * from dp_fksx_mart.fkfx_order_detail_day
where forder_type=250




--车贷风险表现2.0

select '' AS findex,
	   now() AS fmodify_time,
	   1 AS fversion,
       (case when u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)>14 then 4043
	        when (u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)<=14) or u.fsub_bu_type=4041 then 4041
            else u.fsub_bu_type
		end ) as fsub_bu_type,
       count(DISTINCT u.fuid) as fsx_cnt, --授信人数
       count(distinct case when r.fuid is not null then r.fuid end) as fjy_cnt,--交易人数
	   count(distinct case when r.frepay_capital>0 then r.fuid end) as  frepay_cnt,  --在贷人数
	   count(distinct case when r.frepay_date <= date_sub(now(),1) then r.fuid end) as frepay_dpd1_cnt,--达到1+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=1 then r.fuid end) as fdpd1, --逾期用户量
       count(distinct case when date_add(r.frepay_date,7)<=date_sub(now(),1) then r.fuid end) as frepay_dpd7_cnt,--达到7+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=7 and date_add(r.frepay_date,7)<=date_sub(now(),1) then r.fuid end)  as fdpd7, --7+逾期用户量
       count(distinct case when date_add(r.frepay_date,30)<=date_sub(now(),1) then r.fuid end) as  frepay_dpd30_cnt,--达到30+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=30 and date_add(r.frepay_date,30)<=date_sub(now(),1) then r.fuid end)  as fdpd30, --30+逾期用户量
       round(sum(r.fcapital),0) as fcapital, --总交易金额
	   round(sum(t.fcapital),0) as fcapital_lk, -- 总交易金额（乐卡）
	   round(sum(s.fcapital),0) as fcapital_zx, -- 总交易金额（专项）
       round(sum(r.frepay_capital),0) as frepay_capital_total, --总待还本金
	   round(sum(case when r.frepay_date <= date_sub(now(),1) then r.fcapital end ),0) as frepay_capital_total_act1, --达到1+的总交易金额
	   round(sum(case when r.frepay_date <= date_sub(now(),1) then r.frepay_capital end ),0) as fcapital_act1, --达到1+的总待还本金
	   round(sum(case when r.frepay_overdue>=1 then r.frepay_capital end),0) as frepay_capital_due1,  --1+逾期金额
	   round(sum(case when r.frepay_overdue>=7 then r.frepay_capital end),0) as frepay_capital_due7,  --7+逾期金额
	   round(sum(case when r.frepay_overdue>=30 then r.frepay_capital end),0) as frepay_capital_due30  --30+逾期金额
 from 
 (select fuid,max(fsub_bu_type) as fsub_bu_type,min(fcreate_time) as fcreate_time from dp_snap.rc_order_db_t_rc_order 
 where frc_order_state>=350 and fsub_bu_type in (4040,4041,4043,4046,4047) and fcredit_type in (10,20,130) 
 and fcreate_time >= '2017-11-20' and (fuid < 3000000 OR fuid > 5000000)
  group by fuid) u
 inner join dp_fk_mart.fkfx_user_credit_limit c 
 on u.fuid=c.fuid
 left join	
(select fuid,
        max(frepay_overdue) as frepay_overdue,
		sum(frepay_capital)/100 as frepay_capital,
		sum(forder_capital)/100 as fcapital,
        min(fnext_repay_day) as frepay_date 
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
group by fuid) r 
on u.fuid = r.fuid
left join (
	select fuid,
		sum(forder_capital)/100 as fcapital
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
and forder_type <> 250 -- 乐卡
group by fuid
) t
on u.fuid = t.fuid
left join (
	select fuid,
		sum(forder_capital)/100 as fcapital
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
and forder_type = 250 -- 专项
group by fuid
) s
on u.fuid = s.fuid
group by (case when u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)>14 then 4043
	        when (u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)<=14) or u.fsub_bu_type=4041 then 4041
            else u.fsub_bu_type
		end )


--车贷风险表现3.0

select 
       (case when u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)>14 then 4043
	        when (u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)<=14) or u.fsub_bu_type=4041 then 4041
            else u.fsub_bu_type
		end ) as fsub_bu_type,
       count(DISTINCT u.fuid) as fsx_cnt, --授信人数
       count(distinct case when r.fuid is not null then r.fuid end) as fjy_cnt,--交易人数
	   count(distinct case when r.frepay_capital>0 then r.fuid end) as  frepay_cnt,  --在贷人数
	   count(distinct case when r.frepay_date <= date_sub(now(),1) then r.fuid end) as frepay_dpd1_cnt,--达到1+的交易用户量
       count(DISTINCT case when r.frepay_overdue>=1 then r.fuid end) as fdpd1, --逾期1+用户量
	   concat(round(count(DISTINCT case when r.frepay_overdue>=1 then r.fuid end)*100/
	   count(distinct case when r.frepay_date <= date_sub(now(),1) then r.fuid end),2),"%") as percent_1, --1+用户百分比
       count(DISTINCT case when r.frepay_overdue>=7 and date_add(r.frepay_date,7)<=date_sub(now(),1) then r.fuid end)  as fdpd7, --7+逾期用户量
	   concat(round(count(DISTINCT case when r.frepay_overdue>=7 and date_add(r.frepay_date,7)<=date_sub(now(),1) then r.fuid end)*100/
	   count(distinct case when r.frepay_date <= date_sub(now(),1) then r.fuid end),2),"%") as percent_7, --7+用户百分比
       count(DISTINCT case when r.frepay_overdue>=30 and date_add(r.frepay_date,30)<=date_sub(now(),1) then r.fuid end)  as fdpd30, --30+逾期用户量
	   concat(round(count(DISTINCT case when r.frepay_overdue>=30 and date_add(r.frepay_date,30)<=date_sub(now(),1) then r.fuid end)*100/
	   count(distinct case when r.frepay_date <= date_sub(now(),1) then r.fuid end),2),"%") as percent_30, -- 30+用户百分比
       round(sum(r.fcapital),0) as fcapital, --总交易金额
	   round(sum(t.fcapital),0) as fcapital_lk, -- 总交易金额（乐卡）
	   round(sum(s.fcapital),0) as fcapital_zx, -- 总交易金额（专项）
       round(sum(r.frepay_capital),0) as frepay_capital_total, --总待还本金
	   round(sum(case when r.frepay_overdue>=1 then r.frepay_capital end),0) as frepay_capital_due1,  --1+逾期金额
	   concat(round(sum(case when r.frepay_overdue>=1 then r.frepay_capital end)*100/sum(r.frepay_capital),2),"%") as cap_due1_percent, -- 1+金额百分比
	   round(sum(case when r.frepay_overdue>=7 then r.frepay_capital end),0) as frepay_capital_due7,  --7+逾期金额
	   concat(round(sum(case when r.frepay_overdue>=7 then r.frepay_capital end)*100/sum(r.frepay_capital),2),"%") as cap_due7_percent, -- 7+金额百分比
	   round(sum(case when r.frepay_overdue>=30 then r.frepay_capital end),0) as frepay_capital_due30,  --30+逾期金额
	   concat(round(sum(case when r.frepay_overdue>=30 then r.frepay_capital end)*100/sum(r.frepay_capital),2),"%") as cap_due30_percent -- 30+金额百分比
 from 
 (select fuid,max(fsub_bu_type) as fsub_bu_type,min(fcreate_time) as fcreate_time from dp_snap.rc_order_db_t_rc_order 
 where frc_order_state>=350 and fsub_bu_type in (4040,4041,4043,4046,4047) and fcredit_type in (10,20,130) 
 and fcreate_time >= '2017-11-20' and (fuid < 3000000 OR fuid > 5000000)
  group by fuid) u
 inner join dp_fk_mart.fkfx_user_credit_limit c 
 on u.fuid=c.fuid
 left join	
(select fuid,
        max(frepay_overdue) as frepay_overdue,
		sum(frepay_capital)/100 as frepay_capital,
		sum(forder_capital)/100 as fcapital,
        min(fnext_repay_day) as frepay_date 
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
group by fuid) r 
on u.fuid = r.fuid
left join (
	select fuid,
		sum(forder_capital)/100 as fcapital
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
and forder_type <> 250 -- 乐卡
group by fuid
) t
on u.fuid = t.fuid
left join (
	select fuid,
		sum(forder_capital)/100 as fcapital
from dp_fksx_mart.fkfx_order_detail_day 
where f_p_date=to_date(now()) --and faccount_date>= '2017-11-20'
and forder_type = 250 -- 专项
group by fuid
) s
on u.fuid = s.fuid
group by (case when u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)>14 then 4043
	        when (u.fsub_bu_type=4043 and datediff(u.fcreate_time,c.fpocket_auth_time)<=14) or u.fsub_bu_type=4041 then 4041
            else u.fsub_bu_type
		end )
order by fsub_bu_type