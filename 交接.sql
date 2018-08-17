/*锁定乐花部分*/
--锁定乐花当前逾期
--这是计算当前逾期的原版代码，是放在rcdata中的，在notebook上运行完之后需导出到Excel再对每项进行求和
--rcdate 
 --create table dp_fk_tmp.ycj_lehua_risk_far as 

select '' AS findex,
	   now() AS fmodify_time,
	   1 AS fversion,
	   f_p_date as f_p_date, --更新时间，a表
     fbusiness_name as fbusiness_name,
	   fdistrict_name as fdistrict_name,
	   fcenter_name as fcenter_name,
	   fjy_cnt as fjy_cnt,
	   frepay_cnt as  frepay_cnt,  
	   fdpd1 as fdpd1,
	   fdpd7 as fdpd7,
	   fdpd30 as fdpd30,
	   fcapital as fcapital,
	   frepay_capital_total as frepay_capital_total,
	   frepay_capital_due1 as frepay_capital_due1,
	   frepay_capital_due7 as frepay_capital_due7,
	   frepay_capital_due30 as frepay_capital_due30
from 
(select 
       a.f_p_date as f_p_date,
       b.fbusiness_name as fbusiness_name,
	     b.fdistrict_name as fdistrict_name,
	     b.fcenter_name as fcenter_name,
       count(distinct r.forder_id ) as fjy_cnt,--交易单数
	     count(distinct case when r.frepay_capital>0 then r.forder_id end) as  frepay_cnt,  --在贷单数
       count(DISTINCT case when r.frepay_overdue>=1 then r.forder_id end) as fdpd1, --1+逾期单数
       count(DISTINCT case when r.frepay_overdue>=7 then r.forder_id end) as fdpd7, --7+逾期单数
       count(DISTINCT case when r.frepay_overdue>=30 then r.forder_id end) as fdpd30, --30+逾期单数
       sum(r.fcapital) as fcapital, --总交易金额
       sum(r.frepay_capital) as frepay_capital_total, --总待还本金
	     sum(case when r.frepay_overdue>=1 then r.frepay_capital end) as frepay_capital_due1,  --1+逾期金额
	     sum(case when r.frepay_overdue>=7 then r.frepay_capital end) as frepay_capital_due7,  --7+逾期金额
	     sum(case when r.frepay_overdue>=30 then r.frepay_capital end) as frepay_capital_due30  --30+逾期金额
  from 
  (select fuid,
          forder_id,
          to_date(fcreate_time) as f_p_date 
          from dp_fk_mart.fkfx_order_detail 
          where ftotal_amount>0 and forder_state>=350 and fcreate_time>='2017-12-09'  and (fuid < 3000000 OR fuid > 5000000) and fbusiness_two_level_id=202013
          -- ftotal_amount 订单金额，forder_state>=350是筛选出审核通过（具有还款）的订单，fbusiness_two_level_id业务二级id，fuid < 3000000 OR fuid > 5000000是剔除测试用户
   )a 
 inner join (select r.fuid,r.forder_id,
                    max(frepay_overdue) as  frepay_overdue,
                    sum(frepay_capital)/100 as frepay_capital,
					          sum(fcapital)/100 as fcapital,
					          min(frepay_date) as frepay_date
               from dp_fk_mart.fkfx_repay_detail r
              where r.fcreate_time >= '2017-12-09'
              group by r.fuid,forder_id) r
    on a.forder_id = r.forder_id
  inner join dp_fksx_mart.fkfx_user_level_two_detail b 
    on a.fuid=b.fidsec_uid
  group by a.f_p_date,b.fbusiness_name,b.fdistrict_name,b.fcenter_name
)t

--锁定乐花迁徙率
--create table dp_fksx_mart.t_weekly_lehua  as

select substr(date_sub(b.f_p_date,1),1,7)f_p_date, --更新日期减去一天后截取到日期的月份
       SUM(CASE WHEN b.frepay_overdue=0 THEN b.frepay_capital ELSE 0 END)/100 AS M0, --frepay_overdue当前逾期天数
	   SUM(CASE WHEN b.frepay_overdue>=1 AND b.frepay_overdue<=29 THEN b.frepay_capital ELSE 0 END)/100 AS M1,
	   SUM(CASE WHEN b.frepay_overdue>=30 AND b.frepay_overdue<=59 THEN b.frepay_capital  ELSE 0 END)/100 AS M2,
	   SUM(CASE WHEN b.frepay_overdue>=60 AND b.frepay_overdue<=89 THEN b.frepay_capital ELSE 0 END)/100 AS M3,
	   SUM(CASE WHEN b.frepay_overdue>=90 AND b.frepay_overdue<=119 THEN b.frepay_capital  ELSE 0 END)/100 AS M4
from 
  (select forder_id from dp_fk_mart.fkfx_order_detail 
   where ftotal_amount>0 and forder_state>=350 and fcreate_time>='2017-12-09'  and (fuid < 3000000 OR fuid > 5000000) and fbusiness_two_level_id=202013
   )a 	-- 此表的作用是筛选出通过审核的订单的order_id，与b表中的逾期天数等结合，因为b表中没有这些信息
inner join 
(select * from dp_fksx_mart.fkfx_order_detail_day where day(f_p_date)=1 or f_p_date=to_date(now())) b 
on a.forder_id=b.forder_id
group by substr(date_sub(b.f_p_date,1),1,7)

--锁定乐花逾期率
--运行这段代码可直接出来邮件里所需要的数据
select sum(fjy_cnt) as fjy_cnt, --交易单数
	   sum(frepay_cnt) as  frepay_cnt,    --在贷单数
	   sum(fdpd1) as fdpd1,--1+逾期单数
	   concat(round(sum(fdpd1)*100/sum(frepay_cnt),2),'%') as cnt_percent_1, --1+单数比例
	   sum(fdpd7) as fdpd7,--7+逾期单数
	   concat(round(sum(fdpd7)*100/sum(frepay_cnt),2),'%') as cnt_percent_7, --7+单数比例
	   sum(fdpd30) as fdpd30,--30+逾期单数
	   concat(round(sum(fdpd30)*100/sum(frepay_cnt),2),'%') as cnt_percent_30, --30+单数比例
	   round(sum(fcapital),0) as fcapital,--总交易金额
	   round(sum(frepay_capital_total),0) as frepay_capital_total,--总待还本金
	   round(sum(frepay_capital_due1),0) as frepay_capital_due1,--1+逾期金额
	   concat(round(sum(frepay_capital_due1)*100/sum(frepay_capital_total),2),'%') as cap_percent_1, --1+金额比例
	   round(sum(frepay_capital_due7),0) as frepay_capital_due7,--7+逾期金额
	   concat(round(sum(frepay_capital_due7)*100/sum(frepay_capital_total),2),'%') as cap_percent_7, --7+金额比例
	   round(sum(frepay_capital_due30),0) as frepay_capital_due30,--30+逾期金额
	   concat(round(sum(frepay_capital_due30)*100/sum(frepay_capital_total),2),'%') as cap_percent_30 --30+金额比例
from 
(select 
       a.f_p_date as f_p_date,
       b.fbusiness_name as fbusiness_name,
	     b.fdistrict_name as fdistrict_name,
	     b.fcenter_name as fcenter_name,
       count(distinct r.forder_id ) as fjy_cnt,--交易单数
	     count(distinct case when r.frepay_capital>0 then r.forder_id end) as  frepay_cnt,  --在贷单数
       count(DISTINCT case when r.frepay_overdue>=1 then r.forder_id end) as fdpd1, --1+逾期单数
       count(DISTINCT case when r.frepay_overdue>=7 then r.forder_id end) as fdpd7, --7+逾期单数
       count(DISTINCT case when r.frepay_overdue>=30 then r.forder_id end) as fdpd30, --30+逾期单数
       sum(r.fcapital) as fcapital, --总交易金额
       sum(r.frepay_capital) as frepay_capital_total, --总待还本金
	     sum(case when r.frepay_overdue>=1 then r.frepay_capital end) as frepay_capital_due1,  --1+逾期金额
	     sum(case when r.frepay_overdue>=7 then r.frepay_capital end) as frepay_capital_due7,  --7+逾期金额
	     sum(case when r.frepay_overdue>=30 then r.frepay_capital end) as frepay_capital_due30  --30+逾期金额
  from 
  (select fuid,
          forder_id,
          to_date(fcreate_time) as f_p_date 
          from dp_fk_mart.fkfx_order_detail 
          where ftotal_amount>0 and forder_state>=350 and fcreate_time>='2017-12-09'  and (fuid < 3000000 OR fuid > 5000000) and fbusiness_two_level_id=202013
          -- ftotal_amount 订单金额，forder_state>=350是筛选出审核通过（具有还款）的订单，fbusiness_two_level_id业务二级id，fuid < 3000000 OR fuid > 5000000是剔除测试用户
   )a 
 inner join (select r.fuid,r.forder_id,
                    max(frepay_overdue) as  frepay_overdue,
                    sum(frepay_capital)/100 as frepay_capital,
					          sum(fcapital)/100 as fcapital,
					          min(frepay_date) as frepay_date
               from dp_fk_mart.fkfx_repay_detail r
              where r.fcreate_time >= '2017-12-09'
              group by r.fuid,forder_id) r
    on a.forder_id = r.forder_id
  inner join dp_fksx_mart.fkfx_user_level_two_detail b 
    on a.fuid=b.fidsec_uid
  group by a.f_p_date,b.fbusiness_name,b.fdistrict_name,b.fcenter_name
)t

/*车贷部分*/
--车贷风险表现
--这是计算车贷车主贷的原版代码，不过所需要的数据可以再风控平台里取
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


/*下面是一些常用的数据提取*/
--客户逾期订单明细
select c.Fmid ,d.fname,d.fdistrict_name,d.fcenter_name,d.fwork_flag,d.fhire_date,
       r.fuid,r.forder_id,r.fcreate_time,frepay_overdue,fcapital,frepay_capital
from 
(select * from dp_fk_work.fkfx_work_user_credit_limit where Fauth_time>='2018-03-01') b 
inner join (select * from dp_fk_work.fkfx_work_user_detail where fmid IN (22647)
) c 
on b.fuid=c.fuid 
inner join 
(select r.fuid,forder_id,
        max(frepay_overdue) frepay_overdue,
		max(fpayed_overdue) as fpayed_overdue,
        sum(frepay_capital)/100 frepay_capital,
		sum(fcapital)/100 fcapital,
		min(frepay_date)frepay_date,
		min(fcreate_time)fcreate_time
   from dp_fk_mart.fkfx_repay_detail r
  where r.fcreate_time >= '2018-03-01'
  group by fuid,forder_id) r
  on r.fuid=b.fuid
left join dp_fk_work.fkfx_work_mid_detail d 
on c.fmid=d.fmid 
where r.frepay_overdue>=1    --筛逾期时间


--客户逾期明细
select c.Fmid,d.fname,d.fdistrict_name,d.fcenter_name,d.fwork_flag,
       r.fuid,r.fcreate_time,frepay_overdue,fcapital,frepay_capital
from 
(select * from dp_fk_work.fkfx_work_user_credit_limit where Fauth_time>='2017-09-01') b 
inner join (select * from dp_fk_work.fkfx_work_user_detail where fmid IN (22978, 22414, 22691)
) c 
on b.fuid=c.fuid 
inner join 
(select r.fuid,
        max(frepay_overdue) frepay_overdue,
		max(fpayed_overdue) as fpayed_overdue,
        sum(frepay_capital)/100 frepay_capital,
		sum(fcapital)/100 fcapital,
		min(frepay_date)frepay_date,
		min(fcreate_time)fcreate_time
   from dp_fk_mart.fkfx_repay_detail r
  where r.fcreate_time >= '2017-09-01'
  group by fuid) r
  on r.fuid=b.fuid
left join dp_fk_work.fkfx_work_mid_detail d 
on c.fmid=d.fmid 
where r.frepay_overdue>=1    --筛逾期时间



--BD所有交易客户
select c.Fmid,d.fname,d.fdistrict_name,d.fcenter_name,d.fwork_flag,
       r.fuid,r.fcreate_time,frepay_overdue,fpayed_overdue,fcapital,frepay_capital
from 
(select * from dp_ph_mart.yhfx_user_base_info c where fpocket_auth_time>='2017-10-01' and fmid=22357) c
inner join 
(select r.fuid,
        max(frepay_overdue) frepay_overdue,
		max(fpayed_overdue) as fpayed_overdue,
        sum(frepay_capital)/100 frepay_capital,
		sum(fcapital)/100 fcapital,
		min(frepay_date)frepay_date,
		min(fcreate_time)fcreate_time
   from dp_fk_mart.fkfx_repay_detail r
  where r.fcreate_time >= '2018-04-01'
  group by fuid) r
  on r.fuid=c.fuid
left join dp_fk_work.fkfx_work_mid_detail d 
on c.fmid=d.fmid 


-- 查询客户经理所带的客户情况
select a.fuid, fmid, fauth_date from
(select fuid,
fmid
from dp_fk_work.fkfx_work_user_detail
where fmid = 25242) a -- fmid客户经理id
inner join (
select fuid, fauth_date 
from dp_ph_mart.yhfx_user_base_info
where fauth_date >= to_date(date_sub(now(),7)) and fauth_date <= to_date(now()) -- fauth_date授信日期
group by fuid,fauth_date) b
on a.fuid = b.fuid

--查询客户所属的客户经理
select fuid, fmid from dp_fk_work.fkfx_work_user_detail where fuid = 32258119



-- 统计每个客户的同盾多平台的命中数
--这是对每个月的客户同盾多平台数的汇总，代码里有id_num_fraud_apply_count_12mon，这是12个月之内的平台数，可换成id_num_fraud_apply_count_3mon，既3个月内的平台数，如有需要的话
select a.f_p_date as f_p_date, --月份
count(case when id_num_fraud_apply_count_12mon >= 0 then a.fuid end) as fuid_count, --每个月的用户数，这里加了筛选条件是因为有个别是-1的要剔除
sum(a.id_num_fraud_apply_count_12mon) as fraud_count, --统计同盾平台数之和
round(sum(a.id_num_fraud_apply_count_12mon)/count(case when id_num_fraud_apply_count_12mon >= 0 then a.fuid end),3) as avg_fraud, --平均每人命中了多少个
count(case when id_num_fraud_apply_count_12mon = 0 then fuid end) as count_stage_1, --以下五行可根据实际需求删除或修改，是将用户的平台数做分类分了5个等级，里面的数字都可以换
count(case when id_num_fraud_apply_count_12mon between 1 and 3 then fuid end) as count_stage_2,
count(case when id_num_fraud_apply_count_12mon between 4 and 6 then fuid end) as count_stage_3,
count(case when id_num_fraud_apply_count_12mon between 7 and 9 then fuid end) as count_stage_4,
count(case when id_num_fraud_apply_count_12mon >= 10 then fuid end) as count_stage_5 from
(select 
distinct case when fuid is not null then fuid end as fuid, 
concat(year(f_p_date),'-',month(f_p_date)) as f_p_date,
max(regexp_extract(finputvalues,'"id_num_fraud_apply_count_12mon","value":(-[0-9]+|[A-Z|a-z|0-9]+)',1)) as id_num_fraud_apply_count_12mon  --用这个函数计算同盾多平台数（12月之内）
FROM dp_fk_mart.fk_t_rc_engine_record_water
                 WHERE f_p_date between  '2018-04-01'  and to_date(now())
                   AND fdecision_scene=231050 
group by concat(year(f_p_date),'-',month(f_p_date)), fuid) a
group by f_p_date
order by f_p_date


--客户经理逾期的客户名单
select 
	   c.fbusiness_name as fbusiness_name,
	   c.fdistrict_name as fdistrict_name,
	   c.fcenter_name as fcenter_name,
	   c.fuser_type as fuser_type,
	   c.Fmid as Fmid, -- 客户经理id
       d.fname as fname, --客户经理姓名
	   d.fwork_flag as fwork_flag, -- 是否在职
	   d.fdepartment as fdepartment_bd, --部门
	   d.fdistrict_name as fdistrict_name_bd, --大区
	   d.fcenter_name as fcenter_name_bd, --营销中心
	   c.fpocket_auth_time as fpocket_auth_time,--基础授信时间
       r.fuid as  frepay_cnt  --在贷人
from   
(select fuid,
fused_credit/100 as fused_credit ,
fcredit_limit/100 as fcredit_limit , 
to_date(fpocket_auth_date) as fpocket_auth_time,
       c.fbusiness_name,
	   c.fdistrict_name,
	   c.fcenter_name ,
	   c.fbig_company_name,
	   c.fcompany_name,
	   c.farea_name,
	   c.Fmid,
	   case when fuser_type in (0,1,3) then '学生'  when fuser_type =240 then '职校' 
	        when fuser_type in (220,230) then '蓝白领'  when fuser_type =200 then '提钱乐' end as fuser_type
 from dp_ph_mart.yhfx_user_base_info c where fpocket_auth_time>='2017-07-01'  and fuser_type in (0,1,3,240,220,230,200)) c 
 inner join 
 (select fuid,fsub_bu_type from dp_snap.rc_order_db_t_rc_order b 
  where b.fbu_type =40 and b.frc_order_state>=350 and b.Fcredit_type IN (10,20) and fcreate_time>= '2017-07-01')b --7.1之后的，日期课更改范围
 on c.fuid=b.fuid 
left join 
(select case when r.fuid is not null and r.frepay_capital>0 then fuid end as fuid,
        max(frepay_overdue) as frepay_overdue,
		max(fpayed_overdue) as fpayed_overdue,
        sum(frepay_capital)/100 as frepay_capital,
		sum(fcapital)/100 as fcapital,
		min(frepay_date) as frepay_date
   from dp_fk_mart.fkfx_repay_detail r
  where r.fcreate_time >= '2017-07-01'
  group by fuid) r
  on c.fuid=r.fuid
left join dp_fk_work.fkfx_work_mid_detail d 
on c.fmid=d.fmid 
group by
	   c.fbusiness_name,
	   c.fdistrict_name,
	   c.fcenter_name ,
	   c.fuser_type,
	   c.Fmid ,
       d.fname ,
	   d.fwork_flag ,
	   d.fdepartment,
	   d.fdistrict_name,
	   d.fcenter_name,
	   c.fpocket_auth_time,
	   frepay_cnt


-- 锁定乐花分地区的订单数据
select 
       a.f_p_date as f_p_date, --时间
       b.fbusiness_name as fbusiness_name,
	     b.fdistrict_name as fdistrict_name, --从这个字段可筛选地区
	     b.fcenter_name as fcenter_name,
		 a.fuid as fuid, -- 用户id
       r.forder_id  as forder_id,--交易单数
       r.frepay_overdue as frepay_overdue, --逾期天数
       r.fcapital as fcapical, --总交易金额
       r.frepay_capital as frepay_capital_total --总待还本金

  from 
  (select fuid,
          forder_id,
          to_date(fcreate_time) as f_p_date 
     from dp_fk_mart.fkfx_order_detail 
    where ftotal_amount>0 
		  and forder_state>=350 
		  and fcreate_time>='2018-07-01' --日期
		  and fcreate_time<='2018-07-31'  
		  and (fuid < 3000000 OR fuid > 5000000) 
		  and fbusiness_two_level_id=202013
          -- ftotal_amount 订单金额，forder_state>=350是筛选出审核通过（具有还款）的订单，fbusiness_two_level_id业务二级id，fuid < 3000000 OR fuid > 5000000是剔除测试用户
   )a 
 inner join (select r.fuid,r.forder_id,
                    max(frepay_overdue) as  frepay_overdue,
                    sum(frepay_capital)/100 as frepay_capital,
					          sum(fcapital)/100 as fcapital,
					          min(frepay_date) as frepay_date
               from dp_fk_mart.fkfx_repay_detail r
              where fcreate_time>='2018-07-01' and fcreate_time<='2018-07-31'  
              group by r.fuid,forder_id) r
    on a.forder_id = r.forder_id
  inner join dp_fksx_mart.fkfx_user_level_two_detail b 
    on a.fuid=b.fidsec_uid
order by f_p_date



-- 锁定乐花每月交易金额
select 
       concat(year(a.f_p_date),'-',month(a.f_p_date)) as f_p_date, -- 月份
       sum(r.fcapital) as fcapital --总交易金额
  from 
  (select fuid,
          forder_id,
          to_date(fcreate_time) as f_p_date 
          from dp_fk_mart.fkfx_order_detail 
          where ftotal_amount>0 and forder_state>=350 and fcreate_time>='2017-12-09'  and (fuid < 3000000 OR fuid > 5000000) and fbusiness_two_level_id=202013
          -- ftotal_amount 订单金额，forder_state>=350是筛选出审核通过（具有还款）的订单，fbusiness_two_level_id业务二级id，fuid < 3000000 OR fuid > 5000000是剔除测试用户
   )a 
 inner join (select r.fuid,r.forder_id,
                    max(frepay_overdue) as  frepay_overdue,
                    sum(frepay_capital)/100 as frepay_capital,
				    sum(fcapital)/100 as fcapital,
					min(frepay_date) as frepay_date
               from dp_fk_mart.fkfx_repay_detail r
              where r.fcreate_time >= '2017-12-09'
              group by r.fuid,forder_id) r
    on a.forder_id = r.forder_id
  inner join dp_fksx_mart.fkfx_user_level_two_detail b 
    on a.fuid=b.fidsec_uid
  group by concat(year(a.f_p_date),'-',month(a.f_p_date)) 
  order by concat(year(a.f_p_date),'-',month(a.f_p_date))