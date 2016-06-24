create table $mop_brewgraph_input as select a.store_country,a.sm_st_cd,a.sid,a.dts,b.producthash,sum(a.line_item_qty),a.ci2_trans_dt,a.hr_sid, 
(case  when a.hr_sid>=40000 and a.hr_sid < 110000 then 'morning' when a.hr_sid >=110000 and a.hr_sid < 140000 then 'lunch' when a.hr_sid >=140000 and  a.hr_sid < 170000 then 'afternoon' when a.hr_sid >=170000 and  a.hr_sid < 230000 then  'evening' else 'night' END ) as timeofday 
from source.apollo_cos_transactions a inner join  (select distinct * from public.arun_mop_product) b on  b.sku!='' and b.sku not like '%-%' and cast(a.prod_num as bigint)=cast(b.sku as bigint)  where a.ci2_trans_dt > '$a.ci2_trans_dt'
group by a.store_country,a.sm_st_cd,a.sid,a.dts,b.producthash,a.ci2_trans_dt,a.hr_sid;