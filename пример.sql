with dat_cur as not materialized (
select
	tk.REGIONCODE,
	tk2.SKO,
	tk.PATIENT,
	tk.ID,
	tk5.DATE_CHECKOUT,
	tk4.DATE_CHECKIN
from
	scheme.table_1 tk
join scheme.table_2 tk2 on
	tk2.TICKET = tk.ID
join scheme.table_4 tk4 on
	tk4.TICKET = tk.ID
join scheme.table_5 tk5 on
	tk5.TICKET = tk.ID
where
	cast(tk4.DATE_CHECKIN as date) <= $to
	and cast(tk5.DATE_CHECKOUT as date) >= $from
	and tk.STATUS in (6,7)
),
cur as not materialized (
select
	d.sko,
	count(distinct d.PATIENT) pat,
	count(d.ID) cnt,
	sum((case when cast(d.DATE_CHECKOUT as date) > (CASE when current_date < $to then current_date ELSE $to end) then (CASE when current_date < $to then current_date ELSE $to end) else cast(d.DATE_CHECKOUT as date) end) - (case when $from > cast(d.DATE_CHECKIN as date) then $from else cast(d.DATE_CHECKIN as date) end) + 1) pr_q
from
	dat_cur d
group by
	d.sko),
cur_nonresident as not materialized (
select
	d.sko,
	count(distinct d.PATIENT) pat,
	count(d.ID) cnt,
	sum((case when cast(d.DATE_CHECKOUT as date) > (CASE when current_date < $to then current_date ELSE $to end) then (CASE when current_date < $to then current_date ELSE $to end) else cast(d.DATE_CHECKOUT as date) end) - (case when $from > cast(d.DATE_CHECKIN as date) then $from else cast(d.DATE_CHECKIN as date) end) + 1) pr_q
from
	dat_cur d
where
	d.REGIONCODE <> (
	select
		code_okato from mz_nsi_directory.ref_territory where code_constitution = (select REGIONID
	from
		scheme.table_6
	where
		oid = d.SKO))
group by
	d.sko),
task_cur as not materialized (
select
	tbd.sko,
	tbd.QQ
from
	scheme.table_7 tbd
join scheme.table_8 tv on
	tbd.ID_VERS = tv.id
where
	tv.TASK_YEAR = date_part('year', $from )
	and tv.CUR = true
	and tv.enabled = true
	and exists (
	select
		1
	from
		dat_cur dc
	where
		dc.sko = tbd.SKO)),
dat_prev as not materialized (
select
	tk.REGIONCODE,
	tk2.SKO,
	tk.PATIENT,
	tk.ID,
	tk5.DATE_CHECKOUT,
	tk4.DATE_CHECKIN
from
	scheme.table_1 tk
join scheme.table_2 tk2 on
	tk2.TICKET = tk.ID
join scheme.table_3 tk3 on
	tk3.TICKET = tk.ID
join scheme.table_4 tk4 on
	tk4.TICKET = tk.ID
join scheme.table_5 tk5 on
	tk5.TICKET = tk.ID
where
	cast(tk4.DATE_CHECKIN as date) <= cast($to - interval '1 year' as date)
	and cast(tk5.DATE_CHECKOUT as date) >= cast($from - interval '1 year' as date)
	and tk.STATUS in (6,7)
	),
prev as not materialized (
select
	d.sko,
	count(distinct d.PATIENT) pat,
	count(d.ID) cnt,
	sum((case when cast(d.DATE_CHECKOUT as date) > cast($to - interval '1 year' as date) then cast($to - interval '1 year' as date) else cast(d.DATE_CHECKOUT as date) end) - (case when cast($from - interval '1 year' as date) > cast(d.DATE_CHECKIN as date) then cast($from - interval '1 year' as date) else cast(d.DATE_CHECKIN as date) end) + 1) pr_q
from
	dat_prev d
group by
	d.sko),
prev_nonresident as not materialized (
select
	d.sko,
	count(distinct d.PATIENT) pat,
	count(d.ID) cnt,
	sum((case when cast(d.DATE_CHECKOUT as date) > cast($to - interval '1 year' as date) then cast($to - interval '1 year' as date) else cast(d.DATE_CHECKOUT as date) end) - (case when cast($from - interval '1 year' as date) > cast(d.DATE_CHECKIN as date) then cast($from - interval '1 year' as date) else cast(d.DATE_CHECKIN as date) end) + 1) pr_q
from
	dat_prev d
where
	d.REGIONCODE <> (
	select code_okato from mz_nsi_directory.ref_territory where code_constitution = (select REGIONID
	from
		scheme.table_6
	where
		oid = d.SKO))
group by
	d.sko),
task_prev as not materialized (
select
	tbd.sko,
	tbd.QQ
from
	scheme.table_7 tbd
join scheme.table_8 tv on
	tbd.ID_VERS = tv.id
where
	tv.TASK_YEAR = date_part('year', $from)-1
	and tv.CUR = true
	and tv.enabled = true
	and exists (
	select
		1
	from
		dat_prev dp
	where
		dp.sko = tbd.SKO))
select
	org.NAMESHORT,
	case
		when exists (
		select
			1
		from
			scheme.table_9 sc
		join scheme.table_10 cat on
			cat.ID = sc.CATEGORY
		where
			sc.SKO = d.sko
			and cat.KIDS_PLACES = 1
			and exists (
			select
				1
			from
				scheme.table_9 sc
			join scheme.table_10 cat on
				cat.ID = sc.CATEGORY
			where
				sc.SKO = d.sko
				and cat.KIDS_PLACES = 0)) then 'Смешанные'
		when exists (
		select
			1
		from
			scheme.table_9 sc
		join scheme.table_10 cat on
			cat.ID = sc.CATEGORY
		where
			sc.SKO = d.sko
			and cat.KIDS_PLACES = 1) then 'Детские'
		when exists (
		select
			1
		from
			scheme.table_9 sc
		join scheme.table_10 cat on
			cat.ID = sc.CATEGORY
		where
			sc.SKO = d.sko
			and cat.KIDS_PLACES = 0) then 'Взрослые' end categ,
		case
			when exists (
			select
				1
			from
				scheme.table_link_2
			where
				id_type = 3
				and id_profile in (
				select
					PROFILE
				from
					scheme.table_link_3 pr
				where
					pr.SKO = d.sko ))
			and exists (
			select
				1
			from
				scheme.table_link_2
			where
				id_type = 2
				and id_profile in (
				select
					PROFILE
				from
					scheme.table_link_3 pr
				where
					pr.SKO = d.sko )) then 'Все'
			when exists (
			select
				1
			from
				scheme.table_link_2
			where
				id_type = 3
				and id_profile in (
				select
					PROFILE
				from
					scheme.table_link_3 pr
				where
					pr.SKO = d.sko )) then 'Текст1'
			else 'Текст2' end TYPE_SKO,
			tc.qq,
			d.pat pat_cur,
			d.cnt cnt_cur,
			d.pr_q pr_q_cur,
			cn.pat pat_cn,
			cn.cnt cnt_cn,
			cn.pr_q pr_q_cn,
			t_p.qq prev_qq,
			d_p.pat pat_prev,
			d_p.cnt cnt_prev,
			d_p.pr_q pr_q_prev,
			pn.pat pat_pn,
			pn.cnt cnt_pn,
			pn.pr_q pr_q_pn
		from
			cur d
		left join prev d_p on
			d.sko = d_p.sko
		left join cur_nonresident cn on
			d.sko = cn.sko
		left join prev_nonresident pn on
			d_p.sko = pn.sko
		join scheme.table_6 org on
			d.SKO = org.OID
		join scheme.table_link_1 lorg on
			lorg.OID = org.OID
		join task_cur tc on
			tc.sko = d.sko
		left join task_prev t_p on
			t_p.sko = d_p.sko
