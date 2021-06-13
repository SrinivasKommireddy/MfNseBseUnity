-- views.sql, v4, added open, high, low
\timing on
drop materialized view hist5d cascade;
create materialized view hist5d as
  select
    h.market,
    h.code,
    h.date,
    d.seq - m.seq + 1 as period,
    h.nav, h.open, h.high, h.low,
    (max(h.nav) OVER p_market_code_o_date_curr_row - h.nav)*100/(max(h.nav) OVER p_market_code_o_date_curr_row) dd,
    ((h.nav - max(h.nav) OVER p_market_code_o_date_all_dates)*(-100.0))/(max(h.nav) OVER p_market_code_o_date_all_dates)     pct_max_all,
    ((h.nav - min(h.nav) OVER p_market_code_o_date_all_dates)*( 100.0))/(min(h.nav) OVER p_market_code_o_date_all_dates)     pct_min_all,
    ((h.nav - max(h.nav) OVER p_market_code_o_date_52w)*(-100.0))/(max(h.nav) OVER p_market_code_o_date_52w) pct_max_52w,
    ((h.nav - min(h.nav) OVER p_market_code_o_date_52w)*( 100.0))/(min(h.nav) OVER p_market_code_o_date_52w) pct_min_52w,
    h.volume,
    m.date maxdate
  from
    hist h,
    date d,
    (
      select
        i.market,
        i.code,
        i.date,
        d.seq
      from
        (
          select
            i.market,
            i.code,
            max(i.date) date
          from
            hist i,
            (select max(date) max_cal_date from date) d
          where
            i.date >= d.max_cal_date - 9 -- get rid of delisted stocks
          group by
            i.market,
            i.code
        ) i,
        date d
      where
        i.date = d.date
        --AND i.code in ('INFY','TCS')
    ) m
  where
    0 = 0
    and h.date = d.date
    and h.market = m.market
    and h.code = m.code
    and d.seq is not null
    and ( (d.seq - m.seq)%20 = 0 or (d.seq - m.seq + 1) in (1,2,6,11))
    --AND h.code in ('INFY','TCS')
  window 
    p_market_code_o_date_curr_row  as (partition by h.market, h.code order by h.date rows between UNBOUNDED PRECEDING and CURRENT ROW),
    p_market_code_o_date_all_dates as (partition by h.market, h.code order by h.date rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING),
    p_market_code_o_date_52w       as (partition by h.market, h.code order by h.date rows between 13 PRECEDING and CURRENT ROW)
;
CREATE index hist5d_market_code_period on hist5d(market,code,period);
create index hist5d_period_market on hist5d(period,market);
vacuum analyze verbose hist5d;

drop materialized view asset cascade;
create materialized view asset as
  SELECT
    i.market,
    i.code,
    i.period,
    i.date dt,
    i.maxdate date,
	i.nav, i.open, i.high, i.low,
    (i.nav - lead(i.nav,1) over market_code_period)*100/(lead(i.nav,1) over market_code_period) ret,
    i.dd,
    i.pct_max_all, i.pct_min_all, i.pct_max_52w, i.pct_min_52w,
    i.volume,
    0.7 AS rfr
    --1.35 AS rfr
   FROM 
     hist5d i
   where 
     period not in (2,6)
     --and i.code in ('INFY','TCS')
   window market_code_period as (partition by i.market, i.code order by i.market, i.code, i.period)
 ;
CREATE index asset_market_code on asset(market,code);
vacuum analyze verbose asset;