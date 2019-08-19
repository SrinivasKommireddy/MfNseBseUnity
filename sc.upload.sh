# !/bin/bash

#
  source sc.env.sh
  rm tmp.$PID.*.sql

# sql file for master
  if [[ ($MKT = 'mf') || ($MKT = 'bse') || ($MKT = 'nse') ]]
    then
      cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF
        \timing on
        drop foreign table if exists ff_master;
        create foreign table ff_master (
          market    text    not null,
          code    text    not null,
          name     text    not null,
          isin    text    not null,
          type    text    null,
          category  text    null
        )
        server ffdw
        options (filename '$(pwd)/${INS_FILE}', format 'csv', null '');
        
        BEGIN;
        insert into master(market, code, name, isin, type, category)
          with 
            be_bz as 
            (
              select 
                market, code, type, rank() over (partition by market, code order by market, code, type desc) 
              from 
                ff_master
            )
          select 
            ff_master.market, ff_master.code, ff_master.name, ff_master.isin, ff_master.type, ff_master.category
          from 
            ff_master, 
            be_bz 
          where 
            ff_master.market = be_bz.market
            and ff_master.code = be_bz.code
            and ff_master.type = be_bz.type
            and be_bz.rank = 1
          ON CONFLICT (market, code) DO UPDATE
            SET
              name         = EXCLUDED.name    ,
              isin         = EXCLUDED.isin    ,
              type         = EXCLUDED.type    ,
              category     = EXCLUDED.category
        ;
        COMMIT;
        drop foreign table ff_master;

        --drop foreign table if exists ff_master_sector;
        --create foreign table ff_master_sector (
        --  market    text    not null,
        --  code    text    not null,
        --  sector    text    null,
        --  subsector  text    null
        --)
        --server ffdw
        --options (filename '$(pwd)/${MKT}.instrument.sector.csv', format 'csv', null '');
                --
        --BEGIN;
                --update master
                --  set
                --    sector    = ms.sector,
                --    subsector = ms.subsector
                --  from
                --    ff_master_sector ms
                --  where 
                --        master.market = ms.market
                --    and master.code   = ms.code
                --;
        --COMMIT;
        
        VACUUM analyze master;
		EOF
    else
      :
  fi
  
# loading hist, first find out existing count, then use it to load ful or inc file
  cat - > tmp.$PID.$0.$MKT.existingcount.sql <<- EOF
    select count(0) from (select * from hist where market = '$MKT' limit 10) tbl_mkt;
	EOF

  EXISTING_COUNT=$(psql --username=postgres --host=localhost --dbname=unity --no-align --quiet --tuples-only --file=tmp.$PID.$0.$MKT.existingcount.sql)
  
  echo "EXISTING_COUNT for MKT $MKT = " $EXISTING_COUNT
  
  if [ $EXISTING_COUNT -eq 0 ]
    then
      FILE_SCOPE='ful'
    else
      FILE_SCOPE='inc'
  fi

# load either ful or inc file into hist
  
  if [ $FILE_SCOPE = 'ful' ]
    then
      cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF
        \timing on
        copy
          hist(market,code,date,open,high,low,nav,volume,adjclose)
        from
          '$(pwd)/${MKT}.${FILE_SCOPE}.txt'
        WITH
          DELIMITER ','
          NULL 'NULL'
        ;
		EOF
    else
      cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF
        \timing on
        drop foreign table if exists ff_hist;
        create foreign table ff_hist (
          market   text         not null,
          code     text         not null,
          date     date         not null,
          open     numeric(12,4)     null,
          high     numeric(12,4)     null,
          low      numeric(12,4)     null,
          nav      numeric(12,4) not null,
          volume   bigint           null,
          adjclose numeric(12,4)     null
        )
        server ffdw
        options (filename '$(pwd)/${MKT}.${FILE_SCOPE}.txt', delimiter ',', NULL 'NULL');
        BEGIN;
        insert into hist (market, code, date, open, high, low, nav, volume, adjclose)
          select 
            market, code, date, open, high, low, nav, volume, adjclose
          from
            ff_hist
          ON CONFLICT (code, date, market) DO UPDATE
            SET
              open     = EXCLUDED.open     ,
              high     = EXCLUDED.high     ,
              low      = EXCLUDED.low      ,
              nav      = EXCLUDED.nav      ,
              volume   = EXCLUDED.volume   ,
              adjclose = EXCLUDED.adjclose
          ;
        COMMIT;
        drop foreign table ff_hist;
	EOF
  fi

# conditional loads, nse - portfolio & fno, idx - date
  case $MKT in
    mf)
      # remove sat/sun/holiday data for everyday interest bearing funds
      # update nav for debt funds which jump up wrongly
      cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF

        begin;
          copy (select * from hist h where h.nav = 0) to STDOUT;
          delete from hist h where h.nav = 0;
        commit;

        -- begin;
        -- with delrows as
        --   (
        --     select
        --       h.market,
        --       h.code,
        --       h.date
        --     from
        --       hist h,
        --       date d
        --     where
        --       0 = 0
        --       and h.market = 'mf'
        --       and h.date = d.date
        --       and d.seq is null
        --   )
        -- delete 
        -- from 
        --   hist using delrows 
        -- where 
        --   hist.market = delrows.market 
        --   and hist.code = delrows.code 
        --   and hist.date = delrows.date;
        -- commit;
        
        begin;
        update only hist
          set nav = hist.nav/correction.correction
        from 
          (
            select
              i1.market,
              i1.code,
              i1.date,
              10.0 ^ (length(cast(round((i.nav - i1.nav)/i1.nav) as text))) correction
            from
              (select i.market, i.code, i.date, d.seq, i.nav from hist i, date d where i.date = d.date and i.market = 'mf') i,
              (select i.market, i.code, i.date, d.seq, i.nav from hist i, date d where i.date = d.date and i.market = 'mf') i1
            where
              0 = 0
              and i.market = i1.market
              and i.code = i1.code
              and i.seq = i1.seq - 1
              and (i.nav - i1.nav)/i1.nav > 6
            order by 1, 2, 3
          ) correction
        where
          hist.market = correction.market
          and hist.code = correction.code
          and hist.date > correction.date;
        commit;
        
        begin;
          delete
          from 
            master
          where
            (market, code) in (
              select 
                market, code
              from 
                hist, 
                date
              where
                hist.date = date.date
                and date.seq >= 5
                and hist.market != 'mf'
              group by 
                market, code
              having 
                sum(volume) = 0
            )
          ;
        commit;
        begin;
        --delete from master if there is no data in last 10 calandar days
        copy
          (
          select
            'no data in last 10 calandar days' c, m.market, m.code
          from
            master m
          where
            0 = 0
            -- m.market = 'mf'
          EXCEPT
          select
            'no data in last 10 calandar days' c, h.market, h.code
          from
            hist h,
            date d
          where
            h.date = d.date
            and d.seq <= 10
            )
          TO STDOUT;
        delete from master where (market,code) in 
          (
            select
              m.market, m.code
            from
              master m
            where
              0 = 0
              -- m.market = 'mf'
            EXCEPT
            select
              h.market, h.code
            from
              hist h,
              date d
            where
              h.date = d.date
              and d.seq <= 10
          );
        commit;
        
        begin;
          copy
            (
              select
                'max(nav) = min(nav)' c, h.market, h.code
              from
                hist h
              where
                h.date > current_date - 12 
              group by
                h.market, h.code
              having
                max(nav) = min(nav)
            )
            TO STDOUT;
        delete from master where (market,code) in 
          (
            select
              h.market, h.code
            from
              hist h
            where
              h.date > current_date - 12 
            group by
              h.market, h.code
            having
              max(nav) = min(nav)
          );
        commit;

        begin;
          copy (select * from master where upper(name) like upper('%Unclaimed%')) to STDOUT;
          delete from master where upper(name) like upper('%Unclaimed%');
        commit;

        VACUUM analyze verbose hist;
        
        refresh materialized view hist5d;
        VACUUM analyze verbose hist5d;
        
        refresh materialized view asset;
        VACUUM analyze verbose asset;

	EOF
      ;;
    bse)
      : # do nothing
      ;;
    nse)
      cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF
        truncate portfolio;
        copy 
          portfolio (channel, market, code, isin, action, qty, price, date)
        from 
          PROGRAM 'cat $(pwd)/portfolio.watchlist.txt |  cut -d, -f1,2,3,4,6,7,8,9 | tail -n +2'
            DELIMITER ',' NULL ''
        ;
        copy 
          portfolio (channel, market, code, isin, action, qty, price, date)
        from 
          PROGRAM 'cat $(pwd)/portfolio.icici.txt $(pwd)/portfolio.mf.txt | grep -v "Stock Symbol,Company Name,ISIN Code," | cut -d, -f1,3,4,5,6,13,14 | sed -re "s/,NSE/,nse/g" -e "s/,BSE/,bse/g" -e "s/(.*),(.*),(.*),(.*),(.*),(.*),(.*)/\7,\1,\2,\3,\4,\5,\6/g" -e "s/^/icici,/g"'
            DELIMITER ',' NULL ''
        ;
        copy
          portfolio (channel, market, code, isin, action, qty, price, date)
        from 
          PROGRAM 'cat $(pwd)/portfolio.upstox.*.txt | grep -v "Scrip Code" | grep -v "BONUS" | cut -d, -f1,2,3,10,11,12 | tr -d \" | grep -v ",,,,," | sed -r -e "s|,S,|,Sell,|g" -e "s|,B,|,Buy,|g" -e "s/E CM,/E,/g" -e "s/NSE/nse/g" -e "s/BSE/bse/g" -e "s|([0-9]{2})/([0-9]{2})/([0-9]{4}),([bens]{3}),([A-Z0-9]*),([BSuyel]{3,4}),([0-9]*.[0-9]{4}),([0-9]*.[0-9]{2})|\4,\5,,\6,\7,\8,\3-\2-\1|g" -e "s/^/upstox,/g"'
            DELIMITER ',' NULL ''
        ;
        /*Upstox - where stock is traded in both nse and bse, the code is bse. Where only traded in nse, then code is nse code*/
        update portfolio set isin = master.isin from master where master.market = 'bse' and portfolio.code = master.code and portfolio.isin is null;
        update portfolio set isin = master.isin from master where master.market = 'nse' and portfolio.code = master.code and portfolio.isin is null;
        update portfolio set code = master.code from master where portfolio.market = master.market and portfolio.isin = master.isin;
        /*update price to 10 where price is 0.0 */
        update portfolio set price = 10.00 where portfolio.price = 0.00;
        VACUUM analyze portfolio;
        
        drop foreign table if exists ff_nse_fno;
        create foreign table ff_nse_fno (
          code      text    not null
        )
        server ffdw
        options (filename '/home/kommire/eq/nse.fno.txt', format 'csv', null '');
        BEGIN;
          update master set category = 'FNO' from ff_nse_fno where master.code = ff_nse_fno.code and master.market = 'nse';
        COMMIT;
        drop foreign table ff_nse_fno;
	EOF
      ;;
    idx)
      cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF
        \timing on
        truncate date;
        copy
          date(seq,date)
        from
          '$(pwd)/idx.date.txt' 
        DELIMITER ','
        NULL '' ;
        update date as d
        set seq = r.rank
        from
          (
            select 
              d.date, 
              dense_rank() over(order by d.date desc nulls last) rank 
            from 
              date d 
                left outer join 
                  (
                    select 
                      distinct date
                      from hist
                      where market = 'idx'
                  ) df
                  on d.date = df.date 
            where df.date is not null
          ) as r
        where
          d.date = r.date;
        VACUUM analyze date;
	EOF
      ;;
    esac

# execute script  
  echo $(date)
  psql -a --username=postgres --host=localhost --dbname=unity --file=tmp.$PID.$0.$MKT.upload.sql
  echo $(date)

#
  rm tmp.$PID.*