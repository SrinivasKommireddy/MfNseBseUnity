# !/bin/bash

cat - >> tmp.$PID.$0.$MKT.upload.sql <<- EOF
        \pset pager off

        truncate portfolio;

        copy 
          portfolio (channel, market, code, isin, action, qty, price, date)
        from 
          PROGRAM 'cat $(pwd)/portfolio.watchlist.txt |  cut -d, -f1,2,3,4,6,7,8,9'
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
          PROGRAM 'cat $(pwd)/portfolio.upstox.py.txt | grep -v "Scrip Code" | grep -v "BONUS" | cut -d, -f1,2,3,10,11,12 | tr -d \" | grep -v ",,,,," | sed -r -e "s|,S,|,Sell,|g" -e "s|,B,|,Buy,|g" -e "s/E CM,/E,/g" -e "s/NSE/nse/g" -e "s/BSE/bse/g" -e "s|([0-9]{2})/([0-9]{2})/([0-9]{4}),([bens]{3}),([A-Z0-9]*),([BSuyel]{3,4}),([0-9]*.[0-9]{4}),([0-9]*.[0-9]{2})|\4,\5,,\6,\7,\8,\3-\2-\1|g" -e "s/^/upstox,/g"'
            DELIMITER ',' NULL ''
        ;

        copy
          portfolio (channel, market, code, isin, action, qty, price, date)
        from 
          PROGRAM 'cat $(pwd)/portfolio.upstox.cy.txt'
            DELIMITER ',' NULL ''
        ;

        /*Upstox - where stock is traded in both nse and bse, the code is bse. Where only traded in nse, then code is nse code*/

        update portfolio set isin = master.isin from master where master.market = 'bse' and portfolio.code = master.code and portfolio.isin is null;
        update portfolio set isin = master.isin from master where master.market = 'nse' and portfolio.code = master.code and portfolio.isin is null;
        update portfolio set code = master.code from master where portfolio.market = master.market and portfolio.isin = master.isin;

        /*update price to 10 where price is 0.0 */

        select * from portfolio where portfolio.price = 0.00;
        update portfolio set price = 0.01 where portfolio.price = 0.00;

        VACUUM analyze portfolio;
		
		select * from portfolio order by date desc fetch first 7 rows only;
EOF

  echo $(date)
    
# prepare upstox portfolio file

  for FILE in $(ls -x1 $(pwd)/in/portfolio/Trade_Report_106299_????_All.csv)
    do
      dos2unix $FILE
    done

  cat $(pwd)/in/portfolio/Trade_Report_106299_????_All.csv | grep -v "Scrip Code" \
    | sed -e 's/~//g' -e 's/^"//g' -e 's/"$//g' -e 's/","/~/g' -e 's/,//g' -e 's/~/,/g' \
    | grep -v ",,,,,,,,,,,," \
    | awk 'BEGIN { FS=","; OFS = "," }
      {
        CHANNEL     = "upstox"
        MARKET_RAW  = $4    # segment
        CODE        = $5    # scrip code
        ISIN        = ""
        ACTION_RAW  = $11   # side
        QTY         = $12
        PRICE       = $13
        DATE_RAW    = $1
      
        if ( MARKET_RAW == "NSE CM" )
          MARKET = "nse"
        else
          MARKET = "bse"
      
        if (ACTION_RAW == "S") 
            ACTION = "Sell"
        else
            ACTION = "Buy"
            
        split(DATE_RAW,DT,"/"); DATE_YYYYMMDD = DT[3] "-" DT[2] "-" DT[1]
        
        #print $0
        #print "x" $1 "x", $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "x" $13 "x"
        print CHANNEL, MARKET, CODE, ISIN, ACTION, QTY, PRICE, DATE_YYYYMMDD
      }' > portfolio.upstox.cy.txt

    psql -a --username=postgres --host=localhost --dbname=unity --file=tmp.$PID.$0.$MKT.upload.sql
    echo $(date)

#
    rm tmp.$PID.*