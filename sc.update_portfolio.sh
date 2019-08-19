# !/bin/bash

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
				VACUUM analyze portfolio;
EOF

# execute script	
	echo $(date)
	psql -a --username=postgres --host=localhost --dbname=unity --file=tmp.$PID.$0.$MKT.upload.sql
	echo $(date)

#
	rm tmp.$PID.*