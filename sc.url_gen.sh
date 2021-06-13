# !/bin/bash

  source sc.env.sh

# check and refresh masters
  case $MKT in
    idx)
	  # check from file existance
      if [ ! -f $INS_FILE ]
        then
          echo $INS_FILE does not exist in present working directory
          exit 1
      fi
      #cat - >> tmp.$PID.$0.$MKT.gen_master.sql <<- EOF
      #  \timing on
      #  select * from master where market = 'idx';
      #  delete   from master where market = 'idx';
      #  insert   into master (market,code,name,isin,type,category,sector,subsector)
      #  select 
      #    distinct
      #    'idx',code,code,code,code,code,code,code
      #  from
      #    hist
      #  where
      #    market = 'idx';
		#EOF
	  #
      ## execute script  
      #echo $(date)
      #psql -a --username=postgres --host=localhost --dbname=unity --file=tmp.$PID.$0.$MKT.gen_master.sql
      #echo $(date)
	  #
	  #rm tmp.$PID.$0.$MKT.gen_master.sql
      ;;
    mf)
      # get fresh AMC_LIST file
        $WGET --no-check-certificate -O tmp.$PID.$AMC_LIST 'http://www.amfiindia.com/nav-history-download'
        cat tmp.$PID.$AMC_LIST | grep "option value" \
          | grep -E -v "Ended Schemes|Interval Fund|Hard Copy|Email|NavDownMFName|NavDownType" \
          | sed -Ee 's|</option>|"|g' -e 's|<option value=||g' -e 's|>|,"|g' -e "s|^|\"$MKT\",|g" | \
          $SORT > tmp.$PID.$AMC_LIST.sorted
        mv tmp.$PID.$AMC_LIST.sorted $AMC_LIST
      # refresh instrument list (mutual funds, stocks) list if it is more than 28 days old
        #if [ ! -f $INS_FILE ] || [[ $(date --reference=$INS_FILE +%Y%m%d) -lt  $(date --date="$(date) - 9 day" +%Y%m%d) ]]
        #  then
            $WGET -O tmp.$PID.$INS_FILE.downloaded http://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0
            mv tmp.$PID.$INS_FILE.downloaded $INS_FILE.downloaded
            cat $INS_FILE.downloaded | grep --fixed-strings ',Open Ended,' | tr -d '\015' | \
              gawk -F ',' '{ OFS = "," } 
                {
                  if ( $10 != "")
                    print $2,$6,$10,$4,$5
                  else
                    print $2,$6,$2 ,$4,$5
                }' | \
              grep -vE "^Code" | sed -Ee "s|^|$MKT,|g" -e 's|,|","|g' -e 's|^|"|g' -e 's|$|"|g' | $SORT > tmp.$PID.$INS_FILE.sorted
            mv tmp.$PID.$INS_FILE.sorted $INS_FILE
         # fi
      ;; 
    nse)
      $WGET  https://archives.nseindia.com/content/equities/EQUITY_L.csv -O tmp.$PID.$INS_FILE
      iconv --from-code=WINDOWS-1252 --to-code='US-ASCII'//TRANSLIT --verbose --output=tmp.$PID.$INS_FILE.iconv.out tmp.$PID.$INS_FILE
      cat tmp.$PID.$INS_FILE.iconv.out | grep -E ',EQ,|,BE,|,BZ,' | cut -d',' -f1,2,3,7 | gawk -F "," '{OFS=","} {print $1,$2,$4,$3}' | \
        sed -e "s|^|$MKT,|g" -e 's|,|","|g' -e 's|^|"|g' -e 's|$|",""|g' | $SORT -u > tmp.$PID.$INS_FILE.sorted
      mv tmp.$PID.$INS_FILE.sorted $INS_FILE
      mv tmp.$PID.$INS_FILE $INS_FILE.downloaded
      ;;
    bse)
      $WGET https://www.bseindia.com/downloads/Help/file/scrip.zip -O tmp.$PID.bse.instrument.scrip.zip
      SCRIP_FILE=$(unzip -l tmp.$PID.bse.instrument.scrip.zip | grep SCRIP_ | awk '$1=$1' | cut -d' ' -f4)
      unzip -p tmp.$PID.bse.instrument.scrip.zip "${SCRIP_FILE}" \
        | gawk -F"|" -v OFS=\",\" '{if ($6 == "E") {print "\42" "bse" OFS $1 OFS $4 OFS $18 OFS $7 OFS "\42"}}' \
        | grep -v ",\"E\",|,\"F\",|,\"G\",|,\"GC\",|,\"IF\",|,\"IT\"," \
        | $SORT -u > tmp.$PID.bse.instrument.scrip.txt
      cat tmp.$PID.bse.instrument.scrip.txt | grep -Fv ",\"," > bse.instrument.scrip.txt
      rm tmp.$PID.bse.instrument.scrip.txt
      
	  # ListOfScrips.csv is manually downloaded from https://www.bseindia.com/corporates/List_Scrips.aspx
      cp ListOfScrips.csv tmp.$PID.ListOfScrips.csv
      iconv --from-code=UTF8 --to-code='US-ASCII'//TRANSLIT --verbose --output=ListOfScrips.csv tmp.$PID.ListOfScrips.csv

      cat ListOfScrips.csv | grep -v "Security Code" | grep ",Active," | grep ",Equity" \
        | sed -e "s| ,|,|g" -e "s|^|bse,|g" -e "s|\&amp;|\&|g" -e "s|-\\$||g" -e "s|,|\",\"|g"  -e 's|\(.*\)|"\1"|g' \
        | gawk -F, '{gsub(" ","",$6); print $1 FS $2 FS $5 FS $9 FS $7 FS "\42\42"}' \
        | grep -v ",\"E\",|,\"F\",|,\"G\",|,\"GC\",|,\"IF\",|,\"IT\"," \
        | $SORT -u > tmp.$PID.bse.instrument.listofscrips.txt
      mv tmp.$PID.bse.instrument.listofscrips.txt bse.instrument.listofscrips.txt

      #additional master data recrods are created from bhavcopy files in sc.process_convert.sh
      ;;
    *)
      echo "Error MKT = " $MKT
      exit 1
      ;;
  esac

# decide and then get either full or incremental data

  CURR_YEAR_ARCH="./arch/$MKT.raw.$(date +%Y).txt"
  PREV_YEAR_ARCH="./arch/$MKT.raw.$(($(date +%Y) - 1)).txt"

  TODATE=$(date)
  if [ -s $CURR_YEAR_ARCH ]
    then 
      FMDATE=$(date --date="$(cat $CURR_YEAR_ARCH | cut -d',' -f3 | $SORT -u | tail -1 | sed 's;-;;g') - $BCKTRK_DAYS days")
    else
      if [ -s $PREV_YEAR_ARCH ]
        then 
          FMDATE=$(date --date="$(cat $PREV_YEAR_ARCH | cut -d',' -f3 | $SORT -u | tail -1 | sed 's;-;;g') - $BCKTRK_DAYS days")
        else 
          FMDATE=$(date --date=$(date --date="$TODATE - 11 years" +%Y)0101)
      fi
  fi

  # initialize dates & create url file
  case $MKT in
    idx)
      TO_DMY=$(date --date="$TODATE" +%d%m%Y)
      FM_DMY=$(date --date="$FMDATE" +%d%m%Y)
      PERIOD_INTERVAL=$(( (($(date --date="$TODATE" +%s) - $(date --date="$FMDATE" +%s)) / (60*60*24)) + 1 ))
      if [[ $PERIOD_INTERVAL -gt 183 ]]
        then
          PERIOD_INTERVAL="11Y"
          print "cannot handle the situation, exit 0"
          exit 0
          #cut -d',' -f1,2 $INS_FILE | tr -d '"' | sed -re "s|(bse),(.*)|\1,\2,BOM|g" -e "s|(nse),(.*)|\1,\2,NSE|g" \
          #  -e "s|(.*),(.*),(.*$)|$WGET $WGETO_RWAIT --user-agent=${WGET_USER_AGENT} -O ./in/\1.\2.${FM_YMD}_${TO_YMD}.out \'${URL_GOOFIN}\2\&x=\3\&i=86400\&p=$PERIOD_INTERVAL\&f=d,o,h,l,c,v\'|" >> tmp.$PID.$URL_FILE
        else
          #https://www.nseindia.com/content/indices/ind_close_all_25012016.csv
          FD=$(date --date="${FMDATE}" +%s)
          TD=$(date --date="${TODATE}" +%s)
          gawk -F ',' -v FD="${FD}" -v TD="${TD}" \
            'BEGIN {for (i=FD;i<=TD;i+=60*60*24) print strftime("%Y-%m-%d",i)}' > tmp.$PID.$MKT.dwn_dates.txt
          cat tmp.$PID.$MKT.dwn_dates.txt | tr '-' ' ' | \
            gawk -v MKT=${MKT} -v WG="${WGET}" '{ OFS = "," } 
              {
                YYYY=$1
                MM=$2
                DD=$3
                print WG "-O ./in/" MKT "." YYYY MM DD ".csv https://www.niftyindices.com/Daily_Snapshot/ind_close_all_" DD MM YYYY ".csv"
              }' >> tmp.$PID.$URL_FILE
      fi
      ;;

    mf)
      # for MF's AMFI site requires downloads to be < 365 days, we have set this to 6m
      if [[ $(date --date="$FMDATE + 6 months" +%Y%m%d) -lt $(date --date="$TODATE" +%Y%m%d) ]]
        then 
          FMDATE_INC=$FMDATE
          if [[ $(date --date="$FMDATE" +%m) -le "06" ]]
            then
              TODATE_INC=$(date --date=$(date --date="$FMDATE" +%Y)0630)
            else
              TODATE_INC=$(date --date=$(date --date="$FMDATE" +%Y)1231)
          fi
        else
          #FMDATE_INC=$FMDATE
		  FMDATE_INC=$(date --date="$(date --date="$FMDATE -  3 days")") # additional 3 days hist for mf
          TODATE_INC=$TODATE
      fi
  
      # create url file
      while [[ $(date -d "$TODATE" +%Y%m%d) -gt $(date -d "$FMDATE_INC" +%Y%m%d) ]]
        do
          TO_YMD=$(date --date="$TODATE_INC" +%Y%m%d)
          FM_YMD=$(date --date="$FMDATE_INC" +%Y%m%d)
          TO_DMY=$(date --date="$TODATE_INC" +%d-%b-%Y)
          FM_DMY=$(date --date="$FMDATE_INC" +%d-%b-%Y)
      
          #cat $AMC_LIST | gawk -F ',' -v FD=$FM_DMY -v TD=$TO_DMY \
          #  '{
          #      MF=$2; \
          #      print "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=" MF "&tp=1&frmdt=" FD "&todt=" TD \
          #    }' \
          #    >> tmp.$PID.$URL_FILE
          
          cat $AMC_LIST | gawk -F ',' -v MKT=$MKT -v FD=$FM_DMY -v TD=$TO_DMY -v DR=${FM_YMD}_${TO_YMD} -v WG="${WGET}" \
            '{
                MF=$2;            \
                gsub("\"","",MF); \
                print WG          \
                "-O ./in/" MKT "." MF "." DR ".out " \
                "\x27" \
                "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=" MF "&tp=1&frmdt=" FD "&todt=" TD \
                "\x27"
              }' \
              >> tmp.$PID.$URL_FILE
      
          FMDATE_INC=$(date --date="$TODATE_INC + 1 days")
          TODATE_INC_PLUS_6M=$(date --date="$FMDATE_INC + 6 months - 1 day")
            
          if [[ $(date --date="$TODATE" +%Y%m%d) -lt $(date --date="$TODATE_INC_PLUS_6M" +%Y%m%d) ]]
            then TODATE_INC=$TODATE
            else TODATE_INC=$TODATE_INC_PLUS_6M
          fi
        done
      ;;
    nse)
      TO_YMD=$(date --date="$TODATE" +%Y%m%d)
      FM_YMD=$(date --date="$FMDATE" +%Y%m%d)
      PERIOD_INTERVAL=$(( (($(date --date="$TODATE" +%s) - $(date --date="$FMDATE" +%s)) / (60*60*24)) + 1 ))
      if [[ $PERIOD_INTERVAL -gt 183 ]]
        then
          PERIOD_INTERVAL="11Y"
          cut -d',' -f1,2 $INS_FILE | tr -d '"' | sed -re "s|(bse),(.*)|\1,\2,BOM|g" -e "s|(nse),(.*)|\1,\2,NSE|g" \
            -e "s|(.*),(.*),(.*$)|$WGET -O ./in/\1.\2.${FM_YMD}_${TO_YMD}.out \'${URL_GOOFIN}\2\&x=\3\&i=86400\&p=$PERIOD_INTERVAL\&f=d,o,h,l,c,v\'|" >> tmp.$PID.$URL_FILE
        else
          FD=$(date --date="${FMDATE}" +%s)
          TD=$(date --date="${TODATE}" +%s)
          gawk -F ',' -v FD="${FD}" -v TD="${TD}" \
            'BEGIN {for (i=FD;i<=TD;i+=60*60*24) print strftime("%Y-%m-%d",i)}' \
            | grep -F "$(cut -d, -f3 $IDX_FILE | $SORT -u | tail -$(( $PERIOD_INTERVAL + $BCKTRK_DAYS )))" > tmp.$PID.$MKT.dwn_dates.txt
          date "+%d %b %Y" -f tmp.$PID.$MKT.dwn_dates.txt | \
            gawk -v MKT=${MKT} -v WG="${WGET}" '{ OFS = "," } 
              {
                DD=$1
                Mmm=$2
                MMM=toupper(Mmm)
                YYYY=$3

                #print WG "-O ./in/" MKT "." YYYY MMM DD ".zip https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22CM%20-%20Bhavcopy%28csv%29%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%5D\\&date="DD "-" Mmm "-" YYYY
				#https://archives.nseindia.com/content/historical/EQUITIES/2020/SEP/cm21SEP2020bhav.csv.zip
				print WG "-O ./in/" MKT "." YYYY MMM DD ".zip https://archives.nseindia.com/content/historical/EQUITIES/" YYYY "/" MMM "/cm" DD MMM YYYY "bhav.csv.zip"
              }' >> tmp.$PID.$URL_FILE
      fi
      ;;
    bse)
      TO_YMD=$(date --date="$TODATE" +%Y%m%d)
      FM_YMD=$(date --date="$FMDATE" +%Y%m%d)
      PERIOD_INTERVAL=$(( (($(date --date="$TODATE" +%s) - $(date --date="$FMDATE" +%s)) / (60*60*24)) + 1 ))
      if [[ $PERIOD_INTERVAL -gt 183 ]]
        then
          PERIOD_INTERVAL="11Y"
          cut -d',' -f1,2 $INS_FILE | tr -d '"' | sed -re "s|(bse),(.*)|\1,\2,BOM|g" -e "s|(nse),(.*)|\1,\2,NSE|g" \
            -e "s|(.*),(.*),(.*$)|$WGET -O ./in/\1.\2.${FM_YMD}_${TO_YMD}.out \'${URL_GOOFIN}\2\&x=\3\&i=86400\&p=$PERIOD_INTERVAL\&f=d,o,h,l,c,v\'|" >> tmp.$PID.$URL_FILE
        else
          FD=$(date --date="${FMDATE}" +%s)
          TD=$(date --date="${TODATE}" +%s)
          gawk -F ',' -v FD="${FD}" -v TD="${TD}" \
            'BEGIN {for (i=FD;i<=TD;i+=60*60*24) print strftime("%d%m%y",i)}' \
            | grep -F "$(cut -d',' -f3 $IDX_FILE | $SORT -u | tail -$(( $PERIOD_INTERVAL + $BCKTRK_DAYS )) \
            | sed -r 's|([0-9]{2})([0-9]{2})-(.*)-(.*)|\4\3\2|g')" > tmp.$PID.$MKT.dwn_dates.txt
          # 17 april 2020, EQ_ISINCODE changed to EQ_
          gawk -v gMKT=${MKT} -v WG="${WGET}" '{ OFS = "," } {print WG "-O ./in/" gMKT "." $1 ".zip https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_" $1 ".zip"}' tmp.$PID.$MKT.dwn_dates.txt >> tmp.$PID.$URL_FILE
      fi
      ;;
    *)
      echo "Error MKT = " $MKT
      exit 1
      ;;
  esac
  
  $SORT tmp.$PID.$URL_FILE > tmp.$PID.sorted.$URL_FILE
  mv tmp.$PID.sorted.$URL_FILE $URL_FILE
  
  rm tmp.$PID.*