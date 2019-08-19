# !/bin/bash

  source sc.env.sh

# check and refresh masters
  case $MKT in
    idx)
      if [ ! -f $INS_FILE ]
        then
          echo $INS_FILE does not exist in present working directory
          exit 1
      fi
      ;;
    mf)
      # get fresh AMC_LIST file
        $WGET $WGETO_RWAIT -O tmp.$PID.$AMC_LIST 'http://www.amfiindia.com/nav-history-download'
        cat tmp.$PID.$AMC_LIST | grep "option value" \
          | grep -E -v "Ended Schemes|Interval Fund|Hard Copy|Email|NavDownMFName|NavDownType" \
          | sed -Ee 's|</option>|"|g' -e 's|<option value=||g' -e 's|>|,"|g' -e "s|^|\"$MKT\",|g" | \
          $SORT > tmp.$PID.$AMC_LIST.sorted
        mv tmp.$PID.$AMC_LIST.sorted $AMC_LIST
      # refresh instrument list (mutual funds, stocks) list if it is more than 28 days old
        if [ ! -f $INS_FILE ] || [[ $(date --reference=$INS_FILE +%Y%m%d) -lt  $(date --date="$(date) - 9 day" +%Y%m%d) ]]
          then
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
          fi

        #if [ ! -f $INS_FILE ] || [[ $(date --reference=$INS_FILE +%Y%m%d) -lt  $(date --date="$(date) - 28 day" +%Y%m%d) ]]
        #  then
        #    $WGET -O tmp.$PID.$INS_FILE.downloaded http://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0
        #    mv tmp.$PID.$INS_FILE.downloaded $INS_FILE.downloaded
        #    cat $INS_FILE.downloaded | grep --fixed-strings ',N,' |  awk -F ',' '{ OFS = "," } { print $2, $6, $2, $4, $5 }' | \
        #      grep -vE "^Code" | sed -Ee "s|^|$MKT,|g" -e 's|,|","|g' -e 's|^|"|g' -e 's|$|"|g' | \
        #      $SORT > tmp.$PID.$INS_FILE.sorted
        #    mv tmp.$PID.$INS_FILE.sorted $INS_FILE
        #fi
      ;; 
    nse)
      $WGET $WGETO_RWAIT https://www.nseindia.com/content/equities/EQUITY_L.csv -O tmp.$PID.$INS_FILE
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
        | $SORT -u > tmp.$PID.bse.instrument.scrip.txt
      mv tmp.$PID.bse.instrument.scrip.txt bse.instrument.scrip.txt
      
      cp ListOfScrips.csv tmp.$PID.ListOfScrips.csv
      iconv --from-code=UTF8 --to-code='US-ASCII'//TRANSLIT --verbose --output=ListOfScrips.csv tmp.$PID.ListOfScrips.csv

      cat ListOfScrips.csv | grep -v "Security Code" \
	    | sed -e "s|^|bse,|g" -e "s|\&amp;|\&|g" -e "s|-\\$||g" -e "s|,|\",\"|g"  -e 's|\(.*\)|"\1"|g' \
        | gawk -F, '{gsub(" ","",$6); print $1 FS $2 FS $4 FS $8 FS $6 FS "\42\42"}' \
        | $SORT -u > tmp.$PID.bse.instrument.listofscrips.txt
      mv tmp.$PID.bse.instrument.listofscrips.txt bse.instrument.listofscrips.txt

    #cat ListOfScrips.csv | sed -e "s|^|bse,|g" -e "s|\&amp;|\&|g" -e "s|-\\$||g" -e "s|,|\",\"|g"  -e 's|\(.*\)|"\1"|g' | $SORT > tmp.$PID.bse.listofscrips.csv
    #mv tmp.$PID.bse.listofscrips.csv bse.listofscrips.csv
    #cat bse.listofscrips.csv | cut -d',' -f1,2,4 | $SORT -u > tmp.${PID}.bse.listofscrips.csv
    #cat bse.instrument.scrip.txt | $SORT -u > tmp.$PID.bse.instrument.scrip.txt
    #join -1 2 -2 2 -t, -o 1.1,1.2,2.3 tmp.$PID.bse.instrument.scrip.txt tmp.${PID}.bse.listofscrips.csv > bse.instrument.scrip.txt
    #rm tmp.$PID.bse.instrument.scrip.txt
    
      #$WGET 'https://finance.google.com/finance?output=json&q=%5B+%28exchange+%3D%3D+%22BOM%22%29+%5D&restype=company&noIL=1&num=7000' \
      #  -O tmp.$PID.$INS_FILE
      #cat tmp.$PID.$INS_FILE | \
      #  grep -E '"title" :|"ticker" :|}' | tr -d "\n" | \
      #  sed -e 's|}|\n|g' -e 's|"title" : ||g' -e 's|"ticker" : ||g' -e 's|,$||g' | \
      #  sed -r -e 's|^"(.*)","(.*)",$|"\2","\1","\2"|' -e '/^$/d' -e "s/^/\"$MKT\",/g" -e 's|$|,,|g' | $SORT > tmp.$PID.$INS_FILE.sorted
      #mv tmp.$PID.$INS_FILE.sorted $INS_FILE
      #mv tmp.$PID.$INS_FILE $INS_FILE.downloaded
      : #do nothing, master is created from bhavcopy files in sc.process_convert.sh
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
          #  -e "s|(.*),(.*),(.*$)|$WGET $WGETO_RWAIT -O ./in/\1.\2.${FM_YMD}_${TO_YMD}.out \'${URL_GOOFIN}\2\&x=\3\&i=86400\&p=$PERIOD_INTERVAL\&f=d,o,h,l,c,v\'|" >> tmp.$PID.$URL_FILE
        else
          #https://www.nseindia.com/content/indices/ind_close_all_25012016.csv
          FD=$(date --date="${FMDATE}" +%s)
          TD=$(date --date="${TODATE}" +%s)
          gawk -F ',' -v FD="${FD}" -v TD="${TD}" \
            'BEGIN {for (i=FD;i<=TD;i+=60*60*24) print strftime("%Y-%m-%d",i)}' > tmp.$PID.$MKT.dwn_dates.txt
          cat tmp.$PID.$MKT.dwn_dates.txt | tr '-' ' ' | \
            gawk -v MKT=${MKT} '{ OFS = "," } 
              {
                YYYY=$1
                MM=$2
                DD=$3
                print "$WGET $WGETO_RWAIT -O ./in/" MKT "." YYYY MM DD ".csv https://www.nseindia.com/content/indices/ind_close_all_" DD MM YYYY ".csv"
              }' >> tmp.$PID.$URL_FILE
      fi
      ;;

#      while [[ $(date -d "$TODATE" +%Y%m%d) -ge $(date -d "$FMDATE_INC" +%Y%m%d) ]]
#        do
#          FM_YMD=$(date --date="$FMDATE_INC" +%Y%m%d)
#          TO_YMD=$(date --date="$TODATE_INC" +%Y%m%d)
#          FM_DMY=$(date --date="$FMDATE_INC" +%d-%m-%Y)
#          TO_DMY=$(date --date="$TODATE_INC" +%d-%m-%Y)
#        
#          cat $INS_FILE | gawk -F ',' -v MKT=$MKT -v FD=$FM_DMY -v TD=$TO_DMY -v DR=${FM_YMD}_${TO_YMD} -v WG="${WGET}" -v WGO="${WGETO_RWAIT}" \
#            '{
#                STK_20=STK_RAW=STK_TRIM=$1;\
#                gsub(" ","",STK_TRIM); gsub(" ","%20",STK_20); \
#                print WG WGO  \
#                "-O ./in/" MKT "." STK_TRIM "." DR ".out " \
#                "\x27" \
#                "https://www.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=" \
#                STK_20 "&fromDate=" FD "&toDate=" TD \
#                "\x27"
#              }' \
#              >> tmp.$PID.$URL_FILE
#        
#          FMDATE_INC=$(date --date="$TODATE_INC + 1 days")
#          TODATE_INC_PLUS_6M=$(date --date="$FMDATE_INC + 6 months - 1 day")
#            
#          if [[ $(date --date="$TODATE" +%Y%m%d) -lt $(date --date="$TODATE_INC_PLUS_6M" +%Y%m%d) ]]
#            then TODATE_INC=$TODATE
#            else TODATE_INC=$TODATE_INC_PLUS_6M
#          fi
#        done
#      ;;
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
          FMDATE_INC=$FMDATE
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
          
          cat $AMC_LIST | gawk -F ',' -v MKT=$MKT -v FD=$FM_DMY -v TD=$TO_DMY -v DR=${FM_YMD}_${TO_YMD} -v WG="${WGET}" -v WGO="${WGETO_RWAIT}" \
            '{
                MF=$2;           \
                gsub("\"","",MF); \
                print WG WGO     \
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
            -e "s|(.*),(.*),(.*$)|$WGET $WGETO_RWAIT -O ./in/\1.\2.${FM_YMD}_${TO_YMD}.out \'${URL_GOOFIN}\2\&x=\3\&i=86400\&p=$PERIOD_INTERVAL\&f=d,o,h,l,c,v\'|" >> tmp.$PID.$URL_FILE
        else
          FD=$(date --date="${FMDATE}" +%s)
          TD=$(date --date="${TODATE}" +%s)
          gawk -F ',' -v FD="${FD}" -v TD="${TD}" \
            'BEGIN {for (i=FD;i<=TD;i+=60*60*24) print strftime("%Y-%m-%d",i)}' \
            | grep -F "$(cut -d, -f3 $IDX_FILE | $SORT -u | tail -$(( $PERIOD_INTERVAL + $BCKTRK_DAYS )))" > tmp.$PID.$MKT.dwn_dates.txt
          date +%d-%b-%Y -f tmp.$PID.$MKT.dwn_dates.txt | tr [a-z] [A-Z] | tr '-' ' ' | \
            gawk -v MKT=${MKT} '{ OFS = "," } 
              {
                DD=$1
                MMM=$2
                YYYY=$3
                print "$WGET $WGETO_RWAIT -O ./in/" MKT "." YYYY MMM DD ".zip https://www.nseindia.com/content/historical/EQUITIES/" YYYY "/" MMM "/cm" DD MMM YYYY "bhav.csv.zip"
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
            -e "s|(.*),(.*),(.*$)|$WGET $WGETO_RWAIT -O ./in/\1.\2.${FM_YMD}_${TO_YMD}.out \'${URL_GOOFIN}\2\&x=\3\&i=86400\&p=$PERIOD_INTERVAL\&f=d,o,h,l,c,v\'|" >> tmp.$PID.$URL_FILE
        else
          FD=$(date --date="${FMDATE}" +%s)
          TD=$(date --date="${TODATE}" +%s)
          gawk -F ',' -v FD="${FD}" -v TD="${TD}" \
            'BEGIN {for (i=FD;i<=TD;i+=60*60*24) print strftime("%d%m%y",i)}' \
            | grep -F "$(cut -d',' -f3 $IDX_FILE | $SORT -u | tail -$(( $PERIOD_INTERVAL + $BCKTRK_DAYS )) \
            | sed -r 's|([0-9]{2})([0-9]{2})-(.*)-(.*)|\4\3\2|g')" > tmp.$PID.$MKT.dwn_dates.txt
          gawk -v gMKT=${MKT} '{ OFS = "," } {print "$WGET $WGETO_RWAIT -O ./in/" gMKT "." $1 ".zip http://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_" $1 ".zip"}' tmp.$PID.$MKT.dwn_dates.txt >> tmp.$PID.$URL_FILE
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