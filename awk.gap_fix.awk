BEGIN { 
    EXIT = "N";
    OFS=",";
    ACTUALS_STOCK="";
    ACTUALS_DATE="";
    GAPS_STOCK_PREVIOUS="";
  }
  {
    GAPS_STOCK = $1 "," $2
    GAPS_DATE = GAPS_DATE_RAW  = $3
    gsub(/-/," ",GAPS_DATE); gsub(/:/," ",GAPS_DATE)

    #print "GAP/XXX", $0, "ACT", ACTUALS_STOCK, ACTUALS_DATE, ACTUALS_DATA
    
    if ( GAPS_STOCK == ACTUALS_STOCK && GAPS_DATE > ACTUALS_DATE )
      {
        print GAPS_STOCK, GAPS_DATE_RAW, ACTUALS_DATA;
        next;
      }

    if ( ACTUALS_STOCK != "" && GAPS_STOCK > ACTUALS_STOCK )
      {
        next;
      }
      
    while ( (getline < ACTUALS ) != 0 ) 
      {
        ACTUALS_STOCK = $1 "," $2;
        ACTUALS_DATE = ACTUALS_DATE_RAW = $3;
        gsub(/-/," ",ACTUALS_DATE); gsub(/:/," ",ACTUALS_DATE)
        
        ACTUALS_DATA  = $4 "," $5 "," $6 "," $7 "," 0 "," $9
    
        #print "ACT/GAP", GAPS_STOCK, GAPS_DATE, "ACT", ACTUALS_STOCK, ACTUALS_DATE, ACTUALS_DATA
        
        if ( GAPS_STOCK == ACTUALS_STOCK && GAPS_DATE > ACTUALS_DATE )
          {
            print GAPS_STOCK, GAPS_DATE_RAW, ACTUALS_DATA;
            break;
          }

        if ( GAPS_STOCK <= ACTUALS_STOCK )
          {
            continue;
          }

        if ( GAPS_STOCK > ACTUALS_STOCK )
          {
            break;
          }
        }
  }
