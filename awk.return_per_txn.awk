BEGIN { 
    OFS=",";
  }
  {

    SELL_STOCK = $1
    SELL_DATE  = SELL_DATE_RAW = $2
    SELL_QTY   = $4
    SELL_PRICE = $5
  
    gsub(/-/,"",SELL_DATE);
    BAL_SELL_QTY = SELL_QTY;
  
    #print "SELL",$0,"BAL_SELL_QTY",BAL_SELL_QTY,"BAL_BUY_QTY",BAL_BUY_QTY
    
    while ( BAL_SELL_QTY > 0 && BAL_BUY_QTY > 0 && SELL_STOCK == BUY_STOCK && BUY_DATE <= SELL_DATE)
      {
        if ( BAL_SELL_QTY >= BAL_BUY_QTY )
          {
            TXN_RET=((SELL_PRICE-BUY_PRICE)*100)/(BUY_PRICE)
            print SELL_STOCK,SELL_DATE_RAW,BAL_BUY_QTY,TXN_RET
            BAL_SELL_QTY=(BAL_SELL_QTY-BAL_BUY_QTY)
            BAL_BUY_QTY=0
          }
        else 
          # BAL_SELL_QTY < BAL_BUY_QTY
          {
            ALLOC_BUY_QTY=BAL_SELL_QTY
            BAL_SELL_QTY=0
            BAL_BUY_QTY=BAL_BUY_QTY-ALLOC_BUY_QTY
            TXN_RET=((SELL_PRICE-BUY_PRICE)*100)/(BUY_PRICE)
            print SELL_STOCK,SELL_DATE_RAW,ALLOC_BUY_QTY,TXN_RET
			next;
          }
	  }

    if ( SELL_STOCK != "" && SELL_STOCK < BUY_STOCK )
      {
        next;
      }
      
    while ( (getline < BUY_FILE ) != 0 ) 
      {
        BUY_STOCK = $1;
        BUY_DATE  = BUY_DATE_RAW = $2;
        BUY_QTY   = $4;
        BUY_PRICE = $5;
        
        gsub(/-/,"",BUY_DATE);
        BAL_BUY_QTY=BUY_QTY;
            
        #print "BUY",$0,"BAL_SELL_QTY",BAL_SELL_QTY,"BAL_BUY_QTY",BAL_BUY_QTY
        
        while ( BAL_SELL_QTY > 0 && BAL_BUY_QTY > 0 && SELL_STOCK == BUY_STOCK && BUY_DATE <= SELL_DATE )
          {
            if ( BAL_BUY_QTY <= BAL_SELL_QTY )
              {
                TXN_RET=((SELL_PRICE-BUY_PRICE)*100)/(BUY_PRICE)
                print SELL_STOCK,SELL_DATE_RAW,BAL_BUY_QTY,TXN_RET
                BAL_SELL_QTY=(BAL_SELL_QTY-BAL_BUY_QTY)
                BAL_BUY_QTY=0
              }
            else # BAL_BUY_QTY > BAL_SELL_QTY
              {
                ALLOC_BUY_QTY=BAL_SELL_QTY
                BAL_SELL_QTY=0
                BAL_BUY_QTY=BAL_BUY_QTY-ALLOC_BUY_QTY
                TXN_RET=((SELL_PRICE-BUY_PRICE)*100)/(BUY_PRICE)
                print SELL_STOCK,SELL_DATE_RAW,ALLOC_BUY_QTY,TXN_RET
              }
          }
  
        if ( BAL_SELL_QTY == 0)
          {
            break;
          }

        if ( BUY_STOCK <= SELL_STOCK )
          {
            continue;
          }

        if ( BUY_STOCK > SELL_STOCK  )
          {
            break;
          }
        }
  }
