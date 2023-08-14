from app import DB
# VARIABLES
# AT, RH, BARO, DP, AT2, QNH, RAIN, WDI, WSI, GUST, GUSTDIR, QFE, QFF, WDA, WSA, 
# BATTERY, IR, UV, VIS, SLP, SP, ATMAX, ATMIN, SOLRAD, DAYRAIN, SOLRAD_ACC, HOURS_OF_SUN,
# CFB, PLS, RLS, RAIN10M, WL, BV, WL2, ITEMP, BVS, COND, TURB, DO, WTEMP, pH, RAIN05M, LEVEL, LEVEL2, SAL, WL1, QNH_inHg
# getDataFor =  # ,'RAIN':'mm'
# {"param":"AT","unit":"℃","func":"nanmean"},{"param":"ATMAX","unit":"℃","func":"nanmean"},{"param":"ATMIN","unit":"℃","func":"nanmean"},{"param":"WDA","unit":"°","func":"circmean"},{"param":"WSA","unit":"KT","func":"nanmean"},{"param":"RH","unit":"%","func":"nanmean"}
# {"param":"","unit":"","func":""}

if __name__ == '__main__':
    db = DB()

    dbName      = "Server_complete_11082023.db"
    connected   = db.connect(dbName) 

    if connected:
        db.test()
         
        # sq = db.getDatasets({'AT':'℃','ATMAX':'℃','ATMIN':'℃','WDA':'°','WSA':'KT','RH':'%'})        
        # db.dailyAvg(sq,[{"param":"WDA","unit":"°","func":"circmean"},{"param":"WSA","unit":"KT","func":"nanmean"}])            
        


        AT = db.Temperature({'variable':'AT','unit':'℃'})
        # db.TemperatureMax({'variable':'ATMAX','unit':'℃'})
        # db.TemperatureMin({'variable':'ATMIN','unit':'℃'}) 
        AT_MAX_MIN = db.TemperatureMaxAndMin([{"param":"ATMAX","unit":"℃","func":"max"},{"param":"ATMIN","unit":"℃","func":"min"}])  
        WSA = db.WindSpeed({'variable':'WSA','unit':'KT'})
        WDA = db.WindDirection({'variable':'WDA','unit':'°'})
        RAIN = db.Rainfall({'variable':'RAIN','unit':'mm'})
        RH = db.RelativeHumidity({'variable':'RH','unit':'%'})

        db.DownTime([AT,WSA,WDA,RAIN,RH])

    else:
        print(f"Unable to connect to {dbName} \nExiting!!!")
        exit()
    
    