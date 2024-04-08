import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,when, explode, monotonically_increasing_id,regexp_replace, col

def newest(path: str) -> str:
    '''Finds latest created file with links to scrape'''

    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files if (basename.endswith('.parquet') and 'preprocessed' not in basename)]
    return max(paths, key=os.path.getctime)

def remove_accents(input_text):
    '''Removes non latin characters
    '''
    strange='ŮôῡΒძěἊἦëĐᾇόἶἧзвŅῑἼźἓŉἐÿἈΌἢὶЁϋυŕŽŎŃğûλВὦėἜŤŨîᾪĝžἙâᾣÚκὔჯᾏᾢĠфĞὝŲŊŁČῐЙῤŌὭŏყἀхῦЧĎὍОуνἱῺèᾒῘᾘὨШūლἚύсÁóĒἍŷöὄЗὤἥბĔõὅῥŋБщἝξĢюᾫაπჟῸდΓÕűřἅгἰშΨńģὌΥÒᾬÏἴქὀῖὣᾙῶŠὟὁἵÖἕΕῨčᾈķЭτἻůᾕἫжΩᾶŇᾁἣჩαἄἹΖеУŹἃἠᾞåᾄГΠКíōĪὮϊὂᾱიżŦИὙἮὖÛĮἳφᾖἋΎΰῩŚἷРῈĲἁéὃσňİΙῠΚĸὛΪᾝᾯψÄᾭêὠÀღЫĩĈμΆᾌἨÑἑïოĵÃŒŸζჭᾼőΣŻçųøΤΑËņĭῙŘАдὗპŰἤცᾓήἯΐÎეὊὼΘЖᾜὢĚἩħĂыῳὧďТΗἺĬὰὡὬὫÇЩᾧñῢĻᾅÆßшδòÂчῌᾃΉᾑΦÍīМƒÜἒĴἿťᾴĶÊΊȘῃΟúχΔὋŴćŔῴῆЦЮΝΛῪŢὯнῬũãáἽĕᾗნᾳἆᾥйᾡὒსᾎĆрĀüСὕÅýფᾺῲšŵкἎἇὑЛვёἂΏθĘэᾋΧĉᾐĤὐὴιăąäὺÈФĺῇἘſგŜæῼῄĊἏØÉПяწДĿᾮἭĜХῂᾦωთĦлðὩზკίᾂᾆἪпἸиᾠώᾀŪāоÙἉἾρаđἌΞļÔβĖÝᾔĨНŀęᾤÓцЕĽŞὈÞუтΈέıàᾍἛśìŶŬȚĳῧῊᾟάεŖᾨᾉςΡმᾊᾸįᾚὥηᾛġÐὓłγľмþᾹἲἔбċῗჰხοἬŗŐἡὲῷῚΫŭᾩὸùᾷĹēრЯĄὉὪῒᾲΜᾰÌœĥტ /.'
    ascii_replacements='UoyBdeAieDaoiiZVNiIzeneyAOiiEyyrZONgulVoeETUiOgzEaoUkyjAoGFGYUNLCiIrOOoqaKyCDOOUniOeiIIOSulEySAoEAyooZoibEoornBSEkGYOapzOdGOuraGisPngOYOOIikoioIoSYoiOeEYcAkEtIuiIZOaNaicaaIZEUZaiIaaGPKioIOioaizTIYIyUIifiAYyYSiREIaeosnIIyKkYIIOpAOeoAgYiCmAAINeiojAOYzcAoSZcuoTAEniIRADypUitiiIiIeOoTZIoEIhAYoodTIIIaoOOCSonyKaAsSdoACIaIiFIiMfUeJItaKEISiOuxDOWcRoiTYNLYTONRuaaIeinaaoIoysACRAuSyAypAoswKAayLvEaOtEEAXciHyiiaaayEFliEsgSaOiCAOEPYtDKOIGKiootHLdOzkiaaIPIIooaUaOUAIrAdAKlObEYiINleoOTEKSOTuTEeiaAEsiYUTiyIIaeROAsRmAAiIoiIgDylglMtAieBcihkoIrOieoIYuOouaKerYAOOiaMaIoht___'
    if input_text:
        translator=str.maketrans(strange,ascii_replacements)
        return input_text.translate(translator)

def main():
    '''Main function
    '''
    with SparkSession.builder.appName("House_prices").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate() as session:
        file=newest("./data/ads/")
        df=session.read.parquet(file)
        df=df.withColumn("id",monotonically_increasing_id())
        df=df.toDF(*[remove_accents(column) for column in df.columns])
        columns=['id','powierzchnia', 'forma_wlasnosci', 'liczba_pokoi', 'stan_wykonczenia', 'pietro', 'balkon___ogrod___taras',
        'czynsz', 'miejsce_parkingowe', 'ogrzewanie', 'rynek', 'dostepne_od', 'rok_budowy',
        'rodzaj_zabudowy', 'okna', 'winda', 'media', 'zabezpieczenia', 'wyposazenie', 'informacje_dodatkowe',
        'material_budynku', 'adres', 'opis', 'ad_photo', 'price_currency',
        'ad_price', 'city_id', 'poster_type', 'region_id',  'subregion_id', 'latitude', 'longitude']

        df=df.select(columns)

        df=df.na.drop(subset='ad_price').filter(col('price_currency').contains('PLN'))\
            .withColumn("ktore_pietro", when(df["pietro"].contains("/"), split(df["pietro"], "/")[0])\
                                    .otherwise(df["pietro"])).withColumn("ile_pieter", when(df["pietro"].contains("/"), split(df["pietro"], "/")[1])\
                                                                                        .otherwise(None))\
            .withColumn('ktore_pietro', when((col('ktore_pietro') == 'Parter') | (col('ktore_pietro') == 'parter'), 0)\
                        .otherwise(when((col('ktore_pietro')=='Poddasze')|(col('ktore_pietro')=='poddasze'), 15)\
                                   .otherwise(when((col('ktore_pietro')=='Suterena')|(col('ktore_pietro')=='suterena'), -1)\
                                              .otherwise(when(col('ktore_pietro')=='Zapytaj', None)\
                                                         .otherwise(col('ktore_pietro'))))))\
            .withColumn('ktore_pietro', regexp_replace(col('ktore_pietro'),' ','').cast('integer'))\
            .drop('pietro')\
            .withColumn('powierzchnia', regexp_replace(regexp_replace(col('powierzchnia'),',','.'),' m²', '').cast('float'))\
            .withColumn('forma_wlasnosci', when(col('forma_wlasnosci')=='Zapytaj', None).otherwise(col('forma_wlasnosci')))\
            .withColumn('liczba_pokoi', when(col('liczba_pokoi')=='Zapytaj',None)\
                        .otherwise(col('liczba_pokoi')))\
            .withColumn('liczba_pokoi', regexp_replace(regexp_replace(col('liczba_pokoi'),' ',''),'więcejniż10','11'))\
            .withColumn('stan_wykonczenia', when(col('stan_wykonczenia')=='Zapytaj', None).otherwise(col('stan_wykonczenia')))\
            .withColumn('stan_wykonczenia', when(col('stan_wykonczenia').isNotNull(), col('stan_wykonczenia'))\
                    .otherwise(when(col('opis').like("%do remontu%") & col('stan_wykonczenia').isNull(), "do remontu")\
                                .otherwise(when(col('opis').like("%do zamieszkania%") & col('stan_wykonczenia').isNull(), "do zamieszkania")\
                                            .otherwise(when(col('opis').like("%do wykonczenia%") & col('stan_wykonczenia').isNull(), "do wykończenia")))))\
            .join(df.withColumn('tmp',explode(split(col("balkon___ogrod___taras"), ', ')))\
                .crosstab('id','tmp').drop('Zapytaj').withColumnRenamed('id_tmp','id'), on='id')\
            .drop('balkon___ogrod___taras')\
            .withColumn('czynsz', when(col('czynsz')=='Zapytaj',None).otherwise(regexp_replace(col('czynsz'),' zł','')))\
            .withColumn('miejsce_parkingowe', when(col('miejsce_parkingowe')=='Zapytaj', None).otherwise(col('miejsce_parkingowe')))\
            .withColumn('miejsce_parkingowe', when(col('miejsce_parkingowe').isNotNull(), col('miejsce_parkingowe'))\
                    .otherwise(when((col('opis').like("%parking%") | col('opis').like("%garaż%")) & col('miejsce_parkingowe').isNull(), "garaż/miejsce parkingowe")))\
            .withColumn('ogrzewanie', when(col('ogrzewanie')=='Zapytaj', None).otherwise(col('ogrzewanie')))\
            .withColumn('ogrzewanie', when(col('ogrzewanie').isNotNull(), col('ogrzewanie'))\
                    .otherwise(when(col('opis').like("%miejskie%") & col('ogrzewanie').isNull(), "miejskie")\
                                .otherwise(when(col('opis').like("%gazowe%") & col('ogrzewanie').isNull(), "gazowe")\
                                            .otherwise(when(col('opis').like("%elektryczne%") & col('ogrzewanie').isNull(), "elektryczne")))))\
            .withColumn('dostepne_od', when(col('dostepne_od')=='brak informacji', None).otherwise(col('dostepne_od')))\
            .withColumn('rok_budowy', when(col('rok_budowy')=='brak informacji', None).otherwise(col('rok_budowy')))\
            .withColumn('rodzaj_zabudowy', when(col('rodzaj_zabudowy')=='brak informacji', None).otherwise(col('rodzaj_zabudowy')))\
            .withColumn('okna', when(col('okna')=='brak informacji', None).otherwise(col('okna')))\
            .join(df.withColumn('tmp',explode(split(col("media"), ', ')))\
                .crosstab('id','tmp').drop('brak informacji').withColumnRenamed('id_tmp','id'), on='id')\
            .drop('media')\
            .join(df.withColumn('tmp',explode(split(col("zabezpieczenia"), ', ')))\
                .crosstab('id','tmp').drop('brak informacji').withColumnRenamed('id_tmp','id'), on='id')\
            .drop('zabezpieczenia')\
            .join(df.withColumn('tmp',explode(split(col("wyposazenie"), ', ')))\
                .crosstab('id','tmp').drop('brak informacji').withColumnRenamed('id_tmp','id'), on='id')\
            .drop('wyposazenie')\
            .join(df.withColumn('tmp',explode(split(col("informacje_dodatkowe"), ', ')))\
                .crosstab('id','tmp').drop('brak informacji').withColumnRenamed('id_tmp','id'), on='id')\
            .drop('informacje_dodatkowe')\
            .withColumn('material_budynku', when(col('material_budynku')=='brak informacji', None).otherwise(col('material_budynku')))
            
        df=df.toDF(*[remove_accents(column) for column in df.columns])

        df.select("*").write.save(file[:47]+'preprocessed'+file[46:])


if __name__ == "__main__":
    main()
