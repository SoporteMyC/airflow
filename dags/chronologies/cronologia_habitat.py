import warnings
import logging
from airflow.models import DAG
from airflow.utils.dates import timedelta, days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.models import Variable
import datetime
from dateutil.relativedelta import relativedelta
import sqlparse
import os 

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


try:
    database = Variable.get("current_database")
except:
    logging.error("Error al conseguir la variable de base de datos actual.")

default_args = {
    "owner": "adrian",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["soporte@menaresycia.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "catchup_by_default": False,
    "execution_timeout": timedelta(seconds=1800),
}

def crear_script_cron_hab():

    nombre_archivo_sql = "cronologia_habitat.sql"


    hoy  = datetime.datetime.now()
    if hoy.day <= 15:
        fecha_inicial = f'{hoy.year}{hoy.month}01'
    else:
        fecha_inicial = f'{hoy.year}{hoy.month}16'

    today = datetime.date.today()
    first = today.replace(day=1)

    periodo_inicial = (first - relativedelta(months=+5)).strftime("%Y%m")
    periodo_final = (first - relativedelta(months=+2)).strftime("%Y%m") 

    fecha_final = hoy.strftime("%Y%m%d")
    script = f"""
        set dateformat dmy
        declare @UNIDEU numeric(13)
        declare @PERCOT numeric(6)
        declare @NUMRUT numeric(9)

        declare @NROENV numeric(7)
        declare @FECENV	numeric(8)
        declare @ORIDEU	numeric(2)
        declare @ESTJUR	numeric(7)
        declare @COACEJ	numeric(6)
        declare @FEINAC	numeric(8)
        declare @FETEAC	numeric(8)
        declare @ESTADO	varchar(5)
        declare @SUBCO	varchar(3)
        declare @TIPREN	char(2)
        declare @nud varchar(13)
        declare @fch_llamada datetime
        declare @FEC_INAC date
        declare @FEC_SYS date
        declare {fecha_inicial}_a varchar(15)
        declare {fecha_final}_a varchar(15)
        declare {fecha_inicial}_b datetime
        declare {fecha_final}_b datetime
        --declare {periodo_inicial} int



        set {fecha_inicial}_a=convert(varchar,{fecha_inicial})
        set {fecha_final}_a=convert(varchar,{fecha_final})

        set {fecha_inicial}_b =convert(datetime,{fecha_inicial}_a,101)
        set {fecha_final}_b =convert(datetime,{fecha_final}_a,101)
        set {fecha_final}_b =dateadd(hour,23, {fecha_final}_b)

        

        select {fecha_inicial}_b, {fecha_final}_b

        create table #tmp_liquidacion(numero int)

        truncate table ut_cob_filehabitat_t

        print 3
        print convert(varchar,getdate(),121)
        insert into ut_cob_filehabitat_t (TIPREG
                                        ,UNIDEU
                                        ,PERCOT
                                        ,NUMRUT
                                        ,DIGRUT								 
                                        ,MONOBL
                                        ,MONVOL
                                        ,MONAHO
                                        ,MONAPO
                                        ,MONDEP
                                        ,MOREFO
                                        ,ESTADO
                                        ,FEINAC 
                                        ,FETEAC
                                        ,NROCOB
                                        ,FEC_INAC
                                        ,FEC_ING)
        SELECT  3   
            ,res_nud
            ,res_periodo
            ,res_rut
            ,res_rut_dv
            ,(select sum(val_cf) from ut_cob_resolucionafi_t where periodo_cot = res_periodo and nud=res_nud)
            ,(select sum(val_vo) from ut_cob_resolucionafi_t where periodo_cot = res_periodo and nud=res_nud)
            ,(select sum(val_ah) from ut_cob_resolucionafi_t where periodo_cot = res_periodo and nud=res_nud)
            ,(select sum(val_apvce) + sum(val_apvct) from ut_cob_resolucionafi_t where periodo_cot = res_periodo and nud=res_nud)
            ,(select sum(val_dc)  from ut_cob_resolucionafi_t where periodo_cot = res_periodo and nud=res_nud)
            ,res_monto
            ,res_situacion
            ,replace(convert(varchar,isnull(res_modificado,isnull(res_fecha,res_fec_ing)),112),'.','') 
            ,replace(convert(varchar,isnull(res_modificado,isnull(res_fecha,res_fec_ing)),112),'.','') 
            ,res_codigo
            ,isnull(res_modificado,res_fec_ing)
            ,res_fec_ing
        from ut_cob_resolucion_t 
        where (flg_rut_cliente=@rut_cliente or flg_rut_cliente=dbo.getpilotohabitat()  or flg_rut_cliente=dbo.getHabitatEspecial())
        and res_periodo between {periodo_inicial} and {periodo_final}

        select 'ut_cob_filehabitat_t', * from ut_cob_filehabitat_t
        select 'ut_cob_filehabitat_t', count(*) from ut_cob_filehabitat_t
        --select * from ut_cob_resolucion_t
        /* in (
        select distinct flg_rut_deudor from ut_cob_gesdeudor_t where rut_cliente=98000100 and fec_gestion between '01-01-2019 00:00:00' and '15-01-2019 23:59:00')*/

        --select convert(datetime,convert(varchar,{fecha_inicial}),101) , dateadd(hour,23,convert(date,convert(varchar,{fecha_final}),101) )
        select {fecha_inicial}_b, {fecha_final}_b



        print 4
        print convert(varchar,getdate(),121)
            DECLARE archivo CURSOR FOR
            select  UNIDEU,PERCOT, NUMRUT, ESTADO, FEINAC, FETEAC, FEC_INAC from ut_cob_filehabitat_t 
                                                
                    
            OPEN archivo

            FETCH NEXT FROM archivo
            INTO @UNIDEU, @PERCOT, @NUMRUT, @ESTADO, @FEINAC, @FETEAC, @FEC_INAC


            WHILE @@FETCH_STATUS = 0
            BEGIN
                set @FEC_SYS = @FEC_INAC
                set  @nud = right(REPLICATE('0', 13) + CONVERT(VARCHAR(13 ),isnull(@UNIDEU,0)),13) 
                select 
                @NROENV = 1
                ,@FECENV = substring(fec_envio,5,4) + substring(fec_envio,3,2) + substring(fec_envio,1,2)
        --		,@FECENV = fec_envio
                ,@ORIDEU = org_deuda
                ,@ESTJUR = '709'
                ,@TIPREN = 'RC'
                from ut_cob_remesa_t 
                where nud =  @nud
                and per_cotizacion = @PERCOT
                and emp_rut = @NUMRUT

                    
                    
                
                set @COACEJ=8016
                if exists(select 1 from UT_COB_CODIGOSCRONOLOGIA_M WHERE cod_sistema=@ESTADO)
                    begin
                

                        if @ESTADO IN ('CN','EI','PSR','PAD')
                        BEGIN
                            SELECT @COACEJ = cod_afp FROM UT_COB_CODIGOSCRONOLOGIA_M WHERE cod_sistema=@ESTADO and sub_codigo = 3
                            if exists(select 1 from ut_cob_cambiosestado_t 
                                where (rut_cliente=dbo.getHabitat() or rut_cliente=dbo.getPilotoHabitat() or rut_cliente=dbo.getHabitatEspecial())
                                and rut_empleador=@NUMRUT
                                and situacion_nueva IN ('CN','EI','PSR')
                                and fec_insert between {fecha_inicial}_b and {fecha_final}_b)
                            BEGIN
                        --select * from ut_dnpa_gestionllamada_t	
                    
                            select @fch_llamada = max(fec_insert) 
                            , @SUBCO=1
                            from ut_cob_cambiosestado_t
                                where (rut_cliente=dbo.getHabitat() or rut_cliente=dbo.getPilotoHabitat() or rut_cliente=dbo.getHabitatEspecial())
                                and rut_empleador=@NUMRUT
                                and situacion_nueva IN ('CN','EI','PSR')
                                and fec_insert between {fecha_inicial}_b and {fecha_final}_b
                                /*
                                select @fch_llamada = max(fch_llamada) 
                                from ut_dnpa_gestionllamada_t 
                                where rut_empleador = @NUMRUT
                                set @FEC_SYS = @fch_llamada
                                set @FEINAC = replace(convert(varchar,@fch_llamada,112),'.','')
                                set @FETEAC = @FEINAC
                                */
                                --select @SUBCO = isnull(cod_respuesta,0)
                                --from ut_dnpa_gestionllamada_t 
                                --where rut_empleador = @NUMRUT 
                                --and fch_llamada= @fch_llamada
                                if @SUBCO=0
                                begin
                                    set @SUBCO=3
                                end
                                IF EXISTS(SELECT 1 FROM UT_COB_CODIGOSCRONOLOGIA_M WHERE cod_sistema=@ESTADO and sub_codigo = @SUBCO)
                                BEGIN
                                    SELECT @COACEJ = cod_afp FROM UT_COB_CODIGOSCRONOLOGIA_M WHERE cod_sistema=@ESTADO and sub_codigo = @SUBCO
                                END
                            END

                        END
                        else
                        begin
                            if @ESTADO='PE' or @ESTADO='NE'
                            begin
                                truncate table #tmp_liquidacion
                                insert into #tmp_liquidacion
                                select res_liquidacion from ut_cob_resolucion_t where res_nud=@UNIDEU

                                select @fch_llamada = max(neg_f_negocio) from ut_cob_negocio_t where neg_numero in (
                                select l.numero from #tmp_liquidacion l)
                                /*-
                                select @fch_llamada = isnull(pag_f_libera,pag_fecha) from ut_cob_pago_p where pag_rut = @NUMRUT
                                and pag_numero in ()
                                */
                                set @FEC_SYS = @fch_llamada
                                set @FEINAC = replace(convert(varchar,@fch_llamada,112),'.','')
                                set @FETEAC = @FEINAC
                                
                            end

                            /*,FEINAC = @FEINAC
                    ,FETEAC = @FETEAC*/
                            SELECT @COACEJ = cod_afp FROM UT_COB_CODIGOSCRONOLOGIA_M WHERE cod_sistema=@ESTADO

                        end
                    end

                


                update ut_cob_filehabitat_t
                set  NROENV = @NROENV
                    ,FECENV = @FECENV
                    ,ORIDEU = @ORIDEU
                    ,ESTJUR = @ESTJUR
                    ,COACEJ = @COACEJ
                    ,TIPREN = @TIPREN
                    , FEINAC = @FEINAC
                    ,FETEAC = @FETEAC
                    ,FEC_INAC =@FEC_SYS
                where UNIDEU = @UNIDEU
                and PERCOT = @PERCOT
                and NUMRUT = @NUMRUT
                
                
                FETCH NEXT FROM archivo
                INTO @UNIDEU, @PERCOT, @NUMRUT, @ESTADO, @FEINAC, @FETEAC, @FEC_INAC

            END
            CLOSE archivo;
            DEALLOCATE archivo;

            declare @max_ing date

            select * from ut_cob_filehabitat_t
            set @max_ing='2020-10-01'
            /*
            select @max_ing = max(res_fec_ing) 
            from ut_cob_resolucion_t 
            where flg_rut_cliente=@rut_cliente
            and res_periodo between {periodo_inicial} and {periodo_final}
            */
        /*
            update ut_cob_filehabitat_t
            set FEINAC =  replace(convert(varchar,@max_ing,104),'.','') 
            ,FETEAC = replace(convert(varchar,@max_ing,104),'.','') 
            where fec_inac<@max_ing
            */
            /*
            update ut_cob_filehabitat_t
            set FEINAC =  replace(convert(varchar,(select max(res_fec_ing) from ut_cob_resolucion_t r where r.res_periodo=PERCOT and flg_rut_cliente=@rut_cliente),112),'.','') 
            ,FETEAC = replace(convert(varchar,(select max(res_fec_ing) from ut_cob_resolucion_t r where r.res_periodo=PERCOT and flg_rut_cliente=@rut_cliente),112),'.','') 
            where fec_inac<(select max(res_fec_ing) from ut_cob_resolucion_t r where r.res_periodo=PERCOT and flg_rut_cliente=@rut_cliente)
            */
            update ut_cob_filehabitat_t
            set FEINAC =  replace(convert(varchar,@max_ing,112),'.','') 
            ,FETEAC = replace(convert(varchar,@max_ing,112),'.','') 
            where fec_inac<@max_ing
            
            
            /*
            SELECT * FROM ut_cob_filehabitat_t
            
        select    right('0' + CONVERT(VARCHAR(1  ),ISNULL(TIPREG,0)),1)						TIPREG
                , right(REPLICATE('0', 7 ) + CONVERT(VARCHAR(7  ),isnull(NROENV,0)),7 )		NROENV
                , right(REPLICATE('0', 8 ) + CONVERT(VARCHAR(8  ),isnull(FECENV,0)),8 )		FECENV
                , right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2  ),isnull(ORIDEU,0)),2 )		ORIDEU
                , right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2  ),isnull(ESTJUR,0)),2 )		ESTJUR
                , right(REPLICATE('0', 1 ) + CONVERT(VARCHAR(1  ),isnull(ESTRES,0)),1 )		ESTRES
                , right(REPLICATE('0', 13) + CONVERT(VARCHAR(13 ),isnull(UNIDEU,0)),13)		UNIDEU
                , right(REPLICATE('0', 6 ) + CONVERT(VARCHAR(6  ),isnull(PERCOT,0)),6 )		PERCOT
                , right(REPLICATE('0', 11) + CONVERT(VARCHAR(11 ),isnull(NUMRES,0)),11)		NUMRES
                , right(REPLICATE('0', 9 ) + CONVERT(VARCHAR(9  ),isnull(NUMRUT,0)),9 )		NUMRUT
                , ISNULL(DIGRUT,0)															DIGRUT
                , right(REPLICATE('0', 7 ) + CONVERT(VARCHAR(7),isnull(CODACT,0)),7)	    CODACT
                , LEFT(ISNULL(NOMEMP,'') + REPLICATE(' ',60),60)							NOMEMP	
                , LEFT(ISNULL(DIREMP,'') + REPLICATE(' ',60),60)							DIREMP	  
                , LEFT(ISNULL(COMEMP,'') + REPLICATE(' ',40),40)							COMEMP	  
                , LEFT(ISNULL(CIUEMP,'') + REPLICATE(' ',40),40)							CIUEMP	  
                , right(REPLICATE('0', 7 ) + CONVERT(VARCHAR(7),ISNULL(CODCOM,0)),7)		CODCOM	
                , right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2),ISNULL(REGEMP,0)),2)		REGEMP	
                , right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2),ISNULL(SUCAFP,0)),2)		SUCAFP	
                , right(REPLICATE('0', 9 ) + CONVERT(VARCHAR(9),ISNULL(RUTRLE,0)),9)		RUTRLE	
                , ISNULL(DIGRLE,'0')														DIGRLE
                , LEFT(ISNULL(NOMRLE,'')  + REPLICATE(' ',60),60)							NOMRLE
                , RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(TIPJUI,0)),2 )		TIPJUI
                , RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(NROCOB,0)),9 )		NROCOB
                , RIGHT(REPLICATE('0',5 ) + CONVERT(VARCHAR(5 ),ISNULL(NROTRI,0)),5 )		NROTRI
                , RIGHT(REPLICATE('0',7 ) + CONVERT(VARCHAR(7 ),ISNULL(NROROL,0)),7 )		NROROL
                , RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FEPACO,0)),8 )		FEPACO
                , RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FEGITR,0)),8 )		FEGITR
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONOBL,0)),13)		MONOBL
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONVOL,0)),13)		MONVOL
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONAHO,0)),13)		MONAHO
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONAPO,0)),13)		MONAPO
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONDEP,0)),13)		MONDEP
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONFDO,0)),13)		MONFDO
                , RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR(11),ISNULL(MONREA,0)),11)		MONREA
                , RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR(11),ISNULL(MONINT,0)),11)		MONINT
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MOREFO,0)),13)		MOREFO
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONCON,0)),13)		MONCON
                , RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR(11),ISNULL(MONREC,0)),11)		MONREC
                , RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(MOCORE,0)),9 )		MOCORE
                , RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FECAAN,0)),8 )		FECAAN
                , RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MOREAF,0)),13)		MOREAF
                , RIGHT(REPLICATE('0',7 ) + CONVERT(VARCHAR(7 ),ISNULL(NUBOCO,0)),7 )		NUBOCO
                , RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(NUCAAN,0)),9 )		NUCAAN
                , RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(MOANPE,0)),9 )		MOANPE
                , RIGHT(REPLICATE('0',8) + REPLACE(REPLACE(CONVERT(VARCHAR(9 ),ISNULL(MOANUF,0 )),',',''),'.',''),8 )	MOANUF	
                , RIGHT(REPLICATE('0',9) + CONVERT(VARCHAR(9 ),ISNULL(COPEPE,0 )),9 )									COPEPE
                , RIGHT(REPLICATE('0',8) + REPLACE(REPLACE(CONVERT(VARCHAR(9 ),ISNULL(COPEUF,0 )),',',''),'.',''),8 )	COPEUF	
                , RIGHT(REPLICATE('0',9) + CONVERT(VARCHAR(9 ),ISNULL(COPRPE,0 )),9 )									COPRPE
                , RIGHT(REPLICATE('0',8) + REPLACE(REPLACE(CONVERT(VARCHAR(9 ),ISNULL(COPRUF,0 )),',',''),'.',''),8 )	COPRUF	
                , RIGHT(REPLICATE('0',5 ) + CONVERT(VARCHAR(5 ),ISNULL(LINREN,0 )),5 )									LINREN
                , RIGHT(REPLICATE('0',6 ) + CONVERT(VARCHAR(6 ),ISNULL(COACEJ,0 )),6 )									COACEJ
                , RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FEINAC,0 )),8 )									FEINAC
                , RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FETEAC,0 )),8 )									FETEAC
                , RIGHT(REPLICATE('0',1 ) + CONVERT(VARCHAR(1 ),ISNULL(LIPRAF,0 )),1 )									LIPRAF
                , RIGHT(REPLICATE('0',1 ) + CONVERT(VARCHAR(1 ),ISNULL(LIPREJ,0 )),1 )									LIPREJ
                , RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(RECHAF,0 )),2 )									RECHAF
                , RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(RECHEJ,0 )),2 )									RECHEJ
                , RIGHT(REPLICATE('0',4 ) + CONVERT(VARCHAR(4 ),ISNULL(CANAFI,0 )),4 )									CANAFI
                , RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(CANDOC,0 )),2 )									CANDOC
                , RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(ESPDEU,0 )),2 )									ESPDEU
                , LEFT(ISNULL(TIPREN,'')  + REPLICATE(' ',2),2)															TIPREN	
                ,ESTADO ESTADO							
        from ut_cob_filehabitat_t order by NUMRUT, UNIDEU
        */

        select {fecha_inicial} fecha_ini,  {fecha_final} fecha_fin
        --sp_help ut_cob_filehabitat_t
            select  
            TIPREG
        ,NROENV
        ,FECENV
        ,ORIDEU
        ,0 ESTJUR
        ,ESTRES
        ,UNIDEU
        ,PERCOT
        ,NUMRES
        ,NUMRUT
        ,DIGRUT
        ,CODACT
        ,NOMEMP
        ,DIREMP
        ,COMEMP
        ,CIUEMP
        ,CODCOM
        ,REGEMP
        ,SUCAFP
        ,RUTRLE
        ,DIGRLE
        ,NOMRLE
        ,TIPJUI
        ,NROCOB
        ,NROTRI
        ,NROROL
        ,FEPACO
        ,FEGITR
        ,MONOBL
        ,MONVOL
        ,MONAHO
        ,MONAPO
        ,MONDEP
        ,MONFDO
        ,MONREA
        ,MONINT
        ,MOREFO
        ,MONCON
        ,MONREC
        ,MOCORE
        ,FECAAN
        ,MOREAF
        ,ESTJUR NUBOCO
        ,NUCAAN
        ,MOANPE
        ,MOANUF
        ,COPEPE
        ,COPEUF
        ,COPRPE
        ,COPRUF
        ,LINREN
        ,COACEJ
        ,FEINAC
        ,FETEAC
        ,LIPRAF
        ,LIPREJ
        ,RECHAF
        ,RECHEJ
        ,CANAFI
        ,CANDOC
        ,ESPDEU
        ,TIPREN
        ,FCH_INSERT
        ,FCH_ARCHIVO_INICIO
        ,FCH_ARCHIVO_TERMINO
        ,STIMESTAMP
        ,ESTADO
        ,FEC_INAC
        ,FEC_ING
            
            from ut_cob_filehabitat_t
        where feinac between {fecha_inicial} and {fecha_final}

        select   right('0' + CONVERT(VARCHAR(1  ),ISNULL(TIPREG,0)),1)						
                + right(REPLICATE('0', 7 ) + CONVERT(VARCHAR(7  ),isnull(NROENV,0)),7 )		
                + right(REPLICATE('0', 8 ) + CONVERT(VARCHAR(8  ),isnull(FECENV,0)),8 )		
                + right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2  ),isnull(ORIDEU,0)),2 )		
                + right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2  ),isnull(0,0)),2 )		
                + right(REPLICATE('0', 1 ) + CONVERT(VARCHAR(1  ),isnull(ESTRES,0)),1 )		
                + right(REPLICATE('0', 13) + CONVERT(VARCHAR(13 ),isnull(UNIDEU,0)),13)		
                + right(REPLICATE('0', 6 ) + CONVERT(VARCHAR(6  ),isnull(PERCOT,0)),6 )		
                + right(REPLICATE('0', 11) + CONVERT(VARCHAR(11 ),isnull(NUMRES,0)),11)		
                + right(REPLICATE('0', 9 ) + CONVERT(VARCHAR(9  ),isnull(NUMRUT,0)),9 )		
                + ISNULL(DIGRUT,0)															
                + right(REPLICATE('0', 7 ) + CONVERT(VARCHAR(7),isnull(CODACT,0)),7)			
                + LEFT(ISNULL(NOMEMP,'') + REPLICATE(' ',60),60)								
                + LEFT(ISNULL(DIREMP,'') + REPLICATE(' ',60),60)								  
                + LEFT(ISNULL(COMEMP,'') + REPLICATE(' ',40),40)								  
                + LEFT(ISNULL(CIUEMP,'') + REPLICATE(' ',40),40)								  
                + right(REPLICATE('0', 7 ) + CONVERT(VARCHAR(7),ISNULL(CODCOM,0)),7)			
                + right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2),ISNULL(REGEMP,0)),2)			
                + right(REPLICATE('0', 2 ) + CONVERT(VARCHAR(2),ISNULL(SUCAFP,0)),2)			
                + right(REPLICATE('0', 9 ) + CONVERT(VARCHAR(9),ISNULL(RUTRLE,0)),9)			
                + ISNULL(DIGRLE,'0')															
                + LEFT(ISNULL(NOMRLE,'')  + REPLICATE(' ',60),60)							
                + RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(TIPJUI,0)),2 )		
                + RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(NROCOB,0)),9 )		
                + RIGHT(REPLICATE('0',5 ) + CONVERT(VARCHAR(5 ),ISNULL(NROTRI,0)),5 )		
                + RIGHT(REPLICATE('0',7 ) + CONVERT(VARCHAR(7 ),ISNULL(NROROL,0)),7 )		
                + RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FEPACO,0)),8 )		
                + RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FEGITR,0)),8 )		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONOBL,0)),13)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONVOL,0)),13)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONAHO,0)),13)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONAPO,0)),13)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONDEP,0)),13)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONFDO,0)),13)		
                + RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR(11),ISNULL(MONREA,0)),11)		
                + RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR(11),ISNULL(MONINT,0)),11)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MOREFO,0)),13)		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MONCON,0)),13)		
                + RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR(11),ISNULL(MONREC,0)),11)		
                + RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(MOCORE,0)),9 )		
                + RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FECAAN,0)),8 )		
                + RIGHT(REPLICATE('0',13) + CONVERT(VARCHAR(13),ISNULL(MOREAF,0)),13)		
                + RIGHT(REPLICATE('0',7 ) + CONVERT(VARCHAR(7 ),ISNULL(ESTJUR,0)),7 )		
                + RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(NUCAAN,0)),9 )		
                + RIGHT(REPLICATE('0',9 ) + CONVERT(VARCHAR(9 ),ISNULL(MOANPE,0)),9 )		
                + RIGHT(REPLICATE('0',8) + REPLACE(REPLACE(CONVERT(VARCHAR(9 ),ISNULL(MOANUF,0 )),',',''),'.',''),8 )		
                + RIGHT(REPLICATE('0',9) +                 CONVERT(VARCHAR(9 ),ISNULL(COPEPE,0 )),9 )		
                + RIGHT(REPLICATE('0',8) + REPLACE(REPLACE(CONVERT(VARCHAR(9 ),ISNULL(COPEUF,0 )),',',''),'.',''),8 )		
                + RIGHT(REPLICATE('0',9) +                 CONVERT(VARCHAR(9 ),ISNULL(COPRPE,0 )),9 )		
                + RIGHT(REPLICATE('0',8) + REPLACE(REPLACE(CONVERT(VARCHAR(9 ),ISNULL(COPRUF,0 )),',',''),'.',''),8 )		
                + RIGHT(REPLICATE('0',5 ) + CONVERT(VARCHAR(5 ),ISNULL(LINREN,0 )),5 )		
                + RIGHT(REPLICATE('0',6 ) + CONVERT(VARCHAR(6 ),ISNULL(COACEJ,0 )),6 )		
                + RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FEINAC,0 )),8 )		
                + RIGHT(REPLICATE('0',8 ) + CONVERT(VARCHAR(8 ),ISNULL(FETEAC,0 )),8 )		
                + RIGHT(REPLICATE('0',1 ) + CONVERT(VARCHAR(1 ),ISNULL(LIPRAF,0 )),1 )		
                + RIGHT(REPLICATE('0',1 ) + CONVERT(VARCHAR(1 ),ISNULL(LIPREJ,0 )),1 )		
                + RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(RECHAF,0 )),2 )		
                + RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(RECHEJ,0 )),2 )		
                + RIGHT(REPLICATE('0',4 ) + CONVERT(VARCHAR(4 ),ISNULL(CANAFI,0 )),4 )		
                + RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(CANDOC,0 )),2 )		
                + RIGHT(REPLICATE('0',2 ) + CONVERT(VARCHAR(2 ),ISNULL(ESPDEU,0 )),2 )		
                + LEFT(ISNULL(TIPREN,'')  + REPLICATE(' ',2),2)	 linea								
        from ut_cob_filehabitat_t 
        where feinac between {fecha_inicial} and {fecha_final}


        order by NUMRUT, UNIDEU

        drop table #tmp_liquidacion

    """
    logging.warning(os.getcwd())
    script = str(sqlparse.format(script, reindent=False, keyword_case='upper'))

    ruta_archivo = os.path.join("dags", "chronologies", "queries", nombre_archivo_sql)

    try:
        with open(ruta_archivo, "w") as archivo_sql:
            archivo_sql.write(script)
    except Exception as e:
        logging.error(e)

with DAG(
    "crear_cronologia_habitat",
    default_args=default_args,
    description="crea los archivos de Cronologia Habitat",
    schedule_interval=None,
    max_active_runs=1,
    concurrency=4,
    tags=["informes", "sql server"],
) as dag:
    


    crear_sql_script_hab = PythonOperator(
        task_id="crear_script_cron_hab", 
        python_callable=crear_script_cron_hab
    )
   
   
