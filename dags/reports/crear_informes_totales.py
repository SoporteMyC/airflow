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

def crear_sql_hab(**kwargs):
    cliente = kwargs['cliente']
    #-------------------------------------------- OBTENER FECHAS ----------------------------------

    if cliente == "HAB":
        nombre_archivo_sql = "info_estados_hab.sql"
        set_cliente = "set  @rut_cliente = dbo.getHabitat()"
    elif cliente == "PRO":
        nombre_archivo_sql = "info_estados_pro.sql"
        set_cliente = "set  @rut_cliente = dbo.getProvida()"
    else:
        raise "Error de cliente"
    today = datetime.date.today()
    first = today.replace(day=1)

    periodo1 = (first - relativedelta(months=+5)).strftime("%Y%m")
    periodo2 = (first - relativedelta(months=+4)).strftime("%Y%m")
    periodo3 = (first - relativedelta(months=+3)).strftime("%Y%m")
    periodo4 = (first - relativedelta(months=+2)).strftime("%Y%m")

    nombre_tabla_hab = f'INF_{cliente}_TOT_{today.strftime("%Y%m%d")}_AUT'

    meses = []
    for i in list(range(0,12)):
        meses.append((first - relativedelta(months=+i)).strftime("%Y%m"))
    meses = sorted(meses, key=lambda x: int(x[4:]))

    script_habitat = f"""
        use webcob

        select getdate()

        declare @rut_cliente int
        declare @periodos varchar(100)
        declare @estados varchar(100)
        declare @periodo int
        declare @maxFecha date
        declare @maxFecha2 date
        declare @maxFecha3 date
        declare @fechaCese date
        declare @tipoInfo char(1)
        declare @periodopad int
        declare @fechaCargaPeriodo datetime
        declare @fec_capital date
        declare @distribucion char(1)
        declare @flg_consideraPiloto char(1)

        CREATE table #tmp_cliente(rut_cliente int)

        select @fec_capital = max(res_fec_ing) from ut_cob_resolucion_t where flg_rut_cliente=dbo.getCapital()

        set @periodopad = {meses[5]}

        set @distribucion='N'

        set @tipoInfo = 'T'


        set @estados='PE,NE,PAD,PAE,PSR,CCA,EQ,ODE,PRV'
        set @periodos='{periodo1},{periodo2},{periodo3},{periodo4}'

        set @flg_consideraPiloto='N'
        
        {set_cliente}
        
        CREATE TABLE webcob.dbo.prueba1 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );


        insert into #tmp_cliente (rut_cliente) values (@rut_cliente)

        if @flg_consideraPiloto='S'
        begin
            if @rut_cliente=dbo.getHabitat()
            begin
                insert into #tmp_cliente (rut_cliente) values (dbo.getPilotoHabitat())
            end
        end


        select @periodo = max(res_periodo) from ut_cob_resolucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente)

        set @periodo = {periodo4}

        select @periodo ultimo_periodo
        select @maxFecha  = max(flg_insert) from ut_cob_distibucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        select @maxFecha2 = max(flg_insert) from ut_cob_distibucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and flg_insert<@maxFecha
        select @maxFecha3 = max(flg_insert) from ut_cob_distibucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and flg_insert<@maxFecha2


        create table #tmp_pads(codigo int, monto numeric(18), rut_deudor int, periodo int, fecha_info date, fecha_reg date, nud int, monto_for varchar(20))

        create table #tmp_estados(estado varchar(5))
        create table #tmp_periodos(periodo int)
        create table #tmp_periodos_vigente(periodo int)
        create table #tmp_gestionesvalidas(rut_deudor int, cod_gesdeudor int)
        create table #tmp_gestiones_hab (cod_gesdeudor int, cod_gestion varchar(2), cod_est_gestion varchar(2), rut_deudor int, fec_gestion datetime, telefono varchar(200))
        create table #tmp_gestiones_cap (cod_gesdeudor int, cod_gestion varchar(2), cod_est_gestion varchar(2), rut_deudor int, fec_gestion datetime, telefono varchar(200))
        create table #tmp_gestiones_pro (cod_gesdeudor int, cod_gestion varchar(2), cod_est_gestion varchar(2), rut_deudor int, fec_gestion datetime, telefono varchar(200))
        CREATE TABLE #tmp_cambiosestado(
            [id] [numeric](18, 0) NULL,
            [rut_cliente] [int] NULL,
            [res_codigo] [int] NULL,
            [situacion_anterior] [varchar](5) NULL,
            [situacion_nueva] [varchar](5) NULL,
            [rut_usuario] [int] NULL,
            [fec_insert] [datetime] NULL,
            [fec_psr] [date] NULL,
            [fec_pad] [date] NULL
        ) 


        CREATE TABLE #tmp_gesdeudor(
            [cod_gesdeudor]       [int] NULL,
            [cod_gestion]         [char](3) NULL,
            [cod_est_gestion]     [char](3) NULL,
            [flg_rut_deudor]      [varchar](11) NULL,
            [flg_dv_deudor]       [char](1) NULL,
            [flg_rut_usuario]     [varchar](11) NULL,
            [flg_dv_usuario]      [char](1) NULL,
            [cod_login]           [varchar](12) NULL,
            [fec_gestion]         [datetime] NULL,
            [txt_det_ges]         [varchar](500) NULL,
            [rut_cliente]         [int] NULL,
            [txt_numerollamada]   [varchar](100) NULL,
            [txt_periodos]        [varchar](500) NULL,
            [fec_compromiso]      [date] NULL,
            [flg_recepcionsms]    [char](1) NULL,
            [flg_email_enviado]   [char](1) NULL,
            [flg_email_estado]    [char](2) NULL,
            [flg_email_contactar] [char](1) NULL,
            [txt_mail]			  [varchar](100) NULL,
            [NUD] NUMERIC(18) NULL,
            [MONTO] NUMERIC(18) NULL
        ) 


        select @fechaCargaPeriodo= max(res_fec_ing) from ut_cob_resolucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente)



        CREATE TABLE webcob.dbo.prueba15 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        select @fechaCargaPeriodo fechaCargaPeriodo

        print '*** 01 CREACION TABLAS ***'	
        print getdate()	
        insert into #tmp_periodos_vigente
        select distinct periodo from ut_cob_periodos_m where rut_cliente in (select rut_cliente from #tmp_cliente) and flg_asignacion='S'

        create table #tmp_dis(codigo int, rut int, dis char(1))
        create table #tmp_reso(rut int, monto numeric(18), periodo int, situacion varchar(5)
        , ejecutiva varchar(250)
        ,ejecutiva_a0 varchar(250)
        ,ejecutiva_a1 varchar(250)
        ,ejecutiva_a2 varchar(250)
        ,rut_ejecutiva_0 int
        ,rut_ejecutiva_1 int
        ,rut_ejecutiva_2 int)

        create table #tmp_salida(rut int
        , p_{meses[0]} numeric(18)
        , p_{meses[1]} numeric(18)
        , p_{meses[2]} numeric(18)
        , p_{meses[3]} numeric(18)
        , p_{meses[4]} numeric(18)
        , p_{meses[5]} numeric(18)
        , p_{meses[6]} numeric(18)
        , p_{meses[7]} numeric(18)
        , p_{meses[8]} numeric(18)
        , p_{meses[9]} numeric(18)
        , p_{meses[10]} numeric(18)
        , p_{meses[11]} numeric(18)

        , n_{meses[0]} numeric(18)
        , n_{meses[1]} numeric(18)
        , n_{meses[2]} numeric(18)
        , n_{meses[3]} numeric(18)
        , n_{meses[4]} numeric(18)
        , n_{meses[5]} numeric(18)
        , n_{meses[6]} numeric(18)
        , n_{meses[7]} numeric(18)
        , n_{meses[8]} numeric(18)
        , n_{meses[9]} numeric(18)
        , n_{meses[10]} numeric(18)
        , n_{meses[11]} numeric(18)

        ,situacion_01 varchar(max)
        ,situacion_02 varchar(max)
        ,situacion_03 varchar(max)
        ,situacion_04 varchar(max)
        ,situacion_05 varchar(max)
        ,situacion_06 varchar(max)
        ,situacion_07 varchar(max)
        ,situacion_08 varchar(max)
        ,situacion_09 varchar(max)
        ,situacion_10 varchar(max)
        ,situacion_11 varchar(max)
        ,situacion_12 varchar(max)
        ,leybustos_01 varchar(1)
        ,leybustos_02 varchar(1)
        ,leybustos_03 varchar(1)
        ,leybustos_04 varchar(1)
        ,leybustos_05 varchar(1)
        ,leybustos_06 varchar(1)
        ,leybustos_07 varchar(1)
        ,leybustos_08 varchar(1)
        ,leybustos_09 varchar(1)
        ,leybustos_10 varchar(1)
        ,leybustos_11 varchar(1)
        ,leybustos_12 varchar(1)
        ,situacion    varchar(max)
        ,nuevo char(1)
        ,ultPeriodo int
        ,fec_pago date
        , ultEstado varchar(max)
        , ejecutiva varchar(250)
        , fonos_cap varchar(max)
        , fonos_pro varchar(max)
        , fonos_hab varchar(max)
        , correos varchar(max)
        ,estado_cn varchar(100)
        , ejecutiva_a0 varchar(250)
        , ejecutiva_a1 varchar(250)
        , ejecutiva_a2 varchar(250)
        , ejecutiva_ge varchar(250)
        , rut_ejecutiva_ge int
        ,can int
        ,u_pe int
        ,res_juicio int
        ,compromiso_pago int
        ,fec_compromiso date
        ,cod_gesdeudor int
        ,cod_gestion varchar(2)
        ,cod_est_gestion varchar(2)
        ,txt_gestion varchar(500)
        ,txt_estado  varchar(500)
        ,can_correos int
        ,can_correos_vi int
        ,can_llamadas int
        ,can_llamadaSIVAL int
        ,can_llamadaNOVAL int
        ,fecha_gestion date
        ,carga_especial int
        ,carga_mixta char(1)
        , cod_gesdeudor_afp_provida   int
        , cod_gestion_afp_provida     varchar(2)
        , est_gestion_afp_provida     varchar(2)
        , fec_gestion_afp_provida     datetime
        , tel_gestion_afp_provida     varchar(200)
        , tel_01_provida			  varchar(200)
        , estado_afp_provida		  varchar(200)
        , txt_gestion_afp_provida     varchar(200)
        , can_pe_provida			  int
        , can_ne_provida		  int
        , can_pad_provida	  int
        , cod_gesdeudor_afp_habitat   int
        , cod_gestion_afp_habitat     varchar(2)
        , est_gestion_afp_habitat     varchar(2)
        , fec_gestion_afp_habitat     datetime
        , tel_gestion_afp_habitat     varchar(200)
        , tel_01_habitat			  varchar(200)
        , estado_afp_habitat		  varchar(200)
        , txt_gestion_afp_habitat     varchar(200)
        , can_pe_habitat			  int
        , can_ne_habitat		  int
        , can_pad_habitat	  int
        , cod_gesdeudor_afp_capital   int
        , cod_gestion_afp_capital     varchar(2)
        , est_gestion_afp_capital     varchar(2)
        , fec_gestion_afp_capital     datetime
        , tel_gestion_afp_capital     varchar(200)
        , tel_01_capital			  varchar(200)
        , estado_afp_capital		  varchar(200)
        , txt_gestion_afp_capital     varchar(200)
        , can_pe_capital			  int
        , can_ne_capital		  int
        , can_pad_capital	  int
        , cod_gesdeudor_busqueda       int
        , cod_gestion_busqueda      varchar(2)
        , est_gestion_busqueda      varchar(2)
        , fec_gestion_busqueda     datetime
        , txt_gestion_busqueda      varchar(200)
        , carga_ultima_remesa   char(1)
        , fec_carga_periodo_pag date
        , tiene_cca   char(1)
        , ult_periodo_cca int
        , es_dnp int
        , es_car int
        , es_cai  int
        , es_cam  int
        , periodo_carga int
        ,sms_enviado     char(1)
        ,correos_vi char(1)
        ,gestion_valida_cam char(1)
        ,gestion_valida_eje char(1)
        )

        CREATE TABLE webcob.dbo.prueba16 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        print '*** 01.1 INSERTA PERIODOS ***'	

        declare @periodo_1 int
        declare @periodo_2 int
        declare @periodo_3 int

        insert into #tmp_periodos
        select splitdata from dbo.Split(@periodos,',') order by id

        print '*** 01.2 INSERTA PERIODOS ok ***'	


        print '*** 01.3 estados ***'	

        insert into #tmp_estados
        select splitdata from dbo.Split(@estados,',') order by id
        print '*** 02 INSERTA RESOLUCIONES ***'	
        print getdate()	

        select 'tmp_periodos', * from #tmp_periodos


        if @tipoInfo='T' 
        BEGIN
            select 'TOTAL'
            
            insert into #tmp_reso (rut,monto,periodo,situacion)
            select res_rut ,sum(res_monto),res_periodo, res_situacion
            from ut_cob_resolucion_t r
            where flg_rut_cliente in (select rut_cliente from #tmp_cliente)
            and res_periodo  in (select periodo from #tmp_periodos) 
            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
            select periodo from #tmp_periodos
            select sum(monto), count(*) from #tmp_reso where periodo=@periodo
        END

        CREATE TABLE webcob.dbo.prueba17 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );


            CREATE TABLE webcob.dbo.prueba181 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        select @maxFecha maxFecha

        CREATE TABLE webcob.dbo.prueba18 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        update #tmp_reso
        set rut_ejecutiva_0 = (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatos(@rut_cliente,#tmp_reso.rut))
        where isnull(rut_ejecutiva_0,0)=0

        if @rut_cliente=dbo.gethabitat()
        begin
            update #tmp_reso
            set rut_ejecutiva_0 = (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatos(98100,#tmp_reso.rut))
            where isnull(rut_ejecutiva_0,0)=0
        end

        update #tmp_reso
        set rut_ejecutiva_1 =  (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente,#tmp_reso.rut,1))
        where isnull(rut_ejecutiva_1,0)=0


        CREATE TABLE webcob.dbo.prueba19 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        update #tmp_reso
        set rut_ejecutiva_2 =  (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente,#tmp_reso.rut,2))
        where isnull(rut_ejecutiva_2,0)=0

        select @periodo_1 = max(periodo) from ut_cob_distibucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and flg_insert = @maxFecha
        select @periodo_2 = max(periodo) from ut_cob_distibucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and flg_insert = @maxFecha and periodo<@periodo_1
        select @periodo_3 = max(periodo) from ut_cob_distibucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and flg_insert = @maxFecha and periodo<@periodo_2

        update #tmp_reso 
        set rut_ejecutiva_2 = isnull((select top 1 d.usr_rut from ut_cob_desistidosUsuario_t d where d.rut_cliente=@rut_cliente and d.rut_deudor=#tmp_reso.rut and d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_2)
        where #tmp_reso.situacion iN ('PAD','PSR')
        and periodo=@periodo_3

        CREATE TABLE webcob.dbo.prueba2 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        update #tmp_reso 
        set rut_ejecutiva_1 = isnull((select top 1 d.usr_rut from ut_cob_desistidosUsuario_t d where d.rut_cliente=@rut_cliente and d.rut_deudor=#tmp_reso.rut and d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_1)
        where #tmp_reso.situacion iN ('PAD','PSR')
        and periodo=@periodo_2

        update #tmp_reso 
        set rut_ejecutiva_0 = isnull((select top 1 d.usr_rut from ut_cob_desistidosUsuario_t d where d.rut_cliente=@rut_cliente and d.rut_deudor=#tmp_reso.rut and d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_0)
        where #tmp_reso.situacion iN ('PAD','PSR')
        and periodo=@periodo_1

        update #tmp_reso 
        set rut_ejecutiva_2 = isnull((
        select TOP 1 flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
        select top 1 pag_usuario from ut_cob_pago_p where pag_liq= 
        (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
        select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente) and r.res_rut=#tmp_reso.rut and r.res_periodo=@periodo_3 and r.res_situacion  IN ('NE','PE') 
        ))
        )
        ),rut_ejecutiva_2)
        where #tmp_reso.situacion IN ('NE','PE')
        and periodo=@periodo_3


        update #tmp_reso 
        set rut_ejecutiva_1 = isnull((
        select TOP 1 flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
        select top 1 pag_usuario from ut_cob_pago_p where pag_liq= 
        (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
        select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente= @rut_cliente and r.res_rut=#tmp_reso.rut and r.res_periodo=@periodo_2 and r.res_situacion  IN ('NE','PE') 
        ))
        )

        ),rut_ejecutiva_1)
        where #tmp_reso.situacion IN ('NE','PE')
        and periodo=@periodo_2

        update #tmp_reso 
        set rut_ejecutiva_0 = isnull((
        select TOP 1 flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
        select top 1 pag_usuario from ut_cob_pago_p where pag_liq= 
        (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
        select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente= @rut_cliente and r.res_rut=#tmp_reso.rut and r.res_periodo=@periodo_1 and r.res_situacion  IN ('NE','PE') and res_fecha>=@maxFecha
        ))
        )),rut_ejecutiva_0)
        where #tmp_reso.situacion IN ('NE','PE')
        and periodo=@periodo_1

        declare @rut_cliente2 int
        set @rut_cliente2=dbo.getpilotohabitat()

        CREATE TABLE webcob.dbo.prueba21 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );

        if exists (select 1 from #tmp_cliente where rut_cliente=dbo.getpilotohabitat())
        begin

            update #tmp_reso
            set rut_ejecutiva_0 = (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente2,#tmp_reso.rut,0))
            where isnull(rut_ejecutiva_0,0)=0

            update #tmp_reso
            set rut_ejecutiva_1 =  (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente2,#tmp_reso.rut,1))
            where isnull(rut_ejecutiva_1,0)=0

            update #tmp_reso
            set rut_ejecutiva_2 =  (select rut_usuario from webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente2,#tmp_reso.rut,2))
            where isnull(rut_ejecutiva_2,0)=0

            select @periodo_1 = max(periodo) from ut_cob_distibucion_t where flg_rut_cliente = @rut_cliente2 and flg_insert = @maxFecha
            select @periodo_2 = max(periodo) from ut_cob_distibucion_t where flg_rut_cliente = @rut_cliente2 and flg_insert = @maxFecha and periodo<@periodo_1
            select @periodo_3 = max(periodo) from ut_cob_distibucion_t where flg_rut_cliente = @rut_cliente2 and flg_insert = @maxFecha and periodo<@periodo_2

            update #tmp_reso 
            set rut_ejecutiva_2 = isnull((select top 1 d.usr_rut from ut_cob_desistidosUsuario_t d where d.rut_cliente=@rut_cliente2 and d.rut_deudor=#tmp_reso.rut and d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_2)
            where #tmp_reso.situacion iN ('PAD','PSR')
            and periodo=@periodo_3


            update #tmp_reso 
            set rut_ejecutiva_1 = isnull((select top 1 d.usr_rut from ut_cob_desistidosUsuario_t d where d.rut_cliente=@rut_cliente2 and d.rut_deudor=#tmp_reso.rut and d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_1)
            where #tmp_reso.situacion iN ('PAD','PSR')
            and periodo=@periodo_2

            CREATE TABLE webcob.dbo.prueba22 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
            );

            update #tmp_reso 
            set rut_ejecutiva_0 = isnull((select top 1 d.usr_rut from ut_cob_desistidosUsuario_t d where d.rut_cliente=@rut_cliente2 and d.rut_deudor=#tmp_reso.rut and d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_0)
            where #tmp_reso.situacion iN ('PAD','PSR')
            and periodo=@periodo_1

            update #tmp_reso 
            set rut_ejecutiva_2 = isnull((
            select TOP 1 flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
            select top 1 pag_usuario from ut_cob_pago_p where pag_liq= 
            (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
            select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente = @rut_cliente2 and r.res_rut=#tmp_reso.rut and r.res_periodo=@periodo_3 and r.res_situacion  IN ('NE','PE') 
            ))
            )
            ),rut_ejecutiva_2)
            where #tmp_reso.situacion IN ('NE','PE')
            and periodo=@periodo_3


            update #tmp_reso 
            set rut_ejecutiva_1 = isnull((
            select TOP 1 flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
            select top 1 pag_usuario from ut_cob_pago_p where pag_liq= 
            (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
            select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente= @rut_cliente2 and r.res_rut=#tmp_reso.rut and r.res_periodo=@periodo_2 and r.res_situacion  IN ('NE','PE') 
            ))
            )

            ),rut_ejecutiva_1)
            where #tmp_reso.situacion IN ('NE','PE')
            and periodo=@periodo_2

            update #tmp_reso 
            set rut_ejecutiva_0 = isnull((
            select TOP 1 flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
            select top 1 pag_usuario from ut_cob_pago_p where pag_liq= 
            (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
            select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente= @rut_cliente2 and r.res_rut=#tmp_reso.rut and r.res_periodo=@periodo_1 and r.res_situacion  IN ('NE','PE') and res_fecha>=@maxFecha
            ))
            )),rut_ejecutiva_0)
            where #tmp_reso.situacion IN ('NE','PE')
            and periodo=@periodo_1


        end

        update #tmp_reso
        set ejecutiva = (select txt_nombre + ' ' + txt_apellido from ut_cob_usuarios_m where flg_rut_usuario=#tmp_reso.rut_ejecutiva_0)


        update #tmp_reso
        set ejecutiva_a0 = (select txt_nombre + ' ' + txt_apellido from ut_cob_usuarios_m where flg_rut_usuario=#tmp_reso.rut_ejecutiva_0)

        update #tmp_reso
        set ejecutiva_a1 = (select txt_nombre + ' ' + txt_apellido from ut_cob_usuarios_m where flg_rut_usuario=#tmp_reso.rut_ejecutiva_1)

        update #tmp_reso
        set ejecutiva_a2 = (select txt_nombre + ' ' + txt_apellido from ut_cob_usuarios_m where flg_rut_usuario=#tmp_reso.rut_ejecutiva_2)

        print '*** 04 INSERTA RUT SALIDA ***'	
        print getdate()	

        insert into #tmp_salida (rut)
        select rut from #tmp_reso group by rut

        print '*** 05 ACTUALIZA MONTO SALIDA***'	
        print getdate()	

        select * from #tmp_salida
            update #tmp_salida
            set p_{meses[0]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[0]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,	p_{meses[1]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[1]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[2]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[2]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[3]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[3]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[4]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[4]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[5]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[5]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[6]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[6]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[7]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[7]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[8]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[8]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[9]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[9]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[10]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[10]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,   p_{meses[11]} = isnull((select sum(monto) from #tmp_reso where periodo={meses[11]} and #tmp_reso.rut = #tmp_salida.rut),0)  
            ,situacion = (
            select estados=STUFF((
            select ', ' + situacion from #tmp_reso
            where #tmp_reso.rut = r.rut
            group by rut, situacion
            for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
            from #tmp_reso r where r.rut =#tmp_salida.rut
            group by rut
            )

        delete from #tmp_salida where 
            {periodo1}=0
        and {periodo2}=0
        and {periodo3}=0
        and {periodo4}=0
            
        print '*** 05.1 ACTUALIZA LEY BUSTOS SALIDA***'	
        print getdate()	


        print '*** 06 ASOCIA DISTRIBUCION***'	
        print getdate()	
        insert into #tmp_dis(codigo,rut,dis)
        select res_codigo,res_rut,'N' from ut_cob_resolucion_t 
        where res_rut in (select rut from #tmp_salida ) 
        and res_periodo in (select periodo from #tmp_periodos) 
        and flg_rut_cliente in (select rut_cliente from #tmp_cliente)

        print '*** 07 MARCA DISTRIBUIDO***'	
        print getdate()	
        update #tmp_dis
        set dis='S'
        where codigo in (select res_codigo from ut_cob_distibucion_t where flg_insert='2018-09-30')

        update #tmp_salida
        set nuevo='N'

        print '*** 08 INDICA SI ES NUEVO***'	
        select @periodo, @rut_cliente
        print getdate()	
        if @rut_cliente=dbo.getcapital()
        begin
        update #tmp_salida
            set nuevo= case isnull((select count(*) from ut_cob_resolucion_t 
                                where flg_rut_cliente=@rut_cliente
                                and res_rut = #tmp_salida.rut 
                                and res_periodo <@periodo 
                                ),0) 
        when 0 then 'S' else 'N' end
        end
        else
        begin
        update #tmp_salida
        set nuevo= case isnull((select count(*) from ut_cob_resolucion_t 
                                where flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                                and res_rut = #tmp_salida.rut 
                                and res_periodo <@periodo 
                                ),0) 
        when 0 then 'S' else 'N' end
        end

        update #tmp_salida
        set can= isnull((select count(*) from ut_cob_resolucion_t 
                                where flg_rut_cliente in (select rut_cliente from #tmp_cliente) 
                                and res_rut = #tmp_salida.rut 
                                and res_periodo <@periodo 
                                ),0) 


        update #tmp_salida
        set u_pe= isnull((select max(res_periodo) from ut_cob_resolucion_t 
                                where flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                                and res_rut = #tmp_salida.rut 
                                and res_periodo <@periodo 
                                ),0) 


        print '*** 09 ***'	



        
        print getdate()	
        update #tmp_salida
        set ultPeriodo= isnull((select max(res_periodo) 
                                from ut_cob_resolucion_t 
                                where flg_rut_cliente in (select rut_cliente from #tmp_cliente) 
                                and res_rut = #tmp_salida.rut 
                                and res_situacion in ('PE','PAD','PSR','NE')
                                and res_periodo<>@periodo
                                ),0)



        update #tmp_salida
        set fec_pago= isnull((select max(res_fecha)
                                from ut_cob_resolucion_t 
                                where flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                                and res_rut = #tmp_salida.rut 
                                and res_situacion in ('PE','PAD','PSR')
                                and res_periodo = ultPeriodo
                                ),'1900-01-01'
                                
                                )

        where isnull(ultPeriodo,0)>0

        update #tmp_salida
        set fec_pago= 
        (
                                (select max(neg_f_negocio) from ut_cob_negocio_t where neg_numero in (
                                                                                                        (
                                                                                                        select distinct res_codigo
                                                                                                        from ut_cob_resolucion_t 
                                                                                                        where flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                                                                                                        and res_rut = #tmp_salida.rut 
                                                                                                        
                                                                                                        and res_situacion ='NE'
                                                                                                        and res_periodo = ultPeriodo))
                                                                                                        )
                                )
        where ultPeriodo >	0 
        and 	fec_pago is null

        print '*** 10 ***'	
        print getdate()	
        update #tmp_salida
        set ultEstado  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = r.res_periodo
                            group by flg_rut_cliente,res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = #tmp_salida.ultPeriodo
        group by flg_rut_cliente,res_rut, res_periodo
        ),'')

        CREATE TABLE webcob.dbo.prueba3 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );
        print '*** 11 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_01  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[0]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[0]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[0]}>0

        print '*** 12 ***'	
        print getdate()	

        update #tmp_salida
        set situacion_02  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[1]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[1]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[1]}>0

        print '*** 13 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_03  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[2]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[2]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[2]}>0

        print '*** 14 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_04  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[3]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[3]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[3]}>0

        print '*** 15 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_05  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[4]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[4]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[4]}>0

        print '*** 16 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_06  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[5]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[5]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[5]}>0

        print '*** 16.1 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_07  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[6]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[6]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[6]}>0

        print '*** 16.2 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_08  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[7]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[7]}
        group by flg_rut_cliente, res_rut,res_periodo
        ),'')
        where p_{meses[7]}>0

        print '*** 16.3 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_09  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[8]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[8]}
        group by flg_rut_cliente, res_rut,res_periodo

        ),'')
        where p_{meses[8]}>0

        print '*** 16.4 ***'	
        print getdate()	
        update #tmp_salida
        set situacion_10  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[9]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[9]}
        group by flg_rut_cliente, res_rut,res_periodo

        ),'')
        where p_{meses[9]}>0


        update #tmp_salida
        set situacion_11  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[10]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[10]}
        group by flg_rut_cliente, res_rut,res_periodo

        ),'')
        where p_{meses[10]}>0



        update #tmp_salida
        set situacion_12  =isnull( (
        select estados=STUFF((select ', ' + res_situacion 
                            from ut_cob_resolucion_t
                            where ut_cob_resolucion_t.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
                            and ut_cob_resolucion_t.res_rut = r.res_rut
                            and ut_cob_resolucion_t.res_periodo = {meses[11]}
                            group by flg_rut_cliente, res_rut,res_periodo, res_situacion
        for xml path(''),type	).value('.[1]','nvarchar(max)'),1,2,'')
        from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente)
        and r.res_rut =#tmp_salida.rut
        and r.res_periodo = {meses[11]}
        group by flg_rut_cliente, res_rut,res_periodo

        ),'')
        where p_{meses[11]}>0



        CREATE TABLE webcob.dbo.prueba4 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );



        print '*** 17 ***'	
        print getdate()	
        update #tmp_salida
        set ejecutiva =(select top 1 ejecutiva from #tmp_reso
                        where #tmp_reso.rut = #tmp_salida.rut and ejecutiva is not null
                        )


        update #tmp_salida
        set ejecutiva_a0 =(select top 1 ejecutiva_a0 from #tmp_reso
                        where #tmp_reso.rut = #tmp_salida.rut   and ejecutiva_a0 is not null 
                        )

        update #tmp_salida
        set ejecutiva_a1 =(select top 1 ejecutiva_a1 from #tmp_reso
                        where #tmp_reso.rut = #tmp_salida.rut  and ejecutiva_a1 is not null  
                        )

        update #tmp_salida
        set ejecutiva_a2 =(select top 1 ejecutiva_a2 from #tmp_reso
                        where #tmp_reso.rut = #tmp_salida.rut  and ejecutiva_a2 is not null  
                        )





        update #tmp_salida
        set res_juicio=isnull((select count(*) from ut_cob_resolucion_t r where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and res_rut = #tmp_salida.rut and res_juicio='S' and res_periodo in (select periodo from #tmp_periodos)),0)

        update #tmp_salida
        set es_dnp=isnull((select count(*) from ut_cob_resolucion_t r where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and res_rut = #tmp_salida.rut and res_tipo_cobro='DNP' and isnull(res_obs,'')='' and res_periodo in (select periodo from #tmp_periodos)),0)
        ,   es_car=isnull((select count(*) from ut_cob_resolucion_t r where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and res_rut = #tmp_salida.rut and res_tipo_cobro='CAR' and isnull(res_obs,'')='' and res_periodo in (select periodo from #tmp_periodos)),0)
        ,   es_cai=isnull((select count(*) from ut_cob_resolucion_t r where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and res_rut = #tmp_salida.rut and res_tipo_cobro='DNP' and isnull(res_obs,'')='CAI' and res_periodo in (select periodo from #tmp_periodos)),0)
        ,   es_cam=isnull((select count(*) from ut_cob_resolucion_t r where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and res_rut = #tmp_salida.rut and res_tipo_cobro='DNP' and isnull(res_obs,'')='CAM' and res_periodo in (select periodo from #tmp_periodos)),0)

        update #tmp_salida
        set cod_gesdeudor = (select max(a.cod_gesdeudor) from ut_cob_gesdeudor_t a where a.rut_cliente=dbo.getProvida() 
        and (
            (a.cod_gestion='01' and (a.cod_est_gestion='12' or a.cod_est_gestion='21'))
            or (a.cod_gestion='02' and (a.cod_est_gestion='17' or a.cod_est_gestion='21'))
            or (a.cod_gestion='06' and (a.cod_est_gestion='02' or a.cod_est_gestion='21'))
            )
        and a.fec_gestion>=DATEADD(month,-1,getdate())
        and a.fec_compromiso>='2022-06-01'
        and a.flg_rut_deudor=#tmp_salida.rut
        )

        ,compromiso_pago = (select count(*) from ut_cob_gesdeudor_t g where g.rut_cliente=dbo.getProvida() 
        and (
            (g.cod_gestion='01' and g.cod_est_gestion='12')
            or (g.cod_gestion='02' and g.cod_est_gestion='17')
            or (g.cod_gestion='06' and g.cod_est_gestion='02')
            )
        and g.fec_gestion>=DATEADD(month,-3,getdate())
        and g.fec_compromiso>='2023-09-15'
        and g.flg_rut_deudor=#tmp_salida.rut
        )

        update #tmp_salida
        set fec_compromiso = (select g.fec_compromiso from ut_cob_gesdeudor_t g where g.cod_gesdeudor=#tmp_salida.cod_gesdeudor)
        where isnull(compromiso_pago,0)>0

        update #tmp_salida
        set rut_ejecutiva_ge = (select g.flg_rut_usuario
                            from ut_cob_gesdeudor_t g
                            where g.cod_gesdeudor = #tmp_salida.cod_gesdeudor)

        where isnull(cod_gesdeudor,0)>0


        update #tmp_salida
        set ejecutiva_ge = (select ltrim(rtrim(ltrim(rtrim(u.txt_nombre)) + ' ' + ltrim(rtrim(u.txt_apellido)) ))
                            from ut_cob_usuarios_m u
                            where u.flg_rut_usuario = #tmp_salida.rut_ejecutiva_ge)

        where isnull(cod_gesdeudor,0)>0


        if @rut_cliente=dbo.gethabitat()
        begin
            
            update #tmp_salida
            set carga_especial =(select count(*) from ut_cob_resolucion_t r where r.flg_rut_cliente =dbo.getpilotohabitat() 
            and #tmp_salida.rut =r.res_rut and res_periodo>={meses[11]})


        end
        else
        begin
            update #tmp_salida
            set carga_especial =(select count(*) from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente) and #tmp_salida.rut =r.res_rut and r.res_tipo_cobro='CAR')

            update #tmp_salida
            set carga_mixta =(select case count(*) when 0 then 'N' else 'S' end from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente) and #tmp_salida.rut =r.res_rut and r.res_tipo_cobro='CESP')

        end

        declare @fecha_gesAFP datetime

        set @fecha_gesAFP = '2020-07-01'

        update #tmp_salida
        set carga_ultima_remesa='N'

        update #tmp_salida
        set carga_ultima_remesa='S'
        where rut in (select res_rut from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente) and r.res_periodo=@periodo )

        update #tmp_salida
        set fec_carga_periodo_pag = (
        select min(res_fec_ing) from ut_cob_resolucion_t where flg_rut_cliente in (select rut_cliente from #tmp_cliente) and res_periodo=#tmp_salida.ultPeriodo 
        )

        update #tmp_salida
        set ult_periodo_cca = (
                    select max(res_periodo) 
                    from ut_cob_resolucion_t 
                    where flg_rut_cliente in (select rut_cliente from #tmp_cliente) 
                    and res_rut = #tmp_salida.rut and res_situacion ='CCA'
        )

        update #tmp_salida
        set tiene_cca ='N'

        update #tmp_salida
        set tiene_cca ='S'
        where isnull(ult_periodo_cca,0) >0

            select sum([p_{meses[0]}]) s3 from #tmp_salida

            update #tmp_salida
            set periodo_carga = (SELECT max(res_periodo) 
                                from ut_cob_resolucion_t r 
                                where flg_rut_cliente in (select c.rut_cliente from #tmp_cliente c) 
                                and r.res_rut = #tmp_salida.rut
                                and r.res_fec_ing = @fechaCargaPeriodo)


                declare @tipofecha int
        set @tipofecha = 2 

        update #tmp_salida
        set gestion_valida_cam = dbo.getTieneGestionRealizadaFechasCampana(@rut_cliente,rut,'2023-10-01','2023-10-30')
        , gestion_valida_eje = dbo.getTieneGestionValida(@rut_cliente,rut,'2023-10-01','2023-10-30')

        select * from #tmp_salida
        select rut 
        ,flg_dv_deudor
        ,isnull(replace(replace(replace(d.txt_razon_social ,char(10),''),char(13),''),char(9),'' ),'') txt_razon_social
        ,d.flg_rut_representante
        ,d.flg_dv_reperesentante 
        ,isnull(replace(replace(replace(d.txt_representante ,char(10),''),char(13),''),char(9),'' ),'') txt_representante
        ,isnull(replace(replace(replace(d.txt_direccion ,char(10),''),char(13),''),char(9),'' ),'') txt_direccion
        ,isnull(replace(replace(replace(d.txt_ciudad	,char(10),''),char(13),''),char(9),'' ),'')  txt_ciudad	
        ,isnull(replace(replace(replace(d.txt_comuna	,char(10),''),char(13),''),char(9),'' ),'')  txt_comuna	
        ,ISNULL((select top 1 g.comuna from exportacion.dbo.tb_cob_deudor_geo_t g where g.rut_empleador=rut) ,'') COMUNA_INFO
        ,ISNULL((select top 1 g.region from exportacion.dbo.tb_cob_deudor_geo_t g where g.rut_empleador=rut) ,'') REGION_INFO
        ,gestion_valida_cam [GES. VALIDA CAMPAÑA]
        ,gestion_valida_eje [GES. VALIDA EJECUTIVA]
        ,p_{meses[5]} [P {meses[5]}]
        ,p_{meses[6]} [P {meses[6]}]
        ,p_{meses[7]} [P {meses[7]}]
        ,p_{meses[8]} [P {meses[8]}]

        ,isnull(situacion_06,'') situacion_06
        ,isnull(situacion_07,'') situacion_07
        ,isnull(situacion_08,'') situacion_08
        ,isnull(situacion_09,'') situacion_09

        ,nuevo
        ,ultPeriodo   [ULTIMO PERIODO PAGADO]
        ,isnull(format(fec_pago,'dd-MM-yyyy'),'')	 [FECHA ULTIMO PERIODO PAGADO]
        ,fec_carga_periodo_pag FECHA_CARGA_PERIODO
        ,ultEstado
        ,ejecutiva
        , sms_enviado
        , correos_vi
        ,estado_cn
        ,ejecutiva_a0
        ,ejecutiva_a1
        ,ejecutiva_a2
        ,can
        ,u_pe
        ,res_juicio
        ,compromiso_pago
        ,fec_compromiso
        , cod_gesdeudor     [ID GESTION]
        ,isnull(carga_especial,0) [CARTERA ESPECIAL o PILOTO]
        ,isnull(carga_mixta,'N')  [CARTERA MIXTA]
        ,ISNULL(tiene_cca,'N') [CCA INFORMADO]
        ,isnull(ult_periodo_cca,0) [CCA ULT PERIODO]
        ,es_dnp
        ,es_car
        ,es_cai
        ,es_cam
        ,periodo_carga
        into exportacion.dbo.{nombre_tabla_hab}
        from #tmp_salida
        left join ut_cob_deudor_m d
        on d.flg_rut_deudor = rut

        order by  rut

        drop table #tmp_reso
        drop table #tmp_salida
        drop table #tmp_dis
        drop table #tmp_periodos
        drop table #tmp_periodos_vigente
        drop table #tmp_estados
        drop table #tmp_gestionesvalidas
        drop table #tmp_gestiones_hab
        drop table #tmp_gestiones_cap
        drop table #tmp_gestiones_pro
        drop table #tmp_pads
        drop table #tmp_gesdeudor
        drop table #tmp_cambiosestado
        drop table #tmp_cliente;

        CREATE TABLE webcob.dbo.prueba6 (
            ID INT PRIMARY KEY,
            Nombre NVARCHAR(50),
            Edad INT
        );
        """

    logging.warning(os.getcwd())
    script_habitat = str(sqlparse.format(script_habitat, reindent=False, keyword_case='upper'))

    ruta_archivo = os.path.join("dags", "reports", "queries", nombre_archivo_sql)

    try:
        with open(ruta_archivo, "w") as archivo_sql:
            archivo_sql.write(script_habitat)
    except Exception as e:
        logging.error(e)


def drop_temp_tables():
    hook = MsSqlHook(mssql_conn_id=database)
    
    tables = [
        "#tmp_reso",
        "#tmp_salida",
        "#tmp_dis",
        "#tmp_periodos",
        "#tmp_periodos_vigente",
        "#tmp_estados",
        "#tmp_gestionesvalidas",
        "#tmp_gestiones_hab",
        "#tmp_gestiones_cap",
        "#tmp_gestiones_pro",
        "#tmp_pads",
        "#tmp_gesdeudor",
        "#tmp_cambiosestado",
        "#tmp_cliente"
        ]

    for table in tables:
        try:
            hook.run(f"use webcob; drop table {table}")
        except Exception as e:
            logging.error(e)

def ejecutar_script_hab():
    fd = open('dags/reports/queries/info_estados_hab.sql', 'r')
    sqlFile = fd.read()
    fd.close()

    logging.info(sqlFile)
    hook = MsSqlHook(mssql_conn_id=database)
    
    try:
        hook.run(sqlFile, autocommit=True)
    except Exception as e:
        logging.error(e)

with DAG(
    "crear_informes_totales",
    default_args=default_args,
    description="crea el script sql de informe total con fechas actualizadas.",
    schedule_interval="00 7 * * *  ",
    max_active_runs=1,
    concurrency=4,
    tags=["informes", "sql server"],
) as dag:
    
    drop_temporary_tables = PythonOperator(
        task_id="borrar_tablas_temp",
        python_callable=drop_temp_tables
    )

    crear_sql_script_hab = PythonOperator(
        task_id="crear_sql_script_hab", 
        python_callable=crear_sql_hab,
        op_kwargs={'cliente': 'HAB'}
    )

    '''crear_sql_script_pro = PythonOperator(
        task_id="crear_sql_script_pro", 
        python_callable=crear_sql_hab,
        op_kwargs={'cliente': 'PRO'}
    )'''

    ejecuta_script_hab = PythonOperator(
        task_id="ejecutar_script_hab", 
        python_callable=ejecutar_script_hab,
    )

    """create_hab_table = MsSqlOperator(
        task_id="crear_tabla_informe_hab",
        mssql_conn_id=database,
        sql="queries/info_estados_hab.sql",
        split_statements=False,
        autocommit=True,
        dag=dag
    )"""

    '''create_pro_table = MsSqlOperator(
        task_id="crear_tabla_informe_pro",
        mssql_conn_id=database,
        sql="queries/info_estados_pro.sql",
        autocommit=True
    )'''


    
    #drop_temporary_tables >> crear_sql_script_hab >> create_hab_table
    crear_sql_script_hab >> ejecuta_script_hab
     #>> create_hab_table
    #crear_sql_script_pro #>> create_pro_table

   
   
