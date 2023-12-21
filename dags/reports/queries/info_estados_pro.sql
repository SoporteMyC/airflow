USE webcob
SELECT getdate()--select * from ut_cob_cliente_m
 DECLARE @rut_cliente int DECLARE @periodos varchar(100) DECLARE @estados varchar(100) DECLARE @periodo int DECLARE @maxFecha date DECLARE @maxFecha2 date DECLARE @maxFecha3 date DECLARE @fechaCese date DECLARE @tipoInfo char(1) DECLARE @periodopad int DECLARE @fechaCargaPeriodo datetime DECLARE @fec_capital date DECLARE @distribucion char(1) DECLARE @flg_consideraPiloto char(1)
CREATE TABLE #tmp_cliente(rut_cliente int)
SELECT @fec_capital = max(res_fec_ing)
FROM ut_cob_resolucion_t
WHERE flg_rut_cliente=dbo.getCapital()
  SET @periodopad = 202306
  SET @distribucion='N'
  SET @tipoInfo = 'T' --TOTAL

  SET @estados='PE,NE,PAD,PAE,PSR,CCA,EQ,ODE,PRV'
  SET @periodos='202307,202308,202309,202310'
  SET @flg_consideraPiloto='N'
  SET @rut_cliente = dbo.getProvida()
  INSERT INTO #tmp_cliente (rut_cliente)
VALUES (@rut_cliente) IF @flg_consideraPiloto='S' BEGIN IF @rut_cliente=dbo.getHabitat() BEGIN INSERT INTO #tmp_cliente (rut_cliente) VALUES (dbo.getPilotoHabitat()) END END /*
        (select rut_cliente from #tmp_cliente)
        */ SELECT @periodo = max(res_periodo) FROM ut_cob_resolucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente)--and isnull(res_juicio,'N')='N'
 SET @periodo = 202310 SELECT @periodo ultimo_periodo --set @periodo=202308
 SELECT @maxFecha = max(flg_insert) FROM ut_cob_distibucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) SELECT @maxFecha2 = max(flg_insert) FROM ut_cob_distibucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND flg_insert<@maxFecha SELECT @maxFecha3 = max(flg_insert) FROM ut_cob_distibucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND flg_insert<@maxFecha2 --select @maxFecha = @maxFecha2
 --select @maxFecha2
 CREATE TABLE #tmp_pads(codigo int, monto numeric(18), rut_deudor int, periodo int, fecha_info date, fecha_reg date, nud int, monto_for varchar(20)) CREATE TABLE #tmp_estados(estado varchar(5)) CREATE TABLE #tmp_periodos(periodo int) CREATE TABLE #tmp_periodos_vigente(periodo int) CREATE TABLE #tmp_gestionesvalidas(rut_deudor int, cod_gesdeudor int) CREATE TABLE #tmp_gestiones_hab (cod_gesdeudor int, cod_gestion varchar(2), cod_est_gestion varchar(2), rut_deudor int, fec_gestion datetime, telefono varchar(200)) CREATE TABLE #tmp_gestiones_cap (cod_gesdeudor int, cod_gestion varchar(2), cod_est_gestion varchar(2), rut_deudor int, fec_gestion datetime, telefono varchar(200)) CREATE TABLE #tmp_gestiones_pro (cod_gesdeudor int, cod_gestion varchar(2), cod_est_gestion varchar(2), rut_deudor int, fec_gestion datetime, telefono varchar(200)) CREATE TABLE #tmp_cambiosestado([id] [numeric](18, 0) NULL, [rut_cliente] [int] NULL, [res_codigo] [int] NULL, [situacion_anterior] [varchar](5) NULL, [situacion_nueva] [varchar](5) NULL, [rut_usuario] [int] NULL, [fec_insert] [datetime] NULL, [fec_psr] [date] NULL, [fec_pad] [date] NULL) CREATE TABLE #tmp_gesdeudor([cod_gesdeudor] [int] NULL, [cod_gestion] [char](3) NULL, [cod_est_gestion] [char](3) NULL, [flg_rut_deudor] [varchar](11) NULL, [flg_dv_deudor] [char](1) NULL, [flg_rut_usuario] [varchar](11) NULL, [flg_dv_usuario] [char](1) NULL, [cod_login] [varchar](12) NULL, [fec_gestion] [datetime] NULL, [txt_det_ges] [varchar](500) NULL, [rut_cliente] [int] NULL, [txt_numerollamada] [varchar](100) NULL, [txt_periodos] [varchar](500) NULL, [fec_compromiso] [date] NULL, [flg_recepcionsms] [char](1) NULL, [flg_email_enviado] [char](1) NULL, [flg_email_estado] [char](2) NULL, [flg_email_contactar] [char](1) NULL, [txt_mail] [varchar](100) NULL, [NUD] NUMERIC(18) NULL, [MONTO] NUMERIC(18) NULL) SELECT @fechaCargaPeriodo= max(res_fec_ing) FROM ut_cob_resolucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente)--select @fechaCargaPeriodo = '2020-05-01'
 SELECT @fechaCargaPeriodo fechaCargaPeriodo PRINT '*** 01 CREACION TABLAS ***' PRINT getdate() INSERT INTO #tmp_periodos_vigente SELECT DISTINCT periodo FROM ut_cob_periodos_m WHERE rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND flg_asignacion='S' CREATE TABLE #tmp_dis(codigo int, rut int, dis char(1)) CREATE TABLE #tmp_reso(rut int, monto numeric(18), periodo int, situacion varchar(5) , ejecutiva varchar(250) ,ejecutiva_a0 varchar(250) ,ejecutiva_a1 varchar(250) ,ejecutiva_a2 varchar(250) ,rut_ejecutiva_0 int ,rut_ejecutiva_1 int ,rut_ejecutiva_2 int) CREATE TABLE #tmp_salida(rut int , p_202301 numeric(18) , p_202302 numeric(18) , p_202303 numeric(18) , p_202304 numeric(18) , p_202305 numeric(18) , p_202306 numeric(18) , p_202307 numeric(18) , p_202308 numeric(18) , p_202309 numeric(18) , p_202310 numeric(18) , p_202311 numeric(18) , p_202312 numeric(18) , n_202301 numeric(18) , n_202302 numeric(18) , n_202303 numeric(18) , n_202304 numeric(18) , n_202305 numeric(18) , n_202306 numeric(18) , n_202307 numeric(18) , n_202308 numeric(18) , n_202309 numeric(18) , n_202310 numeric(18) , n_202311 numeric(18) , n_202312 numeric(18) ,situacion_01 varchar(MAX) ,situacion_02 varchar(MAX) ,situacion_03 varchar(MAX) ,situacion_04 varchar(MAX) ,situacion_05 varchar(MAX) ,situacion_06 varchar(MAX) ,situacion_07 varchar(MAX) ,situacion_08 varchar(MAX) ,situacion_09 varchar(MAX) ,situacion_10 varchar(MAX) ,situacion_11 varchar(MAX) ,situacion_12 varchar(MAX) ,leybustos_01 varchar(1) ,leybustos_02 varchar(1) ,leybustos_03 varchar(1) ,leybustos_04 varchar(1) ,leybustos_05 varchar(1) ,leybustos_06 varchar(1) ,leybustos_07 varchar(1) ,leybustos_08 varchar(1) ,leybustos_09 varchar(1) ,leybustos_10 varchar(1) ,leybustos_11 varchar(1) ,leybustos_12 varchar(1) ,situacion varchar(MAX) ,nuevo char(1) ,ultPeriodo int ,fec_pago date , ultEstado varchar(MAX) , ejecutiva varchar(250) , fonos_cap varchar(MAX) , fonos_pro varchar(MAX) , fonos_hab varchar(MAX) , correos varchar(MAX) ,estado_cn varchar(100) , ejecutiva_a0 varchar(250) , ejecutiva_a1 varchar(250) , ejecutiva_a2 varchar(250) , ejecutiva_ge varchar(250) , rut_ejecutiva_ge int ,can int ,u_pe int ,res_juicio int ,compromiso_pago int ,fec_compromiso date ,cod_gesdeudor int ,cod_gestion varchar(2) ,cod_est_gestion varchar(2) ,txt_gestion varchar(500) ,txt_estado varchar(500) ,can_correos int ,can_correos_vi int ,can_llamadas int ,can_llamadaSIVAL int ,can_llamadaNOVAL int ,fecha_gestion date ,carga_especial int ,carga_mixta char(1) /*nueva informacion*/ /*PROVIDA*/ , cod_gesdeudor_afp_provida int , cod_gestion_afp_provida varchar(2) , est_gestion_afp_provida varchar(2) , fec_gestion_afp_provida datetime , tel_gestion_afp_provida varchar(200) , tel_01_provida varchar(200) , estado_afp_provida varchar(200) , txt_gestion_afp_provida varchar(200) , can_pe_provida int , can_ne_provida int , can_pad_provida int /*HABITAT*/ , cod_gesdeudor_afp_habitat int , cod_gestion_afp_habitat varchar(2) , est_gestion_afp_habitat varchar(2) , fec_gestion_afp_habitat datetime , tel_gestion_afp_habitat varchar(200) , tel_01_habitat varchar(200) , estado_afp_habitat varchar(200) , txt_gestion_afp_habitat varchar(200) , can_pe_habitat int , can_ne_habitat int , can_pad_habitat int /*CAPITAL*/ , cod_gesdeudor_afp_capital int , cod_gestion_afp_capital varchar(2) , est_gestion_afp_capital varchar(2) , fec_gestion_afp_capital datetime , tel_gestion_afp_capital varchar(200) , tel_01_capital varchar(200) , estado_afp_capital varchar(200) , txt_gestion_afp_capital varchar(200) , can_pe_capital int , can_ne_capital int , can_pad_capital int /*BUSQUEDA*/ , cod_gesdeudor_busqueda int , cod_gestion_busqueda varchar(2) , est_gestion_busqueda varchar(2) , fec_gestion_busqueda datetime , txt_gestion_busqueda varchar(200) , carga_ultima_remesa char(1) , fec_carga_periodo_pag date , tiene_cca char(1) , ult_periodo_cca int , es_dnp int , es_car int , es_cai int , es_cam int , periodo_carga int ,sms_enviado char(1) ,correos_vi char(1) ,gestion_valida_cam char(1) ,gestion_valida_eje char(1)) PRINT '*** 01.1 INSERTA PERIODOS ***' DECLARE @periodo_1 int DECLARE @periodo_2 int DECLARE @periodo_3 int INSERT INTO #tmp_periodos --select 202303
 SELECT splitdata FROM dbo.Split(@periodos,',') ORDER BY id PRINT '*** 01.2 INSERTA PERIODOS ok ***' --select * from #tmp_periodos
 PRINT '*** 01.3 estados ***' INSERT INTO #tmp_estados SELECT splitdata FROM dbo.Split(@estados,',') ORDER BY id --select * from #tmp_periodos
 PRINT '*** 02 INSERTA RESOLUCIONES ***' PRINT getdate()/*2019-01-27*/ SELECT 'tmp_periodos', * FROM #tmp_periodos IF @tipoInfo='T' BEGIN SELECT 'TOTAL' INSERT INTO #tmp_reso (rut,monto,periodo,situacion) SELECT res_rut,sum(res_monto),res_periodo, res_situacion FROM ut_cob_resolucion_t r WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND res_periodo IN (SELECT periodo FROM #tmp_periodos)--and res_codigo in (select p.res_codigo from pagos_previos p)
 --and res_situacion not in ('CCA','ODE')
 --and res_rut = 76964450
 /*	and res_nud  in (
        select numpla from exportacion.dbo.CAR_PRO_CCA_20230123)*/ --select [ID_DEUDA] from exportacion.dbo.RAN_HAB_20230623
 --)
 --	and res_codigo in
 --	(select r2.res_codigo from webcob.dbo.ut_cob_resolucion_t r2 where r2.flg_rut_cliente=dbo.getprovida() and r2.res_periodo>=202311 and r2.res_rut in (select c.RUT from exportacion.dbo.CAR_PRO_CCA_20230408 c) and r2.res_situacion in ('PE','NE'))
 /*
            and res_rut in (
        select rut_empleador from exportacion.dbo.REV_HAB_202310_20230107
        )
        */ --and res_lpe='S'
 --and res_fec_ing='2020-11-25'
 --and res_rut in (
 --select RUT_EMPLEADOR from exportacion.dbo.REV_HAB_20230831
 --)
 /*and res_rut in (
                select distinct x.res_rut from ut_cob_resolucion_t x
            where x.flg_rut_cliente in (select c.rut_cliente from #tmp_cliente c)
            and x.res_periodo  in (select p.periodo from #tmp_periodos p)
            and x.res_tipo_cobro='CAM'
            )
            */ --and res_situacion in ('CCJ')
 --and res_fecha = '2022-08-03'
 --and res_situaci0n in ('PE','NE','PAD','PAE','PSR')
 --and res_situacion in ('AC')20230930
 --and isnull(res_+uicio,'N')='S'
 --and res_fec_ing='2022-06-14'
 /*and res_codigo in (
            select res_codigo from ut_cob_cambiosestado_t where rut_cliente=dbo.getProvida()  and convert(date,fec_insert)='2020-04-08'
            )*/ --and res_tipo_cobro='CAR'
 --and isnull(res_obs,'')='CAM'
 --and res_tipo_cobro='CESP'
 --and res_monto=0
 /*and res_codigo in (
            1238166
            )*/ GROUP BY flg_rut_cliente, res_rut,res_periodo, res_situacion SELECT periodo FROM #tmp_periodos SELECT sum(monto), count(*) FROM #tmp_reso WHERE periodo=@periodo END SELECT DISTINCT rut FROM #tmp_reso /*Carga rut con gestiones validas*/ --select * from ut_cob_est_gestion_t where cod_gestion='02' and flg_cto_valido='S'
 SELECT 'ut_cob_est_gestion_t', * FROM ut_cob_est_gestion_t ORDER BY 1,2 PRINT '*** 03 ACTUALIZA EJECUTIVAS***' PRINT getdate() SELECT @maxFecha maxFecha --select * from #tmp_reso where rut=78016230
 UPDATE #tmp_reso SET rut_ejecutiva_0 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatos(@rut_cliente,#tmp_reso.rut)) WHERE isnull(rut_ejecutiva_0,0)=0 --select *, @rut_cliente from #tmp_reso where rut=78016230
 IF @rut_cliente=dbo.gethabitat() BEGIN UPDATE #tmp_reso SET rut_ejecutiva_0 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatos(98100,#tmp_reso.rut)) WHERE isnull(rut_ejecutiva_0,0)=0 END --select * from #tmp_reso where rut=78016230
 /*
        (select top 1 flg_rut_usuario from ut_cob_distibucion_t where flg_rut_cliente=@rut_cliente and flg_insert=@maxFecha and rut_deudor = #tmp_reso.rut ) -- and cod_grupo = 99)

        select * from EjecutivaDeudorxRutDatos(@rut_cliente,86454800)
        */ UPDATE #tmp_reso SET rut_ejecutiva_1 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente,#tmp_reso.rut,1)) WHERE isnull(rut_ejecutiva_1,0)=0 --(select top 1 flg_rut_usuario from ut_cob_distibucion_t where flg_rut_cliente=@rut_cliente and flg_insert=@maxFecha2 and rut_deudor = #tmp_reso.rut)
 UPDATE #tmp_reso SET rut_ejecutiva_2 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente,#tmp_reso.rut,2)) WHERE isnull(rut_ejecutiva_2,0)=0 --(select top 1 flg_rut_usuario from ut_cob_distibucion_t where flg_rut_cliente=@rut_cliente and flg_insert=@maxFecha3 and rut_deudor = #tmp_reso.rut)
 /*revisa si existen reasignaciones*/ --select * from #tmp_reso where rut =86454800
 SELECT @periodo_1 = max(periodo) FROM ut_cob_distibucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND flg_insert = @maxFecha SELECT @periodo_2 = max(periodo) FROM ut_cob_distibucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND flg_insert = @maxFecha AND periodo<@periodo_1 SELECT @periodo_3 = max(periodo) FROM ut_cob_distibucion_t WHERE flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND flg_insert = @maxFecha AND periodo<@periodo_2 /*
        select @periodo_1
        ,@periodo_2
        ,@periodo_3
        */ UPDATE #tmp_reso SET rut_ejecutiva_2 = isnull((SELECT top 1 d.usr_rut FROM ut_cob_desistidosUsuario_t d WHERE d.rut_cliente=@rut_cliente AND d.rut_deudor=#tmp_reso.rut AND d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_2) WHERE #tmp_reso.situacion IN ('PAD','PSR') AND periodo=@periodo_3 UPDATE #tmp_reso SET rut_ejecutiva_1 = isnull((SELECT top 1 d.usr_rut FROM ut_cob_desistidosUsuario_t d WHERE d.rut_cliente=@rut_cliente AND d.rut_deudor=#tmp_reso.rut AND d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_1) WHERE #tmp_reso.situacion IN ('PAD','PSR') AND periodo=@periodo_2 UPDATE #tmp_reso SET rut_ejecutiva_0 = isnull((SELECT top 1 d.usr_rut FROM ut_cob_desistidosUsuario_t d WHERE d.rut_cliente=@rut_cliente AND d.rut_deudor=#tmp_reso.rut AND d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_0) WHERE #tmp_reso.situacion IN ('PAD','PSR') AND periodo=@periodo_1 --select 'PAD', * from #tmp_reso where rut =86454800
 UPDATE #tmp_reso SET rut_ejecutiva_2 = isnull((SELECT TOP 1 flg_rut_usuario FROM ut_cob_usuarios_m WHERE flg_user =(SELECT top 1 pag_usuario FROM ut_cob_pago_p WHERE pag_liq= (SELECT top 1 neg_numero FROM ut_cob_negocio_t WHERE neg_n_sist=(SELECT max(res_codigo) FROM ut_cob_resolucion_t r WHERE r.flg_rut_cliente IN (SELECT rut_cliente FROM #tmp_cliente) AND r.res_rut=#tmp_reso.rut AND r.res_periodo=@periodo_3 AND r.res_situacion IN ('NE','PE') )) ) ),rut_ejecutiva_2) WHERE #tmp_reso.situacion IN ('NE','PE') AND periodo=@periodo_3 UPDATE #tmp_reso SET rut_ejecutiva_1 = isnull((SELECT TOP 1 flg_rut_usuario FROM ut_cob_usuarios_m WHERE flg_user =(SELECT top 1 pag_usuario FROM ut_cob_pago_p WHERE pag_liq= (SELECT top 1 neg_numero FROM ut_cob_negocio_t WHERE neg_n_sist=(SELECT max(res_codigo) FROM ut_cob_resolucion_t r WHERE r.flg_rut_cliente= @rut_cliente AND r.res_rut=#tmp_reso.rut AND r.res_periodo=@periodo_2 AND r.res_situacion IN ('NE','PE') )) ) ),rut_ejecutiva_1) WHERE #tmp_reso.situacion IN ('NE','PE') AND periodo=@periodo_2 UPDATE #tmp_reso SET rut_ejecutiva_0 = isnull((SELECT TOP 1 flg_rut_usuario FROM ut_cob_usuarios_m WHERE flg_user =(SELECT top 1 pag_usuario FROM ut_cob_pago_p WHERE pag_liq= (SELECT top 1 neg_numero FROM ut_cob_negocio_t WHERE neg_n_sist=(SELECT max(res_codigo) FROM ut_cob_resolucion_t r WHERE r.flg_rut_cliente= @rut_cliente AND r.res_rut=#tmp_reso.rut AND r.res_periodo=@periodo_1 AND r.res_situacion IN ('NE','PE') AND res_fecha>=@maxFecha )) )),rut_ejecutiva_0) WHERE #tmp_reso.situacion IN ('NE','PE') AND periodo=@periodo_1 DECLARE @rut_cliente2 int SET @rut_cliente2=dbo.getpilotohabitat() IF EXISTS (SELECT 1 FROM #tmp_cliente WHERE rut_cliente=dbo.getpilotohabitat()) BEGIN UPDATE #tmp_reso SET rut_ejecutiva_0 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente2,#tmp_reso.rut,0)) WHERE isnull(rut_ejecutiva_0,0)=0 /*
            (select top 1 flg_rut_usuario from ut_cob_distibucion_t where flg_rut_cliente=@rut_cliente and flg_insert=@maxFecha and rut_deudor = #tmp_reso.rut ) -- and cod_grupo = 99)

            select * from EjecutivaDeudorxRutDatos(@rut_cliente,86454800)
            */ UPDATE #tmp_reso SET rut_ejecutiva_1 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente2,#tmp_reso.rut,1)) WHERE isnull(rut_ejecutiva_1,0)=0 --(select top 1 flg_rut_usuario from ut_cob_distibucion_t where flg_rut_cliente=@rut_cliente and flg_insert=@maxFecha2 and rut_deudor = #tmp_reso.rut)
 UPDATE #tmp_reso SET rut_ejecutiva_2 = (SELECT rut_usuario FROM webcob.dbo.EjecutivaDeudorxRutDatosDistAnte(@rut_cliente2,#tmp_reso.rut,2)) WHERE isnull(rut_ejecutiva_2,0)=0 --(select top 1 flg_rut_usuario from ut_cob_distibucion_t where flg_rut_cliente=@rut_cliente and flg_insert=@maxFecha3 and rut_deudor = #tmp_reso.rut)
 /*revisa si existen reasignaciones*/ --select * from #tmp_reso where rut =86454800
 SELECT @periodo_1 = max(periodo) FROM ut_cob_distibucion_t WHERE flg_rut_cliente = @rut_cliente2 AND flg_insert = @maxFecha SELECT @periodo_2 = max(periodo) FROM ut_cob_distibucion_t WHERE flg_rut_cliente = @rut_cliente2 AND flg_insert = @maxFecha AND periodo<@periodo_1 SELECT @periodo_3 = max(periodo) FROM ut_cob_distibucion_t WHERE flg_rut_cliente = @rut_cliente2 AND flg_insert = @maxFecha AND periodo<@periodo_2 /*
            select @periodo_1
            ,@periodo_2
            ,@periodo_3
            */ UPDATE #tmp_reso SET rut_ejecutiva_2 = isnull((SELECT top 1 d.usr_rut FROM ut_cob_desistidosUsuario_t d WHERE d.rut_cliente=@rut_cliente2 AND d.rut_deudor=#tmp_reso.rut AND d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_2) WHERE #tmp_reso.situacion IN ('PAD','PSR') AND periodo=@periodo_3 UPDATE #tmp_reso SET rut_ejecutiva_1 = isnull((SELECT top 1 d.usr_rut FROM ut_cob_desistidosUsuario_t d WHERE d.rut_cliente=@rut_cliente2 AND d.rut_deudor=#tmp_reso.rut AND d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_1) WHERE #tmp_reso.situacion IN ('PAD','PSR') AND periodo=@periodo_2 UPDATE #tmp_reso SET rut_ejecutiva_0 = isnull((SELECT top 1 d.usr_rut FROM ut_cob_desistidosUsuario_t d WHERE d.rut_cliente=@rut_cliente2 AND d.rut_deudor=#tmp_reso.rut AND d.res_periodo=#tmp_reso.periodo),rut_ejecutiva_0) WHERE #tmp_reso.situacion IN ('PAD','PSR') AND periodo=@periodo_1 --select 'PAD', * from #tmp_reso where rut =86454800
 UPDATE #tmp_reso SET rut_ejecutiva_2 = isnull((SELECT TOP 1 flg_rut_usuario FROM ut_cob_usuarios_m WHERE flg_user =(SELECT top 1 pag_usuario FROM ut_cob_pago_p WHERE pag_liq= (SELECT top 1 neg_numero FROM ut_cob_negocio_t WHERE neg_n_sist=(SELECT max(res_codigo) FROM ut_cob_resolucion_t r WHERE r.flg_rut_cliente = @rut_cliente2 AND r.res_rut=#tmp_reso.rut AND r.res_periodo=@periodo_3 AND r.res_situacion IN ('NE','PE') )) ) ),rut_ejecutiva_2) WHERE #tmp_reso.situacion IN ('NE','PE') AND periodo=@periodo_3 UPDATE #tmp_reso SET rut_ejecutiva_1 = isnull((SELECT TOP 1 flg_rut_usuario FROM ut_cob_usuarios_m WHERE flg_user =(SELECT top 1 pag_usuario FROM ut_cob_pago_p WHERE pag_liq= (SELECT top 1 neg_numero FROM ut_cob_negocio_t WHERE neg_n_sist=(SELECT max(res_codigo) FROM ut_cob_resolucion_t r WHERE r.flg_rut_cliente= @rut_cliente2 AND r.res_rut=#tmp_reso.rut AND r.res_periodo=@periodo_2 AND r.res_situacion IN ('NE','PE') )) ) ),rut_ejecutiva_1) WHERE #tmp_reso.situacion IN ('NE','PE') AND periodo=@periodo_2 UPDATE #tmp_reso SET rut_ejecutiva_0 = isnull((SELECT TOP 1 flg_rut_usuario FROM ut_cob_usuarios_m WHERE flg_user =(SELECT top 1 pag_usuario FROM ut_cob_pago_p WHERE pag_liq= (SELECT top 1 neg_numero FROM ut_cob_negocio_t WHERE neg_n_sist=(SELECT max(res_codigo) FROM ut_cob_resolucion_t r WHERE r.flg_rut_cliente= @rut_cliente2 AND r.res_rut=#tmp_reso.rut AND r.res_periodo=@periodo_1 AND r.res_situacion IN ('NE','PE') AND res_fecha>=@maxFecha )) )),rut_ejecutiva_0) WHERE #tmp_reso.situacion IN ('NE','PE') AND periodo=@periodo_1 END --select 'NEPE',* from #tmp_reso where rut =86454800
 /*
        select 'flg_rut_usuario', flg_rut_usuario from ut_cob_usuarios_m where flg_user =(
        select top 1 pag_usuario from ut_cob_pago_p where pag_liq=
        (select top 1 neg_numero from ut_cob_negocio_t where neg_n_sist=(
        select max(res_codigo) from ut_cob_resolucion_t r where r.flg_rut_cliente= @rut_cliente and r.res_rut=86454800 and r.res_periodo=@periodo_1 and r.res_situacion  IN ('NE','PE')
        ))
        )
        */ /**/ UPDATE #tmp_reso SET ejecutiva = (SELECT txt_nombre + ' ' + txt_apellido FROM ut_cob_usuarios_m WHERE flg_rut_usuario=#tmp_reso.rut_ejecutiva_0) UPDATE #tmp_reso SET ejecutiva_a0 = (SELECT txt_nombre + ' ' + txt_apellido FROM ut_cob_usuarios_m WHERE flg_rut_usuario=#tmp_reso.rut_ejecutiva_0) UPDATE #tmp_reso SET ejecutiva_a1 = (SELECT txt_nombre + ' ' + txt_apellido FROM ut_cob_usuarios_m WHERE flg_rut_usuario=#tmp_reso.rut_ejecutiva_1) UPDATE #tmp_reso SET ejecutiva_a2 = (SELECT txt_nombre + ' ' + txt_apellido FROM ut_cob_usuarios_m WHERE flg_rut_usuario=#tmp_reso.rut_ejecutiva_2) --select * from #tmp_reso
 --select * from #tmp_reso order by rut,periodo
 PRINT '*** 04 INSERTA RUT SALIDA ***' PRINT getdate() INSERT INTO #tmp_salida (rut) SELECT rut FROM #tmp_reso GROUP BY rut --select count(*) from #tmp_salida where rut in (select #tmp_reso.rut from #tmp_reso where periodo=@periodo)
 PRINT '*** 05 ACTUALIZA MONTO SALIDA***' PRINT getdate() SELECT * FROM #tmp_salida --select sum(monto) monto_uperiodo from #tmp_reso where periodo=202301
 UPDATE #tmp_salida SET p_202301 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202301 AND #tmp_reso.rut = #tmp_salida.rut),0) ,








                                         p_202302 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202302 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                          p_202303 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202303 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                           p_202304 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202304 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                            p_202305 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202305 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                             p_202306 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202306 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                              p_202307 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202307 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                                               p_202308 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202308 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                                                                p_202309 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202309 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                                                                                 p_202310 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202310 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                                                                                                  p_202311 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202311 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                                                                                                                   p_202312 = isnull((SELECT sum(monto) FROM #tmp_reso WHERE periodo=202312 AND #tmp_reso.rut = #tmp_salida.rut),0) ,
                                                                                                                                                                                                                                    situacion = (SELECT estados=STUFF((SELECT ', ' + situacion FROM #tmp_reso WHERE #tmp_reso.rut = r.rut GROUP BY rut, situacion FOR XML path(''),TYPE).value('.[1]','nvarchar(max)'),1,2,'') FROM #tmp_reso r WHERE r.rut =#tmp_salida.rut GROUP BY rut) --select sum([p_202301]) s1 from #tmp_salida
 DELETE FROM #tmp_salida WHERE 202307=0 AND 202308=0 AND 202309=0 AND 202310=0 --select sum([p_202301]) s2 from #tmp_salida
 --select * from #tmp_salida where rut=70931900
 PRINT '*** 05.1 ACTUALIZA LEY BUSTOS SALIDA***' PRINT getdate() PRINT '*** 06 ASOCIA DISTRIBUCION***' PRINT getdate() INSERT INTO #tmp_dis(codigo,rut,dis)
SELECT res_codigo,
       res_rut,
       'N'
FROM ut_cob_resolucion_t
WHERE res_rut IN
    (SELECT rut
     FROM #tmp_salida)
  AND res_periodo IN
    (SELECT periodo
     FROM #tmp_periodos)
  AND flg_rut_cliente IN
    (SELECT rut_cliente
     FROM #tmp_cliente)--and isnull(res_juicio,'N')='N'
 PRINT '*** 07 MARCA DISTRIBUIDO***' PRINT getdate()
  UPDATE #tmp_dis
  SET dis='S' WHERE codigo IN
    (SELECT res_codigo
     FROM ut_cob_distibucion_t
     WHERE flg_insert='2018-09-30')--select * from #tmp_dis where dis='S'
 --select * from #tmp_dis where dis='N'
 --delete from #tmp_salida where rut in (select rut from #tmp_dis where dis='S')

  UPDATE #tmp_salida
  SET nuevo='N' PRINT '*** 08 INDICA SI ES NUEVO***'
  SELECT @periodo,
         @rut_cliente PRINT getdate() IF @rut_cliente=dbo.getcapital() BEGIN
  UPDATE #tmp_salida
  SET nuevo= CASE isnull(
                           (SELECT count(*)
                            FROM ut_cob_resolucion_t
                            WHERE flg_rut_cliente=@rut_cliente
                              AND res_rut = #tmp_salida.rut
                              AND res_periodo <@periodo --and isnull(res_juicio,'N')='N'
 ),0)
                 WHEN 0 THEN 'S'
                 ELSE 'N'
             END END ELSE BEGIN
  UPDATE #tmp_salida
  SET nuevo= CASE isnull(
                           (SELECT count(*)
                            FROM ut_cob_resolucion_t
                            WHERE flg_rut_cliente IN
                                (SELECT rut_cliente
                                 FROM #tmp_cliente)
                              AND res_rut = #tmp_salida.rut
                              AND res_periodo <@periodo --and isnull(res_juicio,'N')='N'
 ),0)
                 WHEN 0 THEN 'S'
                 ELSE 'N'
             END END
  UPDATE #tmp_salida
  SET can= isnull(
                    (SELECT count(*)
                     FROM ut_cob_resolucion_t
                     WHERE flg_rut_cliente IN
                         (SELECT rut_cliente
                          FROM #tmp_cliente)
                       AND res_rut = #tmp_salida.rut
                       AND res_periodo <@periodo --and isnull(res_juicio,'N')='N'
 ),0)
  UPDATE #tmp_salida
  SET u_pe= isnull(
                     (SELECT max(res_periodo)
                      FROM ut_cob_resolucion_t
                      WHERE flg_rut_cliente IN
                          (SELECT rut_cliente
                           FROM #tmp_cliente)
                        AND res_rut = #tmp_salida.rut
                        AND res_periodo <@periodo --and isnull(res_juicio,'N')='N'
 ),0) PRINT '*** 09 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET ultPeriodo= isnull(
                           (SELECT max(res_periodo)
                            FROM ut_cob_resolucion_t
                            WHERE flg_rut_cliente IN
                                (SELECT rut_cliente
                                 FROM #tmp_cliente)
                              AND res_rut = #tmp_salida.rut
                              AND res_situacion IN ('PE',
                                                    'PAD',
                                                    'PSR',
                                                    'NE')
                              AND res_periodo<>@periodo --and isnull(res_juicio,'N')='N'
 ),0)
  UPDATE #tmp_salida
  SET fec_pago= isnull(
                         (SELECT max(res_fecha)
                          FROM ut_cob_resolucion_t
                          WHERE flg_rut_cliente IN
                              (SELECT rut_cliente
                               FROM #tmp_cliente)
                            AND res_rut = #tmp_salida.rut
                            AND res_situacion IN ('PE',
                                                  'PAD',
                                                  'PSR')
                            AND res_periodo = ultPeriodo --and isnull(res_juicio,'N')='N'
 ),'1900-01-01') WHERE isnull(ultPeriodo,
                              0)>0
  UPDATE #tmp_salida
  SET fec_pago= (
                   (SELECT max(neg_f_negocio)
                    FROM ut_cob_negocio_t
                    WHERE neg_numero IN (
                                           (SELECT DISTINCT res_codigo
                                            FROM ut_cob_resolucion_t
                                            WHERE flg_rut_cliente IN
                                                (SELECT rut_cliente
                                                 FROM #tmp_cliente)
                                              AND res_rut = #tmp_salida.rut
                                              AND res_situacion ='NE'
                                              AND res_periodo = ultPeriodo)) )) WHERE ultPeriodo > 0
  AND fec_pago IS NULL PRINT '*** 10 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET ultEstado =isnull(
                          (SELECT estados=STUFF(
                                                  (SELECT ', ' + res_situacion
                                                   FROM ut_cob_resolucion_t
                                                   WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                       (SELECT rut_cliente
                                                        FROM #tmp_cliente)
                                                     AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                     AND ut_cob_resolucion_t.res_periodo = r.res_periodo --and isnull(res_juicio,'N')='N'

                                                   GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                   FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                           FROM ut_cob_resolucion_t r
                           WHERE r.flg_rut_cliente IN
                               (SELECT rut_cliente
                                FROM #tmp_cliente)
                             AND r.res_rut =#tmp_salida.rut
                             AND r.res_periodo = #tmp_salida.ultPeriodo --and isnull(res_juicio,'N')='N'

                           GROUP BY flg_rut_cliente,
                                    res_rut,
                                    res_periodo),'') PRINT '*** 11 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_01 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202301 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202301 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202301>0 PRINT '*** 12 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_02 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202302 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202302 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202302>0 PRINT '*** 13 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_03 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202303 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202303 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202303>0 PRINT '*** 14 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_04 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202304 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202304 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202304>0 PRINT '*** 15 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_05 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202305 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202305 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202305>0 PRINT '*** 16 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_06 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202306 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202306 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202306>0 PRINT '*** 16.1 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_07 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202307 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202307 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202307>0 PRINT '*** 16.2 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_08 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202308 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202308 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202308>0 PRINT '*** 16.3 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_09 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202309 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202309 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202309>0 PRINT '*** 16.4 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET situacion_10 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202310 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202310 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202310>0
  UPDATE #tmp_salida
  SET situacion_11 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202311 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202311 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202311>0
  UPDATE #tmp_salida
  SET situacion_12 =isnull(
                             (SELECT estados=STUFF(
                                                     (SELECT ', ' + res_situacion
                                                      FROM ut_cob_resolucion_t
                                                      WHERE ut_cob_resolucion_t.flg_rut_cliente IN
                                                          (SELECT rut_cliente
                                                           FROM #tmp_cliente)
                                                        AND ut_cob_resolucion_t.res_rut = r.res_rut
                                                        AND ut_cob_resolucion_t.res_periodo = 202312 --and isnull(res_juicio,'N')='N'

                                                      GROUP BY flg_rut_cliente, res_rut, res_periodo, res_situacion
                                                      FOR XML path(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '')
                              FROM ut_cob_resolucion_t r
                              WHERE r.flg_rut_cliente IN
                                  (SELECT rut_cliente
                                   FROM #tmp_cliente)
                                AND r.res_rut =#tmp_salida.rut
                                AND r.res_periodo = 202312 --and isnull(res_juicio,'N')='N'

                              GROUP BY flg_rut_cliente,
                                       res_rut,
                                       res_periodo),'') WHERE p_202312>0 PRINT '*** 17 ***' PRINT getdate()
  UPDATE #tmp_salida
  SET ejecutiva =
    (SELECT top 1 ejecutiva
     FROM #tmp_reso
     WHERE #tmp_reso.rut = #tmp_salida.rut
       AND ejecutiva IS NOT NULL )
  UPDATE #tmp_salida
  SET ejecutiva_a0 =
    (SELECT top 1 ejecutiva_a0
     FROM #tmp_reso
     WHERE #tmp_reso.rut = #tmp_salida.rut
       AND ejecutiva_a0 IS NOT NULL --and #tmp_reso.periodo = @periodo_1
 )
  UPDATE #tmp_salida
  SET ejecutiva_a1 =
    (SELECT top 1 ejecutiva_a1
     FROM #tmp_reso
     WHERE #tmp_reso.rut = #tmp_salida.rut
       AND ejecutiva_a1 IS NOT NULL --and #tmp_reso.periodo = @periodo_2
 )
  UPDATE #tmp_salida
  SET ejecutiva_a2 =
    (SELECT top 1 ejecutiva_a2
     FROM #tmp_reso
     WHERE #tmp_reso.rut = #tmp_salida.rut
       AND ejecutiva_a2 IS NOT NULL --and #tmp_reso.periodo = @periodo_3
 )
  UPDATE #tmp_salida
  SET res_juicio=isnull(
                          (SELECT count(*)
                           FROM ut_cob_resolucion_t r
                           WHERE flg_rut_cliente IN
                               (SELECT rut_cliente
                                FROM #tmp_cliente)
                             AND res_rut = #tmp_salida.rut
                             AND res_juicio='S'
                             AND res_periodo IN
                               (SELECT periodo
                                FROM #tmp_periodos)),0)
  UPDATE #tmp_salida
  SET es_dnp=isnull(
                      (SELECT count(*)
                       FROM ut_cob_resolucion_t r
                       WHERE flg_rut_cliente IN
                           (SELECT rut_cliente
                            FROM #tmp_cliente)
                         AND res_rut = #tmp_salida.rut
                         AND res_tipo_cobro='DNP'
                         AND isnull(res_obs, '')=''
                         AND res_periodo IN
                           (SELECT periodo
                            FROM #tmp_periodos)),0) , es_car=isnull(
                                                                      (SELECT count(*)
                                                                       FROM ut_cob_resolucion_t r
                                                                       WHERE flg_rut_cliente IN
                                                                           (SELECT rut_cliente
                                                                            FROM #tmp_cliente)
                                                                         AND res_rut = #tmp_salida.rut
                                                                         AND res_tipo_cobro='CAR'
                                                                         AND isnull(res_obs, '')=''
                                                                         AND res_periodo IN
                                                                           (SELECT periodo
                                                                            FROM #tmp_periodos)),0) , es_cai=isnull(
                                                                                                                      (SELECT count(*)
                                                                                                                       FROM ut_cob_resolucion_t r
                                                                                                                       WHERE flg_rut_cliente IN
                                                                                                                           (SELECT rut_cliente
                                                                                                                            FROM #tmp_cliente)
                                                                                                                         AND res_rut = #tmp_salida.rut
                                                                                                                         AND res_tipo_cobro='DNP'
                                                                                                                         AND isnull(res_obs, '')='CAI'
                                                                                                                         AND res_periodo IN
                                                                                                                           (SELECT periodo
                                                                                                                            FROM #tmp_periodos)),0) , es_cam=isnull(
                                                                                                                                                                      (SELECT count(*)
                                                                                                                                                                       FROM ut_cob_resolucion_t r
                                                                                                                                                                       WHERE flg_rut_cliente IN
                                                                                                                                                                           (SELECT rut_cliente
                                                                                                                                                                            FROM #tmp_cliente)
                                                                                                                                                                         AND res_rut = #tmp_salida.rut
                                                                                                                                                                         AND res_tipo_cobro='DNP'
                                                                                                                                                                         AND isnull(res_obs, '')='CAM'
                                                                                                                                                                         AND res_periodo IN
                                                                                                                                                                           (SELECT periodo
                                                                                                                                                                            FROM #tmp_periodos)),0)--select * from #tmp_reso

  UPDATE #tmp_salida
  SET cod_gesdeudor =
    (SELECT max(a.cod_gesdeudor)
     FROM ut_cob_gesdeudor_t a
     WHERE a.rut_cliente=dbo.getProvida()
       AND ((a.cod_gestion='01'
             AND (a.cod_est_gestion='12'
                  OR a.cod_est_gestion='21'))
            OR (a.cod_gestion='02'
                AND (a.cod_est_gestion='17'
                     OR a.cod_est_gestion='21'))
            OR (a.cod_gestion='06'
                AND (a.cod_est_gestion='02'
                     OR a.cod_est_gestion='21')))
       AND a.fec_gestion>=DATEADD(MONTH, -1, getdate())
       AND a.fec_compromiso>='2022-06-01'
       AND a.flg_rut_deudor=#tmp_salida.rut ) ,
      compromiso_pago =
    (SELECT count(*)
     FROM ut_cob_gesdeudor_t g
     WHERE g.rut_cliente=dbo.getProvida()
       AND ((g.cod_gestion='01'
             AND g.cod_est_gestion='12')
            OR (g.cod_gestion='02'
                AND g.cod_est_gestion='17')
            OR (g.cod_gestion='06'
                AND g.cod_est_gestion='02'))
       AND g.fec_gestion>=DATEADD(MONTH, -3, getdate())
       AND g.fec_compromiso>='2023-09-15'
       AND g.flg_rut_deudor=#tmp_salida.rut )
  UPDATE #tmp_salida
  SET fec_compromiso =
    (SELECT g.fec_compromiso
     FROM ut_cob_gesdeudor_t g
     WHERE g.cod_gesdeudor=#tmp_salida.cod_gesdeudor) WHERE isnull(compromiso_pago,
                                                                   0)>0
  UPDATE #tmp_salida
  SET rut_ejecutiva_ge =
    (SELECT g.flg_rut_usuario
     FROM ut_cob_gesdeudor_t g
     WHERE g.cod_gesdeudor = #tmp_salida.cod_gesdeudor) WHERE isnull(cod_gesdeudor,
                                                                     0)>0
  UPDATE #tmp_salida
  SET ejecutiva_ge =
    (SELECT ltrim(rtrim(ltrim(rtrim(u.txt_nombre)) + ' ' + ltrim(rtrim(u.txt_apellido))))
     FROM ut_cob_usuarios_m u
     WHERE u.flg_rut_usuario = #tmp_salida.rut_ejecutiva_ge) WHERE isnull(cod_gesdeudor,
                                                                          0)>0 IF @rut_cliente=dbo.gethabitat() BEGIN
  UPDATE #tmp_salida
  SET carga_especial =
    (SELECT count(*)
     FROM ut_cob_resolucion_t r
     WHERE r.flg_rut_cliente =dbo.getpilotohabitat()
       AND #tmp_salida.rut =r.res_rut
       AND res_periodo>=202312) END ELSE BEGIN
  UPDATE #tmp_salida
  SET carga_especial =
    (SELECT count(*)
     FROM ut_cob_resolucion_t r
     WHERE r.flg_rut_cliente IN
         (SELECT rut_cliente
          FROM #tmp_cliente)
       AND #tmp_salida.rut =r.res_rut
       AND r.res_tipo_cobro='CAR')
  UPDATE #tmp_salida
  SET carga_mixta =
    (SELECT CASE count(*)
                WHEN 0 THEN 'N'
                ELSE 'S'
            END
     FROM ut_cob_resolucion_t r
     WHERE r.flg_rut_cliente IN
         (SELECT rut_cliente
          FROM #tmp_cliente)
       AND #tmp_salida.rut =r.res_rut
       AND r.res_tipo_cobro='CESP') END /*habitat*/ DECLARE @fecha_gesAFP datetime
  SET @fecha_gesAFP = '2020-07-01' /*seleccionar gestiones HABITAT*/ /*correos*/ --select '#tmp_gestiones_hab', * from #tmp_gestiones_hab

  UPDATE #tmp_salida
  SET carga_ultima_remesa='N' --where rut in (select res_rut from ut_cob_resolucion_t r where r.flg_rut_cliente in (select rut_cliente from #tmp_cliente) and r.res_periodo=202305 )

  UPDATE #tmp_salida
  SET carga_ultima_remesa='S' WHERE rut IN
    (SELECT res_rut
     FROM ut_cob_resolucion_t r
     WHERE r.flg_rut_cliente IN
         (SELECT rut_cliente
          FROM #tmp_cliente)
       AND r.res_periodo=@periodo )--delete from #tmp_salida where  carga_ultima_remesa='S'

  UPDATE #tmp_salida
  SET fec_carga_periodo_pag =
    (SELECT min(res_fec_ing)
     FROM ut_cob_resolucion_t
     WHERE flg_rut_cliente IN
         (SELECT rut_cliente
          FROM #tmp_cliente)
       AND res_periodo=#tmp_salida.ultPeriodo )
  UPDATE #tmp_salida
  SET ult_periodo_cca =
    (SELECT max(res_periodo)
     FROM ut_cob_resolucion_t
     WHERE flg_rut_cliente IN
         (SELECT rut_cliente
          FROM #tmp_cliente)
       AND res_rut = #tmp_salida.rut
       AND res_situacion ='CCA' )
  UPDATE #tmp_salida
  SET tiene_cca ='N'
  UPDATE #tmp_salida
  SET tiene_cca ='S' WHERE isnull(ult_periodo_cca,
                                  0) >0
  SELECT sum([p_202301]) s3
  FROM #tmp_salida
  UPDATE #tmp_salida
  SET periodo_carga =
    (SELECT max(res_periodo)
     FROM ut_cob_resolucion_t r
     WHERE flg_rut_cliente IN
         (SELECT c.rut_cliente
          FROM #tmp_cliente c)
       AND r.res_rut = #tmp_salida.rut
       AND r.res_fec_ing = @fechaCargaPeriodo) DECLARE @tipofecha int
  SET @tipofecha = 2 /*ultimos 2 meses*/ /*SOLO PARA PROVIDA*/ /*
        update #tmp_salida
        set sms_enviado = webcob.dbo.getTieneGestionSMS(@rut_cliente,rut,@tipofecha)
        , correos_vi = webcob.dbo.getTieneGestionCorreoVI(@rut_cliente,rut,@tipofecha)
        */
  UPDATE #tmp_salida
  SET gestion_valida_cam = dbo.getTieneGestionRealizadaFechasCampana(@rut_cliente,
                                                                     rut,
                                                                     '2023-10-01',
                                                                     '2023-10-30') , gestion_valida_eje = dbo.getTieneGestionValida(@rut_cliente,
                                                                                                                                    rut,
                                                                                                                                    '2023-10-01',
                                                                                                                                    '2023-10-30')
  SELECT *
  FROM #tmp_salida
  SELECT rut ,
         flg_dv_deudor ,
         isnull(replace(replace(replace(d.txt_razon_social, char(10), ''), char(13), ''), char(9), ''),
                '') txt_razon_social ,
               d.flg_rut_representante ,
               d.flg_dv_reperesentante ,
               isnull(replace(replace(replace(d.txt_representante, char(10), ''), char(13), ''), char(9), ''),
                      '') txt_representante ,
                     isnull(replace(replace(replace(d.txt_direccion, char(10), ''), char(13), ''), char(9), ''),
                            '') txt_direccion ,
                           isnull(replace(replace(replace(d.txt_ciudad, char(10), ''), char(13), ''), char(9), ''),
                                  '') txt_ciudad ,
                                 isnull(replace(replace(replace(d.txt_comuna, char(10), ''), char(13), ''), char(9), ''),
                                        '') txt_comuna ,
                                       ISNULL(
                                                (SELECT top 1 g.comuna
                                                 FROM exportacion.dbo.tb_cob_deudor_geo_t g
                                                 WHERE g.rut_empleador=rut) ,'') COMUNA_INFO ,
                                             ISNULL(
                                                      (SELECT top 1 g.region
                                                       FROM exportacion.dbo.tb_cob_deudor_geo_t g
                                                       WHERE g.rut_empleador=rut) ,'') REGION_INFO ,
                                                   gestion_valida_cam [GES. VALIDA CAMPAÑA] ,
                                                   gestion_valida_eje [GES. VALIDA EJECUTIVA] ,
                                                   p_202306 [P 202306] ,
                                                   p_202307 [P 202307] ,
                                                   p_202308 [P 202308] ,
                                                   p_202309 [P 202309] ,
                                                   isnull(situacion_06,
                                                          '') situacion_06 ,
                                                         isnull(situacion_07,
                                                                '') situacion_07 ,
                                                               isnull(situacion_08,
                                                                      '') situacion_08 ,
                                                                     isnull(situacion_09,
                                                                            '') situacion_09 ,
                                                                           nuevo ,
                                                                           ultPeriodo [ULTIMO PERIODO PAGADO] ,
                                                                           isnull(format(fec_pago, 'dd-MM-yyyy'),
                                                                                  '') [FECHA ULTIMO PERIODO PAGADO] ,
                                                                                 fec_carga_periodo_pag FECHA_CARGA_PERIODO ,
                                                                                 ultEstado ,
                                                                                 ejecutiva ,
                                                                                 sms_enviado ,
                                                                                 correos_vi ,
                                                                                 estado_cn ,
                                                                                 ejecutiva_a0 ,
                                                                                 ejecutiva_a1 ,
                                                                                 ejecutiva_a2 ,
                                                                                 can ,
                                                                                 u_pe ,
                                                                                 res_juicio ,
                                                                                 compromiso_pago ,
                                                                                 fec_compromiso ,
                                                                                 cod_gesdeudor [ID GESTION] ,
                                                                                 isnull(carga_especial,
                                                                                        0) [CARTERA ESPECIAL o PILOTO] ,
                                                                                       isnull(carga_mixta,
                                                                                              'N') [CARTERA MIXTA] ,
                                                                                             ISNULL(tiene_cca,
                                                                                                    'N') [CCA INFORMADO] ,
                                                                                                   isnull(ult_periodo_cca,
                                                                                                          0) [CCA ULT PERIODO] ,
                                                                                                         es_dnp ,
                                                                                                         es_car ,
                                                                                                         es_cai ,
                                                                                                         es_cam ,
                                                                                                         periodo_carga --into	EMMA.dbo.CAM_HAB_20231123_RUT
 -- drop table exportacion.dbo.INF_PRO_CCA_20231110_V2
 INTO exportacion.dbo.INF_HAB_TOT_20231221_AUT
FROM #tmp_salida
LEFT JOIN ut_cob_deudor_m d ON d.flg_rut_deudor = rut
ORDER BY rut --drop table EMMA.dbo.CAM_HAB_20230810_RUT
 --drop table exportacion.dbo.INF_PRO_TOT_20230718_V1

DROP TABLE #tmp_reso
DROP TABLE #tmp_salida
DROP TABLE #tmp_dis
DROP TABLE #tmp_periodos
DROP TABLE #tmp_periodos_vigente
DROP TABLE #tmp_estados
DROP TABLE #tmp_gestionesvalidas
DROP TABLE #tmp_gestiones_hab
DROP TABLE #tmp_gestiones_cap
DROP TABLE #tmp_gestiones_pro
DROP TABLE #tmp_pads
DROP TABLE #tmp_gesdeudor
DROP TABLE #tmp_cambiosestado
DROP TABLE #tmp_cliente /*
        select * from ut_cob_resolucion_t where res_rut=76977140 and flg_rut_cliente=98000100

        select * from  ut_cob_distibucion_t where res_codigo=322713
        */ --select format(getdate(),'dd-MM-yyyy')
