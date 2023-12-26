use webcob;
declare @ema_actual varchar(200);
declare @pas_actual varchar(200);
declare @ema_nuevo varchar(200);
declare @pas_nuevo varchar(200);

select * from ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPL' ;

select * from ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPP';

select @ema_actual = val_texto
      ,@ema_nuevo  = val_texto2 
from ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPL';


select @pas_actual = val_texto
      ,@pas_nuevo  = val_texto2 
from ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPP';

select @ema_actual  ema_actual
	  ,@pas_actual	pas_actual
	  ,@ema_nuevo 	ema_nuevo 
	  ,@pas_nuevo 	pas_nuevo ;


UPDATE ut_cob_parametros_m 
set val_texto = @ema_nuevo
,   val_texto2 = @ema_actual
where cod_app='SMAIL' and cod_parametro='SMTPL';


UPDATE ut_cob_parametros_m 
set val_texto  = @pas_nuevo
,   val_texto2 = @pas_actual
where cod_app='SMAIL' and cod_parametro='SMTPP';



select * from ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPL'; 
select * from ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPP';
