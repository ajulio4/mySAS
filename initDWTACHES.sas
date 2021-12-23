%macro dwBuild(nbTables=);
%do i=31 %to &nbTables.;

	data dw_T_S&i._1;
	set
		ods_dat.T_GRECCO_AGS&i. 
	    ods_dat.T_BAC_AGS&i. 
	    ods_dat.T_DSN_AGS&i.
	    ods_dat.T_GRECCO_RUS&i. 
	    ods_dat.T_BAC_RUS&i. 
	    ods_dat.T_DSN_RUS&i.
		;
	attrib
	nb_taches length= 8 format= best32.6 informat= 8. label= "Compteur tâche " ;
	nb_taches = 1;
	format DT_FIN_PERIODE date9.;
	length DT_PERIODE $7;
	DT_FIN_PERIODE = dt_extraction-2;
	/*DT_PERIODE= "2021S"||%left(%trim(&i.));*/
	DT_PERIODE= cats("2021S",&i.);
	/*if DT_FIN_PERIODE eq .  then 
	   do;
	    format DT_FIN_PERIODE date9.;
		DT_PERIODE =&dt_semaine. ;
		DT_FIN_PERIODE = &DT_DER_VENDREDI. ;

	end;*/

run;

/**/
proc sql;
create table dw_dat.dw_T_S&i. as
select a.dt_extract_init,
       a.id_taches,
       a.co_statut_tache	,
       a.lib_statut_tache,
       a.code_brique	,
       a.co_type_tache,
       a.lib_c_type_tache,
       a.lib_type_tache	,
       a.cat_type_tache	,
       a.mod_affectation	,
       a.id_possesseur	,
       a.login_utilisateur	,
       a.utilisateur	,
       a.no_fonc_entite_gestion	,
       a.entite_gestion	,
       a.priorite,
       a.sensibilite	,
       a.dt_critique	,
       a.co_obj_gestion_ref	,
       a.id_int_obj_gestion_ref	,
       a.id_public_ref,	
       a.designation_ref,
       a.co_obj_gestion_aff	,
       a.id_int_obj_gestion_aff	,
       a.id_public_aff	,
       a.designation_aff	,
       a.co_obj_gestion_ct1	,
       a.id_int_obj_gestion_ct1	,
       a.id_public_ct1	,
       a.designation_ct1,
       a.commentaire_tache	,
       a.info_tache	,
       a.info_tache_a	,
       a.info_tache_b	,
       a.info_tache_c	,
       a.dt_affectation	,
       a.dt_creation	,
       a.utili_creation,
       a.dt_maj	,
       a.utili_maj	,
       a.dt_extraction,
       a.ur_origine,
       a.nb_taches,
       a.DT_FIN_PERIODE,
       a.DT_PERIODE	,
       b.code_sous_brique

from dw_T_S&i._1 a 
left join Ref_dat.dw_rf_retc_code_sous_brique b on a.co_type_tache = b.codeTypeTache
order by a.dt_periode,a.ur_origine,a.code_brique,b.code_sous_brique, id_taches
;
quit;
%end;



%mend;

%dwBuild(nbTables=50);