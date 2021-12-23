
%macro dmBuild(nbTables=);
%do numdw=32 %to &nbTables.;
%let numprec = %eval(&numdw.-1);

	proc sql;
	create table extractDW&numdw. as 
		select max(dt_extraction) as dtMaxDW, max(dt_fin_periode)-5 as dtMaxDWPrec
		from dw_dat.DW_T_S&numdw.
	;
	quit;
	************;
	data _null_ ;
	set extractDW&numdw.;
	call symput(cats('dtMaxDWPrec_',&numdw.), put(dtMaxDWPrec,10.));
	call symput(cats('dtMaxDW_',&numdw.), put(dtMaxDW,10.));
	run;
	*******************;
	proc sql;
	create table dw_t_prec&numdw. as
	select a.id_taches as id_taches_prec,
	   a.lib_statut_tache as lib_statut_tache_prec,
	   a.ur_origine  as ur_origine_prec ,
	   a.code_brique as code_brique_prec,
	   a.dt_extraction as dt_extraction_prec,
	   a.dt_maj as dt_maj_prec,
	   a.co_type_tache as co_type_taches_prec,
	   a.utilisateur as utilisateur_prec,
	   a.no_fonc_entite_gestion as no_fonc_entite_gestion_prec,
	   1 as stock_precedent
   from dw_dat.DW_T_S&numprec.(where=(dt_extraction=&&dtMaxDWPrec_&numdw. and upcase(tranwrd(lib_statut_tache,"é","e")) <> "TERMINE" )) a 
	;
	quit;

	**********************;
	proc sort data=dw_t_prec&numdw. ;by ur_origine_prec code_brique_prec id_taches_prec ; run;
	************************;

	proc sql;
	create table dm01_&numdw. as
	select a.id_taches,
		   a.lib_statut_tache,
		   a.ur_origine,
		   a.code_brique,
		   case 
				when a.code_brique = "A" then "BAC"
				when a.code_brique = "C" then "GRECCO"
				when a.code_brique = "W" then "Centrale DSN"
				else "Inconnue"
		   end as brique ,
		   a.code_sous_brique,
		   a.dt_extraction,
		   a.dt_maj,
		   a.co_type_tache,
		   a.lib_type_tache,
		   case
		  		when upcase(a.co_type_tache) in ("WN2ADNDI","WN2ADNFE","WN2ADNIN","WN1ENTDF","WN1ETADP","WN1ETADI","WN1ETAPI","WN1ETADF") then "TRA"
				when upcase(a.co_type_tache) in ("WN4CTCOT","WN3CATIN","WN3CATMU","WN4CTTAA","WN3COMSA","WN4CTMAN") then "ATT"
				when upcase(a.co_type_tache) in ("WN2ADNIC","WN2ADNIA") then "ANO"
				when upcase(a.co_type_tache) in ("TCMN0301","TCMN0205","TCMN0202","TCMN0302","TCMN0303","TCMN0304","TCMN0802","TCMN0801","TCMN0204","TCMN0201") then "GRECCO"
				else "Non classé  "
		   end as niv_integration,
		   a.utilisateur,
		   a.no_fonc_entite_gestion,
		   reg.LIB_EG_A_AFFICHER as lib_eg ,
		   reg.RS_CG,
		   a.id_int_obj_gestion_ref,
		   a.id_public_ref,
		   a.designation_ref,
		   a.commentaire_tache,
		   a.info_tache,
		   a.info_tache_a,
		   a.info_tache_b,
		   a.info_tache_c,
		   a.dt_affectation,
		   a.dt_creation,
		   a.utili_creation,
		   a.utili_maj,
		   a.nb_taches,
	       0 as entrees,
		   0 as sorties,
		   0 as stock_en_cours,
		   b.stock_precedent,
		   0 as transferees,
		  DT_PERIODE as DT_PERIODE, DT_FIN_PERIODE,
		  case when reg.region = '' then 'Manquant' 
	       else reg.region end as region,
		  case
		  		when upcase(a.co_type_tache) in ("TCMN0302","TCMN0304","TCMN0801","TCMN0802","WN1ENTDF","WN1ETADF","WN1ETADI","WN1ETADP","WN1ETAPI","WN2ADNDI","WN2ADNFE","WN2ADNIN","WN3CATIN","WN3CATMU") then "P1"
				when upcase(a.co_type_tache) in ("TCMN0201","TCMN0202","TCMN0204","TCMN0301","WN2ADNIA","WN2ADNIC") then "P2"
				when upcase(a.co_type_tache) in ("WN4CTCOT","WN4CTMAN") then "P3"
				when upcase(a.co_type_tache) in ("WN4CTTAA","TCMN0205","TCMN0303") then "TA"
				else "NP"
		end as priorite

	from dw_dat.DW_T_S&numdw.(where=(dt_extraction=&&dtMaxDW_&numdw.)) a 
	left join Ref_dat.dw_rf_retc_eg_regions reg on
	a.no_fonc_entite_gestion = reg.no_fonc_eg
	left join dw_t_prec&numdw. b on (a.ur_origine= b.ur_origine_prec and a.code_brique= b.code_brique_prec and a.id_taches= b.id_taches_prec and a.no_fonc_entite_gestion=b.no_fonc_entite_gestion_prec)
	;
	quit;
	*****************************;

	data DM_PIL_0_&numdw.;
	set dm01_&numdw.;
	if (dt_creation ge &&dtMaxDWPrec_&numdw.) then entrees = 1 ;else entrees = 0;
	if (dt_maj ge &&dtMaxDWPrec_&numdw.) and  upcase(tranwrd(lib_statut_tache,"é","e")) = "TERMINE" then sorties = 1; else sorties=0;
	if upcase(lib_statut_tache) in ("EN COURS","INITIAL") and dt_extraction = &&dtMaxDW_&numdw. then stock_en_cours = 1;else stock_en_cours=0;
	/*if dt_creation <=&dtMaxDWPrec. and entrees = 0 and sorties =0 then stock_precedent = 1;else stock_precedent=0;*/
	/* if entrees = 1 and stock_en_cours = 1 and stock_precedent = 1 then transferees = 1; else transferees=0;*/
	run;

	/** integration de la modif de MLC *****/
	proc sort data=DM_PIL_0_&numdw. ;by ur_origine code_brique id_taches no_fonc_entite_gestion; run;
	
	proc sql;
	create table Stock_en_cours_S_1/*(drop=dt_extraction dt_periode dt_fin_periode 
	transferees stock_precedent stock_en_cours)*/ as
	select a.*
	from dm_dat02.DM_PIL_&numprec.(where=(/*dt_extraction=&&dtMaxDWPrec_&numdw. and*/ Stock_en_cours=1 )) a 
	;
	quit;

	proc sort data=Stock_en_cours_S_1 ;by ur_origine code_brique id_taches no_fonc_entite_gestion; run;

	proc sql noprint;
	select distinct dt_extraction into:dtext
	from dw_dat.DW_T_S&numdw. ;
	quit;

	proc sql noprint;
	select distinct dt_periode into:dtper
	from dw_dat.DW_T_S&numdw. ;
	quit;


	proc sql noprint;
	select distinct dt_fin_periode into:dtfinper
	from dw_dat.DW_T_S&numdw. ;
	quit;

	data Stock_en_cours_manquant;
	merge  Stock_en_cours_S_1 (in=a) DM_PIL_0_&numdw. (in=b);
	by ur_origine code_brique id_taches no_fonc_entite_gestion;
	if a and not b;
	transferees=-1;
	stock_precedent=1; /*a voir julio*/
	stock_en_cours=0;
	dt_extraction=mdy(substr("&dtext.",4,2),substr("&dtext.",1,2),substr("&dtext.",7,4));
	dt_periode="&dtper.";
	dt_fin_periode="&dtfinper."d;

	run;

	**************************;

	data dm_dat02.DM_PIL_&numdw.;
    set DM_PIL_0_&numdw. Stock_en_cours_manquant;
    if stock_precedent = . then stock_precedent=0;
    if (stock_precedent = 1) then transferees= (stock_precedent + entrees - sorties - stock_en_cours); else transferees=0;
    run;

%end;

/*
%put dtMaxDWPrec_32 :=== &dtMaxDWPrec_32.;
%put dtMaxDW_35 :=== &dtMaxDW_35.;

%put %SYSFUNC(putn(&dtMaxDWPrec_32,ddmmyy10.));
%put %SYSFUNC(putn(&dtMaxDW_35.,ddmmyy10.));*/

%mend;

%dmBuild(nbTables=50);