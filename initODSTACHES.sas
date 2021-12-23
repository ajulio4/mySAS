options obs= max;
libname tck_dat "\\gie.root.ad\dfs\applications\SID\PR1\DATA\tck";


/*** Définition des champs date ***/
%definition_des_dates(,'H');
%put DT_DER_LUNDI &DT_DER_LUNDI.;
%put %SYSFUNC(putn(&DT_DER_LUNDI,ddmmyy10.));
%put %SYSFUNC(putn(&DT_DER_VENDREDI,ddmmyy10.));

/*definition des répertoires dans lesquels le sfichiers sont déposes par AA*/
/*%let path_ag = \\gie.root.ad\dfs\applications\Retraite\AG2R_Entreprise\PR\EJ20\BST\metro-taches ;
%let path_ru = \\gie.root.ad\dfs\applications\Retraite\Reunica_Entreprise\PR\ET20\BST\metro-taches ; */

%let path_ag = \\ressources\appli\AG2R_Entreprise\prod\EJ20\BST\metro-taches ;
%let path_ru = \\ressources\appli\Reunica_Entreprise\prod\ET20\BST\metro-taches ;

*annne=year(today());
*mois = month(today());

%macro listDir(path= ,brique=, source=);

%if &source.= AG %then %let origine=AG2R ; %else %let origine=REUNICA;

/*
Parcours de l'aboressence pour selectionner les deux fichiers qu'il faut
Les repertoires sont nommées de cette facon AAMMJJ avec
AA le deux derniers chiffres de l'annee encours
MM le numéro du mois encours exple 06 pour Juin
JJ pour le jour du mois encours  exemple 10 

**/

data tabDir(keep=namedir annee extract dtFiles jr wk);
annee=put(year(today()),4.);
mois = put(month(today()),Z2.);
pathDir=filename("pathDir","&path./&brique.");
dir=dopen("pathDir");
nbdir =dnum(dir);
do i=1 to nbdir;
nameDir = dread(dir,i);
dtFiles=mdy(input(substr(nameDir,3,2),2.),input(substr(nameDir,5,2),2.),year(today()));
if substr(nameDir,1,2) = substr(annee,3,2) and substr(nameDir,3,2) in("09","08","10","11","12") then 

do;

format extract ddmmyy10.;
extract = mdy(input(substr(nameDir,3,2),2.),input(substr(nameDir,5,2),2.),year(today()));
jr=weekday(dtFiles);
wk=week(dtFiles,"v");
output;

end;

end;


dirClose=dclose(dir);
run;

/*
data tabdir1;
set tabDir;
if extract >= &DT_DER_VENDREDI+1 and extract <=&DT_DER_VENDREDI+2;
run;*/

/*
on trie la liste des répertoires par le nom () leur nom sont des nombres 
et on garde que le dernier répertoire

*/
/*
proc sort data=tabDir1 ;by annee descending nameDir ; run;
proc sort data=tabDir1 out=repAtraiter nodupkey ; by annee ;run;*/

proc sort data=tabDir ; by wk  descending dtFiles;run;
proc sort data=tabDir out= repAtraiter nodupkey;by wk ;run;

/*  forcçage de la période à calculer  
2021S33  
data repAtraiter;
set repAtraiter;
namedir='210821' ;
extract='21AUG2021'd;
run;
*/
/**

creation de la macrovariable qui va contenir le nom du repertoire dont
on va traiter les fichiers 

*/
/* forcage des date pour récupérer une période bien précise  */
data _null_;
set repAtraiter;
* Le nom du repertoire à traiter;
call symput('rep'||left(trim(_n_)),compress(nameDir));
call symput('dtextract'||left(trim(_n_)),extract);
call symput('ods'||left(trim(_n_)),"S"||left(trim(wk)));	
run;

proc sql noprint;
select distinct count(*) into: nb
from repAtraiter;
quit;

%put DTEXTRACT1  &dtextract1.;
%put &DT_DER_VENDREDI;
%put &nb;
%put &ods9.;


**********************************************;
%if &nb. gt 0 %then
	%do;
		%do i=1 %to &nb.;
			data tUser&&ods&i.;
			attrib
			id_taches length= 8 format= best32.6 informat= 8. label= "Identifiant tâche"
			co_statut_tache length= 8 format= best32.6 informat= 8. label= "Code Statut tâche " 
			lib_statut_tache length= $15. format= $15. informat= $char15. label= "Libellé Statut tâche"
			code_brique length= $1. format= $1. informat= $char1. label= "Code Brique"
			co_type_tache length= $8. format= $8. informat= $char8. label= "Code type tâche"
			lib_c_type_tache length= $10. format= $10. informat= $char10. label= "Libellé court type tâche"
			lib_type_tache length= $32. format= $32. informat= $char32. label= "Libellé  type tâche"
			cat_type_tache length= $20. format= $20. informat= $char20. label= "Catégorie  type tâche"
			mod_affectation length= $12. format= $12. informat= $char12. label= "Mode Affectation"
			id_possesseur length= 8 format= best32.6 informat= 8. label= "Identifiant processeur"
			login_utilisateur length= $128. format= $128. informat= $char128. label= "Login utilisateur"
			utilisateur length= $128. format= $128. informat= $char128. label= "Utilisateur"
			no_fonc_entite_gestion length= $8. format= $8. informat= $char8. label= "Num Fonct. Entite de gestion"
			entite_gestion length= $30. format= $30. informat= $char30. label= "Entite de gestion"
			priorite length= $1. format= $1. informat= $char1. label= "Priorité"
			sensibilite length= $1. format= $1. informat= $char1. label= "Sensibilité"
			dt_critique length = 8 format = ddmmyy10. informat = yymmdd10. label = "Date critique"
			dtCritique length = $10.
			co_obj_gestion_ref length= $50. format= $50. informat= $char50. label= "Code Objet Gestion REF"
			id_int_obj_gestion_ref length= $30. format= $30. informat= $char30. label= "ID Interne Objet Gestion REF"
			id_public_ref length= $50. format= $50. informat= $char50. label= "ID public REF"
			designation_ref length= $100. format= $100. informat= $char100. label= "Designation REF"
			co_obj_gestion_aff length= $50. format= $50. informat= $char50. label= "code Objet Gestion AFF"
			id_int_obj_gestion_aff length= $30. format= $30. informat= $char30. label= "ID interne ObjetGestion AFF"
			id_public_aff length= $50. format= $50. informat= $char50. label= "ID public AFF"
			designation_aff length= $100. format= $100. informat= $char100. label= "Designation AFF"
			co_obj_gestion_ct1 length= $50. format= $50. informat= $char50. label= "code Objet Gestion CT1"
			id_int_obj_gestion_ct1 length= $30. format= $30. informat= $char30. label= "ID interne ObjetGestion CT1"
			id_public_ct1 length= $50. format= $50. informat= $char50. label= "ID public CT1"
			designation_ct1 length= $100. format= $100. informat= $char100. label= "Designation CT1"
			commentaire_tache length= $255. format= $255. informat= $char255. label= "Commentaire tâche"
			info_tache length= $40. format= $40. informat= $char40. label= "Info tâche"
			info_tache_a length= $25. format= $25. informat= $char25. label= "Info tâche A"
			info_tache_b length= $25. format= $25. informat= $char25. label= "Info tâche B"
			info_tache_c length= $25. format= $25. informat= $char25. label= "Info tâche C"
			dt_affectation length = 8 format = ddmmyy10. inFormat = yymmdd10. label = "Date Affectation"
			dtAffectation length = $10.
			dt_creation length = 8 format = ddmmyy10. inFormat = yymmdd10. label = "Date création"
			dtCreation length = $10.
			utili_creation length= $20. format= $20. informat= $char20. label= "Utilisateur création"
			dt_maj length = 8 format = ddmmyy10. informat = yymmdd10. label = "Date de mise à jour"
			dtMaj length = $10.
			utili_maj length= $20. format= $20. informat= $char20. label= "Utilisateur miseà jour"
			dt_extraction length = 8 format = ddmmyy10. inFormat = yymmdd10. label = "Date d'extraction"
			ur_origine length= $10. format= $10. informat= $char10. label= "UR origine"
			;
			infile "&&path.\&&brique.\&&rep&i.\6-TACHES_UTILISATEUR_BRIQUE.CSV"
			lrecl=32000 encoding="WLATIN1" termstr=CRLF dlm=";" missover firstobs=2 dsd ignoredoseof;

			input 
			id_taches :8. co_statut_tache :8. lib_statut_tache :$15. code_brique :$1. co_type_tache :$8. lib_c_type_tache :$10.
			lib_type_tache :$32. cat_type_tache :$20. mod_affectation :$12. id_possesseur :8. login_utilisateur :$128. utilisateur :$128.
			no_fonc_entite_gestion :$8. entite_gestion :$30. priorite :$1. sensibilite :$1. /*dt_critique :yymmdd10.*/ dtCritique $
			co_obj_gestion_ref :$50. id_int_obj_gestion_ref :$30. id_public_ref :$50. designation_ref :$100. co_obj_gestion_aff :$50.
			id_int_obj_gestion_aff :$30. id_public_aff :$50. designation_aff :$100. co_obj_gestion_ct1 :$50. id_int_obj_gestion_ct1 :$30.
			id_public_ct1 :$50. designation_ct1 :$100. commentaire_tache :$255. info_tache :$40. info_tache_a :$25. info_tache_b :$25.
			info_tache_c :$25. /*dt_affectation :yymmdd10.*/ dtAffectation $ /*dt_creation :yymmdd10.*/ dtCreation $
			utili_creation :$20. /*dt_maj :yymmdd10.*/ dtMaj $ utili_maj :$20. ;

			format DT_DER_LUNDI  ddmmyy10 ;
			format dt_extract_init ddmmyy10 ;
			dt_extract_init=&&dtextract&i.;
			if weekday(dt_extract_init)= 1 then dt_extraction =&&dtextract&i.; else dt_extraction =%eval(&&dtextract&i.+1);
			/*dt_extraction=%eval(&DT_DER_LUNDI+6);*/
			/* dt_extraction='22AUG2021'd;  a changer pour forcer la date */
			ur_origine = "&origine.";

			run;

			data tUser&&ods&i.(drop=dtCritique dtcreation dtAffectation dtMaj);
			format dt_extract_init ddmmyy10.;
			set tUser&&ods&i.;
			format dt_critique dt_maj dt_affectation dt_creation  ddmmyy10.;
			if upcase(dtCritique) = "NULL" then dt_critique = .; else dt_critique=input(dtCritique,yymmdd10.);
			if upcase(dtAffectation) = "NULL" then dt_affectation = .; else dt_affectation=input(dtAffectation,yymmdd10.);
			if upcase(dtCreation) = "NULL" then dt_creation = .; else dt_creation=input(dtCreation,yymmdd10.);
			if upcase(dtMaj) = "NULL" then dt_maj = .; else dt_maj=input(dtMaj,yymmdd10.);
			run;
			**************************************************************************************;

			data tBrique&&ods&i.;
			attrib
			id_taches length= 8 format= best32.6 informat= 8. label= "Identifiant tâche"
			co_statut_tache length= 8 format= best32.6 informat= 8. label= "Code Statut tâche " 
			lib_statut_tache length= $15. format= $15. informat= $char15. label= "Libellé Statut tâche"
			code_brique length= $1. format= $1. informat= $char1. label= "Code Brique"
			co_type_tache length= $8. format= $8. informat= $char8. label= "Code type tâche"
			lib_c_type_tache length= $10. format= $10. informat= $char10. label= "Libellé court type tâche"
			lib_type_tache length= $32. format= $32. informat= $char32. label= "Libellé  type tâche"
			cat_type_tache length= $20. format= $20. informat= $char20. label= "Catégorie  type tâche"
			mod_affectation length= $12. format= $12. informat= $char12. label= "Mode Affectation"
			id_possesseur length= 8 format= best32.6 informat= 8. label= "Identifiant processeur"
			login_utilisateur length= $128. format= $128. informat= $char128. label= "Login utilisateur" /*à vide car nexiste pas dans le fichier user*/
			utilisateur length= $128. format= $128. informat= $char128. label= "Utilisateur" /*à vide car nexiste pas dans le fichier user*/
			no_fonc_entite_gestion length= $8. format= $8. informat= $char8. label= "Num Fonct. Entite de gestion"
			entite_gestion length= $30. format= $30. informat= $char30. label= "Entite de gestion"
			priorite length= $1. format= $1. informat= $char1. label= "Priorité"
			sensibilite length= $1. format= $1. informat= $char1. label= "Sensibilité"
			dt_critique length = 8 format = ddmmyy10. informat = yymmdd10. label = "Date critique"
			dtCritique length = $10.
			co_obj_gestion_ref length= $50. format= $50. informat= $char50. label= "Code Objet Gestion REF"
			id_int_obj_gestion_ref length= $30. format= $30. informat= $char30. label= "ID Interne Objet Gestion REF"
			id_public_ref length= $50. format= $50. informat= $char50. label= "ID public REF"
			designation_ref length= $100. format= $100. informat= $char100. label= "Designation REF"
			co_obj_gestion_aff length= $50. format= $50. informat= $char50. label= "code Objet Gestion AFF"
			id_int_obj_gestion_aff length= $30. format= $30. informat= $char30. label= "ID interne ObjetGestion AFF"
			id_public_aff length= $50. format= $50. informat= $char50. label= "ID public AFF"
			designation_aff length= $100. format= $100. informat= $char100. label= "Designation AFF"
			co_obj_gestion_ct1 length= $50. format= $50. informat= $char50. label= "code Objet Gestion CT1"
			id_int_obj_gestion_ct1 length= $30. format= $30. informat= $char30. label= "ID interne ObjetGestion CT1"
			id_public_ct1 length= $50. format= $50. informat= $char50. label= "ID public CT1"
			designation_ct1 length= $100. format= $100. informat= $char100. label= "Designation CT1"
			commentaire_tache length= $255. format= $255. informat= $char255. label= "Commentaire tâche"
			info_tache length= $40. format= $40. informat= $char40. label= "Info tâche"
			info_tache_a length= $25. format= $25. informat= $char25. label= "Info tâche A"
			info_tache_b length= $25. format= $25. informat= $char25. label= "Info tâche B"
			info_tache_c length= $25. format= $25. informat= $char25. label= "Info tâche C"
			dt_affectation length = 8 format = ddmmyy10. inFormat = yymmdd10. label = "Date Affectation"
			dtAffectation length = $10.
			dt_creation length = 8 format = ddmmyy10. inFormat = yymmdd10. label = "Date création"
			dtCreation length = $10.
			utili_creation length= $20. format= $20. informat= $char20. label= "Utilisateur création"
			dt_maj length = 8 format = ddmmyy10. informat = yymmdd10. label = "Date de mise à jour"
			dtMaj length = $10.
			utili_maj length= $20. format= $20. informat= $char20. label= "Utilisateur miseà jour"
			dt_extraction length = 8 format = ddmmyy10. inFormat = yymmdd10. label = "Date d'extraction"
			ur_origine length= $10. format= $10. informat= $char10. label= "UR origine"
			;
			infile "&&path.\&&brique.\&&rep&i.\7-TACHES_EG_BRIQUE.CSV"
			lrecl=32000 encoding="WLATIN1" termstr=CRLF dlm=";" missover firstobs=2 dsd ignoredoseof;

			input 
			id_taches :8. co_statut_tache :8. lib_statut_tache :$15. code_brique :$1. co_type_tache :$8. lib_c_type_tache :$10.
			lib_type_tache :$32. cat_type_tache :$20. mod_affectation :$12. id_possesseur :8. /*login_utilisateur :$128. utilisateur :$128.*/
			no_fonc_entite_gestion :$8. entite_gestion :$30. priorite :$1. sensibilite :$1. /*dt_critique :yymmdd10.*/ dtCritique $
			co_obj_gestion_ref :$50. id_int_obj_gestion_ref :$30. id_public_ref :$50. designation_ref :$100. co_obj_gestion_aff :$50.
			id_int_obj_gestion_aff :$30. id_public_aff :$50. designation_aff :$100. co_obj_gestion_ct1 :$50. id_int_obj_gestion_ct1 :$30.
			id_public_ct1 :$50. designation_ct1 :$100. commentaire_tache :$255. info_tache :$40. info_tache_a :$25. info_tache_b :$25.
			info_tache_c :$25. /*dt_affectation :yymmdd10.*/ dtAffectation $ /*dt_creation :yymmdd10.*/ dtCreation 
			utili_creation :$20. /*dt_maj :yymmdd10.*/ dtMaj $ utili_maj :$20. ;

			format DT_DER_LUNDI  ddmmyy10 ;
			dt_extract_init=&&dtextract&i.;
			if weekday(dt_extract_init)= 1 then dt_extraction =&&dtextract&i.; else dt_extraction =%eval(&&dtextract&i.+1);
			/*dt_extraction=%eval(&DT_DER_LUNDI+6);*/
			/* dt_extraction='22AUG2021'd;  a changer pour forcer la date */
			ur_origine="&origine.";

			run;

			data tBrique&&ods&i.(drop=dtCritique dtcreation dtAffectation dtMaj);
			format dt_extract_init ddmmyy10.;
			set tBrique&&ods&i.;
			format dt_critique dt_maj dt_affectation dt_creation  ddmmyy10.;
			if upcase(dtCritique) = "NULL" then dt_critique = .; else dt_critique=input(dtCritique,yymmdd10.);
			if upcase(dtAffectation) = "NULL" then dt_affectation = .; else dt_affectation=input(dtAffectation,yymmdd10.);
			if upcase(dtCreation) = "NULL" then dt_creation = .; else dt_creation=input(dtCreation,yymmdd10.);
			if upcase(dtMaj) = "NULL" then dt_maj = .; else dt_maj=input(dtMaj,yymmdd10.);
			run;
			**************************************************************************************;
			/*consilidation des deux fichiers chargés*/

			data ods_dat.T_&&brique._&&source.&&ods&i.;
			set tUser&&ods&i. tBrique&&ods&i.;
			run;


		%end; /*fin boucle do*/
	%put ok;
	%end;

	%else %do;

	%put ko;

	%end; /*sortie chargement de tous les fichiers disponibles*/

*******************************DW***************************************;

%mend;

%listDir(path=&path_ag.,brique=GRECCO,source=AG);
%listDir(path=&path_ag.,brique=DSN,source=AG);
%listDir(path=&path_ag.,brique=BAC,source=AG);

%listDir(path=&path_ru.,brique=GRECCO,source=RU);
%listDir(path=&path_ru.,brique=DSN,source=RU);
%listDir(path=&path_ru.,brique=BAC,source=RU);


*******************************DW***************************************;

