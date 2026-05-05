"""Upload processing service — parses CSV/Excel files and loads into DB."""

from __future__ import annotations

import ast
import csv
import io
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import structlog
from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.core import (
    Balance,
    CenterMapping,
    Employee,
    Entity,
    GLAccountSKA1,
    GLAccountSKB1,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
    UploadBatch,
    UploadError,
)

logger = structlog.get_logger()


def _flush_progress(batch_id: int, count: int, total: int | None = None) -> None:
    """Write rows_processed to DB via an independent session.

    Uses a separate connection so the caller's main transaction is never
    committed or disturbed — only the progress counter row is updated.
    """
    from sqlalchemy import text as sa_text

    from app.infra.db.session import SessionLocal

    s = SessionLocal()
    try:
        if total is not None:
            s.execute(
                sa_text(
                    "UPDATE cleanup.upload_batch "
                    "SET rows_processed = :p, rows_total = :t WHERE id = :id"
                ),
                {"p": count, "t": total, "id": batch_id},
            )
        else:
            s.execute(
                sa_text("UPDATE cleanup.upload_batch SET rows_processed = :p WHERE id = :id"),
                {"p": count, "id": batch_id},
            )
        s.commit()
    except Exception:
        logger.warning("Failed to flush progress for batch %s", batch_id, exc_info=True)
    finally:
        s.close()


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y%m%d",
)


def _parse_date(raw: str) -> datetime | None:
    """Try common date formats; return None if unparseable."""
    raw = raw.strip()
    if not raw:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue
    return None


# Column mappings: normalize header names to model fields
CC_COLUMNS = {
    # SAP technical names (CSKS/CSKT) — 1:1 mapping
    "MANDT": "mandt",
    "KOKRS": "coarea",
    "KOSTL": "cctr",
    "KTEXT": "txtsh",
    "LTEXT": "txtmi",
    "DATBI": "datbi",
    "DATAB": "datab",
    "BKZKP": "bkzkp",
    "PKZKP": "pkzkp",
    "BUKRS": "ccode",
    "GSBER": "gsber",
    "KOSAR": "cctrcgy",
    "VERAK": "responsible",
    "VERAK_USER": "verak_user",
    "WAERS": "currency",
    "KALSM": "kalsm",
    "TXJCD": "txjcd",
    "PRCTR": "pctr",
    "WERKS": "werks",
    "LOGSYSTEM": "logsystem",
    "ERSDA": "ersda",
    "USNAM": "usnam",
    "BKZKS": "bkzks",
    "BKZER": "bkzer",
    "BKZOB": "bkzob",
    "PKZKS": "pkzks",
    "PKZER": "pkzer",
    "VMETH": "vmeth",
    "MGEFL": "mgefl",
    "ABTEI": "abtei",
    "NKOST": "nkost",
    "KVEWE": "kvewe",
    "KAPPL": "kappl",
    "KOSZSCHL": "koszschl",
    "LAND1": "land1",
    "ANRED": "anred",
    "NAME1": "name1",
    "NAME2": "name2",
    "NAME3": "name3",
    "NAME4": "name4",
    "ORT01": "ort01",
    "ORT02": "ort02",
    "STRAS": "stras",
    "PFACH": "pfach",
    "PSTLZ": "pstlz",
    "PSTL2": "pstl2",
    "REGIO": "regio",
    "SPRAS": "spras",
    "TELBX": "telbx",
    "TELF1": "telf1",
    "TELF2": "telf2",
    "TELFX": "telfx",
    "TELTX": "teltx",
    "TELX1": "telx1",
    "DATLT": "datlt",
    "DRNAM": "drnam",
    "KHINR": "khinr",
    "CCKEY": "cckey",
    "KOMPL": "kompl",
    "STAKZ": "stakz",
    "OBJNR": "objnr",
    "FUNKT": "funkt",
    "AFUNK": "afunk",
    "CPI_TEMPL": "cpi_templ",
    "CPD_TEMPL": "cpd_templ",
    "FUNC_AREA": "func_area",
    "SCI_TEMPL": "sci_templ",
    "SCD_TEMPL": "scd_templ",
    "SKI_TEMPL": "ski_templ",
    "SKD_TEMPL": "skd_templ",
    # Customer fields (CI_CSKS)
    "ZZCUEMNCFU": "zzcuemncfu",
    "ZZCUEABACC": "zzcueabacc",
    "ZZCUEGBCD": "zzcuegbcd",
    "ZZCUEUBCD": "zzcueubcd",
    "ZZCUENKOS": "zzcuenkos",
    "ZZSTRPCTYP": "zzstrpctyp",
    "ZZSTRKKLAS": "zzstrkklas",
    "ZZSTRAAGCD": "zzstraagcd",
    "ZZSTRGFD": "zzstrgfd",
    "ZZSTRFST": "zzstrfst",
    "ZZSTRMACVE": "zzstrmacve",
    "ZZSTRABUKR": "zzstrabukr",
    "ZZSTRUGCD": "zzstrugcd",
    "ZZSTRINADT": "zzstrinadt",
    "ZZSTRKSTYP": "zzstrkstyp",
    "ZZSTRVERIK": "zzstrverik",
    "ZZSTRCURR2": "zzstrcurr2",
    "ZZSTRLCCID": "zzstrlccid",
    "ZZSTRMALOC": "zzstrmaloc",
    "ZZSTRTAXCD": "zzstrtaxcd",
    "ZZSTRGRPID": "zzstrgrpid",
    "ZZSTRREGCODE": "zzstrregcode",
    "ZZSTRTAXAREA": "zzstrtaxarea",
    "ZZSTRREPSIT": "zzstrrepsit",
    "ZZSTRGSM": "zzstrgsm",
    "ZZCEMAPAR": "zzcemapar",
    "ZZLEDGER": "zzledger",
    "ZZHDSTAT": "zzhdstat",
    "ZZHDTYPE": "zzhdtype",
    "ZZFMD": "zzfmd",
    "ZZFMDCC": "zzfmdcc",
    "ZZFMDNODE": "zzfmdnode",
    "ZZSTATE": "zzstate",
    "ZZTAX": "zztax",
    "ZZSTRENTSA": "zzstrentsa",
    "ZZSTRENTZU": "zzstrentzu",
    "XBLNR": "xblnr",
    # JV fields
    "VNAME": "vname",
    "RECID": "recid",
    "ETYPE": "etype",
    "JV_OTYPE": "jv_otype",
    "JV_JIBCL": "jv_jibcl",
    "JV_JIBSA": "jv_jibsa",
    "FERC_IND": "ferc_ind",
    # Legacy aliases
    "COAREA": "coarea",
    "CCTR": "cctr",
    "TXTSH": "txtsh",
    "TXTMI": "txtmi",
    "CCTRRESPP": "responsible",
    "RESPONSIBLE": "responsible",
    "CCTRCGY": "cctrcgy",
    "CCODECCTR": "ccode",
    "CCODE": "ccode",
    "CURRCCTR": "currency",
    "CURRENCY": "currency",
    "PCTRCCTR": "pctr",
    "PCTR": "pctr",
    "IS_ACTIVE": "is_active",
}
_CC_MODEL_FIELDS = set(CC_COLUMNS.values())
PC_COLUMNS = {
    # SAP technical names (CEPC/CEPCT) — 1:1 mapping
    "MANDT": "mandt",
    "PRCTR": "pctr",
    "DATBI": "datbi",
    "KOKRS": "coarea",
    "DATAB": "datab",
    "ERSDA": "ersda",
    "USNAM": "usnam",
    "MERKMAL": "merkmal",
    "ABTEI": "department",
    "VERAK": "responsible",
    "VERAK_USER": "verak_user",
    "WAERS": "currency",
    "NPRCTR": "nprctr",
    "LAND1": "land1",
    "ANRED": "anred",
    "NAME1": "name1",
    "NAME2": "name2",
    "NAME3": "name3",
    "NAME4": "name4",
    "ORT01": "ort01",
    "ORT02": "ort02",
    "STRAS": "stras",
    "PFACH": "pfach",
    "PSTLZ": "pstlz",
    "PSTL2": "pstl2",
    "SPRAS": "language",
    "TELBX": "telbx",
    "TELF1": "telf1",
    "TELF2": "telf2",
    "TELFX": "telfx",
    "TELTX": "teltx",
    "TELX1": "telx1",
    "DATLT": "datlt",
    "DRNAM": "drnam",
    "KHINR": "khinr",
    "BUKRS": "ccode",
    "VNAME": "vname",
    "RECID": "recid",
    "ETYPE": "etype",
    "TXJCD": "txjcd",
    "REGIO": "regio",
    "KVEWE": "kvewe",
    "KAPPL": "kappl",
    "KALSM": "kalsm",
    "LOGSYSTEM": "logsystem",
    "LOCK_IND": "lock_ind",
    "PCA_TEMPLATE": "pca_template",
    "SEGMENT": "segment",
    "KTEXT": "txtsh",
    "LTEXT": "txtmi",
    # Legacy aliases
    "COAREA": "coarea",
    "PCTR": "pctr",
    "TXTMI": "txtmi",
    "TXTSH": "txtsh",
    "PCTRDEPT": "department",
    "DEPARTMENT": "department",
    "PCTRRESPP": "responsible",
    "RESPONSIBLE": "responsible",
    "PC_SPRAS": "language",
    "PCTRCCALL": "ccode",
    "CCODE": "ccode",
    "CURRPCTR": "currency",
    "CURRENCY": "currency",
    "IS_ACTIVE": "is_active",
}
_PC_MODEL_FIELDS = set(PC_COLUMNS.values())
BALANCE_COLUMNS = {
    "COAREA": "coarea",
    "COMPANY_CODE": "ccode",
    "CCODE": "ccode",
    "SAP_MANAGEMENT_CENTER": "cctr",
    "CCTR": "cctr",
    "FISCAL_YEAR": "fiscal_year",
    "PERIOD_YYYYMM": "period_raw",
    "PERIOD": "period_raw",
    "ACCOUNT": "account",
    "CURR_CODE_ISO_TC": "currency_tc",
    "CURRENCY_TC": "currency_tc",
    "CURRENCY_GC": "currency_gc",
    "CURRENCY_GC2": "currency_gc2",
    "SUM_TC": "tc_amt",
    "TC_AMT": "tc_amt",
    "SUM(P.GCR_POSTING_AMT_TC)": "tc_amt",
    "GC_AMT": "gc_amt",
    "SUM_GC2": "gc2_amt",
    "GC2_AMT": "gc2_amt",
    "SUM(P.GCR_POSTING_AMT_GC2)": "gc2_amt",
    "COUNT": "posting_count",
    "COUNT(*)": "posting_count",
    "POSTING_COUNT": "posting_count",
    "ACCOUNT_CLASS": "account_class",
}
ENTITY_COLUMNS = {
    # SAP technical names (T001) — 1:1 mapping
    "MANDT": "mandt",
    "BUKRS": "ccode",
    "BUTXT": "name",
    "ORT01": "city",
    "LAND1": "country",
    "WAERS": "currency",
    "SPRAS": "language",
    "KTOPL": "chart_of_accounts",
    "WAABW": "waabw",
    "PERIV": "fiscal_year_variant",
    "KOKFI": "kokfi",
    "RCOMP": "company",
    "ADRNR": "adrnr",
    "STCEG": "stceg",
    "FIKRS": "fikrs",
    "XFMCO": "xfmco",
    "XFMCB": "xfmcb",
    "XFMCA": "xfmca",
    "TXJCD": "txjcd",
    "FMHRDATE": "fmhrdate",
    "BUVAR": "buvar",
    "FDBUK": "fdbuk",
    "XFDIS": "xfdis",
    "XVALV": "xvalv",
    "XSKFN": "xskfn",
    "KKBER": "credit_control_area",
    "XMWSN": "xmwsn",
    "MREGL": "mregl",
    "XGSBE": "xgsbe",
    "XGJRV": "xgjrv",
    "XKDFT": "xkdft",
    "XPROD": "xprod",
    "XEINK": "xeink",
    "XJVAA": "xjvaa",
    "XVVWA": "xvvwa",
    "XSLTA": "xslta",
    "XFDMM": "xfdmm",
    "XFDSD": "xfdsd",
    "XEXTB": "xextb",
    "EBUKR": "ebukr",
    "KTOP2": "ktop2",
    "UMKRS": "umkrs",
    "BUKRS_GLOB": "bukrs_glob",
    "FSTVA": "fstva",
    "OPVAR": "opvar",
    "XCOVR": "xcovr",
    "TXKRS": "txkrs",
    "WFVAR": "wfvar",
    "XBBBF": "xbbbf",
    "XBBBE": "xbbbe",
    "XBBBA": "xbbba",
    "XBBKO": "xbbko",
    "XSTDT": "xstdt",
    "MWSKV": "mwskv",
    "MWSKA": "mwska",
    "IMPDA": "impda",
    "XNEGP": "xnegp",
    "XKKBI": "xkkbi",
    "WT_NEWWT": "wt_newwt",
    "PP_PDATE": "pp_pdate",
    "INFMT": "infmt",
    "FSTVARE": "fstvare",
    "KOPIM": "kopim",
    "DKWEG": "dkweg",
    "OFFSACCT": "offsacct",
    "BAPOVAR": "bapovar",
    "XCOS": "xcos",
    "XCESSION": "xcession",
    "XSPLT": "xsplt",
    "SURCCM": "surccm",
    "DTPROV": "dtprov",
    "DTAMTC": "dtamtc",
    "DTTAXC": "dttaxc",
    "DTTDSP": "dttdsp",
    "DTAXR": "dtaxr",
    "XVATDATE": "xvatdate",
    "PST_PER_VAR": "pst_per_var",
    "XBBSC": "xbbsc",
    "F_OBSOLETE": "f_obsolete",
    # Legacy aliases
    "COMPANY_CODE": "ccode",
    "CCODE": "ccode",
    "NAME": "name",
    "COUNTRY": "country",
    "REGION": "region",
    "CURRENCY": "currency",
    "IS_ACTIVE": "is_active",
    "CITY": "city",
    "LANGUAGE": "language",
    "FMHRP": "fm_area",
}

_ENTITY_MODEL_FIELDS = set(ENTITY_COLUMNS.values())
# Employee columns — SAP ZUHL_GRD_GPF 1:1 mapping + legacy aliases
EMPLOYEE_COLUMNS = {
    # SAP technical names (ZUHL_GRD_GPF)
    "MANDT": "mandt",
    "GPN": "gpn",
    "NAME": "name",
    "VORNAME": "vorname",
    "SPRACHENSCHLUESS": "sprachenschluess",
    "ANREDECODE": "anredecode",
    "USERID": "userid",
    "EINTRITTSDATUM": "eintrittsdatum",
    "OE_LEITER": "oe_leiter",
    "INT_TEL_NR_1AP": "int_tel_nr_1ap",
    "EXT_TEL_NR_1AP": "ext_tel_nr_1ap",
    "NL_CODE_GEB_1AP": "nl_code_geb_1ap",
    "STRASSE_GEB_1AP": "strasse_geb_1ap",
    "STOCKWERK_1AP": "stockwerk_1ap",
    "BUERONUMMER_1AP": "bueronummer_1ap",
    "KSTST": "kstst",
    "KSTST_TEXT": "kstst_text",
    "OE_OBJEKT_ID": "oe_objekt_id",
    "OE_CODE": "oe_code",
    "OE_TEXT": "oe_text",
    "SAP_BUKRS": "sap_bukrs",
    "SAP_BUKRS_TEXT": "sap_bukrs_text",
    "T_NUMMER": "t_nummer",
    "INSTRAD_1": "instrad_1",
    "INSTRAD_2": "instrad_2",
    "KSTST_EINSATZ_OE": "kstst_einsatz_oe",
    "PERSONALBER_TEXT": "personalber_text",
    "NL_OE_MA": "nl_oe_ma",
    "NL_TEXT": "nl_text",
    "GSFLD_OE_MA": "gsfld_oe_ma",
    "GSFLD_OE_MA_TEXT": "gsfld_oe_ma_text",
    "MA_GRUPPE": "ma_gruppe",
    "MA_GRUPPE_TEXT": "ma_gruppe_text",
    "MA_KREIS": "ma_kreis",
    "MA_KREIS_TEXT": "ma_kreis_text",
    "RANG_CODE": "rang_code",
    "RANG_TEXT": "rang_text",
    "AKADEMISCHER_TIT": "akademischer_tit",
    "UBS_FUNK": "ubs_funk",
    "UBS_FUNK_TEXT": "ubs_funk_text",
    "GPN_VG_MA": "gpn_vg_ma",
    "NAME_VG_MA": "name_vg_ma",
    "UEG_OE_OBJEKTID": "ueg_oe_objektid",
    "UEG_OE_BEZ": "ueg_oe_bez",
    "UEG_OE_KRZ": "ueg_oe_krz",
    "BSCHGRAD": "bschgrad",
    "PERSONALBEREICH": "personalbereich",
    "FAX_EXT_1AP": "fax_ext_1ap",
    "EMAIL_ADRESSE": "email_adresse",
    "MA_KZ": "ma_kz",
    "FIRMA_EXT_MA": "firma_ext_ma",
    "BEGDAT_ORGWECHS": "begdat_orgwechs",
    "AUSTRITT_DATUM": "austritt_datum",
    "NATEL_NUMMER": "natel_nummer",
    "PAGER_NUMMER": "pager_nummer",
    "PLZ_GEB_1AP": "plz_geb_1ap",
    "ORT_GEB_1AP": "ort_geb_1ap",
    "EINSATZ_OE_KRZ": "einsatz_oe_krz",
    "EINSATZ_OE_TEXT": "einsatz_oe_text",
    "DIVISION": "division",
    "GEB_COD_1AP": "geb_cod_1ap",
    "RANG_KRZ": "rang_krz",
    "SYSTEMDATUM": "systemdatum",
    "AP_NUMMER": "ap_nummer",
    "EINSATZ_OE_OBJID": "einsatz_oe_objid",
    "INT_TEL_NR_2AP": "int_tel_nr_2ap",
    "EXT_TEL_NR_2AP": "ext_tel_nr_2ap",
    "BUERONUMMER_2AP": "bueronummer_2ap",
    "GEB_COD_2AP": "geb_cod_2ap",
    "STRASSE_GEB_2AP": "strasse_geb_2ap",
    "PLZ_GEB_2AP": "plz_geb_2ap",
    "ORT_GEB_2AP": "ort_geb_2ap",
    "GEB_COD_GEB_2AP": "geb_cod_geb_2ap",
    "FAX_NR_2AP": "fax_nr_2ap",
    "STOCKWERK_2AP": "stockwerk_2ap",
    "GPIN_NUMMER": "gpin_nummer",
    "NAT": "nat",
    "LAND_GEB_1AP": "land_geb_1ap",
    "REG_NR_1AP": "reg_nr_1ap",
    "POSTF_1AP": "postf_1ap",
    "PLZ_POSTFADR_1AP": "plz_postfadr_1ap",
    "ORT_POSTFADR_1AP": "ort_postfadr_1ap",
    "LAND_GEB_2AP": "land_geb_2ap",
    "REG_NR_2AP": "reg_nr_2ap",
    "POSTF_2AP": "postf_2ap",
    "PLZ_POSTFADR_2AP": "plz_postfadr_2ap",
    "ORT_POSTFADR_2AP": "ort_postfadr_2ap",
    "LETZTER_ARB_TAG": "letzter_arb_tag",
    "ABAC_NL_AG_EINOE": "abac_nl_ag_einoe",
    "VERTR_ENDE_EXMA": "vertr_ende_exma",
    "UNTERGRP_CODE": "untergrp_code",
    "BS_FIRST_NAME": "bs_first_name",
    "BS_LAST_NAME": "bs_last_name",
    "NAME_UC": "name_uc",
    "VORNAME_UC": "vorname_uc",
    "NAME_PH": "name_ph",
    "VORNAME_PH": "vorname_ph",
    "MA_OE": "ma_oe",
    "UPDATED_ID": "updated_id",
    "MA_KSTST": "ma_kstst",
    "BUSINESS_NAME": "business_name",
    "JOB_CATEG_CODE": "job_categ_code",
    "JOB_CATEG_DESCR": "job_categ_descr",
    "COSTCENTER_CODE": "costcenter_code",
    "COSTCENTER_DESCR": "costcenter_descr",
    "MANACS_FUNC_CODE": "manacs_func_code",
    "MANACS_FUNC_DESC": "manacs_func_desc",
    "MANACS_SEGM_CODE": "manacs_segm_code",
    "MANACS_SEGM_DESC": "manacs_segm_desc",
    "MANACS_SECT_CODE": "manacs_sect_code",
    "MANACS_SECT_DESC": "manacs_sect_desc",
    "MANACS_BSAR_CODE": "manacs_bsar_code",
    "MANACS_BSAR_DESC": "manacs_bsar_desc",
    "MANACS_BSUN_CODE": "manacs_bsun_code",
    "MANACS_BSUN_DESC": "manacs_bsun_desc",
    "MANACS_BSGP_CODE": "manacs_bsgp_code",
    "MANACS_BSGP_DESC": "manacs_bsgp_desc",
    "MANACS_REG_CODE": "manacs_reg_code",
    "MANACS_REG_DESCR": "manacs_reg_descr",
    "MANACS_LOC_CODE": "manacs_loc_code",
    "MANACS_LOC_DESCR": "manacs_loc_descr",
    "REGULATORY_REG": "regulatory_reg",
    "SUPERVISORS_GPIN": "supervisors_gpin",
    "UUNAME": "uuname",
    "WEB_SSO": "web_sso",
    "SAP_USER": "sap_user",
    "HR_COMPANY": "hr_company",
    "REGULATORY_REGST": "regulatory_regst",
    "GLOBAL_CC": "global_cc",
    # Legacy aliases (backward compat)
    "BS_NAME": "bs_name",
    "BS_FIRSTNAME": "bs_firstname",
    "BS_LASTNAME": "bs_lastname",
    "LEGAL_FAMILY_NAM": "legal_family_name",
    "LEGAL_FIRST_NAME": "legal_first_name",
    "EMAIL_ADDRESS": "email_address",
    "EMP_STATUS": "emp_status",
    "VALID_FROM": "valid_from",
    "VALID_TO": "valid_to",
    "GENDER_CODE": "gender_code",
    "USER_ID_PID": "user_id_pid",
    "USER_ID_TNUMBER": "user_id_tnumber",
    "OU_PK": "ou_pk",
    "OU_CD": "ou_cd",
    "OU_DESC": "ou_desc",
    "WRK_IN_OU_PK": "wrk_in_ou_pk",
    "WRK_IN_OU_CD": "wrk_in_ou_cd",
    "WRK_IN_OU_DESC": "wrk_in_ou_desc",
    "LOCAL_CC_CD": "local_cc_cd",
    "LOCAL_CC_DESC": "local_cc_desc",
    "GCRS_COMP_CD": "gcrs_comp_cd",
    "GCRS_COMP_DESC": "gcrs_comp_desc",
    "COST_PC_CD_E_OU": "cost_pc_cd_e_ou",
    "COST_PC_CD_W_OU": "cost_pc_cd_w_ou",
    "LM_GPN": "lm_gpn",
    "LM_BS_FIRSTNAME": "lm_bs_firstname",
    "LM_BS_LASTNAME": "lm_bs_lastname",
    "SUPERVISOR_GPN": "supervisor_gpn",
    "RANK_CD": "rank_cd",
    "RANK_DESC": "rank_desc",
    "JOB_DESC": "job_desc",
    "EMPL_CLASS": "empl_class",
    "FULL_TIME_EQ": "full_time_eq",
    "HEAD_OF_OWN_OU": "head_of_own_ou",
    "REG_REGION": "reg_region",
    "LOCN_CITY_NAME_1": "locn_city_name_1",
    "LOCN_CTRY_CD_1": "locn_ctry_cd_1",
    "BUILDING_CD_1": "building_cd_1",
}
_EMPLOYEE_MODEL_FIELDS = set(EMPLOYEE_COLUMNS.values())

HIERARCHY_FLAT_COLUMNS = {
    "MANDT": "mandt",
    "PERIOD": "period",
    "NODEID": "nodeid",
    "NODETYPE": "nodetype",
    "NODENAME": "nodename",
    "PARENTID": "parentid",
    "CHILDID": "childid",
    "NEXTID": "nextid",
    "NODETEXT": "nodetext",
}

SKA1_COLUMNS = {
    "MANDT": "mandt",
    "KTOPL": "ktopl",
    "SAKNR": "saknr",
    "XBILK": "xbilk",
    "SAKAN": "sakan",
    "BILKT": "bilkt",
    "ERDAT": "erdat",
    "ERNAM": "ernam",
    "GVTYP": "gvtyp",
    "KTOKS": "ktoks",
    "MUSTR": "mustr",
    "VBUND": "vbund",
    "XLOEV": "xloev",
    "XSPEA": "xspea",
    "XSPEB": "xspeb",
    "XSPEP": "xspep",
    "MCOD1": "mcod1",
    "FUNC_AREA": "func_area",
    "GLACCOUNT_TYPE": "glaccount_type",
    "GLACCOUNT_SUBTYPE": "glaccount_subtype",
    "MAIN_SAKNR": "main_saknr",
    "LAST_CHANGED_TS": "last_changed_ts",
    "TXT20": "txt20",
    "TXT50": "txt50",
}
_SKA1_MODEL_FIELDS = set(SKA1_COLUMNS.values())

SKB1_COLUMNS = {
    "MANDT": "mandt",
    "BUKRS": "bukrs",
    "SAKNR": "saknr",
    "BEGRU": "begru",
    "BUSAB": "busab",
    "DATLZ": "datlz",
    "ERDAT": "erdat",
    "ERNAM": "ernam",
    "FDGRV": "fdgrv",
    "FDLEV": "fdlev",
    "FIPLS": "fipls",
    "FSTAG": "fstag",
    "HBKID": "hbkid",
    "HKTID": "hktid",
    "KDFSL": "kdfsl",
    "MITKZ": "mitkz",
    "MWSKZ": "mwskz",
    "STEXT": "stext",
    "VZSKZ": "vzskz",
    "WAERS": "waers",
    "WMETH": "wmeth",
    "XGKON": "xgkon",
    "XINTB": "xintb",
    "XKRES": "xkres",
    "XLOEB": "xloeb",
    "XNKON": "xnkon",
    "XOPVW": "xopvw",
    "XSPEB": "xspeb",
    "ZINDT": "zindt",
    "ZINRT": "zinrt",
    "ZUAWA": "zuawa",
    "ALTKT": "altkt",
    "XMITK": "xmitk",
    "RECID": "recid",
    "FIPOS": "fipos",
    "XMWNO": "xmwno",
    "XSALH": "xsalh",
    "BEWGP": "bewgp",
    "INFKY": "infky",
    "TOGRU": "togru",
    "XLGCLR": "xlgclr",
    "X_UJ_CLR": "x_uj_clr",
    "MCAKEY": "mcakey",
    "COCHANGED": "cochanged",
    "LAST_CHANGED_TS": "last_changed_ts",
}
_SKB1_MODEL_FIELDS = set(SKB1_COLUMNS.values())

TARGET_CC_COLUMNS = {
    "COAREA": "coarea",
    "KOKRS": "coarea",
    "CCTR": "cctr",
    "KOSTL": "cctr",
    "TXTSH": "txtsh",
    "KTEXT": "txtsh",
    "TXTMI": "txtmi",
    "LTEXT": "txtmi",
    "RESPONSIBLE": "responsible",
    "VERAK": "responsible",
    "CCODE": "ccode",
    "BUKRS": "ccode",
    "CCTRCGY": "cctrcgy",
    "KOSAR": "cctrcgy",
    "CURRENCY": "currency",
    "WAERS": "currency",
    "PCTR": "pctr",
    "PRCTR": "pctr",
    "IS_ACTIVE": "is_active",
    "MDG_STATUS": "mdg_status",
    "MDG_CHANGE_REQUEST_ID": "mdg_change_request_id",
}
_TARGET_CC_MODEL_FIELDS = {
    "coarea",
    "cctr",
    "txtsh",
    "txtmi",
    "responsible",
    "ccode",
    "cctrcgy",
    "currency",
    "pctr",
    "is_active",
    "mdg_status",
    "mdg_change_request_id",
}

TARGET_PC_COLUMNS = {
    "COAREA": "coarea",
    "KOKRS": "coarea",
    "PCTR": "pctr",
    "PRCTR": "pctr",
    "TXTSH": "txtsh",
    "KTEXT": "txtsh",
    "TXTMI": "txtmi",
    "LTEXT": "txtmi",
    "RESPONSIBLE": "responsible",
    "VERAK": "responsible",
    "CCODE": "ccode",
    "BUKRS": "ccode",
    "DEPARTMENT": "department",
    "ABTEI": "department",
    "CURRENCY": "currency",
    "WAERS": "currency",
    "IS_ACTIVE": "is_active",
}
_TARGET_PC_MODEL_FIELDS = {
    "coarea",
    "pctr",
    "txtsh",
    "txtmi",
    "responsible",
    "ccode",
    "department",
    "currency",
    "is_active",
}

CENTER_MAPPING_COLUMNS = {
    "OBJECT_TYPE": "object_type",
    "TYPE": "object_type",
    "LEGACY_COAREA": "legacy_coarea",
    "LEGACY_KOKRS": "legacy_coarea",
    "LEGACY_CENTER": "legacy_center",
    "LEGACY_KOSTL": "legacy_center",
    "LEGACY_PRCTR": "legacy_center",
    "LEGACY_NAME": "legacy_name",
    "TARGET_COAREA": "target_coarea",
    "TARGET_KOKRS": "target_coarea",
    "TARGET_CENTER": "target_center",
    "TARGET_KOSTL": "target_center",
    "TARGET_PRCTR": "target_center",
    "TARGET_NAME": "target_name",
    "MAPPING_TYPE": "mapping_type",
    "NOTES": "notes",
}
_CENTER_MAPPING_MODEL_FIELDS = {
    "object_type",
    "legacy_coarea",
    "legacy_center",
    "legacy_name",
    "target_coarea",
    "target_center",
    "target_name",
    "mapping_type",
    "notes",
}


def _read_file(path: str) -> list[dict[str, str]]:
    """Read CSV or Excel file and return list of row dicts."""
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        try:
            import openpyxl

            wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
            ws = wb.active
            rows_iter = ws.iter_rows(values_only=True)
            headers = [str(h or "").strip() for h in next(rows_iter)]
            result = []
            for row in rows_iter:
                d = {}
                for i, val in enumerate(row):
                    if i < len(headers) and headers[i]:
                        d[headers[i]] = str(val) if val is not None else ""
                result.append(d)
            wb.close()
            return result
        except ImportError as exc:
            raise ValueError("openpyxl not installed") from exc
    else:
        # Try UTF-8 first, fall back to cp1252 (European Excel default)
        for enc in ("utf-8-sig", "cp1252"):
            try:
                content = p.read_text(encoding=enc)
                break
            except (UnicodeDecodeError, ValueError):
                continue
        else:
            content = p.read_bytes().decode("utf-8", errors="replace")
        # Skip MDG header lines starting with *
        lines = content.split("\n")
        clean_lines = [ln for ln in lines if not ln.startswith("*")]
        if not clean_lines or not clean_lines[0].strip():
            return []
        # Detect delimiter: comma, semicolon, or tab
        header_line = clean_lines[0]
        if "\t" in header_line:
            delim = "\t"
        elif ";" in header_line and "," not in header_line:
            delim = ";"
        else:
            delim = ","
        reader = csv.DictReader(io.StringIO("\n".join(clean_lines)), delimiter=delim)
        return [dict(row) for row in reader]


def _normalize_headers(rows: list[dict[str, str]], mapping: dict[str, str]) -> list[dict[str, str]]:
    """Normalize column headers using mapping."""
    result = []
    for row in rows:
        normalized: dict[str, str] = {}
        extras: dict[str, str] = {}
        for key, val in row.items():
            upper_key = key.strip().upper()
            if upper_key in mapping:
                normalized[mapping[upper_key]] = val.strip() if val else ""
            else:
                extras[key.strip()] = val.strip() if val else ""
        if extras:
            normalized["_extras"] = str(extras)
        result.append(normalized)
    return result


def validate_upload(batch_id: int, db: Session) -> dict:
    """Validate an uploaded file and return summary."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if not batch.storage_uri:
        raise ValueError("No file associated with this batch")

    logger.info(
        "upload.validate.start",
        batch_id=batch_id,
        kind=batch.kind,
        storage_uri=batch.storage_uri,
    )

    supported = (
        "cost_center",
        "cost_centers",
        "profit_center",
        "profit_centers",
        "balance",
        "balances",
        "balances_gcr",
        "entity",
        "entities",
        "hierarchy",
        "hierarchies",
        "hierarchies_flat",
        "employee",
        "employees",
        "gl_accounts_ska1",
        "gl_accounts_skb1",
        "target_cost_centers",
        "target_profit_centers",
        "center_mapping",
    )
    if batch.kind not in supported:
        raise ValueError(f"Upload kind '{batch.kind}' is not yet supported")

    batch.status = "validating"
    db.execute(sa_delete(UploadError).where(UploadError.batch_id == batch.id))
    db.commit()

    try:
        rows = _read_file(batch.storage_uri)
    except Exception as e:
        logger.error(
            "upload.validate.file_read_error",
            batch_id=batch_id,
            storage_uri=batch.storage_uri,
            error=str(e),
        )
        batch.status = "failed"
        db.add(
            UploadError(
                batch_id=batch.id,
                row_number=0,
                error_code="FILE_READ",
                message=f"Cannot read file: {e}",
            )
        )
        db.commit()
        return {"status": "failed", "error": str(e)}

    mapping = {
        "cost_center": CC_COLUMNS,
        "cost_centers": CC_COLUMNS,
        "profit_center": PC_COLUMNS,
        "profit_centers": PC_COLUMNS,
        "balance": BALANCE_COLUMNS,
        "balances": BALANCE_COLUMNS,
        "balances_gcr": BALANCE_COLUMNS,
        "entity": ENTITY_COLUMNS,
        "entities": ENTITY_COLUMNS,
        "employee": EMPLOYEE_COLUMNS,
        "employees": EMPLOYEE_COLUMNS,
        "hierarchies_flat": HIERARCHY_FLAT_COLUMNS,
        "gl_accounts_ska1": SKA1_COLUMNS,
        "gl_accounts_skb1": SKB1_COLUMNS,
        "target_cost_centers": TARGET_CC_COLUMNS,
        "target_profit_centers": TARGET_PC_COLUMNS,
        "center_mapping": CENTER_MAPPING_COLUMNS,
    }.get(batch.kind, {})

    normalized = _normalize_headers(rows, mapping) if mapping else rows

    # Publish total + reset progress so frontend can show a progress bar
    _flush_progress(batch.id, 0, len(normalized))

    errors: list[dict] = []
    error_rows: set[int] = set()

    for i, row in enumerate(normalized, start=1):
        if i % 100 == 0:
            _flush_progress(batch.id, i)
        if batch.kind in ("cost_center", "cost_centers"):
            if not row.get("cctr"):
                errors.append(
                    {"row": i, "col": "CCTR", "code": "REQUIRED", "msg": "CCTR is required"},
                )
                error_rows.add(i)
            if not row.get("coarea"):
                errors.append(
                    {"row": i, "col": "COAREA", "code": "REQUIRED", "msg": "COAREA is required"},
                )
                error_rows.add(i)
        elif batch.kind in ("employee", "employees"):
            if not row.get("gpn"):
                errors.append(
                    {"row": i, "col": "GPN", "code": "REQUIRED", "msg": "GPN is required"},
                )
                error_rows.add(i)
        elif batch.kind in ("profit_center", "profit_centers"):
            if not row.get("pctr"):
                errors.append(
                    {"row": i, "col": "PCTR", "code": "REQUIRED", "msg": "PCTR is required"},
                )
                error_rows.add(i)
        elif batch.kind == "target_cost_centers":
            if not row.get("cctr"):
                errors.append(
                    {"row": i, "col": "CCTR", "code": "REQUIRED", "msg": "CCTR is required"},
                )
                error_rows.add(i)
            if not row.get("coarea"):
                errors.append(
                    {"row": i, "col": "COAREA", "code": "REQUIRED", "msg": "COAREA is required"},
                )
                error_rows.add(i)
        elif batch.kind == "target_profit_centers":
            if not row.get("pctr"):
                errors.append(
                    {"row": i, "col": "PCTR", "code": "REQUIRED", "msg": "PCTR is required"},
                )
                error_rows.add(i)
            if not row.get("coarea"):
                errors.append(
                    {"row": i, "col": "COAREA", "code": "REQUIRED", "msg": "COAREA is required"},
                )
                error_rows.add(i)
        elif batch.kind == "center_mapping":
            if not row.get("legacy_center"):
                errors.append(
                    {
                        "row": i,
                        "col": "LEGACY_CENTER",
                        "code": "REQUIRED",
                        "msg": "LEGACY_CENTER is required",
                    },
                )
                error_rows.add(i)
            if not row.get("target_center"):
                errors.append(
                    {
                        "row": i,
                        "col": "TARGET_CENTER",
                        "code": "REQUIRED",
                        "msg": "TARGET_CENTER is required",
                    },
                )
                error_rows.add(i)
            if not row.get("object_type"):
                errors.append(
                    {
                        "row": i,
                        "col": "OBJECT_TYPE",
                        "code": "REQUIRED",
                        "msg": "OBJECT_TYPE is required",
                    },
                )
                error_rows.add(i)
            elif (row.get("object_type") or "").strip().lower() not in (
                "cost_center",
                "profit_center",
            ):
                errors.append(
                    {
                        "row": i,
                        "col": "OBJECT_TYPE",
                        "code": "INVALID",
                        "msg": "OBJECT_TYPE must be 'cost_center' or 'profit_center'",
                    },
                )
                error_rows.add(i)
        elif batch.kind in ("balance", "balances", "balances_gcr"):
            if not row.get("cctr"):
                errors.append(
                    {
                        "row": i,
                        "col": "SAP_MANAGEMENT_CENTER",
                        "code": "REQUIRED",
                        "msg": "SAP_MANAGEMENT_CENTER is required",
                    }
                )
                error_rows.add(i)
            pr = row.get("period_raw", "")
            fy = row.get("fiscal_year", "")
            if fy and pr:
                if not fy.isdigit():
                    msg = f"FISCAL_YEAR must be numeric, got: {fy}"
                    errors.append(
                        {"row": i, "col": "FISCAL_YEAR", "code": "FORMAT", "msg": msg},
                    )
                    error_rows.add(i)
                if not pr.isdigit():
                    msg = f"PERIOD must be numeric, got: {pr}"
                    errors.append(
                        {"row": i, "col": "PERIOD", "code": "FORMAT", "msg": msg},
                    )
                    error_rows.add(i)
            elif pr and (len(pr) != 6 or not pr.isdigit()):
                errors.append(
                    {
                        "row": i,
                        "col": "PERIOD_YYYYMM",
                        "code": "FORMAT",
                        "msg": f"Period must be YYYYMM, got: {pr}",
                    }
                )
                error_rows.add(i)
        elif batch.kind in ("entity", "entities"):
            if not row.get("ccode"):
                errors.append(
                    {
                        "row": i,
                        "col": "COMPANY_CODE",
                        "code": "REQUIRED",
                        "msg": "COMPANY_CODE is required",
                    }
                )
                error_rows.add(i)
        elif batch.kind in ("hierarchy", "hierarchies"):
            row_type = (row.get("row_type") or "").upper()
            if row_type not in ("SETHEADER", "SETNODE", "SETLEAF"):
                errors.append(
                    {
                        "row": i,
                        "col": "ROW_TYPE",
                        "code": "INVALID",
                        "msg": f"ROW_TYPE must be SETHEADER/SETNODE/SETLEAF, got '{row_type}'",
                    }
                )
                error_rows.add(i)
            elif row_type == "SETHEADER" and not row.get("setname"):
                errors.append(
                    {
                        "row": i,
                        "col": "SETNAME",
                        "code": "REQUIRED",
                        "msg": "SETNAME required",
                    }
                )
                error_rows.add(i)
            elif row_type == "SETNODE" and (
                not row.get("parent_setname") or not row.get("child_setname")
            ):
                errors.append(
                    {
                        "row": i,
                        "col": "PARENT/CHILD",
                        "code": "REQUIRED",
                        "msg": "PARENT_SETNAME and CHILD_SETNAME required",
                    }
                )
                error_rows.add(i)
            elif row_type == "SETLEAF" and not row.get("value"):
                errors.append(
                    {
                        "row": i,
                        "col": "VALUE",
                        "code": "REQUIRED",
                        "msg": "VALUE required",
                    }
                )
                error_rows.add(i)
        elif batch.kind == "hierarchies_flat":
            if not row.get("nodeid"):
                errors.append(
                    {"row": i, "col": "NODEID", "code": "REQUIRED", "msg": "NODEID is required"}
                )
                error_rows.add(i)
            if not row.get("nodename"):
                errors.append(
                    {
                        "row": i,
                        "col": "NODENAME",
                        "code": "REQUIRED",
                        "msg": "NODENAME is required",
                    }
                )
                error_rows.add(i)
        elif batch.kind == "gl_accounts_ska1":
            if not row.get("saknr"):
                errors.append(
                    {"row": i, "col": "SAKNR", "code": "REQUIRED", "msg": "SAKNR is required"}
                )
                error_rows.add(i)
            if not row.get("ktopl"):
                errors.append(
                    {"row": i, "col": "KTOPL", "code": "REQUIRED", "msg": "KTOPL is required"}
                )
                error_rows.add(i)
        elif batch.kind == "gl_accounts_skb1":
            if not row.get("saknr"):
                errors.append(
                    {"row": i, "col": "SAKNR", "code": "REQUIRED", "msg": "SAKNR is required"}
                )
                error_rows.add(i)
            if not row.get("bukrs"):
                errors.append(
                    {"row": i, "col": "BUKRS", "code": "REQUIRED", "msg": "BUKRS is required"}
                )
                error_rows.add(i)

    # Store errors
    for err in errors[:5000]:
        db.add(
            UploadError(
                batch_id=batch.id,
                row_number=err["row"],
                column_name=err["col"],
                error_code=err["code"],
                message=err["msg"],
            )
        )

    batch.rows_total = len(normalized)
    batch.rows_valid = len(normalized) - len(error_rows)
    batch.rows_error = len(error_rows)
    batch.rows_processed = len(normalized)
    batch.status = "validated"
    batch.validated_at = datetime.now(UTC)
    db.commit()

    return {
        "status": "validated",
        "rows_total": batch.rows_total,
        "rows_valid": batch.rows_valid,
        "rows_error": batch.rows_error,
    }


def load_upload(batch_id: int, db: Session) -> dict:
    """Load validated upload into target tables."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status not in ("validated", "loading"):
        raise ValueError(f"Batch must be validated first (status: {batch.status})")

    if batch.status != "loading":
        batch.status = "loading"
        db.commit()

    try:
        rows = _read_file(batch.storage_uri)
    except Exception as e:
        batch.status = "failed"
        db.commit()
        return {"status": "failed", "error": str(e)}

    mapping = {
        "cost_center": CC_COLUMNS,
        "cost_centers": CC_COLUMNS,
        "profit_center": PC_COLUMNS,
        "profit_centers": PC_COLUMNS,
        "balance": BALANCE_COLUMNS,
        "balances": BALANCE_COLUMNS,
        "balances_gcr": BALANCE_COLUMNS,
        "entity": ENTITY_COLUMNS,
        "entities": ENTITY_COLUMNS,
        "employee": EMPLOYEE_COLUMNS,
        "employees": EMPLOYEE_COLUMNS,
        "hierarchies_flat": HIERARCHY_FLAT_COLUMNS,
        "gl_accounts_ska1": SKA1_COLUMNS,
        "gl_accounts_skb1": SKB1_COLUMNS,
        "target_cost_centers": TARGET_CC_COLUMNS,
        "target_profit_centers": TARGET_PC_COLUMNS,
        "center_mapping": CENTER_MAPPING_COLUMNS,
    }.get(batch.kind, {})

    normalized = _normalize_headers(rows, mapping) if mapping else rows
    loaded = 0

    # Read scope + data_category from batch (defaults for backward compat)
    batch_scope = getattr(batch, "scope", None) or "cleanup"
    batch_category = getattr(batch, "data_category", None) or "legacy"

    # Publish total + reset progress for load phase
    _flush_progress(batch.id, 0, len(normalized))

    if batch.kind in ("cost_center", "cost_centers"):
        for row in normalized:
            if not row.get("cctr") or not row.get("coarea"):
                continue
            existing = db.execute(
                select(LegacyCostCenter).where(
                    LegacyCostCenter.scope == batch_scope,
                    LegacyCostCenter.coarea == row["coarea"],
                    LegacyCostCenter.cctr == row["cctr"],
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            cc_kwargs: dict = {}
            for field_name in _CC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    cc_kwargs[field_name] = val if val else None
            cc_kwargs["coarea"] = row["coarea"]
            cc_kwargs["cctr"] = row["cctr"]
            if row.get("is_active"):
                cc_kwargs["is_active"] = is_act
            # Populate legacy valid_from/valid_to from SAP DATS or legacy keys
            for sap_key, legacy_key in (("datab", "valid_from"), ("datbi", "valid_to")):
                raw = row.get(legacy_key) or row.get(sap_key)
                if raw and isinstance(raw, str):
                    cc_kwargs[legacy_key] = _parse_date(raw)
            if existing:
                for k, v in cc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                _cc_defaults = (
                    "txtsh",
                    "txtmi",
                    "responsible",
                    "cctrcgy",
                    "ccode",
                    "currency",
                    "pctr",
                )
                for fld in _cc_defaults:
                    if cc_kwargs.get(fld) is None:
                        cc_kwargs[fld] = ""
                cc_kwargs.setdefault("is_active", True)
                cc_kwargs["refresh_batch"] = batch.id
                cc_kwargs["scope"] = batch_scope
                cc_kwargs["data_category"] = batch_category
                db.add(LegacyCostCenter(**cc_kwargs))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind in ("profit_center", "profit_centers"):
        for row in normalized:
            if not row.get("pctr"):
                continue
            existing = db.execute(
                select(LegacyProfitCenter).where(
                    LegacyProfitCenter.scope == batch_scope,
                    LegacyProfitCenter.coarea == row.get("coarea", ""),
                    LegacyProfitCenter.pctr == row["pctr"],
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            pc_kwargs: dict = {}
            for field_name in _PC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    pc_kwargs[field_name] = val if val else None
            pc_kwargs["coarea"] = row.get("coarea") or ""
            pc_kwargs["pctr"] = row["pctr"]
            if row.get("is_active"):
                pc_kwargs["is_active"] = is_act
            # Populate legacy valid_from/valid_to from SAP DATS or legacy keys
            for sap_key, legacy_key in (("datab", "valid_from"), ("datbi", "valid_to")):
                raw = row.get(legacy_key) or row.get(sap_key)
                if raw and isinstance(raw, str):
                    pc_kwargs[legacy_key] = _parse_date(raw)
            if existing:
                for k, v in pc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                for fld in ("txtsh", "txtmi", "responsible", "ccode", "department", "currency"):
                    if pc_kwargs.get(fld) is None:
                        pc_kwargs[fld] = ""
                pc_kwargs.setdefault("is_active", True)
                pc_kwargs["refresh_batch"] = batch.id
                pc_kwargs["scope"] = batch_scope
                pc_kwargs["data_category"] = batch_category
                db.add(LegacyProfitCenter(**pc_kwargs))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind in ("balance", "balances", "balances_gcr"):
        for row in normalized:
            if not row.get("cctr"):
                continue
            pr = row.get("period_raw", "")
            fy_str = row.get("fiscal_year", "")
            try:
                if fy_str:
                    fy = int(fy_str)
                    per = int(pr) if pr else 0
                elif pr and len(pr) == 6:
                    fy = int(pr[:4])
                    per = int(pr[4:])
                else:
                    fy = 0
                    per = 0
            except (ValueError, TypeError):
                fy = 0
                per = 0
            try:
                tc = Decimal(row.get("tc_amt", "0") or "0")
            except InvalidOperation:
                tc = Decimal("0")
            try:
                gc = Decimal(row.get("gc_amt", "0") or "0")
            except InvalidOperation:
                gc = Decimal("0")
            try:
                gc2 = Decimal(row.get("gc2_amt", "0") or "0")
            except InvalidOperation:
                gc2 = Decimal("0")
            try:
                pc = int(row.get("posting_count", "0") or "0")
            except ValueError:
                pc = 0
            db.add(
                Balance(
                    scope=batch_scope,
                    data_category=batch_category,
                    coarea=row.get("coarea", ""),
                    cctr=row["cctr"],
                    ccode=row.get("ccode", ""),
                    fiscal_year=fy,
                    period=per,
                    account=row.get("account", ""),
                    account_class=row.get("account_class", ""),
                    tc_amt=tc,
                    gc_amt=gc,
                    gc2_amt=gc2,
                    currency_tc=row.get("currency_tc", ""),
                    posting_count=pc,
                    refresh_batch=batch.id,
                )
            )
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind in ("entity", "entities"):
        for row in normalized:
            if not row.get("ccode"):
                continue
            existing = db.execute(
                select(Entity).where(
                    Entity.scope == batch_scope,
                    Entity.ccode == row["ccode"],
                )
            ).scalar_one_or_none()
            ent_kwargs: dict = {}
            for field_name in _ENTITY_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    ent_kwargs[field_name] = val if val else None
            ent_kwargs["ccode"] = row["ccode"]
            if row.get("is_active"):
                ent_kwargs["is_active"] = row["is_active"].upper() not in (
                    "FALSE",
                    "0",
                    "NO",
                    "N",
                )
            if existing:
                for k, v in ent_kwargs.items():
                    if k != "ccode" and v is not None:
                        setattr(existing, k, v)
            else:
                if ent_kwargs.get("name") is None:
                    ent_kwargs["name"] = row["ccode"]
                ent_kwargs["scope"] = batch_scope
                ent_kwargs["data_category"] = batch_category
                db.add(Entity(**ent_kwargs))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind in ("employee", "employees"):
        for row in normalized:
            gpn = row.get("gpn", "").strip()
            if not gpn:
                continue
            existing = (
                db.execute(
                    select(Employee).where(
                        Employee.scope == batch_scope,
                        Employee.gpn == gpn,
                        Employee.refresh_batch == batch.id,
                    )
                )
                .scalars()
                .first()
            )
            # Separate model fields from extra attrs
            model_kwargs: dict = {}
            extra_attrs: dict = {}
            for k, v in row.items():
                if k in _EMPLOYEE_MODEL_FIELDS:
                    model_kwargs[k] = v if v else None
                elif k and k != "_extras" and v:
                    extra_attrs[k] = v
            # Recover unmapped CSV columns from _extras (stored as repr by _normalize_headers)
            extras_raw = row.get("_extras")
            if extras_raw and isinstance(extras_raw, str):
                try:
                    parsed = ast.literal_eval(extras_raw)
                    if isinstance(parsed, dict):
                        extra_attrs.update(parsed)
                except (ValueError, SyntaxError):
                    pass
            model_kwargs["attrs"] = extra_attrs if extra_attrs else None
            # Parse datetime fields from various CSV date formats
            for dt_field in ("valid_from", "valid_to"):
                raw = model_kwargs.get(dt_field)
                if raw and isinstance(raw, str):
                    model_kwargs[dt_field] = _parse_date(raw)
            model_kwargs["refresh_batch"] = batch.id
            if existing:
                for k, v in model_kwargs.items():
                    if k != "refresh_batch" and v is not None:
                        setattr(existing, k, v)
            else:
                model_kwargs["scope"] = batch_scope
                model_kwargs["data_category"] = batch_category
                db.add(Employee(**model_kwargs))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind in ("hierarchy", "hierarchies"):
        # Pass 1: create Hierarchy headers
        hier_map: dict[tuple[str, str], Hierarchy] = {}
        for row in normalized:
            row_type = (row.get("row_type") or "").upper()
            if row_type != "SETHEADER":
                continue
            setclass = row.get("setclass", "0101")
            setname = row.get("setname", "")
            if not setname:
                continue
            existing = db.execute(
                select(Hierarchy).where(
                    Hierarchy.scope == batch_scope,
                    Hierarchy.setclass == setclass,
                    Hierarchy.setname == setname,
                    Hierarchy.refresh_batch == batch.id,
                )
            ).scalar_one_or_none()
            if not existing:
                h = Hierarchy(
                    scope=batch_scope,
                    data_category=batch_category,
                    setclass=setclass,
                    setname=setname,
                    description=row.get("description", ""),
                    coarea=row.get("coarea", ""),
                    refresh_batch=batch.id,
                )
                db.add(h)
                db.flush()
                hier_map[(setclass, setname)] = h
            else:
                hier_map[(setclass, setname)] = existing
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

        # Pass 2: create nodes
        for row in normalized:
            row_type = (row.get("row_type") or "").upper()
            if row_type != "SETNODE":
                continue
            setclass = row.get("setclass", "0101")
            setname = row.get("setname", "")
            key = (setclass, setname)
            hier = hier_map.get(key)
            if not hier:
                hier = db.execute(
                    select(Hierarchy).where(
                        Hierarchy.scope == batch_scope,
                        Hierarchy.setclass == setclass,
                        Hierarchy.setname == setname,
                    )
                ).scalar_one_or_none()
                if hier:
                    hier_map[key] = hier
            if not hier:
                continue
            seq = int(row.get("seq") or "0")
            db.add(
                HierarchyNode(
                    hierarchy_id=hier.id,
                    parent_setname=row.get("parent_setname", ""),
                    child_setname=row.get("child_setname", ""),
                    seq=seq,
                )
            )
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

        # Pass 3: create leaves
        for row in normalized:
            row_type = (row.get("row_type") or "").upper()
            if row_type != "SETLEAF":
                continue
            setclass = row.get("setclass", "0101")
            setname = row.get("setname", "")
            key = (setclass, setname)
            hier = hier_map.get(key)
            if not hier:
                hier = db.execute(
                    select(Hierarchy).where(
                        Hierarchy.scope == batch_scope,
                        Hierarchy.setclass == setclass,
                        Hierarchy.setname == setname,
                    )
                ).scalar_one_or_none()
                if hier:
                    hier_map[key] = hier
            if not hier:
                continue
            parent_set = row.get("parent_setname") or row.get("setname", "")
            seq = int(row.get("seq") or "0")
            db.add(
                HierarchyLeaf(
                    hierarchy_id=hier.id,
                    setname=parent_set,
                    value=row.get("value", ""),
                    seq=seq,
                )
            )
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind == "hierarchies_flat":
        # Build hierarchy from flat SAP node export (NODEID/PARENTID/CHILDID).
        # Identify root nodes (no PARENTID) — each becomes a Hierarchy header.
        # Nodes with children become HierarchyNodes; leaf-level rows become HierarchyLeaves.
        node_lookup: dict[str, dict] = {}
        children_of: dict[str, list[str]] = {}
        for row in normalized:
            nid = row.get("nodeid", "").strip()
            if not nid:
                continue
            node_lookup[nid] = row
            pid = row.get("parentid", "").strip()
            if pid:
                children_of.setdefault(pid, []).append(nid)

        # Find root nodes (no parent)
        roots = [
            row for row in normalized if row.get("nodeid") and not row.get("parentid", "").strip()
        ]
        if not roots:
            # Fallback: treat all nodes whose parentid is not in the dataset as roots
            all_ids = set(node_lookup.keys())
            roots = [
                row
                for row in normalized
                if row.get("nodeid") and row.get("parentid", "").strip() not in all_ids
            ]

        hier_map_flat: dict[str, Hierarchy] = {}
        for root_row in roots:
            root_id = root_row.get("nodeid", "").strip()
            setname = root_row.get("nodename", root_id)
            description = root_row.get("nodetext", "")
            h = Hierarchy(
                scope=batch_scope,
                data_category=batch_category,
                setclass="FLAT",
                setname=setname,
                description=description,
                coarea="",
                refresh_batch=batch.id,
            )
            db.add(h)
            db.flush()
            hier_map_flat[root_id] = h
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

        # BFS to create nodes and leaves
        from collections import deque

        queue: deque[tuple[str, Hierarchy]] = deque()
        for root_row in roots:
            rid = root_row.get("nodeid", "").strip()
            if rid in hier_map_flat:
                queue.append((rid, hier_map_flat[rid]))

        visited: set[str] = set()
        seq_counter: dict[int, int] = {}
        while queue:
            parent_nid, hier = queue.popleft()
            if parent_nid in visited:
                continue
            visited.add(parent_nid)
            parent_row = node_lookup.get(parent_nid, {})
            parent_name = parent_row.get("nodename", parent_nid)
            child_ids = children_of.get(parent_nid, [])
            for child_nid in child_ids:
                child_row = node_lookup.get(child_nid, {})
                child_name = child_row.get("nodename", child_nid)
                hid = hier.id
                seq_counter.setdefault(hid, 0)
                seq_counter[hid] += 1
                has_children = child_nid in children_of
                if has_children:
                    db.add(
                        HierarchyNode(
                            hierarchy_id=hid,
                            parent_setname=parent_name,
                            child_setname=child_name,
                            seq=seq_counter[hid],
                        )
                    )
                    queue.append((child_nid, hier))
                else:
                    db.add(
                        HierarchyLeaf(
                            hierarchy_id=hid,
                            setname=parent_name,
                            value=child_name,
                            seq=seq_counter[hid],
                        )
                    )
                loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind == "gl_accounts_ska1":
        for row in normalized:
            saknr = (row.get("saknr") or "").strip()
            ktopl = (row.get("ktopl") or "").strip()
            if not saknr or not ktopl:
                continue
            existing = db.execute(
                select(GLAccountSKA1).where(
                    GLAccountSKA1.scope == batch_scope,
                    GLAccountSKA1.ktopl == ktopl,
                    GLAccountSKA1.saknr == saknr,
                )
            ).scalar_one_or_none()
            kwargs: dict = {}
            for field_name in _SKA1_MODEL_FIELDS:
                val = row.get(field_name)
                if val is not None:
                    kwargs[field_name] = val if val else None
            kwargs["ktopl"] = ktopl
            kwargs["saknr"] = saknr
            if existing:
                for k, v in kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                kwargs["refresh_batch"] = batch.id
                kwargs["scope"] = batch_scope
                kwargs["data_category"] = batch_category
                db.add(GLAccountSKA1(**kwargs))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind == "gl_accounts_skb1":
        for row in normalized:
            saknr = (row.get("saknr") or "").strip()
            bukrs = (row.get("bukrs") or "").strip()
            if not saknr or not bukrs:
                continue
            existing = db.execute(
                select(GLAccountSKB1).where(
                    GLAccountSKB1.scope == batch_scope,
                    GLAccountSKB1.bukrs == bukrs,
                    GLAccountSKB1.saknr == saknr,
                )
            ).scalar_one_or_none()
            kwargs_b: dict = {}
            for field_name in _SKB1_MODEL_FIELDS:
                val = row.get(field_name)
                if val is not None:
                    kwargs_b[field_name] = val if val else None
            kwargs_b["bukrs"] = bukrs
            kwargs_b["saknr"] = saknr
            if existing:
                for k, v in kwargs_b.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                kwargs_b["refresh_batch"] = batch.id
                kwargs_b["scope"] = batch_scope
                kwargs_b["data_category"] = batch_category
                db.add(GLAccountSKB1(**kwargs_b))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind == "target_cost_centers":
        for row in normalized:
            cctr = (row.get("cctr") or "").strip()
            coarea = (row.get("coarea") or "").strip()
            if not cctr or not coarea:
                continue
            existing = db.execute(
                select(TargetCostCenter).where(
                    TargetCostCenter.scope == batch_scope,
                    TargetCostCenter.coarea == coarea,
                    TargetCostCenter.cctr == cctr,
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            tcc_kwargs: dict = {}
            for field_name in _TARGET_CC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    tcc_kwargs[field_name] = val if val else None
            tcc_kwargs["coarea"] = coarea
            tcc_kwargs["cctr"] = cctr
            if row.get("is_active"):
                tcc_kwargs["is_active"] = is_act
            if existing:
                for k, v in tcc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                tcc_kwargs.setdefault("is_active", True)
                tcc_kwargs["refresh_batch"] = batch.id
                tcc_kwargs["scope"] = batch_scope
                tcc_kwargs["data_category"] = batch_category
                db.add(TargetCostCenter(**tcc_kwargs))
            loaded += 1

    elif batch.kind == "target_profit_centers":
        for row in normalized:
            pctr = (row.get("pctr") or "").strip()
            coarea = (row.get("coarea") or "").strip()
            if not pctr or not coarea:
                continue
            existing = db.execute(
                select(TargetProfitCenter).where(
                    TargetProfitCenter.scope == batch_scope,
                    TargetProfitCenter.coarea == coarea,
                    TargetProfitCenter.pctr == pctr,
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            tpc_kwargs: dict = {}
            for field_name in _TARGET_PC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    tpc_kwargs[field_name] = val if val else None
            tpc_kwargs["coarea"] = coarea
            tpc_kwargs["pctr"] = pctr
            if row.get("is_active"):
                tpc_kwargs["is_active"] = is_act
            if existing:
                for k, v in tpc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                tpc_kwargs.setdefault("is_active", True)
                tpc_kwargs["refresh_batch"] = batch.id
                tpc_kwargs["scope"] = batch_scope
                tpc_kwargs["data_category"] = batch_category
                db.add(TargetProfitCenter(**tpc_kwargs))
            loaded += 1

    elif batch.kind == "center_mapping":
        for row in normalized:
            legacy_center = (row.get("legacy_center") or "").strip()
            target_center = (row.get("target_center") or "").strip()
            obj_type = (row.get("object_type") or "").strip().lower()
            if not legacy_center or not target_center or not obj_type:
                continue
            if obj_type not in ("cost_center", "profit_center"):
                continue
            legacy_co = (row.get("legacy_coarea") or "").strip() or ""
            target_co = (row.get("target_coarea") or "").strip() or ""
            existing = db.execute(
                select(CenterMapping).where(
                    CenterMapping.scope == batch_scope,
                    CenterMapping.object_type == obj_type,
                    CenterMapping.legacy_coarea == legacy_co,
                    CenterMapping.legacy_center == legacy_center,
                    CenterMapping.target_coarea == target_co,
                    CenterMapping.target_center == target_center,
                )
            ).scalar_one_or_none()
            cm_kwargs: dict = {}
            for field_name in _CENTER_MAPPING_MODEL_FIELDS:
                val = row.get(field_name)
                if val is not None:
                    cm_kwargs[field_name] = val if val else None
            cm_kwargs["object_type"] = obj_type
            cm_kwargs["legacy_center"] = legacy_center
            cm_kwargs["target_center"] = target_center
            cm_kwargs["legacy_coarea"] = legacy_co
            cm_kwargs["target_coarea"] = target_co
            if existing:
                for k, v in cm_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                cm_kwargs["refresh_batch"] = batch.id
                cm_kwargs["scope"] = batch_scope
                cm_kwargs["data_category"] = batch_category
                db.add(CenterMapping(**cm_kwargs))
            loaded += 1

    batch.rows_loaded = loaded
    batch.rows_processed = loaded
    batch.status = "loaded"
    batch.loaded_at = datetime.now(UTC)
    db.commit()

    return {"status": "loaded", "rows_loaded": loaded}


def rollback_upload(batch_id: int, db: Session) -> dict:
    """Rollback a loaded upload batch."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status != "loaded":
        raise ValueError(f"Only loaded batches can be rolled back (status: {batch.status})")

    deleted = 0
    if batch.kind in ("cost_center", "cost_centers"):
        r = db.execute(
            sa_delete(LegacyCostCenter).where(LegacyCostCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind in ("profit_center", "profit_centers"):
        r = db.execute(
            sa_delete(LegacyProfitCenter).where(LegacyProfitCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind in ("balance", "balances", "balances_gcr"):
        r = db.execute(sa_delete(Balance).where(Balance.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind in ("entity", "entities"):
        raise ValueError("Entity uploads cannot be rolled back (no batch tracking on entities)")
    elif batch.kind in ("employee", "employees"):
        r = db.execute(sa_delete(Employee).where(Employee.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind in ("hierarchy", "hierarchies", "hierarchies_flat"):
        hier_ids = [
            h.id
            for h in db.execute(select(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
            .scalars()
            .all()
        ]
        for hid in hier_ids:
            db.execute(sa_delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == hid))
            db.execute(sa_delete(HierarchyNode).where(HierarchyNode.hierarchy_id == hid))
        r = db.execute(sa_delete(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind == "gl_accounts_ska1":
        r = db.execute(sa_delete(GLAccountSKA1).where(GLAccountSKA1.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind == "gl_accounts_skb1":
        r = db.execute(sa_delete(GLAccountSKB1).where(GLAccountSKB1.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind == "target_cost_centers":
        r = db.execute(
            sa_delete(TargetCostCenter).where(TargetCostCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind == "target_profit_centers":
        r = db.execute(
            sa_delete(TargetProfitCenter).where(TargetProfitCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind == "center_mapping":
        r = db.execute(sa_delete(CenterMapping).where(CenterMapping.refresh_batch == batch.id))
        deleted = r.rowcount

    rows_loaded = batch.rows_loaded or 0
    rows_updated = max(0, rows_loaded - deleted)
    batch.status = "rolled_back"
    db.commit()
    result: dict = {"status": "rolled_back", "rows_deleted": deleted}
    if rows_updated > 0:
        result["rows_updated_not_reverted"] = rows_updated
        result["warning"] = (
            f"{rows_updated} existing records were updated during upload "
            "and could not be reverted by rollback."
        )
    return result
