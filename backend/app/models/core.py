"""Core ORM models for the cleanup schema (section 03 of spec)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

# ---------- reference / source data ----------


class Entity(TimestampMixin, Base):
    """Company code / entity master — aligned with SAP T001."""

    __tablename__ = "entity"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    # --- key fields ---
    mandt: Mapped[str | None] = mapped_column(String(3))
    ccode: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)  # BUKRS
    # --- T001 master fields ---
    name: Mapped[str] = mapped_column(String(200), nullable=False)  # BUTXT
    city: Mapped[str | None] = mapped_column(String(25))  # ORT01
    country: Mapped[str | None] = mapped_column(String(3))  # LAND1
    region: Mapped[str | None] = mapped_column(String(50))
    currency: Mapped[str | None] = mapped_column(String(3))  # WAERS
    language: Mapped[str | None] = mapped_column(String(2))  # SPRAS
    chart_of_accounts: Mapped[str | None] = mapped_column(String(4))  # KTOPL
    waabw: Mapped[str | None] = mapped_column(String(2))
    fiscal_year_variant: Mapped[str | None] = mapped_column(String(2))  # PERIV
    kokfi: Mapped[str | None] = mapped_column(String(1))
    company: Mapped[str | None] = mapped_column(String(6))  # RCOMP
    adrnr: Mapped[str | None] = mapped_column(String(10))
    stceg: Mapped[str | None] = mapped_column(String(20))
    fikrs: Mapped[str | None] = mapped_column(String(4))  # FM area
    fm_area: Mapped[str | None] = mapped_column(String(4))  # FIKRS alias
    xfmco: Mapped[str | None] = mapped_column(String(1))
    xfmcb: Mapped[str | None] = mapped_column(String(1))
    xfmca: Mapped[str | None] = mapped_column(String(1))
    txjcd: Mapped[str | None] = mapped_column(String(15))
    fmhrdate: Mapped[str | None] = mapped_column(String(8))
    # --- SI_T001 include ---
    buvar: Mapped[str | None] = mapped_column(String(1))
    fdbuk: Mapped[str | None] = mapped_column(String(4))
    xfdis: Mapped[str | None] = mapped_column(String(1))
    xvalv: Mapped[str | None] = mapped_column(String(1))
    xskfn: Mapped[str | None] = mapped_column(String(1))
    credit_control_area: Mapped[str | None] = mapped_column(String(4))  # KKBER
    xmwsn: Mapped[str | None] = mapped_column(String(1))
    mregl: Mapped[str | None] = mapped_column(String(4))
    xgsbe: Mapped[str | None] = mapped_column(String(1))
    xgjrv: Mapped[str | None] = mapped_column(String(1))
    xkdft: Mapped[str | None] = mapped_column(String(1))
    xprod: Mapped[str | None] = mapped_column(String(1))
    xeink: Mapped[str | None] = mapped_column(String(1))
    xjvaa: Mapped[str | None] = mapped_column(String(1))
    xvvwa: Mapped[str | None] = mapped_column(String(1))
    xslta: Mapped[str | None] = mapped_column(String(1))
    xfdmm: Mapped[str | None] = mapped_column(String(1))
    xfdsd: Mapped[str | None] = mapped_column(String(1))
    xextb: Mapped[str | None] = mapped_column(String(1))
    ebukr: Mapped[str | None] = mapped_column(String(4))
    ktop2: Mapped[str | None] = mapped_column(String(4))
    umkrs: Mapped[str | None] = mapped_column(String(4))
    bukrs_glob: Mapped[str | None] = mapped_column(String(6))
    fstva: Mapped[str | None] = mapped_column(String(4))
    opvar: Mapped[str | None] = mapped_column(String(4))
    xcovr: Mapped[str | None] = mapped_column(String(1))
    txkrs: Mapped[str | None] = mapped_column(String(1))
    wfvar: Mapped[str | None] = mapped_column(String(4))
    xbbbf: Mapped[str | None] = mapped_column(String(1))
    xbbbe: Mapped[str | None] = mapped_column(String(1))
    xbbba: Mapped[str | None] = mapped_column(String(1))
    xbbko: Mapped[str | None] = mapped_column(String(1))
    xstdt: Mapped[str | None] = mapped_column(String(1))
    mwskv: Mapped[str | None] = mapped_column(String(2))
    mwska: Mapped[str | None] = mapped_column(String(2))
    impda: Mapped[str | None] = mapped_column(String(1))
    xnegp: Mapped[str | None] = mapped_column(String(1))
    xkkbi: Mapped[str | None] = mapped_column(String(1))
    wt_newwt: Mapped[str | None] = mapped_column(String(1))
    pp_pdate: Mapped[str | None] = mapped_column(String(1))
    infmt: Mapped[str | None] = mapped_column(String(4))
    fstvare: Mapped[str | None] = mapped_column(String(4))
    kopim: Mapped[str | None] = mapped_column(String(1))
    dkweg: Mapped[str | None] = mapped_column(String(1))
    offsacct: Mapped[str | None] = mapped_column(String(1))
    bapovar: Mapped[str | None] = mapped_column(String(2))
    xcos: Mapped[str | None] = mapped_column(String(1))
    xcession: Mapped[str | None] = mapped_column(String(1))
    xsplt: Mapped[str | None] = mapped_column(String(1))
    surccm: Mapped[str | None] = mapped_column(String(1))
    dtprov: Mapped[str | None] = mapped_column(String(2))
    dtamtc: Mapped[str | None] = mapped_column(String(2))
    dttaxc: Mapped[str | None] = mapped_column(String(2))
    dttdsp: Mapped[str | None] = mapped_column(String(2))
    dtaxr: Mapped[str | None] = mapped_column(String(4))
    xvatdate: Mapped[str | None] = mapped_column(String(1))
    pst_per_var: Mapped[str | None] = mapped_column(String(1))
    xbbsc: Mapped[str | None] = mapped_column(String(1))
    f_obsolete: Mapped[str | None] = mapped_column(String(1))
    # --- app fields ---
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    attrs: Mapped[dict | None] = mapped_column(JSONB)


class Employee(TimestampMixin, Base):
    """Employee master data — aligned with SAP ZUHL_GRD_GPF."""

    __tablename__ = "employee"
    __table_args__ = (
        UniqueConstraint("gpn", "refresh_batch"),
        Index("ix_emp_user_id", "user_id_pid"),
        Index("ix_emp_ou_cd", "ou_cd"),
        Index("ix_emp_cost_pc", "local_cc_cd"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    # --- key fields (ZUHL_GRD_GPF) ---
    mandt: Mapped[str | None] = mapped_column(String(3))
    gpn: Mapped[str] = mapped_column(String(20), nullable=False)  # GPN
    # --- personal data ---
    name: Mapped[str | None] = mapped_column(String(40))  # NAME (Nachname)
    vorname: Mapped[str | None] = mapped_column(String(40))  # VORNAME
    sprachenschluess: Mapped[str | None] = mapped_column(String(1))
    anredecode: Mapped[str | None] = mapped_column(String(1))
    userid: Mapped[str | None] = mapped_column(String(6))
    eintrittsdatum: Mapped[str | None] = mapped_column(String(8))
    oe_leiter: Mapped[str | None] = mapped_column(String(1))
    # --- workplace 1 ---
    int_tel_nr_1ap: Mapped[str | None] = mapped_column(String(20))
    ext_tel_nr_1ap: Mapped[str | None] = mapped_column(String(25))
    nl_code_geb_1ap: Mapped[str | None] = mapped_column(String(8))
    strasse_geb_1ap: Mapped[str | None] = mapped_column(String(30))
    stockwerk_1ap: Mapped[str | None] = mapped_column(String(10))
    bueronummer_1ap: Mapped[str | None] = mapped_column(String(8))
    # --- cost center / org ---
    kstst: Mapped[str | None] = mapped_column(String(10))
    kstst_text: Mapped[str | None] = mapped_column(String(20))
    oe_objekt_id: Mapped[str | None] = mapped_column(String(8))
    oe_code: Mapped[str | None] = mapped_column(String(12))
    oe_text: Mapped[str | None] = mapped_column(String(40))
    sap_bukrs: Mapped[str | None] = mapped_column(String(4))
    sap_bukrs_text: Mapped[str | None] = mapped_column(String(25))
    t_nummer: Mapped[str | None] = mapped_column(String(12))
    instrad_1: Mapped[str | None] = mapped_column(String(35))
    instrad_2: Mapped[str | None] = mapped_column(String(35))
    kstst_einsatz_oe: Mapped[str | None] = mapped_column(String(10))
    personalber_text: Mapped[str | None] = mapped_column(String(30))
    nl_oe_ma: Mapped[str | None] = mapped_column(String(12))
    nl_text: Mapped[str | None] = mapped_column(String(40))
    gsfld_oe_ma: Mapped[str | None] = mapped_column(String(3))
    gsfld_oe_ma_text: Mapped[str | None] = mapped_column(String(30))
    ma_gruppe: Mapped[str | None] = mapped_column(String(3))
    ma_gruppe_text: Mapped[str | None] = mapped_column(String(20))
    ma_kreis: Mapped[str | None] = mapped_column(String(2))
    ma_kreis_text: Mapped[str | None] = mapped_column(String(25))
    rang_code: Mapped[str | None] = mapped_column(String(4))
    rang_text: Mapped[str | None] = mapped_column(String(35))
    akademischer_tit: Mapped[str | None] = mapped_column(String(15))
    ubs_funk: Mapped[str | None] = mapped_column(String(5))
    ubs_funk_text: Mapped[str | None] = mapped_column(String(50))
    # --- manager ---
    gpn_vg_ma: Mapped[str | None] = mapped_column(String(8))
    name_vg_ma: Mapped[str | None] = mapped_column(String(50))
    ueg_oe_objektid: Mapped[str | None] = mapped_column(String(8))
    ueg_oe_bez: Mapped[str | None] = mapped_column(String(12))
    ueg_oe_krz: Mapped[str | None] = mapped_column(String(40))
    bschgrad: Mapped[str | None] = mapped_column(String(3))
    personalbereich: Mapped[str | None] = mapped_column(String(4))
    fax_ext_1ap: Mapped[str | None] = mapped_column(String(25))
    email_adresse: Mapped[str | None] = mapped_column(String(80))
    ma_kz: Mapped[str | None] = mapped_column(String(3))
    firma_ext_ma: Mapped[str | None] = mapped_column(String(40))
    begdat_orgwechs: Mapped[str | None] = mapped_column(String(8))
    austritt_datum: Mapped[str | None] = mapped_column(String(8))
    natel_nummer: Mapped[str | None] = mapped_column(String(25))
    pager_nummer: Mapped[str | None] = mapped_column(String(25))
    plz_geb_1ap: Mapped[str | None] = mapped_column(String(10))
    ort_geb_1ap: Mapped[str | None] = mapped_column(String(20))
    einsatz_oe_krz: Mapped[str | None] = mapped_column(String(12))
    einsatz_oe_text: Mapped[str | None] = mapped_column(String(40))
    division: Mapped[str | None] = mapped_column(String(14))
    geb_cod_1ap: Mapped[str | None] = mapped_column(String(8))
    rang_krz: Mapped[str | None] = mapped_column(String(4))
    systemdatum: Mapped[str | None] = mapped_column(String(8))
    ap_nummer: Mapped[str | None] = mapped_column(String(30))
    einsatz_oe_objid: Mapped[str | None] = mapped_column(String(8))
    # --- workplace 2 ---
    int_tel_nr_2ap: Mapped[str | None] = mapped_column(String(20))
    ext_tel_nr_2ap: Mapped[str | None] = mapped_column(String(25))
    bueronummer_2ap: Mapped[str | None] = mapped_column(String(8))
    geb_cod_2ap: Mapped[str | None] = mapped_column(String(8))
    strasse_geb_2ap: Mapped[str | None] = mapped_column(String(30))
    plz_geb_2ap: Mapped[str | None] = mapped_column(String(10))
    ort_geb_2ap: Mapped[str | None] = mapped_column(String(20))
    geb_cod_geb_2ap: Mapped[str | None] = mapped_column(String(8))
    fax_nr_2ap: Mapped[str | None] = mapped_column(String(25))
    stockwerk_2ap: Mapped[str | None] = mapped_column(String(10))
    gpin_nummer: Mapped[str | None] = mapped_column(String(9))
    nat: Mapped[str | None] = mapped_column(String(3))
    # --- address 1 ---
    land_geb_1ap: Mapped[str | None] = mapped_column(String(3))
    reg_nr_1ap: Mapped[str | None] = mapped_column(String(4))
    postf_1ap: Mapped[str | None] = mapped_column(String(30))
    plz_postfadr_1ap: Mapped[str | None] = mapped_column(String(10))
    ort_postfadr_1ap: Mapped[str | None] = mapped_column(String(20))
    # --- address 2 ---
    land_geb_2ap: Mapped[str | None] = mapped_column(String(3))
    reg_nr_2ap: Mapped[str | None] = mapped_column(String(4))
    postf_2ap: Mapped[str | None] = mapped_column(String(30))
    plz_postfadr_2ap: Mapped[str | None] = mapped_column(String(10))
    ort_postfadr_2ap: Mapped[str | None] = mapped_column(String(20))
    letzter_arb_tag: Mapped[str | None] = mapped_column(String(8))
    abac_nl_ag_einoe: Mapped[str | None] = mapped_column(String(6))
    vertr_ende_exma: Mapped[str | None] = mapped_column(String(8))
    untergrp_code: Mapped[str | None] = mapped_column(String(4))
    # --- business name variants ---
    bs_first_name: Mapped[str | None] = mapped_column(String(50))
    bs_last_name: Mapped[str | None] = mapped_column(String(50))
    name_uc: Mapped[str | None] = mapped_column(String(40))
    vorname_uc: Mapped[str | None] = mapped_column(String(40))
    name_ph: Mapped[str | None] = mapped_column(String(20))
    vorname_ph: Mapped[str | None] = mapped_column(String(20))
    ma_oe: Mapped[str | None] = mapped_column(String(4))
    updated_id: Mapped[str | None] = mapped_column(String(10))
    ma_kstst: Mapped[str | None] = mapped_column(String(4))
    business_name: Mapped[str | None] = mapped_column(String(50))
    # --- job category ---
    job_categ_code: Mapped[str | None] = mapped_column(String(6))
    job_categ_descr: Mapped[str | None] = mapped_column(String(30))
    costcenter_code: Mapped[str | None] = mapped_column(String(10))
    costcenter_descr: Mapped[str | None] = mapped_column(String(50))
    # --- GCRS/Management accounting ---
    manacs_func_code: Mapped[str | None] = mapped_column(String(10))
    manacs_func_desc: Mapped[str | None] = mapped_column(String(50))
    manacs_segm_code: Mapped[str | None] = mapped_column(String(10))
    manacs_segm_desc: Mapped[str | None] = mapped_column(String(50))
    manacs_sect_code: Mapped[str | None] = mapped_column(String(10))
    manacs_sect_desc: Mapped[str | None] = mapped_column(String(50))
    manacs_bsar_code: Mapped[str | None] = mapped_column(String(10))
    manacs_bsar_desc: Mapped[str | None] = mapped_column(String(50))
    manacs_bsun_code: Mapped[str | None] = mapped_column(String(10))
    manacs_bsun_desc: Mapped[str | None] = mapped_column(String(50))
    manacs_bsgp_code: Mapped[str | None] = mapped_column(String(10))
    manacs_bsgp_desc: Mapped[str | None] = mapped_column(String(50))
    manacs_reg_code: Mapped[str | None] = mapped_column(String(10))
    manacs_reg_descr: Mapped[str | None] = mapped_column(String(50))
    manacs_loc_code: Mapped[str | None] = mapped_column(String(10))
    manacs_loc_descr: Mapped[str | None] = mapped_column(String(50))
    regulatory_reg: Mapped[str | None] = mapped_column(String(5))
    supervisors_gpin: Mapped[str | None] = mapped_column(String(11))
    uuname: Mapped[str | None] = mapped_column(String(20))
    web_sso: Mapped[str | None] = mapped_column(String(20))
    sap_user: Mapped[str | None] = mapped_column(String(12))
    hr_company: Mapped[str | None] = mapped_column(String(3))
    regulatory_regst: Mapped[str | None] = mapped_column(String(3))
    global_cc: Mapped[str | None] = mapped_column(String(10))
    # --- legacy app fields (backward compat) ---
    bs_name: Mapped[str | None] = mapped_column(String(200))
    bs_firstname: Mapped[str | None] = mapped_column(String(100))
    bs_lastname: Mapped[str | None] = mapped_column(String(100))
    legal_family_name: Mapped[str | None] = mapped_column(String(100))
    legal_first_name: Mapped[str | None] = mapped_column(String(100))
    email_address: Mapped[str | None] = mapped_column(String(200))
    emp_status: Mapped[str | None] = mapped_column(String(20))
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    gender_code: Mapped[str | None] = mapped_column(String(5))
    user_id_pid: Mapped[str | None] = mapped_column(String(30))
    user_id_tnumber: Mapped[str | None] = mapped_column(String(30))
    ou_pk: Mapped[str | None] = mapped_column(String(20))
    ou_cd: Mapped[str | None] = mapped_column(String(20))
    ou_desc: Mapped[str | None] = mapped_column(String(200))
    wrk_in_ou_pk: Mapped[str | None] = mapped_column(String(20))
    wrk_in_ou_cd: Mapped[str | None] = mapped_column(String(20))
    wrk_in_ou_desc: Mapped[str | None] = mapped_column(String(200))
    local_cc_cd: Mapped[str | None] = mapped_column(String(20))
    local_cc_desc: Mapped[str | None] = mapped_column(String(200))
    gcrs_comp_cd: Mapped[str | None] = mapped_column(String(20))
    gcrs_comp_desc: Mapped[str | None] = mapped_column(String(200))
    cost_pc_cd_e_ou: Mapped[str | None] = mapped_column(String(20))
    cost_pc_cd_w_ou: Mapped[str | None] = mapped_column(String(20))
    lm_gpn: Mapped[str | None] = mapped_column(String(20))
    lm_bs_firstname: Mapped[str | None] = mapped_column(String(100))
    lm_bs_lastname: Mapped[str | None] = mapped_column(String(100))
    supervisor_gpn: Mapped[str | None] = mapped_column(String(20))
    rank_cd: Mapped[str | None] = mapped_column(String(20))
    rank_desc: Mapped[str | None] = mapped_column(String(200))
    job_desc: Mapped[str | None] = mapped_column(String(200))
    empl_class: Mapped[str | None] = mapped_column(String(20))
    full_time_eq: Mapped[str | None] = mapped_column(String(10))
    head_of_own_ou: Mapped[str | None] = mapped_column(String(5))
    reg_region: Mapped[str | None] = mapped_column(String(50))
    locn_city_name_1: Mapped[str | None] = mapped_column(String(100))
    locn_ctry_cd_1: Mapped[str | None] = mapped_column(String(5))
    building_cd_1: Mapped[str | None] = mapped_column(String(20))
    # --- overflow ---
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )

    @property
    def display_name(self) -> str:
        """Format as 'GPN Name' for owner display."""
        sap_name = f"{self.vorname or ''} {self.name or ''}".strip()
        legacy_name = self.bs_name or f"{self.bs_firstname or ''} {self.bs_lastname or ''}".strip()
        name = sap_name or legacy_name
        return f"{self.gpn} {name}".strip()


class LegacyCostCenter(TimestampMixin, Base):
    """Cost center master — aligned with SAP CSKS/CSKT."""

    __tablename__ = "legacy_cost_center"
    __table_args__ = (
        UniqueConstraint("coarea", "cctr", "refresh_batch"),
        Index("ix_lcc_ccode", "ccode"),
        Index("ix_lcc_coarea_cctr", "coarea", "cctr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    # --- key fields (CSKS) ---
    mandt: Mapped[str | None] = mapped_column(String(3))
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)  # KOKRS
    cctr: Mapped[str] = mapped_column(String(20), nullable=False)  # KOSTL
    # --- descriptive (CSKT) ---
    txtsh: Mapped[str | None] = mapped_column(String(40))  # KTEXT
    txtmi: Mapped[str | None] = mapped_column(String(200))  # LTEXT
    # --- CSKS standard fields ---
    datbi: Mapped[str | None] = mapped_column(String(8))
    datab: Mapped[str | None] = mapped_column(String(8))
    bkzkp: Mapped[str | None] = mapped_column(String(1))
    pkzkp: Mapped[str | None] = mapped_column(String(1))
    ccode: Mapped[str | None] = mapped_column(String(10))  # BUKRS
    gsber: Mapped[str | None] = mapped_column(String(4))
    cctrcgy: Mapped[str | None] = mapped_column(String(4))  # KOSAR
    responsible: Mapped[str | None] = mapped_column(String(100))  # VERAK
    verak_user: Mapped[str | None] = mapped_column(String(12))
    currency: Mapped[str | None] = mapped_column(String(5))  # WAERS
    kalsm: Mapped[str | None] = mapped_column(String(6))
    txjcd: Mapped[str | None] = mapped_column(String(15))
    pctr: Mapped[str | None] = mapped_column(String(20))  # PRCTR
    werks: Mapped[str | None] = mapped_column(String(4))
    logsystem: Mapped[str | None] = mapped_column(String(10))
    # --- SI_CSKS include ---
    ersda: Mapped[str | None] = mapped_column(String(8))
    usnam: Mapped[str | None] = mapped_column(String(12))
    bkzks: Mapped[str | None] = mapped_column(String(1))
    bkzer: Mapped[str | None] = mapped_column(String(1))
    bkzob: Mapped[str | None] = mapped_column(String(1))
    pkzks: Mapped[str | None] = mapped_column(String(1))
    pkzer: Mapped[str | None] = mapped_column(String(1))
    vmeth: Mapped[str | None] = mapped_column(String(2))
    mgefl: Mapped[str | None] = mapped_column(String(1))
    abtei: Mapped[str | None] = mapped_column(String(12))
    nkost: Mapped[str | None] = mapped_column(String(10))
    kvewe: Mapped[str | None] = mapped_column(String(1))
    kappl: Mapped[str | None] = mapped_column(String(2))
    koszschl: Mapped[str | None] = mapped_column(String(6))
    land1: Mapped[str | None] = mapped_column(String(3))
    anred: Mapped[str | None] = mapped_column(String(15))
    name1: Mapped[str | None] = mapped_column(String(35))
    name2: Mapped[str | None] = mapped_column(String(35))
    name3: Mapped[str | None] = mapped_column(String(35))
    name4: Mapped[str | None] = mapped_column(String(35))
    ort01: Mapped[str | None] = mapped_column(String(35))
    ort02: Mapped[str | None] = mapped_column(String(35))
    stras: Mapped[str | None] = mapped_column(String(35))
    pfach: Mapped[str | None] = mapped_column(String(10))
    pstlz: Mapped[str | None] = mapped_column(String(10))
    pstl2: Mapped[str | None] = mapped_column(String(10))
    regio: Mapped[str | None] = mapped_column(String(3))
    spras: Mapped[str | None] = mapped_column(String(1))
    telbx: Mapped[str | None] = mapped_column(String(15))
    telf1: Mapped[str | None] = mapped_column(String(16))
    telf2: Mapped[str | None] = mapped_column(String(16))
    telfx: Mapped[str | None] = mapped_column(String(31))
    teltx: Mapped[str | None] = mapped_column(String(30))
    telx1: Mapped[str | None] = mapped_column(String(30))
    datlt: Mapped[str | None] = mapped_column(String(14))
    drnam: Mapped[str | None] = mapped_column(String(4))
    khinr: Mapped[str | None] = mapped_column(String(12))
    cckey: Mapped[str | None] = mapped_column(String(23))
    kompl: Mapped[str | None] = mapped_column(String(1))
    stakz: Mapped[str | None] = mapped_column(String(1))
    objnr: Mapped[str | None] = mapped_column(String(22))
    funkt: Mapped[str | None] = mapped_column(String(3))
    afunk: Mapped[str | None] = mapped_column(String(3))
    cpi_templ: Mapped[str | None] = mapped_column(String(10))
    cpd_templ: Mapped[str | None] = mapped_column(String(10))
    func_area: Mapped[str | None] = mapped_column(String(16))  # FKBER
    sci_templ: Mapped[str | None] = mapped_column(String(10))
    scd_templ: Mapped[str | None] = mapped_column(String(10))
    ski_templ: Mapped[str | None] = mapped_column(String(10))
    skd_templ: Mapped[str | None] = mapped_column(String(10))
    # --- CI_CSKS customer fields ---
    zzcuemncfu: Mapped[str | None] = mapped_column(String(5))
    zzcueabacc: Mapped[str | None] = mapped_column(String(4))
    zzcuegbcd: Mapped[str | None] = mapped_column(String(4))
    zzcueubcd: Mapped[str | None] = mapped_column(String(4))
    zzcuenkos: Mapped[str | None] = mapped_column(String(10))
    zzstrpctyp: Mapped[str | None] = mapped_column(String(3))
    zzstrkklas: Mapped[str | None] = mapped_column(String(4))
    zzstraagcd: Mapped[str | None] = mapped_column(String(2))
    zzstrgfd: Mapped[str | None] = mapped_column(String(3))
    zzstrfst: Mapped[str | None] = mapped_column(String(2))
    zzstrmacve: Mapped[str | None] = mapped_column(String(6))
    zzstrabukr: Mapped[str | None] = mapped_column(String(4))
    zzstrugcd: Mapped[str | None] = mapped_column(String(4))
    zzstrinadt: Mapped[str | None] = mapped_column(String(8))
    zzstrkstyp: Mapped[str | None] = mapped_column(String(1))
    zzstrverik: Mapped[str | None] = mapped_column(String(20))
    zzstrcurr2: Mapped[str | None] = mapped_column(String(3))
    zzstrlccid: Mapped[str | None] = mapped_column(String(10))
    zzstrmaloc: Mapped[str | None] = mapped_column(String(10))
    zzstrtaxcd: Mapped[str | None] = mapped_column(String(4))
    zzstrgrpid: Mapped[str | None] = mapped_column(String(4))
    zzstrregcode: Mapped[str | None] = mapped_column(String(6))
    zzstrtaxarea: Mapped[str | None] = mapped_column(String(10))
    zzstrrepsit: Mapped[str | None] = mapped_column(String(10))
    zzstrgsm: Mapped[str | None] = mapped_column(String(10))
    zzcemapar: Mapped[str | None] = mapped_column(String(10))
    zzledger: Mapped[str | None] = mapped_column(String(5))
    zzhdstat: Mapped[str | None] = mapped_column(String(1))
    zzhdtype: Mapped[str | None] = mapped_column(String(1))
    zzfmd: Mapped[str | None] = mapped_column(String(5))
    zzfmdcc: Mapped[str | None] = mapped_column(String(3))
    zzfmdnode: Mapped[str | None] = mapped_column(String(5))
    zzstate: Mapped[str | None] = mapped_column(String(2))
    zztax: Mapped[str | None] = mapped_column(String(2))
    zzstrentsa: Mapped[str | None] = mapped_column(String(11))
    zzstrentzu: Mapped[str | None] = mapped_column(String(11))
    xblnr: Mapped[str | None] = mapped_column(String(16))
    # --- JV fields ---
    vname: Mapped[str | None] = mapped_column(String(6))
    recid: Mapped[str | None] = mapped_column(String(2))
    etype: Mapped[str | None] = mapped_column(String(3))
    jv_otype: Mapped[str | None] = mapped_column(String(4))
    jv_jibcl: Mapped[str | None] = mapped_column(String(3))
    jv_jibsa: Mapped[str | None] = mapped_column(String(5))
    ferc_ind: Mapped[str | None] = mapped_column(String(4))
    # --- validity (app fields) ---
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # --- overflow ---
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class LegacyProfitCenter(TimestampMixin, Base):
    """Profit center master — aligned with SAP CEPC/CEPCT."""

    __tablename__ = "legacy_profit_center"
    __table_args__ = (
        UniqueConstraint("coarea", "pctr", "refresh_batch"),
        Index("ix_lpc_ccode", "ccode"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    # --- key fields (CEPC) ---
    mandt: Mapped[str | None] = mapped_column(String(3))
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)  # KOKRS
    pctr: Mapped[str] = mapped_column(String(20), nullable=False)  # PRCTR
    datbi: Mapped[str | None] = mapped_column(String(8))
    # --- descriptive (CEPCT) ---
    txtsh: Mapped[str | None] = mapped_column(String(40))  # KTEXT
    txtmi: Mapped[str | None] = mapped_column(String(200))  # LTEXT
    # --- CEPC standard fields ---
    datab: Mapped[str | None] = mapped_column(String(8))
    ersda: Mapped[str | None] = mapped_column(String(8))
    usnam: Mapped[str | None] = mapped_column(String(12))
    merkmal: Mapped[str | None] = mapped_column(String(30))
    department: Mapped[str | None] = mapped_column(String(20))  # ABTEI
    responsible: Mapped[str | None] = mapped_column(String(100))  # VERAK
    verak_user: Mapped[str | None] = mapped_column(String(12))
    currency: Mapped[str | None] = mapped_column(String(5))  # WAERS
    nprctr: Mapped[str | None] = mapped_column(String(10))
    land1: Mapped[str | None] = mapped_column(String(3))
    anred: Mapped[str | None] = mapped_column(String(15))
    name1: Mapped[str | None] = mapped_column(String(35))
    name2: Mapped[str | None] = mapped_column(String(35))
    name3: Mapped[str | None] = mapped_column(String(35))
    name4: Mapped[str | None] = mapped_column(String(35))
    ort01: Mapped[str | None] = mapped_column(String(35))
    ort02: Mapped[str | None] = mapped_column(String(35))
    stras: Mapped[str | None] = mapped_column(String(35))
    pfach: Mapped[str | None] = mapped_column(String(10))
    pstlz: Mapped[str | None] = mapped_column(String(10))
    pstl2: Mapped[str | None] = mapped_column(String(10))
    language: Mapped[str | None] = mapped_column(String(2))  # SPRAS
    telbx: Mapped[str | None] = mapped_column(String(15))
    telf1: Mapped[str | None] = mapped_column(String(16))
    telf2: Mapped[str | None] = mapped_column(String(16))
    telfx: Mapped[str | None] = mapped_column(String(31))
    teltx: Mapped[str | None] = mapped_column(String(30))
    telx1: Mapped[str | None] = mapped_column(String(30))
    datlt: Mapped[str | None] = mapped_column(String(14))
    drnam: Mapped[str | None] = mapped_column(String(4))
    khinr: Mapped[str | None] = mapped_column(String(12))
    ccode: Mapped[str | None] = mapped_column(String(10))  # BUKRS
    vname: Mapped[str | None] = mapped_column(String(6))
    recid: Mapped[str | None] = mapped_column(String(2))
    etype: Mapped[str | None] = mapped_column(String(3))
    txjcd: Mapped[str | None] = mapped_column(String(15))
    regio: Mapped[str | None] = mapped_column(String(3))
    kvewe: Mapped[str | None] = mapped_column(String(1))
    kappl: Mapped[str | None] = mapped_column(String(2))
    kalsm: Mapped[str | None] = mapped_column(String(6))
    logsystem: Mapped[str | None] = mapped_column(String(10))
    lock_ind: Mapped[str | None] = mapped_column(String(1))
    pca_template: Mapped[str | None] = mapped_column(String(10))
    segment: Mapped[str | None] = mapped_column(String(10))
    # --- validity (app fields) ---
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # --- overflow ---
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class Balance(TimestampMixin, Base):
    __tablename__ = "balance"
    __table_args__ = (
        Index("ix_bal_coarea_cctr", "coarea", "cctr"),
        Index("ix_bal_period", "fiscal_year", "period"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    cctr: Mapped[str] = mapped_column(String(20), nullable=False)
    ccode: Mapped[str | None] = mapped_column(String(10))
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    account: Mapped[str | None] = mapped_column(String(20))
    account_class: Mapped[str | None] = mapped_column(String(20))
    tc_amt: Mapped[Decimal | None] = mapped_column(Numeric(23, 2))
    gc_amt: Mapped[Decimal | None] = mapped_column(Numeric(23, 2))
    gc2_amt: Mapped[Decimal | None] = mapped_column(Numeric(23, 2))
    currency_tc: Mapped[str | None] = mapped_column(String(3))
    currency_gc: Mapped[str | None] = mapped_column(String(3))
    currency_gc2: Mapped[str | None] = mapped_column(String(3))
    posting_count: Mapped[int] = mapped_column(Integer, default=0)
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class Hierarchy(TimestampMixin, Base):
    __tablename__ = "hierarchy"
    __table_args__ = (
        UniqueConstraint("setclass", "setname", "refresh_batch"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    setclass: Mapped[str] = mapped_column(String(10), nullable=False)
    setname: Mapped[str] = mapped_column(String(40), nullable=False)
    label: Mapped[str | None] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(String(200))
    coarea: Mapped[str | None] = mapped_column(String(10))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )

    nodes: Mapped[list[HierarchyNode]] = relationship(back_populates="hierarchy")
    leaves: Mapped[list[HierarchyLeaf]] = relationship(back_populates="hierarchy")


class HierarchyNode(Base):
    __tablename__ = "hierarchy_node"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    hierarchy_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"), nullable=False
    )
    parent_setname: Mapped[str] = mapped_column(String(40), nullable=False)
    child_setname: Mapped[str] = mapped_column(String(40), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, default=0)

    hierarchy: Mapped[Hierarchy] = relationship(back_populates="nodes")


class HierarchyLeaf(Base):
    __tablename__ = "hierarchy_leaf"
    __table_args__ = (
        Index("ix_hleaf_cctr", "value"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    hierarchy_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"), nullable=False
    )
    setname: Mapped[str] = mapped_column(String(40), nullable=False)
    value: Mapped[str] = mapped_column(String(20), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, default=0)

    hierarchy: Mapped[Hierarchy] = relationship(back_populates="leaves")


# ---------- wave & analysis ----------


class Wave(TimestampMixin, Base):
    __tablename__ = "wave"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="draft"
    )  # draft|analysing|proposed|locked|in_review|signed_off|closed|cancelled
    is_full_scope: Mapped[bool] = mapped_column(Boolean, default=False)
    exclude_prior: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict | None] = mapped_column(JSONB)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    signed_off_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    preferred_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="SET NULL")
    )
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )

    entities: Mapped[list[WaveEntity]] = relationship(back_populates="wave")
    hierarchy_scopes: Mapped[list[WaveHierarchyScope]] = relationship(back_populates="wave")
    runs: Mapped[list[AnalysisRun]] = relationship(
        back_populates="wave", foreign_keys="[AnalysisRun.wave_id]"
    )
    scopes: Mapped[list[ReviewScope]] = relationship(back_populates="wave")


class WaveEntity(Base):
    __tablename__ = "wave_entity"
    __table_args__ = (
        UniqueConstraint("wave_id", "entity_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    entity_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.entity.id", ondelete="CASCADE"), nullable=False
    )

    wave: Mapped[Wave] = relationship(back_populates="entities")
    entity: Mapped[Entity] = relationship()


class WaveHierarchyScope(Base):
    """Links a wave to specific hierarchy nodes for scoping."""

    __tablename__ = "wave_hierarchy_scope"
    __table_args__ = (
        UniqueConstraint("wave_id", "hierarchy_id", "node_setname"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    hierarchy_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"), nullable=False
    )
    node_setname: Mapped[str] = mapped_column(String(40), nullable=False)

    wave: Mapped[Wave] = relationship(back_populates="hierarchy_scopes")
    hierarchy: Mapped[Hierarchy] = relationship()


class AnalysisConfig(TimestampMixin, Base):
    __tablename__ = "analysis_config"
    __table_args__ = (
        UniqueConstraint("code", "version"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    parent_code: Mapped[str | None] = mapped_column(String(60))
    status: Mapped[str] = mapped_column(String(20), default="active")
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


class Routine(TimestampMixin, Base):
    __tablename__ = "routine"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)  # rule|ml|llm|aggregate
    tree: Mapped[str | None] = mapped_column(String(20))  # cleansing|mapping
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, default="builtin"
    )  # builtin|plugin|dsl
    params_schema: Mapped[dict | None] = mapped_column(JSONB)
    default_params: Mapped[dict | None] = mapped_column(JSONB)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    order: Mapped[int] = mapped_column(Integer, default=100)


class AnalysisRun(TimestampMixin, Base):
    __tablename__ = "analysis_run"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=True
    )
    config_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_config.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )  # pending|running|completed|failed|cancelled
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    kpis: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    data_snapshot: Mapped[str | None] = mapped_column(String(64))
    triggered_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )

    wave: Mapped[Wave | None] = relationship(back_populates="runs", foreign_keys=[wave_id])
    config: Mapped[AnalysisConfig] = relationship()
    outputs: Mapped[list[RoutineOutput]] = relationship(back_populates="run")
    proposals: Mapped[list[CenterProposal]] = relationship(back_populates="run")
    llm_passes: Mapped[list[LLMReviewPass]] = relationship(back_populates="run")


class RoutineOutput(Base):
    __tablename__ = "routine_output"
    __table_args__ = (
        Index("ix_ro_run_center", "run_id", "legacy_cc_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="CASCADE"), nullable=False
    )
    routine_code: Mapped[str] = mapped_column(String(60), nullable=False)
    legacy_cc_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.legacy_cost_center.id", ondelete="CASCADE"), nullable=False
    )
    verdict: Mapped[str | None] = mapped_column(String(30))
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    payload: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    run: Mapped[AnalysisRun] = relationship(back_populates="outputs")


class LLMReviewPass(TimestampMixin, Base):
    __tablename__ = "llm_review_pass"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="CASCADE"), nullable=False
    )
    mode: Mapped[str] = mapped_column(String(20), nullable=False)  # SINGLE|SEQUENTIAL|DEBATE
    stage: Mapped[str | None] = mapped_column(String(30))
    model: Mapped[str | None] = mapped_column(String(100))
    skill_id: Mapped[str | None] = mapped_column(String(64))
    skill_version: Mapped[str | None] = mapped_column(String(20))
    prompt_template: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    total_centers: Mapped[int] = mapped_column(Integer, default=0)
    completed_centers: Mapped[int] = mapped_column(Integer, default=0)
    tokens_in: Mapped[int] = mapped_column(Integer, default=0)
    tokens_out: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    run: Mapped[AnalysisRun] = relationship(back_populates="llm_passes")


# ---------- proposals & review ----------


class CenterProposal(TimestampMixin, Base):
    __tablename__ = "center_proposal"
    __table_args__ = (
        UniqueConstraint("run_id", "legacy_cc_id"),
        Index("ix_cp_outcome", "cleansing_outcome"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="CASCADE"), nullable=False
    )
    legacy_cc_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.legacy_cost_center.id", ondelete="CASCADE"), nullable=False
    )
    entity_code: Mapped[str | None] = mapped_column(String(10))
    cleansing_outcome: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # KEEP|RETIRE|MERGE_MAP|REDESIGN
    target_object: Mapped[str | None] = mapped_column(
        String(20)
    )  # CC|PC|CC_AND_PC|PC_ONLY|WBS_REAL|WBS_STAT|NONE
    merge_into_cctr: Mapped[str | None] = mapped_column(String(20))
    rule_path: Mapped[dict | None] = mapped_column(JSONB)
    ml_scores: Mapped[dict | None] = mapped_column(JSONB)
    llm_commentary: Mapped[dict | None] = mapped_column(JSONB)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    override_outcome: Mapped[str | None] = mapped_column(String(20))
    override_target: Mapped[str | None] = mapped_column(String(20))
    override_reason: Mapped[str | None] = mapped_column(Text)
    override_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    override_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    run: Mapped[AnalysisRun] = relationship(back_populates="proposals")
    legacy_cc: Mapped[LegacyCostCenter] = relationship()


class ReviewScope(TimestampMixin, Base):
    __tablename__ = "review_scope"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    scope_type: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # entity|hierarchy_node|list
    scope_filter: Mapped[dict] = mapped_column(JSONB, nullable=False)
    token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    reviewer_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    token_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # pending|invited|in_progress|completed|expired|revoked
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    signed_off_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    invited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reviewer_name: Mapped[str | None] = mapped_column(String(200))
    reviewer_email: Mapped[str | None] = mapped_column(String(320))

    wave: Mapped[Wave] = relationship(back_populates="scopes")
    items: Mapped[list[ReviewItem]] = relationship(back_populates="scope")


class ReviewItem(TimestampMixin, Base):
    __tablename__ = "review_item"
    __table_args__ = (
        UniqueConstraint("scope_id", "proposal_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scope_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.review_scope.id", ondelete="CASCADE"), nullable=False
    )
    proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="CASCADE"), nullable=True
    )
    decision: Mapped[str] = mapped_column(
        String(20), default="PENDING"
    )  # PENDING|APPROVED|NOT_REQUIRED|COMMENTED
    comment: Mapped[str | None] = mapped_column(Text)
    decided_by: Mapped[str | None] = mapped_column(String(100))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    scope: Mapped[ReviewScope] = relationship(back_populates="items")
    proposal: Mapped[CenterProposal] = relationship()


# ---------- target objects ----------


class TargetCostCenter(TimestampMixin, Base):
    __tablename__ = "target_cost_center"
    __table_args__ = (
        UniqueConstraint("coarea", "cctr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    cctr: Mapped[str] = mapped_column(String(20), nullable=False)
    txtsh: Mapped[str | None] = mapped_column(String(40))
    txtmi: Mapped[str | None] = mapped_column(String(200))
    responsible: Mapped[str | None] = mapped_column(String(100))
    ccode: Mapped[str | None] = mapped_column(String(10))
    cctrcgy: Mapped[str | None] = mapped_column(String(4))
    currency: Mapped[str | None] = mapped_column(String(3))
    pctr: Mapped[str | None] = mapped_column(String(20))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    source_proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")
    )
    approved_in_wave: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="SET NULL")
    )
    mdg_status: Mapped[str | None] = mapped_column(String(30))
    mdg_change_request_id: Mapped[str | None] = mapped_column(String(40))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TargetProfitCenter(TimestampMixin, Base):
    __tablename__ = "target_profit_center"
    __table_args__ = (
        UniqueConstraint("coarea", "pctr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    pctr: Mapped[str] = mapped_column(String(20), nullable=False)
    txtsh: Mapped[str | None] = mapped_column(String(40))
    txtmi: Mapped[str | None] = mapped_column(String(200))
    responsible: Mapped[str | None] = mapped_column(String(100))
    ccode: Mapped[str | None] = mapped_column(String(10))
    department: Mapped[str | None] = mapped_column(String(20))
    currency: Mapped[str | None] = mapped_column(String(3))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    source_proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")
    )
    approved_in_wave: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="SET NULL")
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# ---------- housekeeping ----------


class HousekeepingCycle(TimestampMixin, Base):
    __tablename__ = "housekeeping_cycle"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    period: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="scheduled"
    )  # scheduled|running|review_open|closed
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    review_opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config: Mapped[dict | None] = mapped_column(JSONB)
    kpis: Mapped[dict | None] = mapped_column(JSONB)


class HousekeepingItem(TimestampMixin, Base):
    __tablename__ = "housekeeping_item"
    __table_args__ = (
        UniqueConstraint("cycle_id", "target_cc_id", "flag"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    cycle_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.housekeeping_cycle.id", ondelete="CASCADE"), nullable=False
    )
    target_cc_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.target_cost_center.id", ondelete="CASCADE"), nullable=False
    )
    flag: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # UNUSED|LOW_VOLUME|NO_OWNER|ANOMALY
    owner_email: Mapped[str | None] = mapped_column(String(320))
    owner_token: Mapped[str | None] = mapped_column(String(64))
    decision: Mapped[str | None] = mapped_column(String(20))  # KEEP|CLOSE|DEFER
    decision_comment: Mapped[str | None] = mapped_column(Text)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reminders_sent: Mapped[int] = mapped_column(Integer, default=0)
    details: Mapped[dict | None] = mapped_column(JSONB)


# ---------- ingest ----------


class UploadBatch(TimestampMixin, Base):
    __tablename__ = "upload_batch"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # cost_center|profit_center|balance|hierarchy
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    storage_uri: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(
        String(20), default="uploaded"
    )  # uploaded|validating|validated|loading|loaded|failed|rolled_back
    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, default=0)
    rows_error: Mapped[int] = mapped_column(Integer, default=0)
    rows_loaded: Mapped[int] = mapped_column(Integer, default=0)
    uploaded_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    loaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UploadError(Base):
    __tablename__ = "upload_error"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="CASCADE"), nullable=False
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    column_name: Mapped[str | None] = mapped_column(String(100))
    error_code: Mapped[str] = mapped_column(String(40), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    raw_value: Mapped[str | None] = mapped_column(Text)


# ---------- SAP connections ----------


class SAPConnection(TimestampMixin, Base):
    __tablename__ = "sap_connection"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(500))
    system_type: Mapped[str] = mapped_column(String(20), nullable=False)
    landscape_type: Mapped[str | None] = mapped_column(String(10))
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    # Split address fields (preferred over base_url)
    host: Mapped[str | None] = mapped_column(String(200))
    port: Mapped[str | None] = mapped_column(String(10))
    conn_protocol: Mapped[str | None] = mapped_column(
        String(10), default="https"
    )  # https|http
    client: Mapped[str] = mapped_column(String(3), nullable=False, default="100")
    language: Mapped[str] = mapped_column(String(2), nullable=False, default="EN")
    username: Mapped[str] = mapped_column(String(100), nullable=False)
    password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    protocol: Mapped[str] = mapped_column(
        String(20), nullable=False, default="odata"
    )  # odata|adt|soap_rfc
    verify_ssl: Mapped[bool] = mapped_column(Boolean, default=True)
    use_proxy: Mapped[bool] = mapped_column(Boolean, default=False)
    saml2_disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    allowed_tables: Mapped[str | None] = mapped_column(Text)
    fiori_launchpad_url: Mapped[str | None] = mapped_column(String(500))
    webgui_url: Mapped[str | None] = mapped_column(String(500))
    # Web Dispatcher — alternative entry point
    webdisp_host: Mapped[str | None] = mapped_column(String(200))
    webdisp_port: Mapped[str | None] = mapped_column(String(10))
    webdisp_protocol: Mapped[str | None] = mapped_column(String(10), default="https")
    use_webdisp: Mapped[bool] = mapped_column(Boolean, default=False)
    # Per-endpoint Web Dispatcher routing
    adt_use_webdisp: Mapped[bool | None] = mapped_column(Boolean)
    soap_use_webdisp: Mapped[bool | None] = mapped_column(Boolean)
    odata_use_webdisp: Mapped[bool | None] = mapped_column(Boolean)
    # Per-endpoint overrides (None = inherit from global)
    adt_verify_ssl: Mapped[bool | None] = mapped_column(Boolean)
    adt_use_proxy: Mapped[bool | None] = mapped_column(Boolean)
    adt_saml2_disabled: Mapped[bool | None] = mapped_column(Boolean)
    soap_verify_ssl: Mapped[bool | None] = mapped_column(Boolean)
    soap_use_proxy: Mapped[bool | None] = mapped_column(Boolean)
    soap_saml2_disabled: Mapped[bool | None] = mapped_column(Boolean)
    odata_verify_ssl: Mapped[bool | None] = mapped_column(Boolean)
    odata_use_proxy: Mapped[bool | None] = mapped_column(Boolean)
    odata_saml2_disabled: Mapped[bool | None] = mapped_column(Boolean)
    # ICF node aliases
    use_icf_aliases: Mapped[bool] = mapped_column(Boolean, default=False)
    adt_icf_source: Mapped[str | None] = mapped_column(String(20))
    soap_icf_source: Mapped[str | None] = mapped_column(String(20))
    odata_icf_source: Mapped[str | None] = mapped_column(String(20))
    adt_icf_cert: Mapped[str | None] = mapped_column(String(200))
    soap_icf_cert: Mapped[str | None] = mapped_column(String(200))
    odata_icf_cert: Mapped[str | None] = mapped_column(String(200))
    adt_icf_basic: Mapped[str | None] = mapped_column(String(200))
    soap_icf_basic: Mapped[str | None] = mapped_column(String(200))
    odata_icf_basic: Mapped[str | None] = mapped_column(String(200))
    # Per-endpoint client certificate source
    adt_cert_source: Mapped[str | None] = mapped_column(String(20))
    soap_cert_source: Mapped[str | None] = mapped_column(String(20))
    odata_cert_source: Mapped[str | None] = mapped_column(String(20))
    # Principal Propagation (Entra ID -> SAP)
    pp_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    pp_sap_oauth_token_url: Mapped[str | None] = mapped_column(
        String(200), default="/sap/bc/sec/oauth2/token"
    )
    pp_sap_oauth_client_id: Mapped[str | None] = mapped_column(String(200))
    pp_sap_oauth_client_secret_enc: Mapped[str | None] = mapped_column(Text)
    pp_saml_issuer: Mapped[str | None] = mapped_column(String(200))
    pp_saml_audience: Mapped[str | None] = mapped_column(String(200))
    pp_user_mapping: Mapped[str | None] = mapped_column(String(20), default="email")
    attrs: Mapped[dict | None] = mapped_column(JSONB)


class SAPObjectBinding(TimestampMixin, Base):
    __tablename__ = "sap_object_binding"
    __table_args__ = (
        UniqueConstraint("connection_id", "object_type"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.sap_connection.id", ondelete="CASCADE"), nullable=False
    )
    object_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # cost_center|profit_center|hierarchy|balance|gl_account
    entity_set: Mapped[str | None] = mapped_column(String(200))
    path: Mapped[str | None] = mapped_column(String(500))
    params: Mapped[dict | None] = mapped_column(JSONB)
    schedule_cron: Mapped[str | None] = mapped_column(String(100))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class SAPConnectionProbe(Base):
    __tablename__ = "sap_connection_probe"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.sap_connection.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # ok|error
    protocol: Mapped[str] = mapped_column(String(20), nullable=False)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[dict | None] = mapped_column(JSONB)
    probed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    probed_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


# ---------- auth & admin ----------


class AppUser(TimestampMixin, Base):
    __tablename__ = "app_user"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    email: Mapped[str | None] = mapped_column(String(320))
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    password_hash: Mapped[str | None] = mapped_column(String(200))
    role: Mapped[str] = mapped_column(
        String(20), nullable=False, default="analyst"
    )  # admin|analyst|reviewer|auditor|owner
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    entraid_oid: Mapped[str | None] = mapped_column(String(64))
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_logins: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    onboarding_done: Mapped[bool] = mapped_column(Boolean, default=False)
    attrs: Mapped[dict | None] = mapped_column(JSONB)


class AppConfig(Base):
    __tablename__ = "app_config"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    value: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    updated_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


class AppConfigSecret(Base):
    __tablename__ = "app_config_secret"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    encrypted_value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class AuditLog(Base):
    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_action", "action"),
        Index("ix_audit_actor", "actor_id"),
        Index("ix_audit_ts", "created_at"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    action: Mapped[str] = mapped_column(String(60), nullable=False)
    entity_type: Mapped[str | None] = mapped_column(String(60))
    entity_id: Mapped[str | None] = mapped_column(String(60))
    actor_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    actor_email: Mapped[str | None] = mapped_column(String(320))
    before: Mapped[dict | None] = mapped_column(JSONB)
    after: Mapped[dict | None] = mapped_column(JSONB)
    ip_address: Mapped[str | None] = mapped_column(String(45))
    request_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class TaskRun(TimestampMixin, Base):
    __tablename__ = "task_run"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(100), nullable=False)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # pending|running|completed|failed|cancelled
    args_summary: Mapped[dict | None] = mapped_column(JSONB)
    result_summary: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="SET NULL")
    )


class NamingSequence(Base):
    __tablename__ = "naming_sequence"
    __table_args__ = (
        UniqueConstraint("object_type", "coarea", "prefix"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    object_type: Mapped[str] = mapped_column(String(10), nullable=False)  # cc|pc|wbs
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    prefix: Mapped[str] = mapped_column(String(20), nullable=False)
    next_value: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    reserved_by_wave: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="SET NULL")
    )
    reserved_range_start: Mapped[int | None] = mapped_column(Integer)
    reserved_range_end: Mapped[int | None] = mapped_column(Integer)


class ActivityFeedEntry(Base):
    """Activity feed for audit trail and notifications."""

    __tablename__ = "activity_feed"
    __table_args__ = (
        Index("ix_activity_feed_user", "user_id"),
        Index("ix_activity_feed_ts", "created_at"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_type: Mapped[str | None] = mapped_column(String(30))
    entity_id: Mapped[int | None] = mapped_column(Integer)
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    detail: Mapped[dict | None] = mapped_column(JSONB)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class WaveTemplate(TimestampMixin, Base):
    """Reusable wave configuration template (§07.2)."""

    __tablename__ = "wave_template"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict | None] = mapped_column(JSONB)
    is_full_scope: Mapped[bool] = mapped_column(Boolean, default=False)
    exclude_prior: Mapped[bool] = mapped_column(Boolean, default=True)
    entity_ccodes: Mapped[list | None] = mapped_column(JSONB)
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


class GLAccountClassRange(TimestampMixin, Base):
    """GL account class ranges for balance classification (§03.5)."""

    __tablename__ = "gl_account_class_range"
    __table_args__ = (
        UniqueConstraint("class_code", "from_account"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    class_code: Mapped[str] = mapped_column(String(20), nullable=False)
    class_label: Mapped[str] = mapped_column(String(100), nullable=False)
    from_account: Mapped[str] = mapped_column(String(20), nullable=False)
    to_account: Mapped[str] = mapped_column(String(20), nullable=False)
    category: Mapped[str | None] = mapped_column(String(40))  # bs|rev|opex|other


class NamingPool(Base):
    """Pool of allocatable CC/PC IDs per wave (supports ID recycling)."""

    __tablename__ = "naming_pool"
    __table_args__ = (
        UniqueConstraint("wave_id", "pool_type"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    pool_type: Mapped[str] = mapped_column(String(10), nullable=False)  # CC or PC
    range_start: Mapped[int] = mapped_column(Integer, nullable=False)
    range_end: Mapped[int] = mapped_column(Integer, nullable=False)
    next_value: Mapped[int] = mapped_column(Integer, nullable=False)

    allocations: Mapped[list[NamingAllocation]] = relationship(back_populates="pool")


class NamingAllocation(Base):
    """Individual ID allocation from a naming pool, supports release/recycle."""

    __tablename__ = "naming_allocation"
    __table_args__ = (
        Index("ix_nalloc_pool", "pool_id"),
        Index("ix_nalloc_proposal", "proposal_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    pool_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.naming_pool.id", ondelete="CASCADE"), nullable=False
    )
    proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")
    )
    allocated_value: Mapped[str] = mapped_column(String(20), nullable=False)
    is_released: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    pool: Mapped[NamingPool] = relationship(back_populates="allocations")


# ---------- GL Account master data (SAP SKA1 / SKB1) ----------


class GLAccountSKA1(TimestampMixin, Base):
    """GL account chart-of-accounts level data — SAP SKA1."""

    __tablename__ = "gl_account_ska1"
    __table_args__ = (
        UniqueConstraint("ktopl", "saknr"),
        Index("ix_ska1_saknr", "saknr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    mandt: Mapped[str | None] = mapped_column(String(3))
    ktopl: Mapped[str] = mapped_column(String(4), nullable=False)
    saknr: Mapped[str] = mapped_column(String(10), nullable=False)
    xbilk: Mapped[str | None] = mapped_column(String(1))
    sakan: Mapped[str | None] = mapped_column(String(10))
    bilkt: Mapped[str | None] = mapped_column(String(10))
    erdat: Mapped[str | None] = mapped_column(String(8))
    ernam: Mapped[str | None] = mapped_column(String(12))
    gvtyp: Mapped[str | None] = mapped_column(String(2))
    ktoks: Mapped[str | None] = mapped_column(String(4))
    mustr: Mapped[str | None] = mapped_column(String(10))
    vbund: Mapped[str | None] = mapped_column(String(6))
    xloev: Mapped[str | None] = mapped_column(String(1))
    xspea: Mapped[str | None] = mapped_column(String(1))
    xspeb: Mapped[str | None] = mapped_column(String(1))
    xspep: Mapped[str | None] = mapped_column(String(1))
    mcod1: Mapped[str | None] = mapped_column(String(25))
    func_area: Mapped[str | None] = mapped_column(String(16))
    glaccount_type: Mapped[str | None] = mapped_column(String(1))
    glaccount_subtype: Mapped[str | None] = mapped_column(String(1))
    main_saknr: Mapped[str | None] = mapped_column(String(10))
    last_changed_ts: Mapped[str | None] = mapped_column(String(15))
    # description from SKAT
    txt20: Mapped[str | None] = mapped_column(String(20))
    txt50: Mapped[str | None] = mapped_column(String(50))
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class GLAccountSKB1(TimestampMixin, Base):
    """GL account company-code level data — SAP SKB1."""

    __tablename__ = "gl_account_skb1"
    __table_args__ = (
        UniqueConstraint("bukrs", "saknr"),
        Index("ix_skb1_saknr", "saknr"),
        Index("ix_skb1_bukrs", "bukrs"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    mandt: Mapped[str | None] = mapped_column(String(3))
    bukrs: Mapped[str] = mapped_column(String(4), nullable=False)
    saknr: Mapped[str] = mapped_column(String(10), nullable=False)
    begru: Mapped[str | None] = mapped_column(String(4))
    busab: Mapped[str | None] = mapped_column(String(2))
    datlz: Mapped[str | None] = mapped_column(String(8))
    erdat: Mapped[str | None] = mapped_column(String(8))
    ernam: Mapped[str | None] = mapped_column(String(12))
    fdgrv: Mapped[str | None] = mapped_column(String(10))
    fdlev: Mapped[str | None] = mapped_column(String(2))
    fipls: Mapped[str | None] = mapped_column(String(3))
    fstag: Mapped[str | None] = mapped_column(String(4))
    hbkid: Mapped[str | None] = mapped_column(String(5))
    hktid: Mapped[str | None] = mapped_column(String(5))
    kdfsl: Mapped[str | None] = mapped_column(String(4))
    mitkz: Mapped[str | None] = mapped_column(String(1))
    mwskz: Mapped[str | None] = mapped_column(String(2))
    stext: Mapped[str | None] = mapped_column(String(50))
    vzskz: Mapped[str | None] = mapped_column(String(2))
    waers: Mapped[str | None] = mapped_column(String(5))
    wmeth: Mapped[str | None] = mapped_column(String(2))
    xgkon: Mapped[str | None] = mapped_column(String(1))
    xintb: Mapped[str | None] = mapped_column(String(1))
    xkres: Mapped[str | None] = mapped_column(String(1))
    xloeb: Mapped[str | None] = mapped_column(String(1))
    xnkon: Mapped[str | None] = mapped_column(String(1))
    xopvw: Mapped[str | None] = mapped_column(String(1))
    xspeb: Mapped[str | None] = mapped_column(String(1))
    zindt: Mapped[str | None] = mapped_column(String(8))
    zinrt: Mapped[str | None] = mapped_column(String(2))
    zuawa: Mapped[str | None] = mapped_column(String(3))
    altkt: Mapped[str | None] = mapped_column(String(10))
    xmitk: Mapped[str | None] = mapped_column(String(1))
    recid: Mapped[str | None] = mapped_column(String(2))
    fipos: Mapped[str | None] = mapped_column(String(14))
    xmwno: Mapped[str | None] = mapped_column(String(1))
    xsalh: Mapped[str | None] = mapped_column(String(1))
    bewgp: Mapped[str | None] = mapped_column(String(10))
    infky: Mapped[str | None] = mapped_column(String(8))
    togru: Mapped[str | None] = mapped_column(String(4))
    xlgclr: Mapped[str | None] = mapped_column(String(1))
    x_uj_clr: Mapped[str | None] = mapped_column(String(1))
    mcakey: Mapped[str | None] = mapped_column(String(5))
    cochanged: Mapped[str | None] = mapped_column(String(1))
    last_changed_ts: Mapped[str | None] = mapped_column(String(15))
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


# ---------- data explorer display config ----------


class ExplorerDisplayConfig(TimestampMixin, Base):
    """Global display configuration for Data Explorer — which columns to show."""

    __tablename__ = "explorer_display_config"
    __table_args__ = (
        UniqueConstraint("object_type"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    object_type: Mapped[str] = mapped_column(String(30), nullable=False)
    table_columns: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    detail_columns: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    default_sort_column: Mapped[str | None] = mapped_column(String(50))
    default_sort_dir: Mapped[str | None] = mapped_column(String(4), default="asc")
    updated_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


# ---------- data explorer source config ----------


class ExplorerSourceConfig(TimestampMixin, Base):
    """Per-object-type data source configuration for the public Data Explorer."""

    __tablename__ = "explorer_source_config"
    __table_args__ = (
        UniqueConstraint("object_type", "area"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    object_type: Mapped[str] = mapped_column(String(30), nullable=False)
    area: Mapped[str] = mapped_column(
        String(10), nullable=False, default="legacy"
    )  # legacy | amplifi
    label: Mapped[str] = mapped_column(String(100), nullable=False)
    source_system: Mapped[str] = mapped_column(
        String(30), nullable=False, default="local_db"
    )  # local_db | sap_s4 | sap_mdg | datasphere | custom_api
    protocol: Mapped[str] = mapped_column(
        String(20), nullable=False, default="db_query"
    )  # db_query | odata | adt | rfc | rest
    mode: Mapped[str] = mapped_column(
        String(15), nullable=False, default="replicated"
    )  # in_place | replicated
    connection_ref: Mapped[str | None] = mapped_column(
        String(200)
    )  # SAP connection name, DSP URL, API base URL, etc.
    endpoint: Mapped[str | None] = mapped_column(
        String(500)
    )  # OData entity set, RFC function, REST path, table name
    replication_cron: Mapped[str | None] = mapped_column(
        String(50)
    )  # cron expression if mode=replicated
    extra_config: Mapped[dict | None] = mapped_column(JSONB)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    display_order: Mapped[int] = mapped_column(Integer, default=0)
    updated_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


# ---------- datasphere integration ----------

# Data domains that can be routed to Datasphere
DATASPHERE_DOMAINS = [
    "cost_center",
    "profit_center",
    "entity",
    "hierarchy",
    "hierarchy_node",
    "hierarchy_leaf",
    "balance",
    "gl_account",
    "employee",
    "analysis_run",
    "center_proposal",
    "routine_output",
    "target_cost_center",
    "target_profit_center",
    "signoff",
]

# Domains that ALWAYS stay local (application/workflow data)
LOCAL_ONLY_DOMAINS = [
    "app_user",
    "app_config",
    "sap_connection",
    "sap_object_binding",
    "wave",
    "wave_entity",
    "wave_hierarchy_scope",
    "review_scope",
    "review_item",
    "upload_batch",
    "upload_error",
    "audit_log",
    "activity_feed",
    "task_run",
    "naming_pool",
    "naming_allocation",
    "naming_sequence",
    "wave_template",
    "gl_account_class_range",
    "analysis_config",
    "routine",
    "llm_review_pass",
    "housekeeping_cycle",
    "housekeeping_item",
]


class DatasphereConfig(TimestampMixin, Base):
    """Per-domain storage routing config for SAP Datasphere integration."""

    __tablename__ = "datasphere_config"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    # Connection
    ds_url: Mapped[str | None] = mapped_column(String(500))
    ds_schema: Mapped[str] = mapped_column(String(100), nullable=False, default="ACM")
    ds_user: Mapped[str | None] = mapped_column(String(200))
    ds_password_encrypted: Mapped[str | None] = mapped_column(Text)
    ds_use_ssl: Mapped[bool] = mapped_column(Boolean, default=True)
    # Per-domain table mappings (JSONB: { domain: { enabled, table_name } })
    domain_config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # Global toggle
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
