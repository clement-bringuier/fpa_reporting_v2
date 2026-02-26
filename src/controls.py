import pandas as pd

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────

VALID_PL_CODES = {
    "a1", "a2", "a3", "a4", "a5", "a6",
    "b1", "b2", "b3",
    "c1",
    "d1", "d2", "d3", "d4", "d5",
    "e1", "e2", "e3", "e4",
    "f1", "f2", "f3",
    "g1",
    "h1", "h2",
    "i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "i10", "i11", "i12",
    "i2_ifrs16",
    "j1", "j2", "j3", "j4",
    "k1",
    "m1", "m2", "m3", "m4",
    "n1",
    "rev_to_allocate", "cogs_to_allocate", "rh_to_allocate",
    "below_ebit", "management_fees", "NA",
}

RH_TYPES_EXPECTED = {
    "Operating staff costs", "Non-operating staff costs",
    "Operating activation", "Activation Liveops",
    "Activation Internal Projects", "CIJV", "Non-operating activation",
}

RH_GROUPS = {
    "PID+FR":           ["FR", "PID"],
    "CELSIUS+VERTICAL": ["CELSIUS", "VERTICAL"],
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _ok(label):
    return {"statut": "✅ OK", "detail": label, "valeur": ""}

def _warn(label, valeur=""):
    return {"statut": "⚠️  ATTENTION", "detail": label, "valeur": str(valeur)}

def _err(label, valeur=""):
    return {"statut": "❌ ERREUR", "detail": label, "valeur": str(valeur)}


# ─────────────────────────────────────────────
# CONTRÔLES FEC
# ─────────────────────────────────────────────

def check_fec(fec: pd.DataFrame, entity: str, period: str) -> list:
    rows = []
    p = pd.to_datetime(period, format="%Y%m")

    # Lignes du mois hors AN
    df = fec[
        (fec["JournalCode"] != "AN") &
        (fec["EcritureDate"].dt.year == p.year) &
        (fec["EcritureDate"].dt.month == p.month)
    ]

    # Nombre de lignes
    n = len(df)
    if n == 0:
        rows.append(_err(f"FEC {entity} — aucune ligne pour {period}"))
    else:
        rows.append(_ok(f"FEC {entity} — {n:,} lignes"))

    # Équilibre débit/crédit
    debit  = df["Debit"].sum()
    credit = df["Credit"].sum()
    ecart  = abs(debit - credit)
    if ecart > 1:
        rows.append(_err(f"FEC {entity} — déséquilibre débit/crédit", f"{ecart:,.2f} €"))
    else:
        rows.append(_ok(f"FEC {entity} — débit/crédit équilibrés"))

    # Dates nulles
    null_dates = fec["EcritureDate"].isna().sum()
    if null_dates > 0:
        rows.append(_warn(f"FEC {entity} — dates non parsées", f"{null_dates} lignes"))

    # Montants négatifs suspects
    neg = df[(df["Debit"] < 0) | (df["Credit"] < 0)]
    if not neg.empty:
        rows.append(_warn(f"FEC {entity} — montants négatifs", f"{len(neg)} lignes"))

    return rows


# ─────────────────────────────────────────────
# CONTRÔLES MAPPING
# ─────────────────────────────────────────────

def check_mapping(mapping: pd.DataFrame, fec: pd.DataFrame, entity: str, period: str) -> list:
    rows = []
    p = pd.to_datetime(period, format="%Y%m")

    # Doublons de comptes dans le mapping
    dupes = mapping[mapping.duplicated("numero_compte", keep=False)]
    if not dupes.empty:
        rows.append(_err(f"Mapping {entity} — comptes dupliqués", dupes["numero_compte"].unique().tolist()))
    else:
        rows.append(_ok(f"Mapping {entity} — pas de doublons"))

    # Codes mapping invalides
    invalids = mapping[~mapping["mapping_pl_detail"].isin(VALID_PL_CODES)]
    invalids = mapping[~mapping["mapping_pl_detail"].apply(
    lambda x: str(x).strip().lower() in {
        "nan", "", "na", "none", *[v.lower() for v in VALID_PL_CODES]
    }
)]
    if not invalids.empty:
        rows.append(_warn(f"Mapping {entity} — codes inconnus", invalids["mapping_pl_detail"].unique().tolist()))

    # Comptes 6x/7x dans le FEC non mappés
    df = fec[
        (fec["JournalCode"] != "AN") &
        (fec["EcritureDate"].dt.year == p.year) &
        (fec["EcritureDate"].dt.month == p.month)
    ]
    fec_comptes = set(df[df["CompteNum"].str.startswith(("6", "7"))]["CompteNum"].unique())
    mapped_comptes = set(mapping["numero_compte"].unique())
    non_mappes = fec_comptes - mapped_comptes
    if non_mappes:
        rows.append(_warn(f"Mapping {entity} — comptes 6x/7x non mappés", sorted(non_mappes)))
    else:
        rows.append(_ok(f"Mapping {entity} — tous les comptes 6x/7x sont mappés"))

    return rows


# ─────────────────────────────────────────────
# CONTRÔLES SPLIT RH
# ─────────────────────────────────────────────

def check_split_rh(split_rh: pd.DataFrame, period: str) -> list:
    rows = []

    for group in RH_GROUPS:
        df = split_rh[split_rh["entite"] == group]

        if df.empty:
            rows.append(_err(f"Split RH — groupe {group} absent"))
            continue

        # Types attendus présents
        types_presents = set(df["type"].unique())
        manquants = RH_TYPES_EXPECTED - types_presents
        if manquants:
            rows.append(_warn(f"Split RH {group} — types manquants", manquants))
        else:
            rows.append(_ok(f"Split RH {group} — tous les types présents"))

        # Montants nuls
        nuls = df[df["montant"] == 0]
        if not nuls.empty:
            rows.append(_warn(f"Split RH {group} — montants à zéro", nuls["type"].tolist()))

    return rows


# ─────────────────────────────────────────────
# CONTRÔLES SPLIT CA/COGS
# ─────────────────────────────────────────────

def check_split_ca(split_ca: pd.DataFrame, fecs: dict, mappings: dict, period: str) -> list:
    from src.transformations import compute_net_by_account, apply_mapping
    rows = []

    for entity in ["FR", "PID", "CELSIUS", "VERTICAL"]:
        if entity not in fecs or entity not in mappings:
            continue

        net    = compute_net_by_account(fecs[entity], period)
        mapped = apply_mapping(net, mappings[entity])

        for fec_code, split_type in [("rev_to_allocate", "Revenue"), ("cogs_to_allocate", "COGS")]:
            total_fec = mapped[mapped["mapping_pl_detail"] == fec_code]["montant_net"].sum()
            if total_fec == 0:
                continue

            df = split_ca[(split_ca["entite"] == entity) & (split_ca["type"] == split_type)]
            if df.empty:
                rows.append(_err(f"Split CA {entity} — {split_type} absent alors que FEC = {total_fec:,.0f} €"))
            else:
                rows.append(_ok(f"Split CA {entity} — {split_type} présent ({total_fec:,.0f} € dans FEC)"))

    return rows


# ─────────────────────────────────────────────
# CONTRÔLES P&L OUTPUT
# ─────────────────────────────────────────────

def check_pl_output(pl_dict: dict) -> list:
    rows = []

    for sheet, pl_df in pl_dict.items():
        # EBITDA négatif suspect
        if "EBITDA" in pl_df.index:
            ebitda = pl_df.loc["EBITDA", "Montant"]
            if ebitda < -500_000:
                rows.append(_warn(f"{sheet} — EBITDA très négatif", f"{ebitda:,.0f} €"))
            elif pd.isna(ebitda):
                rows.append(_err(f"{sheet} — EBITDA est NaN"))
            else:
                rows.append(_ok(f"{sheet} — EBITDA = {ebitda:,.0f} €"))

        # Lignes NaN
        nans = pl_df[pl_df["Montant"].isna()]
        if not nans.empty:
            rows.append(_warn(f"{sheet} — lignes NaN", nans.index.tolist()))

    return rows


# ─────────────────────────────────────────────
# PIPELINE COMPLET
# ─────────────────────────────────────────────

def run_all_controls(
    fecs: dict,
    mappings: dict,
    split_rh: pd.DataFrame,
    split_ca: pd.DataFrame,
    pl_dict: dict,
    period: str,
) -> pd.DataFrame:
    """
    Lance tous les contrôles et retourne un DataFrame prêt pour export Excel.
    """
    all_rows = []

    # FEC + Mapping par entité
    for entity in ["FR", "PID", "CELSIUS", "VERTICAL"]:
        if entity in fecs:
            all_rows += check_fec(fecs[entity], entity, period)
        if entity in fecs and entity in mappings:
            all_rows += check_mapping(mappings[entity], fecs[entity], entity, period)

    # Split RH
    all_rows += check_split_rh(split_rh, period)

    # Split CA/COGS
    all_rows += check_split_ca(split_ca, fecs, mappings, period)

    # P&L output
    all_rows += check_pl_output(pl_dict)

    return pd.DataFrame(all_rows, columns=["statut", "detail", "valeur"])
